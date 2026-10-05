package influence

import (
	"context"
	"errors"
	"sync"
	"time"
)

// taskHandle is the live state of one task.
type taskHandle struct {
	mu sync.Mutex

	state   TaskState
	estCfg  *EstimateConfig
	selCfg  *SelectionConfig
	batches []BatchStats

	// snap is the immutable graph snapshot the task is bound to. It is fixed
	// exactly once — at submission for new tasks, at resume time for
	// recovered tasks — and never changes afterwards, so a task's result
	// depends only on that one graph content.
	snap   *GraphSnapshot
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	// stopping records that cancellation is caused by service shutdown
	// (crash/restart), not by an explicit user request.
	stopping bool
}

func (h *taskHandle) snapshot() TaskState {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.state
}

// runTask executes a recovered or newly submitted task to a terminal state.
// The task runs strictly on the graph snapshot it was bound to (at submission
// or at resume); the live graph is never consulted, so concurrent or
// intervening graph updates cannot perturb the result.
func (s *Service) runTask(h *taskHandle) {
	defer s.wg.Done()
	defer close(h.done)

	s.mu.RLock()
	closed := s.closed
	s.mu.RUnlock()
	if closed {
		// Service shut down without completing the task; leave it persisted in
		// a non-terminal state so ResumePending can continue it later.
		return
	}

	h.mu.Lock()
	snap := h.snap
	h.state.Status = StatusRunning
	h.state.UpdatedAt = time.Now()
	h.mu.Unlock()
	if snap == nil {
		// submit and ResumePending always bind a snapshot before starting a
		// task; reaching this point is a programming error, not a user error.
		h.fail(errors.New("influence: internal error: task started without a bound graph snapshot"), s.store)
		return
	}

	var err error
	if h.estCfg != nil {
		err = s.runEstimationTask(h, snap)
	} else {
		err = s.runSelectionTask(h, snap)
	}
	if err == nil {
		return
	}
	h.mu.Lock()
	stopping := h.stopping
	h.mu.Unlock()
	if stopping {
		// Shutdown: leave the persisted record non-terminal for resume.
		return
	}
	if errors.Is(err, context.Canceled) || errors.Is(h.ctx.Err(), context.Canceled) {
		h.markCancelled(s.store)
		return
	}
	h.fail(err, s.store)
}

func (s *Service) runEstimationTask(h *taskHandle, snap *GraphSnapshot) error {
	cfg := *h.estCfg
	total := cfg.totalBatches()

	var mu sync.Mutex
	progress := func(b BatchStats) {
		mu.Lock()
		defer mu.Unlock()
		h.mu.Lock()
		for _, x := range h.batches {
			if x.Index == b.Index {
				h.mu.Unlock()
				return
			}
		}
		h.batches = append(h.batches, b)
		h.state.Progress = float64(len(h.batches)) / float64(total)
		h.state.UpdatedAt = time.Now()
		rec := h.recordLocked()
		h.mu.Unlock()
		// Persist every completed batch atomically; a crash resumes from here
		// and never recomputes this batch.
		_ = s.store.SaveTask(rec)
	}

	res, err := runEstimation(h.ctx, snap, cfg, h.batches, progress)
	if err != nil {
		return err
	}
	h.completeEstimate(res, s.store)
	return nil
}

func (s *Service) runSelectionTask(h *taskHandle, snap *GraphSnapshot) error {
	cfg := *h.selCfg
	totalUnits := cfg.K + len(normalizeCandidates(cfg.Candidates))

	var mu sync.Mutex
	progress := func(b BatchStats) {
		mu.Lock()
		defer mu.Unlock()
		h.mu.Lock()
		// The same batch can be reported for a position that was already
		// persisted in an earlier CELF evaluation; keep batches unique.
		known := false
		for _, x := range h.batches {
			if x.Index == b.Index {
				known = true
				break
			}
		}
		if !known {
			h.batches = append(h.batches, b)
		}
		h.state.UpdatedAt = time.Now()
		rec := h.recordLocked()
		h.mu.Unlock()
		_ = s.store.SaveTask(rec)
	}
	done := 0
	onComplete := func() {
		mu.Lock()
		defer mu.Unlock()
		h.mu.Lock()
		done++
		p := float64(done) / float64(totalUnits)
		if p > 1 {
			p = 1
		}
		h.state.Progress = p
		h.state.UpdatedAt = time.Now()
		rec := h.recordLocked()
		h.mu.Unlock()
		_ = s.store.SaveTask(rec)
	}

	res, err := runSelection(h.ctx, snap, cfg, h.batches, progress, onComplete)
	if err != nil {
		return err
	}
	h.completeSelection(res, s.store)
	return nil
}

// submit performs the shared submission bookkeeping for both task kinds.
func (s *Service) submit(kind TaskKind, est *EstimateConfig, sel *SelectionConfig) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return "", ErrServiceClosed
	}

	snap := s.graph.Snapshot()
	if est != nil {
		if err := validateEstimateConfig(snap, *est); err != nil {
			return "", err
		}
	} else {
		if err := validateSelectionConfig(snap, *sel); err != nil {
			return "", err
		}
	}

	// Persist the bound snapshot before the task record, so a crash can never
	// leave behind a task whose bound graph content is unrecoverable. The
	// store is content-addressed: identical graph content is written once.
	if err := s.store.SaveSnapshot(snap); err != nil {
		return "", err
	}

	now := time.Now().UTC()
	ctx, cancel := context.WithCancel(context.Background())
	h := &taskHandle{
		snap:    snap,
		ctx:     ctx,
		cancel:  cancel,
		batches: []BatchStats{},
		done:    make(chan struct{}),
	}
	h.state = TaskState{
		ID:           newTaskID(),
		Kind:         kind,
		Status:       StatusQueued,
		GraphVersion: snap.Version(),
		GraphHash:    snap.Hash(),
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	if est != nil {
		c := *est
		h.estCfg = &c
	} else {
		c := *sel
		h.selCfg = &c
	}

	if err := s.store.SaveTask(h.recordLocked()); err != nil {
		cancel()
		return "", err
	}
	s.tasks[h.state.ID] = h
	s.wg.Add(1)
	go s.runTask(h)
	return h.state.ID, nil
}
