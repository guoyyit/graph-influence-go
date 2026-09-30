package influence

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
)

// Task kinds.
const (
	TaskKindEstimate  = "estimate"
	TaskKindSelection = "selection"
)

// Service runs influence jobs locally with persistence, resumability,
// cancellation and bounded concurrency.
type Service struct {
	store   Store
	cfg     serviceConfig
	mu      sync.Mutex
	running map[string]*runningTask
	pending []string
	wake    chan struct{}
	stop    chan struct{}
	wg      sync.WaitGroup
	closed  atomic.Bool
}

type runningTask struct {
	state  TaskState
	cancel *CancellationToken
}

type serviceConfig struct {
	workers int
}

// ServiceOption configures a Service.
type ServiceOption func(*serviceConfig)

// WithWorkers bounds the number of concurrently executing tasks.
func WithWorkers(n int) ServiceOption {
	return func(c *serviceConfig) {
		if n > 0 {
			c.workers = n
		}
	}
}

func NewService(store Store, opts ...ServiceOption) *Service {
	cfg := serviceConfig{workers: 1}
	for _, o := range opts {
		o(&cfg)
	}
	s := &Service{
		store:   store,
		cfg:     cfg,
		running: make(map[string]*runningTask),
		wake:    make(chan struct{}, 1),
		stop:    make(chan struct{}),
	}
	return s
}

// Start launches worker goroutines and recovers interrupted tasks.
func (s *Service) Start(ctx context.Context) error {
	for i := 0; i < s.cfg.workers; i++ {
		s.wg.Add(1)
		go s.worker(i)
	}
	return s.Resume(ctx)
}

// estimatePayload is the persisted, resumable definition of an
// estimation task. The graph snapshot is embedded so a task always runs
// against exactly the version it was submitted on, even after updates.
type estimatePayload struct {
	Request    EstimateRequest `json:"request"`
	Graph      graphDoc        `json:"graph"`
	Confidence float64         `json:"confidence"`
	Workers    int             `json:"workers"`
}

type selectionPayload struct {
	Request SelectRequest `json:"request"`
	Graph   graphDoc      `json:"graph"`
}

// SubmitEstimate enqueues an estimation job and returns its task id.
func (s *Service) SubmitEstimate(ctx context.Context, req EstimateRequest) (string, error) {
	g, err := s.resolveGraph(req.Graph)
	if err != nil {
		return "", err
	}
	if req.Batches <= 0 {
		return "", newError(ErrInvalidBatches, "batches must be positive, got %d", req.Batches)
	}
	payload := estimatePayload{
		Request: EstimateRequest{
			Seeds:      seedCanonical(req.Seeds),
			Config:     req.Config,
			Batches:    req.Batches,
			Confidence: req.Confidence,
		},
		Graph:      graphToDoc(g),
		Confidence: req.Confidence,
		Workers:    req.Workers,
	}
	return s.submit(ctx, TaskKindEstimate, g.version, req.Batches, payload)
}

// SubmitSelection enqueues a greedy seed-selection job.
func (s *Service) SubmitSelection(ctx context.Context, req SelectRequest) (string, error) {
	g, err := s.resolveGraph(req.Graph)
	if err != nil {
		return "", err
	}
	if req.Budget <= 0 {
		return "", newError(ErrInvalidBudget, "budget must be positive, got %d", req.Budget)
	}
	payload := selectionPayload{
		Request: SelectRequest{
			Config:     req.Config,
			Batches:    req.Batches,
			Rounds:     req.Rounds,
			Budget:     req.Budget,
			Candidate:  seedCanonical(req.Candidate),
			Confidence: req.Confidence,
			Workers:    req.Workers,
		},
		Graph: graphToDoc(g),
	}
	total := req.Batches * req.Budget * len(g.Nodes())
	return s.submit(ctx, TaskKindSelection, g.version, total, payload)
}

func (s *Service) resolveGraph(g *Graph) (*Graph, error) {
	if g != nil {
		return g, nil
	}
	if s.store == nil {
		return nil, newError(ErrEmptyGraph, "no graph provided and no store configured")
	}
	loaded, err := s.store.LoadGraph(context.Background())
	if err != nil {
		return nil, err
	}
	if loaded == nil {
		return nil, newError(ErrEmptyGraph, "no persisted graph")
	}
	return loaded, nil
}

func (s *Service) submit(ctx context.Context, kind string, version int64, totalBatches int, payload any) (string, error) {
	data, err := json.Marshal(payload)
	if err != nil {
		return "", newError(ErrPersist, "encode payload: %v", err)
	}
	id := newTaskID(kind, version)
	state := TaskState{
		ID:           id,
		Kind:         kind,
		Status:       TaskPending,
		GraphVersion: version,
		DoneBatches:  0,
		TotalBatches: totalBatches,
		Payload:      data,
	}
	if s.store != nil {
		if err := s.store.SaveTask(ctx, state); err != nil {
			return "", err
		}
	}
	s.mu.Lock()
	s.running[id] = &runningTask{state: state}
	s.pending = append(s.pending, id)
	s.mu.Unlock()
	s.signal()
	return id, nil
}

func (s *Service) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// GetTask returns the persisted (or in-memory) task state.
func (s *Service) GetTask(ctx context.Context, id string) (TaskState, error) {
	s.mu.Lock()
	if rt, ok := s.running[id]; ok {
		st := rt.state
		s.mu.Unlock()
		return st, nil
	}
	s.mu.Unlock()
	if s.store == nil {
		return TaskState{}, newError(ErrTaskNotFound, "task %q not found", id)
	}
	st, ok, err := s.store.LoadTask(ctx, id)
	if err != nil {
		return TaskState{}, err
	}
	if !ok {
		return TaskState{}, newError(ErrTaskNotFound, "task %q not found", id)
	}
	return st, nil
}

// TaskResult decodes a completed task's result.
func (s *Service) TaskResult(ctx context.Context, id string, out any) error {
	st, err := s.GetTask(ctx, id)
	if err != nil {
		return err
	}
	if st.Status != TaskCompleted {
		return newError(ErrTaskNotRunning, "task %q is %s, not completed", id, st.Status)
	}
	return json.Unmarshal(st.Result, out)
}

// CancelTask marks a task canceled. Completed batches stay cached, but
// no partial task result is written; a later resume skips the task.
func (s *Service) CancelTask(ctx context.Context, id string) error {
	s.mu.Lock()
	rt, ok := s.running[id]
	if ok {
		rt.cancel.Cancel()
		rt.state.Status = TaskCanceled
		st := rt.state
		s.mu.Unlock()
		if s.store != nil {
			return s.store.SaveTask(ctx, st)
		}
		return nil
	}
	s.mu.Unlock()
	if s.store == nil {
		return newError(ErrTaskNotFound, "task %q not found", id)
	}
	st, exists, err := s.store.LoadTask(ctx, id)
	if err != nil {
		return err
	}
	if !exists {
		return newError(ErrTaskNotFound, "task %q not found", id)
	}
	if st.Status == TaskCompleted || st.Status == TaskCanceled {
		return nil
	}
	st.Status = TaskCanceled
	return s.store.SaveTask(ctx, st)
}

// Resume recovers unfinished tasks after a restart: tasks left
// "running" by a crashed process are reset to "pending" and requeued.
// Already completed batches are reused via the batch cache, so work is
// never recomputed.
func (s *Service) Resume(ctx context.Context) error {
	if s.store == nil {
		return nil
	}
	tasks, err := s.store.ListTasks(ctx)
	if err != nil {
		return err
	}
	for _, st := range tasks {
		if st.Status == TaskRunning || st.Status == TaskPending {
			st.Status = TaskPending
			if err := s.store.SaveTask(ctx, st); err != nil {
				return err
			}
			s.mu.Lock()
			if _, exists := s.running[st.ID]; !exists {
				s.running[st.ID] = &runningTask{
					state:  st,
					cancel: NewCancellationToken(),
				}
				s.pending = append(s.pending, st.ID)
			}
			s.mu.Unlock()
		}
	}
	s.signal()
	return nil
}

// UpdateGraph persists a new graph snapshot. Tasks already submitted
// remain pinned to their own snapshot version and are not affected;
// their cached batches are keyed by the old version and thus excluded
// from new computations, which is the explicit invalidation rule.
func (s *Service) UpdateGraph(ctx context.Context, g *Graph) error {
	if g == nil {
		return newError(ErrEmptyGraph, "graph must not be nil")
	}
	if s.store == nil {
		return newError(ErrPersist, "update requires a persistent store")
	}
	return s.store.SaveGraph(ctx, g)
}

func (s *Service) worker(id int) {
	defer s.wg.Done()
	for {
		task := s.claim()
		if task == nil {
			return
		}
		s.execute(task)
	}
}

func (s *Service) claim() *runningTask {
	for {
		s.mu.Lock()
		var rt *runningTask
		for len(s.pending) > 0 {
			id := s.pending[0]
			s.pending = s.pending[1:]
			cand := s.running[id]
			if cand != nil && cand.state.Status == TaskPending {
				cand.state.Status = TaskRunning
				cand.cancel = NewCancellationToken()
				rt = cand
				break
			}
		}
		if rt != nil {
			st := rt.state
			s.mu.Unlock()
			s.persist(st)
			return rt
		}
		s.mu.Unlock()
		select {
		case <-s.stop:
			return nil
		case <-s.wake:
		}
	}
}

func (s *Service) persist(st TaskState) {
	if s.store != nil {
		_ = s.store.SaveTask(context.Background(), st)
	}
}

func (s *Service) setState(id string, mutate func(*TaskState)) TaskState {
	s.mu.Lock()
	rt := s.running[id]
	if rt != nil {
		mutate(&rt.state)
		st := rt.state
		s.mu.Unlock()
		s.persist(st)
		return st
	}
	s.mu.Unlock()
	return TaskState{}
}

func (s *Service) execute(rt *runningTask) {
	id := rt.state.ID
	var result []byte
	var runErr error
	switch rt.state.Kind {
	case TaskKindEstimate:
		result, runErr = s.runEstimate(rt)
	case TaskKindSelection:
		result, runErr = s.runSelection(rt)
	default:
		runErr = newError(ErrPersist, "unknown task kind %q", rt.state.Kind)
	}
	if runErr != nil {
		if ErrorCodeOf(runErr) == ErrTaskCanceled || rt.cancel.Canceled() {
			s.setState(id, func(st *TaskState) {
				st.Status = TaskCanceled
				st.Error = runErr.Error()
			})
		} else {
			s.setState(id, func(st *TaskState) {
				st.Status = TaskFailed
				st.Error = runErr.Error()
			})
		}
		return
	}
	s.setState(id, func(st *TaskState) {
		st.Status = TaskCompleted
		st.Result = result
		st.DoneBatches = st.TotalBatches
	})
}

func (s *Service) runEstimate(rt *runningTask) ([]byte, error) {
	var payload estimatePayload
	if err := json.Unmarshal(rt.state.Payload, &payload); err != nil {
		return nil, newError(ErrPersist, "decode estimate payload: %v", err)
	}
	g, err := docToGraph(payload.Graph)
	if err != nil {
		return nil, err
	}
	req := payload.Request
	req.Graph = g
	req.Store = s.store
	req.Cancel = rt.cancel
	req.Confidence = payload.Confidence
	if payload.Workers > 0 {
		req.Workers = payload.Workers
	}
	req.Progress = func(done, total int) {
		s.setState(rt.state.ID, func(st *TaskState) {
			st.DoneBatches = done
		})
	}
	res, err := Estimate(req)
	if err != nil {
		return nil, err
	}
	return json.Marshal(res)
}

func (s *Service) runSelection(rt *runningTask) ([]byte, error) {
	var payload selectionPayload
	if err := json.Unmarshal(rt.state.Payload, &payload); err != nil {
		return nil, newError(ErrPersist, "decode selection payload: %v", err)
	}
	g, err := docToGraph(payload.Graph)
	if err != nil {
		return nil, err
	}
	req := payload.Request
	req.Graph = g
	req.Store = s.store
	req.Cancel = rt.cancel
	roundCandidates := len(g.Nodes())
	if len(payload.Request.Candidate) > 0 {
		roundCandidates = len(payload.Request.Candidate)
	}
	req.Progress = func(selected, budget int, _ NodeID, _ float64) {
		done := selected * roundCandidates * req.Batches
		s.setState(rt.state.ID, func(st *TaskState) {
			st.DoneBatches = done
		})
	}
	res, err := Select(req)
	if err != nil {
		return nil, err
	}
	return json.Marshal(res)
}

// Close stops workers. Tasks still in flight are marked pending so the
// next Start/Resume continues them; nothing is left half-written.
func (s *Service) Close() error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}
	close(s.stop)
	s.mu.Lock()
	for _, rt := range s.running {
		rt.cancel.Cancel()
		if rt.state.Status == TaskRunning {
			rt.state.Status = TaskPending
			st := rt.state
			if s.store != nil {
				_ = s.store.SaveTask(context.Background(), st)
			}
		}
	}
	s.mu.Unlock()
	s.wg.Wait()
	return nil
}
