package influence

import (
	"context"
	"sort"
	"time"
)

// recordLocked builds a persistence record; the caller must hold h.mu while
// reading mutable state.
func (h *taskHandle) recordLocked() taskRecord {
	batches := append([]BatchStats(nil), h.batches...)
	sort.Slice(batches, func(i, j int) bool { return batches[i].Index < batches[j].Index })
	return taskRecord{
		State:         h.state,
		EstimateCfg:   h.estCfg,
		SelectCfg:     h.selCfg,
		Batches:       batches,
		BoundSnapshot: h.bound,
	}
}

// cancel requests cancellation. Safe to call before the handle has a context
// only after submit/recovery initialised one; a nil cancel is ignored.
func (h *taskHandle) cancelReq() {
	if h.cancel != nil {
		h.cancel()
	}
}

// stopReq marks cancellation as a service shutdown (not a user cancel) and
// detaches the in-memory context.
func (h *taskHandle) stopReq() {
	h.mu.Lock()
	h.stopping = true
	h.mu.Unlock()
	h.cancelReq()
}

func (h *taskHandle) completeEstimate(res EstimateResult, st *Store) {
	h.mu.Lock()
	h.state.Status = StatusCompleted
	h.state.Progress = 1
	h.state.Estimate = &res
	h.state.Error = ""
	h.state.UpdatedAt = time.Now().UTC()
	rec := h.recordLocked()
	h.mu.Unlock()
	_ = st.SaveTask(rec)
}

func (h *taskHandle) completeSelection(res SelectionResult, st *Store) {
	h.mu.Lock()
	h.state.Status = StatusCompleted
	h.state.Progress = 1
	h.state.Selection = &res
	h.state.Error = ""
	h.state.UpdatedAt = time.Now().UTC()
	rec := h.recordLocked()
	h.mu.Unlock()
	_ = st.SaveTask(rec)
}

func (h *taskHandle) fail(err error, st *Store) {
	h.mu.Lock()
	h.state.Status = StatusFailed
	h.state.Error = err.Error()
	h.state.UpdatedAt = time.Now().UTC()
	rec := h.recordLocked()
	h.mu.Unlock()
	_ = st.SaveTask(rec)
}

func (h *taskHandle) markCancelled(st *Store) {
	h.mu.Lock()
	h.state.Status = StatusCancelled
	h.state.Error = context.Canceled.Error()
	h.state.UpdatedAt = time.Now().UTC()
	rec := h.recordLocked()
	h.mu.Unlock()
	// Keep persisted completed batches (they can be reused later), but mark
	// the task cancelled; no partial estimate or selection is written.
	_ = st.SaveTask(rec)
}
