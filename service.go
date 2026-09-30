package influence

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"
	"time"
)

// TaskKind identifies a task type.
type TaskKind string

const (
	TaskEstimation TaskKind = "estimation"
	TaskSelection  TaskKind = "selection"
)

// TaskStatus is the lifecycle state of a task.
type TaskStatus string

const (
	StatusQueued    TaskStatus = "queued"
	StatusRunning   TaskStatus = "running"
	StatusCompleted TaskStatus = "completed"
	StatusFailed    TaskStatus = "failed"
	StatusCancelled TaskStatus = "cancelled"
)

// TaskState is a snapshot of task state safe to return to callers and persist.
type TaskState struct {
	ID           string           `json:"id"`
	Kind         TaskKind         `json:"kind"`
	Status       TaskStatus       `json:"status"`
	GraphVersion uint64           `json:"graph_version"`
	GraphHash    string           `json:"graph_hash"`
	Progress     float64          `json:"progress"`
	Estimate     *EstimateResult  `json:"estimate,omitempty"`
	Selection    *SelectionResult `json:"selection,omitempty"`
	Error        string           `json:"error,omitempty"`
	CreatedAt    time.Time        `json:"created_at"`
	UpdatedAt    time.Time        `json:"updated_at"`
}

// Service orchestrates the graph, task execution, cancellation and recovery.
type Service struct {
	store  *Store
	mu     sync.RWMutex
	graph  *Graph
	tasks  map[string]*taskHandle
	wg     sync.WaitGroup
	closed bool
}

// NewService opens a service backed by dir (created if missing). Graph and
// unfinished tasks are recovered from dir on startup.
func NewService(dir string) (*Service, error) {
	st, err := OpenStore(dir)
	if err != nil {
		return nil, err
	}
	s := &Service{store: st, tasks: make(map[string]*taskHandle)}

	g, err := st.LoadGraph()
	if err != nil {
		return nil, err
	}
	if g == nil {
		g = NewGraph()
	}
	s.graph = g

	recs, err := st.ListTasks()
	if err != nil {
		return nil, err
	}
	for _, rec := range recs {
		ctx, cancel := context.WithCancel(context.Background())
		h := &taskHandle{
			ctx:     ctx,
			cancel:  cancel,
			state:   rec.State,
			estCfg:  rec.EstimateCfg,
			selCfg:  rec.SelectCfg,
			batches: rec.Batches,
			done:    make(chan struct{}),
		}
		if h.batches == nil {
			h.batches = []BatchStats{}
		}
		switch h.state.Status {
		case StatusQueued, StatusRunning:
			h.state.Status = StatusQueued
			s.tasks[h.state.ID] = h
		default:
			close(h.done)
			s.tasks[h.state.ID] = h
		}
	}
	return s, nil
}

// ResumePending resumes all tasks persisted in queued/running state. It is
// explicit so construction (recovery) never starts work before the caller has
// restored or inspected its graph. Resumed tasks continue from their already
// persisted batches without recomputing them.
func (s *Service) ResumePending() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	var ids []string
	for _, h := range s.tasks {
		if h.state.Status != StatusQueued {
			continue
		}
		ids = append(ids, h.state.ID)
		s.wg.Add(1)
		go s.runTask(h)
	}
	sort.Strings(ids)
	return ids
}

// Close stops accepting tasks and releases resources.
func (s *Service) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	for _, h := range s.tasks {
		if h.state.Status == StatusQueued || h.state.Status == StatusRunning {
			// Detach the in-memory context without persisting a terminal
			// status: a process shutdown is a crash/restart, not a user
			// cancellation. Already-completed batches stay on disk.
			h.stopReq()
		}
	}
	s.mu.Unlock()

	s.wg.Wait()
	return s.store.Close()
}

// GraphVersion returns the current graph version.
func (s *Service) GraphVersion() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.graph.Version()
}

// AddNode / RemoveNode / AddEdge / RemoveEdge / SetEdgeWeight apply online
// graph updates and bump the graph version.
func (s *Service) AddNode(id NodeID) error {
	return s.mutate(func(g *Graph) error { return g.AddNode(id) })
}
func (s *Service) RemoveNode(id NodeID) error {
	return s.mutate(func(g *Graph) error { return g.RemoveNode(id) })
}
func (s *Service) AddEdge(from, to NodeID, weight float64) error {
	return s.mutate(func(g *Graph) error { return g.AddEdge(from, to, weight) })
}
func (s *Service) RemoveEdge(from, to NodeID) error {
	return s.mutate(func(g *Graph) error { return g.RemoveEdge(from, to) })
}
func (s *Service) SetEdgeWeight(from, to NodeID, weight float64) error {
	return s.mutate(func(g *Graph) error { return g.SetEdgeWeight(from, to, weight) })
}

// mutate applies a graph mutation, then persists the new graph. Running
// tasks pin earlier immutable snapshots and are never affected.
func (s *Service) mutate(fn func(*Graph) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrServiceClosed
	}
	if err := fn(s.graph); err != nil {
		return err
	}
	return s.store.SaveGraph(s.graph.Snapshot())
}

// SubmitEstimation queues an influence estimation task.
func (s *Service) SubmitEstimation(cfg EstimateConfig) (string, error) {
	return s.submit(TaskEstimation, &cfg, nil)
}

// SubmitSelection queues a budget-constrained seed selection task.
func (s *Service) SubmitSelection(cfg SelectionConfig) (string, error) {
	return s.submit(TaskSelection, nil, &cfg)
}

// GetTask returns a copy of the current task state.
func (s *Service) GetTask(id string) (TaskState, error) {
	s.mu.RLock()
	h, ok := s.tasks[id]
	s.mu.RUnlock()
	if !ok {
		return TaskState{}, &taskNotFoundError{id: id}
	}
	return h.snapshot(), nil
}

// Cancel requests cancellation of a queued or running task.
func (s *Service) Cancel(id string) error {
	s.mu.RLock()
	h, ok := s.tasks[id]
	closed := s.closed
	s.mu.RUnlock()
	if !ok {
		return &taskNotFoundError{id: id}
	}
	if closed {
		return ErrServiceClosed
	}
	h.mu.Lock()
	switch h.state.Status {
	case StatusCompleted, StatusFailed, StatusCancelled:
		h.mu.Unlock()
		return ErrTaskTerminal
	}
	h.cancelReq()
	h.mu.Unlock()
	return nil
}

// WaitTask blocks until the task reaches a terminal state or ctx expires.
func (s *Service) WaitTask(ctx context.Context, id string) (TaskState, error) {
	s.mu.RLock()
	h, ok := s.tasks[id]
	s.mu.RUnlock()
	if !ok {
		return TaskState{}, &taskNotFoundError{id: id}
	}
	for {
		st := h.snapshot()
		if isTerminal(st.Status) {
			return st, nil
		}
		select {
		case <-h.done:
			return h.snapshot(), nil
		case <-ctx.Done():
			return h.snapshot(), ctx.Err()
		}
	}
}

type taskNotFoundError struct{ id string }

func (e *taskNotFoundError) Error() string {
	return fmt.Sprintf("%s: %q", ErrTaskNotFound, e.id)
}
func (e *taskNotFoundError) Unwrap() error { return ErrTaskNotFound }

func isTerminal(st TaskStatus) bool {
	return st == StatusCompleted || st == StatusFailed || st == StatusCancelled
}

func newTaskID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failure is not recoverable for deterministic IDs; the
		// platform guarantees /dev/urandom, but keep the call explicit.
		panic(err)
	}
	return hex.EncodeToString(b[:])
}
