package influence

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func newTestService(t *testing.T) *Service {
	t.Helper()
	s, err := NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func sampleGraph(t *testing.T, s *Service) {
	t.Helper()
	for _, n := range []NodeID{"a", "b", "c", "d"} {
		if err := s.AddNode(n); err != nil {
			t.Fatal(err)
		}
	}
	edges := []struct {
		from, to NodeID
		w        float64
	}{
		{"a", "b", 0.7}, {"b", "c", 0.6}, {"c", "d", 0.5}, {"a", "c", 0.3},
	}
	for _, e := range edges {
		if err := s.AddEdge(e.from, e.to, e.w); err != nil {
			t.Fatal(err)
		}
	}
}

func estCfg() EstimateConfig {
	return EstimateConfig{
		Model:      ModelConfig{Model: ModelIndependentCascade, UseEdgeWeights: true},
		Seeds:      []NodeID{"a"},
		Rounds:     128,
		BatchSize:  16,
		Seed:       42,
		Confidence: 0.95,
	}
}

func waitDone(t *testing.T, s *Service, id string) TaskState {
	t.Helper()
	st, err := s.WaitTask(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func TestServiceEstimateEndToEnd(t *testing.T) {
	s := newTestService(t)
	sampleGraph(t, s)
	id, err := s.SubmitEstimation(estCfg())
	if err != nil {
		t.Fatal(err)
	}
	st := waitDone(t, s, id)
	if st.Status != StatusCompleted {
		t.Fatalf("status = %s err = %s", st.Status, st.Error)
	}
	if st.Estimate == nil || st.GraphHash == "" {
		t.Fatalf("incomplete state: %+v", st)
	}
	if st.Estimate.Mean < 1 || st.Estimate.Mean > 4 {
		t.Fatalf("mean = %v", st.Estimate.Mean)
	}
}

func TestServiceDeterministicAcrossServices(t *testing.T) {
	dir := t.TempDir()

	run := func() EstimateResult {
		s, err := NewService(dir)
		if err != nil {
			t.Fatal(err)
		}
		// Build the same graph deterministically on each process start.
		sampleGraph(t, s)
		id, err := s.SubmitEstimation(estCfg())
		if err != nil {
			t.Fatal(err)
		}
		st := waitDone(t, s, id)
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		if st.Status != StatusCompleted {
			t.Fatalf("status=%s err=%s", st.Status, st.Error)
		}
		return *st.Estimate
	}

	// Second "process" must recover the graph from disk and identical inputs
	// must produce bit-identical estimates even though task IDs differ.
	r1 := run()

	s2, err := NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	cfg := estCfg()
	id2, err := s2.SubmitEstimation(cfg)
	if err != nil {
		t.Fatal(err)
	}
	st2 := waitDone(t, s2, id2)
	r2 := *st2.Estimate
	_ = s2.Close()

	if r1 != r2 {
		t.Fatalf("cross-restart mismatch:\n%+v\n%+v", r1, r2)
	}
}

func TestServiceConcurrentEstimatesDeterministic(t *testing.T) {
	s := newTestService(t)
	sampleGraph(t, s)

	const n = 8
	var wg sync.WaitGroup
	results := make([]EstimateResult, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id, err := s.SubmitEstimation(estCfg())
			if err != nil {
				errs[i] = err
				return
			}
			st := waitDone(t, s, id)
			if st.Estimate != nil {
				results[i] = *st.Estimate
			}
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := 1; i < n; i++ {
		if results[0] != results[i] {
			t.Fatalf("concurrent estimates differ:\n%+v\n%+v", results[0], results[i])
		}
	}
}

func TestServiceCrashRecoveryResumes(t *testing.T) {
	dir := t.TempDir()

	// Start a service, submit a large selection task, cancel via Close while
	// it runs, then reopen and verify the task resumes and eventually finishes.
	startAndInterrupt := func() (string, *Service) {
		s, err := NewService(dir)
		if err != nil {
			t.Fatal(err)
		}
		sampleGraph(t, s)
		cfg := SelectionConfig{
			Model:      ModelConfig{Model: ModelIndependentCascade, UseEdgeWeights: true},
			K:          2,
			Rounds:     200,
			BatchSize:  20,
			Seed:       5,
			Confidence: 0.95,
			Candidates: []NodeID{"a", "b", "c", "d"},
		}
		id, err := s.SubmitSelection(cfg)
		if err != nil {
			t.Fatal(err)
		}
		// Poll until at least one batch is persisted, then "crash".
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			recs, err := s.store.ListTasks()
			if err == nil && len(recs) > 0 && len(recs[0].Batches) > 0 {
				break
			}
			time.Sleep(2 * time.Millisecond)
		}
		_ = s.Close()
		return id, s
	}
	id, _ := startAndInterrupt()

	s2, err := NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s2.Close() })
	s2.ResumePending()

	st, err := s2.WaitTask(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if st.Status != StatusCompleted {
		t.Fatalf("recovered task status = %s err = %s", st.Status, st.Error)
	}
}

func TestServiceGraphUpdatePinsRunningTasks(t *testing.T) {
	s := newTestService(t)
	sampleGraph(t, s)

	// Pin the old graph via a slow-ish task by using many rounds, then mutate.
	cfg := estCfg()
	cfg.Rounds = 4096
	cfg.BatchSize = 64
	id, err := s.SubmitEstimation(cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Concurrent update: boost all edge weights. Running task must still use
	// the snapshot it pinned and report the pinned graph hash.
	if err := s.SetEdgeWeight("a", "b", 0.99); err != nil {
		t.Fatal(err)
	}
	if err := s.AddNode("e"); err != nil {
		t.Fatal(err)
	}

	st := waitDone(t, s, id)
	if st.Status != StatusCompleted {
		t.Fatalf("status = %s err = %s", st.Status, st.Error)
	}
	if st.GraphHash == "" {
		t.Fatal("task must record a pinned graph hash")
	}
	pinnedVersion := st.GraphVersion
	if pinnedVersion < 8 {
		t.Fatalf("task pinned pre-submit version %d", pinnedVersion)
	}

	// New submissions use the updated graph and can seed the new node.
	cfg2 := estCfg()
	cfg2.Seeds = []NodeID{"e"}
	id2, err := s.SubmitEstimation(cfg2)
	if err != nil {
		t.Fatal(err)
	}
	st2 := waitDone(t, s, id2)
	if st2.Status != StatusCompleted || st2.Estimate.Mean != 1 {
		t.Fatalf("isolated seed estimate = %+v", st2)
	}
	if st2.GraphVersion <= st.GraphVersion {
		t.Fatal("new task did not pin newer graph version")
	}
}

func TestServiceInvalidSubmissions(t *testing.T) {
	s := newTestService(t)
	sampleGraph(t, s)

	cfg := estCfg()
	cfg.Seeds = []NodeID{"ghost"}
	if _, err := s.SubmitEstimation(cfg); !errors.Is(err, ErrSeedNotFound) {
		t.Fatalf("err = %v, want ErrSeedNotFound", err)
	}

	sel := SelectionConfig{
		Model:      ModelConfig{Model: ModelIndependentCascade, UseEdgeWeights: true},
		K:          99,
		Rounds:     4,
		BatchSize:  2,
		Confidence: 0.9,
		Candidates: []NodeID{"a"},
	}
	if _, err := s.SubmitSelection(sel); !errors.Is(err, ErrSeedCountExceedsNodes) {
		t.Fatalf("err = %v", err)
	}
}

func TestServiceCancelLeavesNoPartialResult(t *testing.T) {
	s := newTestService(t)
	sampleGraph(t, s)
	cfg := estCfg()
	cfg.Rounds = 1_000_000
	cfg.BatchSize = 1
	id, err := s.SubmitEstimation(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Cancel(id); err != nil {
		t.Fatal(err)
	}
	st := waitDone(t, s, id)
	if st.Status != StatusCancelled {
		t.Fatalf("status = %s", st.Status)
	}
	if st.Estimate != nil {
		t.Fatal("cancelled task must not carry a partial estimate")
	}
	if err := s.Cancel(id); !errors.Is(err, ErrTaskTerminal) {
		t.Fatalf("second cancel err = %v", err)
	}
}

func TestServiceGetUnknownTask(t *testing.T) {
	s := newTestService(t)
	if _, err := s.GetTask("nope"); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("err = %v", err)
	}
}

func TestServiceSelectionEndToEnd(t *testing.T) {
	s := newTestService(t)
	sampleGraph(t, s)
	cfg := SelectionConfig{
		Model:      ModelConfig{Model: ModelIndependentCascade, UseEdgeWeights: true},
		K:          2,
		Rounds:     64,
		BatchSize:  16,
		Seed:       11,
		Confidence: 0.9,
		Candidates: []NodeID{"a", "b", "c", "d"},
	}
	id, err := s.SubmitSelection(cfg)
	if err != nil {
		t.Fatal(err)
	}
	st := waitDone(t, s, id)
	if st.Status != StatusCompleted {
		t.Fatalf("status = %s err = %s", st.Status, st.Error)
	}
	if st.Selection == nil || len(st.Selection.Seeds) != 2 {
		t.Fatalf("selection = %+v", st.Selection)
	}
}

func TestServiceGraphUpdateChangesNewResults(t *testing.T) {
	s := newTestService(t)
	// Two nodes, edge a->b probability 0: seed a only reaches itself.
	if err := s.AddNode("a"); err != nil {
		t.Fatal(err)
	}
	if err := s.AddNode("b"); err != nil {
		t.Fatal(err)
	}
	if err := s.AddEdge("a", "b", 0); err != nil {
		t.Fatal(err)
	}
	cfg := estCfg()
	cfg.Rounds = 32
	cfg.BatchSize = 8
	id1, err := s.SubmitEstimation(cfg)
	if err != nil {
		t.Fatal(err)
	}
	r1 := waitDone(t, s, id1)
	if r1.Estimate.Mean != 1 {
		t.Fatalf("mean with p=0 = %v, want 1", r1.Estimate.Mean)
	}

	// Online update: set the edge to always propagate. The old conclusion must
	// not be reused; a new estimate must reflect the updated graph.
	if err := s.SetEdgeWeight("a", "b", 1); err != nil {
		t.Fatal(err)
	}
	id2, err := s.SubmitEstimation(cfg)
	if err != nil {
		t.Fatal(err)
	}
	r2 := waitDone(t, s, id2)
	if r2.GraphHash == r1.GraphHash {
		t.Fatal("graph hash must change after weight update")
	}
	if r2.Estimate.Mean != 2 {
		t.Fatalf("mean with p=1 = %v, want 2", r2.Estimate.Mean)
	}
}

func TestServiceCancelledTaskStaysCancelledAfterRestart(t *testing.T) {
	dir := t.TempDir()
	var id string
	func() {
		s, err := NewService(dir)
		if err != nil {
			t.Fatal(err)
		}
		sampleGraph(t, s)
		cfg := estCfg()
		cfg.Rounds = 1_000_000
		cfg.BatchSize = 1
		id, err = s.SubmitEstimation(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Cancel(id); err != nil {
			t.Fatal(err)
		}
		st := waitDone(t, s, id)
		if st.Status != StatusCancelled {
			t.Fatalf("status = %s", st.Status)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	s2, err := NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s2.Close() })
	ids := s2.ResumePending()
	for _, resumed := range ids {
		if resumed == id {
			t.Fatal("explicitly cancelled task must not be resumed")
		}
	}
	st, err := s2.GetTask(id)
	if err != nil {
		t.Fatal(err)
	}
	if st.Status != StatusCancelled {
		t.Fatalf("status after restart = %s, want cancelled", st.Status)
	}
}

// guard against accidental import path drift.
var _ = filepath.Join
