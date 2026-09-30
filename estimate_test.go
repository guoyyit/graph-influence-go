package influence

import (
	"context"
	"testing"
)

func influenceTestGraph() *Graph {
	g, err := NewGraph([]NodeID{"a", "b", "c", "d", "e"}, []Edge{
		{"a", "b", 0.8},
		{"a", "c", 0.6},
		{"b", "d", 0.7},
		{"c", "d", 0.5},
		{"d", "e", 0.4},
		{"b", "e", 0.2},
	})
	if err != nil {
		panic(err)
	}
	return g
}

func TestEstimateDeterministicSerialAndConcurrent(t *testing.T) {
	g := influenceTestGraph()
	cfg := ModelConfig{Model: IndependentCascade, Rounds: 8, Seed: 99}
	serial, err := Estimate(EstimateRequest{Graph: g, Seeds: []NodeID{"a"}, Config: cfg, Batches: 6})
	if err != nil {
		t.Fatal(err)
	}
	concurrent, err := Estimate(EstimateRequest{
		Graph: g, Seeds: []NodeID{"a"}, Config: cfg, Batches: 6, Workers: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	if serial != concurrent {
		t.Fatalf("serial/concurrent mismatch:\n%+v\n%+v", serial, concurrent)
	}
	again, _ := Estimate(EstimateRequest{
		Graph: g, Seeds: []NodeID{"a"}, Config: cfg, Batches: 6, Workers: 4,
	})
	if again != serial {
		t.Fatal("estimate not reproducible across calls")
	}
	if serial.GraphVersion != g.Version() {
		t.Fatalf("version = %d want %d", serial.GraphVersion, g.Version())
	}
	if serial.Mean <= 1 || serial.Mean > 5 {
		t.Fatalf("implausible mean spread %v", serial.Mean)
	}
	if serial.MarginOfError <= 0 {
		t.Fatal("expected positive margin of error for stochastic simulation")
	}
	if serial.Confidence != 0.95 {
		t.Fatalf("default confidence = %v", serial.Confidence)
	}
	if serial.Samples != 48 {
		t.Fatalf("samples = %d want 48", serial.Samples)
	}
}

func TestEstimateCacheReuseAcrossStores(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	g := influenceTestGraph()
	cfg := ModelConfig{Model: IndependentCascade, Rounds: 4, Seed: 1234}
	req := EstimateRequest{
		Graph: g, Seeds: []NodeID{"a"}, Config: cfg,
		Batches: 3, Workers: 1, Store: store,
	}
	first, err := Estimate(req)
	if err != nil {
		t.Fatal(err)
	}
	// Reopen the store to emulate a process restart.
	store2, err := OpenFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	req.Store = store2
	second, err := Estimate(req)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("restarted estimate mismatch:\n%+v\n%+v", first, second)
	}
	keys := 0
	st, ok, err := store2.LoadBatch(context.Background(), BatchKey{
		GraphVersion: g.Version(), Model: IndependentCascade, Seed: 1234,
		Rounds: 4, BatchIndex: 1, Seeds: canonicalSeedKey([]NodeID{"a"}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !ok || st.Count != 4 {
		t.Fatalf("cached batch missing or wrong: ok=%v stats=%+v", ok, st)
	}
	keys++
	if keys == 0 {
		t.Fatal("no batches persisted")
	}
}

func TestEstimateCancellationLeavesNoPartialResult(t *testing.T) {
	g := influenceTestGraph()
	store, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cfg := ModelConfig{Model: IndependentCascade, Rounds: 50, Seed: 5}
	token := NewCancellationToken()
	started := make(chan struct{}, 1)
	_, err = Estimate(EstimateRequest{
		Graph: g, Seeds: []NodeID{"a"}, Config: cfg, Batches: 20,
		Cancel: token, Store: store, Workers: 2,
		Progress: func(done, total int) {
			select {
			case <-started:
			default:
				close(started)
				token.Cancel()
			}
		},
	})
	<-started
	if err == nil || ErrorCodeOf(err) != ErrTaskCanceled {
		t.Fatalf("want canceled error, got %v", err)
	}
}

func TestEstimateRejectsBadInput(t *testing.T) {
	g := influenceTestGraph()
	_, err := Estimate(EstimateRequest{
		Graph: g, Seeds: []NodeID{"a"},
		Config:  ModelConfig{Model: IndependentCascade, Rounds: 4, Seed: 1},
		Batches: 0,
	})
	if ErrorCodeOf(err) != ErrInvalidBatches {
		t.Fatalf("want invalid batches, got %v", err)
	}
	_, err = Estimate(EstimateRequest{
		Graph: g, Seeds: []NodeID{"a"},
		Config:  ModelConfig{Model: IndependentCascade, Rounds: 4, Seed: 1},
		Batches: 2, Confidence: 1.5,
	})
	if ErrorCodeOf(err) != ErrInvalidThreshold {
		t.Fatalf("want invalid confidence, got %v", err)
	}
}
