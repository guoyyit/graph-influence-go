package influence

import (
	"context"
	"testing"
)

// TestRegistryResumesFromPersistedBatches verifies that a freshly built
// registry seeded with persisted batches yields an identical estimate without
// recomputing them.
func TestRegistryResumesFromPersistedBatches(t *testing.T) {
	snap := func() *GraphSnapshot {
		g := NewGraph()
		_ = g.AddNode("a")
		_ = g.AddNode("b")
		_ = g.AddEdge("a", "b", 0.5)
		return g.Snapshot()
	}()
	cfg := EstimateConfig{
		Model:      ModelConfig{Model: ModelIndependentCascade, UseEdgeWeights: true},
		Seeds:      []NodeID{"a"},
		Rounds:     50,
		BatchSize:  10,
		Seed:       88,
		Confidence: 0.95,
	}

	// Full reference run.
	full, err := runEstimation(context.Background(), snap, cfg, nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Simulate first "process": capture the first 3 persisted batches.
	var firstBatches []BatchStats
	reg := newBatchRegistry(snap, cfg.Seed)
	spec := estimationSpec{model: cfg.Model, seeds: uniqueSortedSeeds(cfg.Seeds)}
	_, err = reg.evaluate(context.Background(), spec, 0, 3, cfg.BatchSize, cfg.Rounds,
		func(b BatchStats) { firstBatches = append(firstBatches, b) }, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Simulate restart: a new registry seeded from disk.
	resumed := runEstimationFromBatches(t, snap, cfg, firstBatches)
	if resumed != full {
		t.Fatalf("resumed estimate differs:\n%+v\n%+v", resumed, full)
	}
}

func runEstimationFromBatches(t *testing.T, snap *GraphSnapshot, cfg EstimateConfig, batches []BatchStats) EstimateResult {
	t.Helper()
	res, err := runEstimation(context.Background(), snap, cfg, batches, nil)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

// TestStoreRoundTrip verifies graph and task records survive a store reopen.
func TestStoreRoundTrip(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	g := NewGraph()
	_ = g.AddNode("a")
	_ = g.AddNode("b")
	_ = g.AddEdge("a", "b", 0.25)
	snap := g.Snapshot()
	if err := st.SaveGraph(snap); err != nil {
		t.Fatal(err)
	}

	rec := taskRecord{
		State: TaskState{ID: "t1", Kind: TaskEstimation, Status: StatusRunning},
		EstimateCfg: &EstimateConfig{
			Model:      ModelConfig{Model: ModelIndependentCascade, UseEdgeWeights: true},
			Seeds:      []NodeID{"a"},
			Rounds:     8,
			BatchSize:  4,
			Confidence: .9,
		},
		Batches: []BatchStats{{Index: 0, SpecKey: "k", Position: 0, Rounds: 4, Sum: 10, SumSq: 28}},
	}
	if err := st.SaveTask(rec); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st2, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	g2, err := st2.LoadGraph()
	if err != nil {
		t.Fatal(err)
	}
	snap2 := g2.Snapshot()
	if snap2.NodeCount() != 2 || snap2.EdgeCount() != 1 {
		t.Fatalf("graph not restored: %+v", snap2)
	}
	if snap2.Hash() != snap.Hash() {
		t.Fatal("graph hash changed across store round trip")
	}
	rec2, err := st2.LoadTask("t1")
	if err != nil {
		t.Fatal(err)
	}
	if rec2.State.Status != StatusRunning || len(rec2.Batches) != 1 || rec2.Batches[0].Sum != 10 {
		t.Fatalf("task not restored: %+v", rec2)
	}
}

// TestGraphVersionSurvivesRestart verifies the monotone version is restored
// even when node additions/removals change the node count.
func TestGraphVersionSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	g := NewGraph()
	_ = g.AddNode("a")
	_ = g.AddNode("b")
	_ = g.RemoveNode("b") // version 3, one node remains
	if err := st.SaveGraph(g.Snapshot()); err != nil {
		t.Fatal(err)
	}
	_ = st.Close()

	st2, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	g2, err := st2.LoadGraph()
	if err != nil {
		t.Fatal(err)
	}
	if g2.Version() != 3 {
		t.Fatalf("restored version = %d, want 3", g2.Version())
	}
}
