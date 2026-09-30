package influence

import "testing"

func chainSnapshot() *GraphSnapshot {
	g := NewGraph()
	_ = g.AddNode("a")
	_ = g.AddNode("b")
	_ = g.AddNode("c")
	_ = g.AddEdge("a", "b", 1)
	_ = g.AddEdge("b", "c", 1)
	return g.Snapshot()
}

func TestICCascadeProbabilityOne(t *testing.T) {
	snap := chainSnapshot()
	cfg := ModelConfig{Model: ModelIndependentCascade, UseEdgeWeights: true}
	key := rngSeed(42, 0)
	got := simulateRound(snap, cfg, []NodeID{"a"}, key, 0)
	if len(got) != 3 {
		t.Fatalf("influenced = %v, want all 3", got)
	}
}

func TestICCascadeProbabilityZero(t *testing.T) {
	g := NewGraph()
	_ = g.AddNode("a")
	_ = g.AddNode("b")
	_ = g.AddEdge("a", "b", 0)
	snap := g.Snapshot()
	cfg := ModelConfig{Model: ModelIndependentCascade, UseEdgeWeights: true}
	key := rngSeed(1, 0)
	got := simulateRound(snap, cfg, []NodeID{"a"}, key, 0)
	if len(got) != 1 || got[0] != "a" {
		t.Fatalf("influenced = %v, want only seed", got)
	}
}

func TestICDefaultProbability(t *testing.T) {
	// Edge weights ignored; DefaultProbability=1 propagates everywhere.
	snap := chainSnapshot()
	cfg := ModelConfig{Model: ModelIndependentCascade, DefaultProbability: 1}
	got := simulateRound(snap, cfg, []NodeID{"a"}, rngSeed(7, 0), 0)
	if len(got) != 3 {
		t.Fatalf("influenced = %v, want all 3", got)
	}
}

func TestLTThresholdSemantics(t *testing.T) {
	g := NewGraph()
	for _, n := range []NodeID{"a", "b", "c"} {
		_ = g.AddNode(n)
	}
	// c needs both a and b, each weight 0.6; threshold 1.
	_ = g.AddEdge("a", "c", 0.6)
	_ = g.AddEdge("b", "c", 0.6)
	snap := g.Snapshot()

	cfg := ModelConfig{
		Model:      ModelLinearThreshold,
		Thresholds: map[NodeID]float64{"c": 1},
	}
	key := rngSeed(9, 0)
	onlyA := simulateRound(snap, cfg, []NodeID{"a"}, key, 0)
	if len(onlyA) != 1 {
		t.Fatalf("single seed influenced %v, c needs weight 1", onlyA)
	}
	both := simulateRound(snap, cfg, []NodeID{"a", "b"}, key, 0)
	if len(both) != 3 {
		t.Fatalf("both seeds influenced %v, want all", both)
	}
}

func TestDeterminismAcrossRoundsAndSeeds(t *testing.T) {
	snap := chainSnapshot()
	cfg := ModelConfig{Model: ModelIndependentCascade, UseEdgeWeights: true}

	// Same key/round twice -> identical outcome.
	r1 := simulateRound(snap, cfg, []NodeID{"a"}, rngSeed(123, 0), 5)
	r2 := simulateRound(snap, cfg, []NodeID{"a"}, rngSeed(123, 0), 5)
	if len(r1) != len(r2) {
		t.Fatalf("runs differ: %v vs %v", r1, r2)
	}

	// Edge-keyed randomness: adding an extra seed that is already unreachable
	// through another path must not change the draw on edge a->b. Build a
	// graph with an isolated node d and verify a->b decision is independent.
	g2 := NewGraph()
	_ = g2.AddNode("a")
	_ = g2.AddNode("b")
	_ = g2.AddNode("d")
	_ = g2.AddEdge("a", "b", 0.999)
	snap2 := g2.Snapshot()
	k := rngSeed(55, 0)
	x := simulateRound(snap2, cfg, []NodeID{"a"}, k, 0)
	y := simulateRound(snap2, cfg, []NodeID{"d", "a"}, k, 0)
	activatedX := containsNode(x, "b")
	activatedY := containsNode(y, "b")
	if activatedX != activatedY {
		t.Fatalf("edge a->b decision depended on unrelated seed: %v vs %v", x, y)
	}
}

func containsNode(ids []NodeID, want NodeID) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

func TestNormalQuantile(t *testing.T) {
	if q := normalQuantile(0.975); q < 1.95 || q > 1.97 {
		t.Fatalf("z(0.975) = %v, want ~1.96", q)
	}
	if q := normalQuantile(0.5); q > 1e-9 || q < -1e-9 {
		t.Fatalf("z(0.5) = %v, want 0", q)
	}
}
