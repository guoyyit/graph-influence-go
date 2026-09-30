package influence

import (
	"context"
	"errors"
	"testing"
)

// twoCommunities builds a graph with two dense clusters joined by one weak
// bridge. Simulation-based selection should pick one seed per cluster rather
// than a high-degree node centrality ranking would.
func twoCommunities() *GraphSnapshot {
	g := NewGraph()
	nodes := []NodeID{"x1", "x2", "x3", "y1", "y2", "y3"}
	for _, n := range nodes {
		_ = g.AddNode(n)
	}
	cluster := func(prefix NodeID) {
		for _, a := range nodes {
			for _, b := range nodes {
				if a == b || a[0] != prefix[0] || b[0] != prefix[0] {
					continue
				}
				if a < b {
					_ = g.AddEdge(a, b, 0.9)
					_ = g.AddEdge(b, a, 0.9)
				}
			}
		}
	}
	cluster("x")
	cluster("y")
	_ = g.AddEdge("x1", "y1", 0.05)
	return g.Snapshot()
}

func TestSelectionDeterministic(t *testing.T) {
	snap := twoCommunities()
	cfg := SelectionConfig{
		Model:      ModelConfig{Model: ModelIndependentCascade, UseEdgeWeights: true},
		K:          2,
		Rounds:     40,
		BatchSize:  10,
		Seed:       2024,
		Confidence: 0.95,
		Candidates: snap.Nodes(),
	}
	r1, err := runSelection(context.Background(), snap, cfg, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := runSelection(context.Background(), snap, cfg, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(r1.Seeds) != 2 {
		t.Fatalf("seeds = %v", r1.Seeds)
	}
	if !equalNodeIDs(r1.Seeds, r2.Seeds) || r1.Spread != r2.Spread {
		t.Fatalf("non-deterministic selection:\n%+v\n%+v", r1, r2)
	}
	// Two clusters -> one seed from each.
	if r1.Seeds[0][0] == r1.Seeds[1][0] {
		t.Fatalf("expected one seed per cluster, got %v", r1.Seeds)
	}
}

func TestSelectionBatchDeduplication(t *testing.T) {
	snap := twoCommunities()
	cfg := SelectionConfig{
		Model:      ModelConfig{Model: ModelIndependentCascade, UseEdgeWeights: true},
		K:          1,
		Rounds:     20,
		BatchSize:  5,
		Seed:       7,
		Confidence: 0.95,
		Candidates: snap.Nodes(),
	}
	seen := make(map[int]int)
	progress := func(b BatchStats) { seen[b.Index]++ }
	r, err := runSelection(context.Background(), snap, cfg, nil, progress, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Seeds) != 1 {
		t.Fatalf("seeds = %v", r.Seeds)
	}
	for idx, n := range seen {
		if n > 1 {
			t.Fatalf("batch %d reported %d times", idx, n)
		}
	}
}

func TestSelectionInvalidInputs(t *testing.T) {
	snap := chainSnapshot()
	valid := ModelConfig{Model: ModelIndependentCascade, UseEdgeWeights: true}

	base := SelectionConfig{
		Model:      valid,
		K:          1,
		Rounds:     4,
		BatchSize:  2,
		Confidence: 0.95,
		Candidates: []NodeID{"a", "b"},
	}
	cases := []struct {
		name string
		mod  func(*SelectionConfig)
		want error
	}{
		{"k zero", func(c *SelectionConfig) { c.K = 0 }, ErrInvalidSeedCount},
		{"k too big", func(c *SelectionConfig) { c.K = 5 }, ErrSeedCountExceedsNodes},
		{"no candidates", func(c *SelectionConfig) { c.Candidates = nil }, ErrNoCandidates},
		{"unknown candidate", func(c *SelectionConfig) { c.Candidates = []NodeID{"a", "zz"} }, ErrSeedNotFound},
		{"bad rounds", func(c *SelectionConfig) { c.Rounds = 0 }, ErrInvalidRounds},
		{"bad batch", func(c *SelectionConfig) { c.BatchSize = 0 }, ErrInvalidBatchSize},
		{"bad confidence", func(c *SelectionConfig) { c.Confidence = 1.2 }, ErrInvalidConfidence},
		{"unknown model", func(c *SelectionConfig) { c.Model = ModelConfig{Model: "m"} }, ErrUnknownModel},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			tc.mod(&cfg)
			_, err := runSelection(context.Background(), snap, cfg, nil, nil, nil)
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

func equalNodeIDs(a, b []NodeID) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
