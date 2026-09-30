package influence

import (
	"errors"
	"testing"
)

func TestGraphMutationsAndVersion(t *testing.T) {
	g := NewGraph()
	if g.Version() != 0 {
		t.Fatalf("empty version = %d, want 0", g.Version())
	}
	if err := g.AddNode("a"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddNode("b"); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge("a", "b", 0.5); err != nil {
		t.Fatal(err)
	}
	if g.Version() != 3 {
		t.Fatalf("version = %d, want 3", g.Version())
	}
	snap := g.Snapshot()
	if snap.NodeCount() != 2 || snap.EdgeCount() != 1 {
		t.Fatalf("counts = %d/%d", snap.NodeCount(), snap.EdgeCount())
	}
	if e, ok := snap.Edge("a", "b"); !ok || e.Weight != 0.5 {
		t.Fatalf("edge lookup = %+v %v", e, ok)
	}
	if got := snap.OutEdges("a"); len(got) != 1 || got[0].To != "b" {
		t.Fatalf("out edges = %+v", got)
	}
	if got := snap.InEdges("b"); len(got) != 1 || got[0].From != "a" {
		t.Fatalf("in edges = %+v", got)
	}
}

func TestSnapshotImmuneToMutations(t *testing.T) {
	g := NewGraph()
	_ = g.AddNode("a")
	_ = g.AddNode("b")
	_ = g.AddEdge("a", "b", 0.5)
	before := g.Snapshot()

	_ = g.AddNode("c")
	_ = g.AddEdge("c", "a", 0.2)
	_ = g.SetEdgeWeight("a", "b", 0.9)

	if before.NodeCount() != 2 || before.EdgeCount() != 1 {
		t.Fatalf("snapshot mutated: %+v", before)
	}
	if e, _ := before.Edge("a", "b"); e.Weight != 0.5 {
		t.Fatalf("snapshot edge weight changed to %v", e.Weight)
	}
	if _, ok := before.Edge("c", "a"); ok {
		t.Fatal("snapshot gained new edge")
	}
	if g.Snapshot().Version() != before.Version()+3 {
		t.Fatal("version not bumped after update")
	}
}

func TestHashStable(t *testing.T) {
	build := func() *GraphSnapshot {
		g := NewGraph()
		for _, n := range []NodeID{"a", "b", "c"} {
			_ = g.AddNode(n)
		}
		_ = g.AddEdge("a", "b", 0.3)
		_ = g.AddEdge("b", "c", 0.7)
		return g.Snapshot()
	}
	h1 := build().Hash()
	h2 := build().Hash()
	if h1 != h2 || h1 == "" {
		t.Fatalf("content hash not stable: %q vs %q", h1, h2)
	}
}

func TestGraphInvalidInputs(t *testing.T) {
	g := NewGraph()
	_ = g.AddNode("a")

	cases := []struct {
		name string
		do   func() error
		want error
	}{
		{"empty node", func() error { return g.AddNode("") }, ErrEmptyNodeID},
		{"duplicate node", func() error { return g.AddNode("a") }, ErrNodeExists},
		{"remove missing", func() error { return g.RemoveNode("zz") }, ErrNodeNotFound},
		{"self loop", func() error { return g.AddEdge("a", "a", 0.5) }, ErrSelfLoop},
		{"negative weight", func() error {
			_ = g.AddNode("b")
			return g.AddEdge("a", "b", -0.1)
		}, ErrWeightOutOfRange},
		{"missing endpoint from", func() error {
			return g.AddEdge("x", "a", 0.5)
		}, ErrNodeNotFound},
		{"duplicate edge", func() error {
			_ = g.AddNode("b")
			_ = g.AddEdge("a", "b", 0.5)
			return g.AddEdge("a", "b", 0.2)
		}, ErrDuplicateEdge},
		{"remove missing edge", func() error {
			_ = g.AddNode("c")
			return g.RemoveEdge("a", "c")
		}, ErrEdgeNotFound},
		{"set missing edge", func() error {
			_ = g.AddNode("c")
			return g.SetEdgeWeight("a", "c", 0.5)
		}, ErrEdgeNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.do()
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want wrap of %v", err, tc.want)
			}
		})
	}
}

func TestRemoveNodeCascadesEdges(t *testing.T) {
	g := NewGraph()
	_ = g.AddNode("a")
	_ = g.AddNode("b")
	_ = g.AddNode("c")
	_ = g.AddEdge("a", "b", 0.5)
	_ = g.AddEdge("b", "c", 0.5)
	_ = g.RemoveNode("b")
	snap := g.Snapshot()
	if snap.EdgeCount() != 0 {
		t.Fatalf("edges remain after node removal: %d", snap.EdgeCount())
	}
	if len(snap.InEdges("a")) != 0 || len(snap.OutEdges("c")) != 0 {
		t.Fatal("incident edges not removed")
	}
}
