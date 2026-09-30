package influence

import (
	"errors"
	"testing"
)

func TestNewGraphValidationErrors(t *testing.T) {
	cases := []struct {
		name  string
		nodes []NodeID
		edges []Edge
		code  ErrorCode
	}{
		{"empty graph", nil, nil, ErrEmptyGraph},
		{"self loop", []NodeID{"a", "b"}, []Edge{{"a", "a", 0.5}}, ErrSelfLoop},
		{"weight too high", []NodeID{"a", "b"}, []Edge{{"a", "b", 1.1}}, ErrInvalidWeight},
		{"weight negative", []NodeID{"a", "b"}, []Edge{{"a", "b", -0.1}}, ErrInvalidWeight},
		{"missing endpoint", []NodeID{"a"}, []Edge{{"a", "b", 0.5}}, ErrMissingEndpoint},
		{"duplicate edge", []NodeID{"a", "b"}, []Edge{{"a", "b", 0.2}, {"a", "b", 0.3}}, ErrDuplicateEdge},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewGraph(tc.nodes, tc.edges)
			if err == nil {
				t.Fatalf("expected error %s", tc.code)
			}
			var ge *GraphError
			if !errors.As(err, &ge) {
				t.Fatalf("expected *GraphError, got %T", err)
			}
			if ge.Code != tc.code {
				t.Fatalf("code = %s, want %s", ge.Code, tc.code)
			}
		})
	}
}

func TestGraphSnapshotIsImmutableAcrossBuilder(t *testing.T) {
	g, err := NewGraph([]NodeID{"a", "b"}, []Edge{{"a", "b", 0.5}})
	if err != nil {
		t.Fatal(err)
	}
	b := NewGraphBuilder(g)
	g2, err := b.AddNode("c")
	if err != nil {
		t.Fatal(err)
	}
	if g.HasNode("c") {
		t.Fatal("old snapshot must not see the new node")
	}
	if !g2.HasNode("c") {
		t.Fatal("new snapshot must contain the new node")
	}
	if g2.Version() != g.Version()+1 {
		t.Fatalf("version = %d, want %d", g2.Version(), g.Version()+1)
	}
	got := g.OutEdges("a")
	if len(got) != 1 || got[0].Weight != 0.5 {
		t.Fatalf("old snapshot edges mutated: %+v", got)
	}
}

func TestBuilderUpdates(t *testing.T) {
	g, _ := NewGraph([]NodeID{"a", "b", "c"}, []Edge{{"a", "b", 0.2}})
	b := NewGraphBuilder(g)

	next, err := b.AddEdge(Edge{"b", "c", 0.4})
	if err != nil {
		t.Fatal(err)
	}
	if e, ok := next.Edge("b", "c"); !ok || e.Weight != 0.4 {
		t.Fatal("edge not added")
	}
	next, err = b.SetWeight("b", "c", 0.9)
	if err != nil {
		t.Fatal(err)
	}
	if e, _ := next.Edge("b", "c"); e.Weight != 0.9 {
		t.Fatalf("weight = %v", e.Weight)
	}
	next, err = b.RemoveEdge("a", "b")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := next.Edge("a", "b"); ok {
		t.Fatal("edge not removed")
	}
	next, err = b.RemoveNode("c")
	if err != nil {
		t.Fatal(err)
	}
	if next.HasNode("c") {
		t.Fatal("node not removed")
	}
	if _, ok := next.Edge("b", "c"); ok {
		t.Fatal("incident edge not removed with node")
	}

	if _, err := b.AddEdge(Edge{"a", "zzz", 0.1}); ErrorCodeOf(err) != ErrMissingEndpoint {
		t.Fatalf("want missing endpoint, got %v", err)
	}
	if _, err := b.SetWeight("a", "b", 0.5); ErrorCodeOf(err) != ErrUnknownNode {
		t.Fatalf("want unknown edge, got %v", err)
	}
}

func TestNodeAndEdgeOrderingStable(t *testing.T) {
	g, _ := NewGraph([]NodeID{"c", "a", "b"}, []Edge{
		{"a", "c", 0.1},
		{"a", "b", 0.2},
	})
	nodes := g.Nodes()
	if nodes[0] != "a" || nodes[1] != "b" || nodes[2] != "c" {
		t.Fatalf("unstable node order: %v", nodes)
	}
	out := g.OutEdges("a")
	if out[0].To != "b" || out[1].To != "c" {
		t.Fatalf("unstable edge order: %+v", out)
	}
}
