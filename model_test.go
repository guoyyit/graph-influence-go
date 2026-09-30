package influence

import "testing"

func chainGraph() *Graph {
	g, err := NewGraph([]NodeID{"1", "2", "3", "4"}, []Edge{
		{"1", "2", 1},
		{"2", "3", 1},
		{"3", "4", 1},
	})
	if err != nil {
		panic(err)
	}
	return g
}

func TestSimulateICDeterministicChain(t *testing.T) {
	g := chainGraph()
	cfg := ModelConfig{Model: IndependentCascade, Rounds: 1, Seed: 42}
	first, err := Simulate(g, []NodeID{"1"}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 4 {
		t.Fatalf("weight-1 chain should activate all nodes, got %v", first)
	}
	for i := 0; i < 5; i++ {
		again, _ := Simulate(g, []NodeID{"1"}, cfg)
		if !sameNodeSet(first, again) {
			t.Fatalf("nondeterministic IC result: %v vs %v", first, again)
		}
	}
	// Seed order must not change the result.
	shuffled, _ := Simulate(g, []NodeID{"3", "1"}, cfg)
	ordered, _ := Simulate(g, []NodeID{"1", "3"}, cfg)
	if !sameNodeSet(shuffled, ordered) {
		t.Fatalf("seed-order dependent result: %v vs %v", shuffled, ordered)
	}
}

func TestSimulateICZeroWeight(t *testing.T) {
	g, _ := NewGraph([]NodeID{"a", "b"}, []Edge{{"a", "b", 0}})
	out, err := Simulate(g, []NodeID{"a"}, ModelConfig{Model: IndependentCascade, Rounds: 1, Seed: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 || out[0] != "a" {
		t.Fatalf("zero-weight edge must not propagate, got %v", out)
	}
}

func TestSimulateLTDeterministic(t *testing.T) {
	g, _ := NewGraph([]NodeID{"a", "b", "c"}, []Edge{
		{"a", "c", 0.6},
		{"b", "c", 0.6},
	})
	cfg := ModelConfig{Model: LinearThreshold, Rounds: 1, Seed: 7}
	out1, err := Simulate(g, []NodeID{"a", "b"}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		out2, _ := Simulate(g, []NodeID{"a", "b"}, cfg)
		if !sameNodeSet(out1, out2) {
			t.Fatalf("nondeterministic LT result: %v vs %v", out1, out2)
		}
	}
	// With a fixed threshold of 0.5 both seeds together always activate c.
	out, err := Simulate(g, []NodeID{"a", "b"}, ModelConfig{
		Model: LinearThreshold, Rounds: 1, Seed: 7, FixedThreshold: 0.5,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 3 {
		t.Fatalf("expected all nodes with fixed threshold, got %v", out)
	}
}

func TestSimulateInvalidInput(t *testing.T) {
	g := chainGraph()
	_, err := Simulate(g, []NodeID{"1"}, ModelConfig{Model: "bogus", Rounds: 1, Seed: 1})
	if ErrorCodeOf(err) != ErrUnknownModel {
		t.Fatalf("want unknown model, got %v", err)
	}
	_, err = Simulate(g, []NodeID{"1"}, ModelConfig{Model: IndependentCascade, Rounds: 0, Seed: 1})
	if ErrorCodeOf(err) != ErrInvalidRounds {
		t.Fatalf("want invalid rounds, got %v", err)
	}
	_, err = Simulate(g, nil, ModelConfig{Model: IndependentCascade, Rounds: 1, Seed: 1})
	if ErrorCodeOf(err) != ErrUnknownNode {
		t.Fatalf("want empty seed error, got %v", err)
	}
	_, err = Simulate(g, []NodeID{"x"}, ModelConfig{Model: IndependentCascade, Rounds: 1, Seed: 1})
	if ErrorCodeOf(err) != ErrUnknownNode {
		t.Fatalf("want unknown seed, got %v", err)
	}
	_, err = Simulate(g, []NodeID{"1"}, ModelConfig{
		Model: LinearThreshold, Rounds: 1, Seed: 1, FixedThreshold: 2,
	})
	if ErrorCodeOf(err) != ErrInvalidThreshold {
		t.Fatalf("want invalid threshold, got %v", err)
	}
}

func sameNodeSet(a, b []NodeID) bool {
	if len(a) != len(b) {
		return false
	}
	m := map[NodeID]int{}
	for _, x := range a {
		m[x]++
	}
	for _, x := range b {
		m[x]--
		if m[x] < 0 {
			return false
		}
	}
	return true
}
