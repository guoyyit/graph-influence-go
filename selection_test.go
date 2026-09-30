package influence

import "testing"

func TestSelectDeterministicAndSimulationBased(t *testing.T) {
	g := influenceTestGraph()
	cfg := ModelConfig{Model: IndependentCascade, Rounds: 20, Seed: 2024}
	req := SelectRequest{
		Graph: g, Config: cfg, Batches: 4, Budget: 2,
		Candidate: []NodeID{"a", "b", "c", "e"},
	}
	first, err := Select(req)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Seeds) != 2 {
		t.Fatalf("want 2 seeds, got %v", first.Seeds)
	}
	// "a" dominates by simulation: it reaches b/c and onward.
	if first.Seeds[0] != "a" {
		t.Fatalf("expected strongest sim seed a first, got %v", first.Seeds)
	}
	if first.ExpectedSpread <= 1 {
		t.Fatalf("implausible spread %v", first.ExpectedSpread)
	}
	for i := 0; i < 3; i++ {
		again, err := Select(req)
		if err != nil {
			t.Fatal(err)
		}
		if !sameNodeSet(again.Seeds, first.Seeds) || again.ExpectedSpread != first.ExpectedSpread {
			t.Fatalf("nondeterministic selection:\n%+v\n%+v", first, again)
		}
	}
	// Concurrency inside estimates must not change selection.
	parReq := req
	parReq.Workers = 4
	par, err := Select(parReq)
	if err != nil {
		t.Fatal(err)
	}
	if !sameNodeSet(par.Seeds, first.Seeds) {
		t.Fatalf("parallel selection differs: %v vs %v", par.Seeds, first.Seeds)
	}
}

func TestSelectLTModel(t *testing.T) {
	g, _ := NewGraph([]NodeID{"1", "2", "3", "4", "5"}, []Edge{
		{"1", "3", 0.5},
		{"2", "3", 0.5},
		{"3", "4", 0.7},
		{"4", "5", 0.7},
	})
	cfg := ModelConfig{Model: LinearThreshold, Rounds: 16, Seed: 11, FixedThreshold: 0.5}
	res, err := Select(SelectRequest{
		Graph: g, Config: cfg, Batches: 3, Budget: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Seeds) != 2 {
		t.Fatalf("want 2 seeds, got %v", res.Seeds)
	}
}

func TestSelectRejectsInvalid(t *testing.T) {
	g := influenceTestGraph()
	base := SelectRequest{
		Graph:   g,
		Config:  ModelConfig{Model: IndependentCascade, Rounds: 4, Seed: 1},
		Batches: 2, Budget: 99,
	}
	if _, err := Select(base); ErrorCodeOf(err) != ErrInvalidBudget {
		t.Fatalf("want budget error, got %v", err)
	}
	base.Budget = 1
	base.Candidate = []NodeID{"a", "ghost"}
	if _, err := Select(base); ErrorCodeOf(err) != ErrUnknownNode {
		t.Fatalf("want unknown candidate, got %v", err)
	}
	base.Candidate = nil
	base.Budget = 0
	if _, err := Select(base); ErrorCodeOf(err) != ErrInvalidBudget {
		t.Fatalf("want zero budget error, got %v", err)
	}
	base.Budget = 1
	base.Batches = 0
	if _, err := Select(base); ErrorCodeOf(err) != ErrInvalidBatches {
		t.Fatalf("want batches error, got %v", err)
	}
}

func TestSelectCancellation(t *testing.T) {
	g := influenceTestGraph()
	token := NewCancellationToken()
	token.Cancel()
	_, err := Select(SelectRequest{
		Graph:   g,
		Config:  ModelConfig{Model: IndependentCascade, Rounds: 10, Seed: 1},
		Batches: 2,
		Budget:  2,
		Cancel:  token,
	})
	if ErrorCodeOf(err) != ErrTaskCanceled {
		t.Fatalf("want canceled, got %v", err)
	}
}
