package influence

import (
	"context"
	"errors"
	"testing"
)

func TestEstimateDeterministic(t *testing.T) {
	snap := func() *GraphSnapshot {
		g := NewGraph()
		_ = g.AddNode("a")
		_ = g.AddNode("b")
		_ = g.AddNode("c")
		_ = g.AddEdge("a", "b", 0.5)
		_ = g.AddEdge("b", "c", 0.5)
		return g.Snapshot()
	}()
	cfg := EstimateConfig{
		Model:      ModelConfig{Model: ModelIndependentCascade, UseEdgeWeights: true},
		Seeds:      []NodeID{"a"},
		Rounds:     64,
		BatchSize:  10,
		Seed:       12345,
		Confidence: 0.95,
	}
	r1, err := runEstimation(context.Background(), snap, cfg, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	r2, err := runEstimation(context.Background(), snap, cfg, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if r1 != r2 {
		t.Fatalf("non-deterministic estimate:\n%+v\n%+v", r1, r2)
	}
	if r1.Rounds != 64 || r1.TotalBatches != 7 || r1.CompletedBatches != 7 {
		t.Fatalf("batch accounting wrong: %+v", r1)
	}
	if r1.Mean < 1 || r1.Mean > 3 {
		t.Fatalf("mean out of range: %v", r1.Mean)
	}
	if !(r1.LowerBound <= r1.Mean && r1.Mean <= r1.UpperBound) {
		t.Fatalf("CI does not contain mean: %+v", r1)
	}
	if r1.MarginOfError <= 0 {
		t.Fatal("margin must be positive for stochastic rounds")
	}
}

func TestEstimateSeedOrderIndependent(t *testing.T) {
	snap := chainSnapshot()
	base := EstimateConfig{
		Model:      ModelConfig{Model: ModelIndependentCascade, UseEdgeWeights: true},
		Rounds:     32,
		BatchSize:  8,
		Seed:       3,
		Confidence: 0.9,
	}
	cfgA := base
	cfgA.Seeds = []NodeID{"a", "b"}
	cfgB := base
	cfgB.Seeds = []NodeID{"b", "a"}
	rA, _ := runEstimation(context.Background(), snap, cfgA, nil, nil)
	rB, _ := runEstimation(context.Background(), snap, cfgB, nil, nil)
	if rA != rB {
		t.Fatalf("seed order changed estimate: %+v vs %+v", rA, rB)
	}
}

func TestEstimateResumeUsesCompletedBatches(t *testing.T) {
	snap := chainSnapshot()
	cfg := EstimateConfig{
		Model:      ModelConfig{Model: ModelIndependentCascade, UseEdgeWeights: true},
		Seeds:      []NodeID{"a"},
		Rounds:     20,
		BatchSize:  5,
		Seed:       99,
		Confidence: 0.95,
	}

	// First run produces deterministic batches.
	var saved []BatchStats
	_, err := runEstimation(context.Background(), snap, cfg, nil, func(b BatchStats) {
		saved = append(saved, b)
	})
	if err != nil {
		t.Fatal(err)
	}

	// Second run fed all batches must report them and produce the same result.
	var reported []BatchStats
	full, err := runEstimation(context.Background(), snap, cfg, saved, func(b BatchStats) {
		reported = append(reported, b)
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(reported) != len(saved) {
		t.Fatalf("resume reported %d batches, want %d", len(reported), len(saved))
	}
	fresh, _ := runEstimation(context.Background(), snap, cfg, nil, nil)
	if full != fresh {
		t.Fatalf("resumed estimate differs: %+v vs %+v", full, fresh)
	}

	// Partial resume: feed first 2 batches, verify identical aggregate.
	partial, err := runEstimation(context.Background(), snap, cfg, saved[:2], nil)
	if err != nil {
		t.Fatal(err)
	}
	if partial != fresh {
		t.Fatalf("partial resume mismatch: %+v vs %+v", partial, fresh)
	}
}

func TestEstimateCancellation(t *testing.T) {
	snap := chainSnapshot()
	cfg := EstimateConfig{
		Model:      ModelConfig{Model: ModelIndependentCascade, UseEdgeWeights: true},
		Seeds:      []NodeID{"a"},
		Rounds:     1_000_000,
		BatchSize:  1,
		Seed:       1,
		Confidence: 0.95,
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := runEstimation(ctx, snap, cfg, nil, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestEstimateInvalidInputs(t *testing.T) {
	snap := chainSnapshot()
	valid := ModelConfig{Model: ModelIndependentCascade, UseEdgeWeights: true}

	cases := []struct {
		name string
		cfg  EstimateConfig
		want error
	}{
		{"empty seeds", EstimateConfig{Model: valid, Rounds: 1, BatchSize: 1, Confidence: .95}, ErrEmptySeedSet},
		{"unknown seed", EstimateConfig{Model: valid, Seeds: []NodeID{"zz"}, Rounds: 1, BatchSize: 1, Confidence: .95}, ErrSeedNotFound},
		{"bad rounds", EstimateConfig{Model: valid, Seeds: []NodeID{"a"}, Rounds: 0, BatchSize: 1, Confidence: .95}, ErrInvalidRounds},
		{"bad batch", EstimateConfig{Model: valid, Seeds: []NodeID{"a"}, Rounds: 1, BatchSize: 0, Confidence: .95}, ErrInvalidBatchSize},
		{"bad confidence high", EstimateConfig{Model: valid, Seeds: []NodeID{"a"}, Rounds: 1, BatchSize: 1, Confidence: 1}, ErrInvalidConfidence},
		{"bad confidence low", EstimateConfig{Model: valid, Seeds: []NodeID{"a"}, Rounds: 1, BatchSize: 1, Confidence: 0}, ErrInvalidConfidence},
		{"unknown model", EstimateConfig{Model: ModelConfig{Model: "x"}, Seeds: []NodeID{"a"}, Rounds: 1, BatchSize: 1, Confidence: .95}, ErrUnknownModel},
		{"bad default prob", EstimateConfig{
			Model: ModelConfig{Model: ModelIndependentCascade, DefaultProbability: 2},
			Seeds: []NodeID{"a"}, Rounds: 1, BatchSize: 1, Confidence: .95,
		}, ErrProbabilityOutOfRange},
		{"bad threshold", EstimateConfig{
			Model: ModelConfig{Model: ModelLinearThreshold, Thresholds: map[NodeID]float64{"a": -1}},
			Seeds: []NodeID{"a"}, Rounds: 1, BatchSize: 1, Confidence: .95,
		}, ErrThresholdOutOfRange},
		{"threshold unknown node", EstimateConfig{
			Model: ModelConfig{Model: ModelLinearThreshold, Thresholds: map[NodeID]float64{"zz": .5}},
			Seeds: []NodeID{"a"}, Rounds: 1, BatchSize: 1, Confidence: .95,
		}, ErrNodeNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := runEstimation(context.Background(), snap, tc.cfg, nil, nil)
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestEstimateEmptyGraph(t *testing.T) {
	snap := NewGraph().Snapshot()
	cfg := EstimateConfig{
		Model:      ModelConfig{Model: ModelIndependentCascade, DefaultProbability: .5},
		Seeds:      []NodeID{"a"},
		Rounds:     1,
		BatchSize:  1,
		Confidence: .95,
	}
	_, err := runEstimation(context.Background(), snap, cfg, nil, nil)
	if !errors.Is(err, ErrEmptyGraph) {
		t.Fatalf("err = %v, want ErrEmptyGraph", err)
	}
}
