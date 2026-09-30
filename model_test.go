package influence

import (
	"errors"
	"testing"
)

func TestModelValidation(t *testing.T) {
	g := NewGraph()
	_ = g.AddNode("a")
	_ = g.AddNode("b")
	_ = g.AddEdge("a", "b", 1.5)
	snap := g.Snapshot()

	// IC with edge weights > 1 rejected as out-of-range probability.
	ic := ModelConfig{Model: ModelIndependentCascade, UseEdgeWeights: true}
	if err := ic.validate(snap); !errors.Is(err, ErrProbabilityOutOfRange) {
		t.Fatalf("IC heavy edge err = %v, want ErrProbabilityOutOfRange", err)
	}

	// LT accepts influence weights > 1.
	lt := ModelConfig{Model: ModelLinearThreshold}
	if err := lt.validate(snap); err != nil {
		t.Fatalf("LT heavy edge rejected: %v", err)
	}

	// IC using global default ignores edge weights as probabilities.
	icDefault := ModelConfig{Model: ModelIndependentCascade, DefaultProbability: 0.5}
	if err := icDefault.validate(snap); err != nil {
		t.Fatalf("IC default prob rejected: %v", err)
	}

	// Out-of-range default probability.
	bad := ModelConfig{Model: ModelIndependentCascade, DefaultProbability: 1.1}
	if err := bad.validate(snap); !errors.Is(err, ErrProbabilityOutOfRange) {
		t.Fatalf("err = %v", err)
	}

	// Unknown model.
	if err := (ModelConfig{Model: "weird"}).validate(snap); !errors.Is(err, ErrUnknownModel) {
		t.Fatalf("err = %v", err)
	}
}
