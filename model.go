package influence

import "math"

// ModelType selects the propagation semantics.
type ModelType string

const (
	// ModelIndependentCascade is the independent cascade model.
	ModelIndependentCascade ModelType = "independent_cascade"
	// ModelLinearThreshold is the linear threshold model.
	ModelLinearThreshold ModelType = "linear_threshold"
)

// ModelConfig configures propagation semantics.
//
// Independent cascade: each active node attempts each outgoing edge exactly
// once. The attempt succeeds with the edge propagation probability. When
// UseEdgeWeights is false every edge uses DefaultProbability; otherwise the
// edge weight is used as its probability and DefaultProbability is ignored.
//
// Linear threshold: a node activates when the summed weights of its active
// in-neighbours reach its threshold. Nodes present in Thresholds use that
// fixed threshold in [0,1]; every other node draws an independent uniform
// threshold in [0,1] at the start of each simulation round.
type ModelConfig struct {
	Model              ModelType
	UseEdgeWeights     bool
	DefaultProbability float64
	Thresholds         map[NodeID]float64
}

func probabilityInRange(p float64) bool {
	return !math.IsNaN(p) && !math.IsInf(p, 0) && p >= 0 && p <= 1
}

func thresholdInRange(t float64) bool {
	return !math.IsNaN(t) && !math.IsInf(t, 0) && t >= 0 && t <= 1
}

// validate checks model configuration against a pinned graph snapshot and
// returns a classifiable error for the first invalid parameter.
func (c ModelConfig) validate(snap *GraphSnapshot) error {
	switch c.Model {
	case ModelIndependentCascade:
		if !c.UseEdgeWeights && !probabilityInRange(c.DefaultProbability) {
			return &ValueError{Err: ErrProbabilityOutOfRange, Field: "default_probability", Value: c.DefaultProbability}
		}
		if c.UseEdgeWeights {
			for _, n := range snap.nodes {
				for _, e := range snap.out[n] {
					if !probabilityInRange(e.Weight) {
						return &EdgeError{Err: &ValueError{Err: ErrProbabilityOutOfRange, Field: "probability", Value: e.Weight}, From: e.From, To: e.To}
					}
				}
			}
		}
		for node, t := range c.Thresholds {
			if !snap.HasNode(node) {
				return &NodeError{Err: ErrNodeNotFound, Node: node}
			}
			if !thresholdInRange(t) {
				return &NodeError{Err: &ValueError{Err: ErrThresholdOutOfRange, Field: "threshold", Value: t}, Node: node}
			}
		}
		return nil
	case ModelLinearThreshold:
		for node, t := range c.Thresholds {
			if !snap.HasNode(node) {
				return &NodeError{Err: ErrNodeNotFound, Node: node}
			}
			if !thresholdInRange(t) {
				return &NodeError{Err: &ValueError{Err: ErrThresholdOutOfRange, Field: "threshold", Value: t}, Node: node}
			}
		}
		return nil
	default:
		return ErrUnknownModel
	}
}

// edgeProbability returns the propagation probability of an edge under the
// independent cascade model.
func (c ModelConfig) edgeProbability(e Edge) float64 {
	if c.UseEdgeWeights {
		return e.Weight
	}
	return c.DefaultProbability
}
