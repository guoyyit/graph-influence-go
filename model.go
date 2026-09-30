package influence

import (
	"math/rand/v2"
)

// ModelKind selects the propagation semantics.
type ModelKind string

const (
	// IndependentCascade: each newly active node u gets exactly one
	// attempt per still-inactive neighbor v, which succeeds with edge
	// probability weight(u,v). Successful attempts activate v in the next
	// wave; failed edges are never retried.
	IndependentCascade ModelKind = "independent_cascade"
	// LinearThreshold: each node samples one uniform threshold in [0,1)
	// before propagation; it activates as soon as the total weight of its
	// active in-neighbors reaches the threshold.
	LinearThreshold ModelKind = "linear_threshold"
)

// ModelConfig configures a propagation simulation.
type ModelConfig struct {
	Model  ModelKind
	Rounds int
	// Seed seeds the deterministic random stream. Equal (graph, seeds,
	// config) inputs produce bit-identical activation sets across runs
	// and across process restarts.
	Seed int64
	// FixedThreshold, when in (0,1], is used as every node's threshold
	// instead of drawing one per node (linear threshold model only).
	FixedThreshold float64
}

// Validate checks model parameters against their legal ranges.
func (c ModelConfig) Validate() error {
	switch c.Model {
	case IndependentCascade, LinearThreshold:
	default:
		return newError(ErrUnknownModel, "unknown propagation model %q", c.Model)
	}
	if c.Rounds <= 0 {
		return newError(ErrInvalidRounds, "rounds must be positive, got %d", c.Rounds)
	}
	if c.FixedThreshold != 0 {
		if c.FixedThreshold <= 0 || c.FixedThreshold > 1 {
			return newError(ErrInvalidThreshold, "fixed threshold %v out of range (0,1]", c.FixedThreshold)
		}
	}
	return nil
}

// seedCanonical renders a seed set into a stable string so that equal
// sets given in different orders produce identical random streams.
func seedCanonical(seeds []NodeID) []NodeID {
	out := append([]NodeID(nil), seeds...)
	sortNodes(out)
	return out
}

// streamSeed derives a stable uint64 stream seed from the config seed
// and the canonical seed-set contents.
func streamSeed(base int64, seeds []NodeID) uint64 {
	h := mix64(uint64(base))
	for _, id := range seeds {
		h = mix64(h ^ hashString(string(id)))
	}
	return h
}

// Simulate runs a single deterministic propagation pass from seeds and
// returns the full activated set (including the seeds).
func Simulate(g *Graph, seeds []NodeID, cfg ModelConfig) ([]NodeID, error) {
	return simulate(g, seeds, cfg, 0, 0)
}

// simulate runs one pass whose random stream is derived from
// (cfg.Seed, seeds, batchIndex, roundIndex), so concurrent batches never
// share or interleave a random source and remain bit-deterministic.
func simulate(g *Graph, seeds []NodeID, cfg ModelConfig, batchIndex, roundIndex int) ([]NodeID, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if g == nil || len(g.nodes) == 0 {
		return nil, newError(ErrEmptyGraph, "cannot simulate on an empty graph")
	}
	if len(seeds) == 0 {
		return nil, newError(ErrUnknownNode, "seed set must not be empty")
	}
	ordered := seedCanonical(seeds)
	for _, id := range ordered {
		if !g.HasNode(id) {
			return nil, newError(ErrUnknownNode, "seed node %q is not in the graph", id)
		}
	}
	s0 := streamSeed(cfg.Seed, ordered)
	s0 = mix64(s0 ^ hashString(string(cfg.Model)))
	if cfg.FixedThreshold != 0 {
		s0 = mix64(s0 ^ hashString(floatKey(cfg.FixedThreshold)))
	}
	s1 := mix64(mix64(uint64(batchIndex)+1) ^ uint64(roundIndex) + 1)
	rng := rand.New(rand.NewPCG(mix64(s0^s1), mix64(s0+s1)))
	active := make(map[NodeID]struct{}, len(ordered))
	for _, id := range ordered {
		active[id] = struct{}{}
	}
	switch cfg.Model {
	case IndependentCascade:
		simulateIC(g, ordered, active, rng)
	case LinearThreshold:
		simulateLT(g, ordered, active, rng, cfg.FixedThreshold)
	}
	out := make([]NodeID, 0, len(active))
	for id := range active {
		out = append(out, id)
	}
	sortNodes(out)
	return out, nil
}

func simulateIC(g *Graph, frontier []NodeID, active map[NodeID]struct{}, rng *rand.Rand) {
	for len(frontier) > 0 {
		var next []NodeID
		for _, u := range frontier {
			for _, e := range g.OutEdges(u) {
				if _, on := active[e.To]; on {
					continue
				}
				if rng.Float64() < e.Weight {
					active[e.To] = struct{}{}
					next = append(next, e.To)
				}
			}
		}
		if len(next) == 0 {
			return
		}
		sortNodes(next)
		frontier = uniqueNodes(next)
	}
}

func simulateLT(g *Graph, seeds []NodeID, active map[NodeID]struct{}, rng *rand.Rand, fixed float64) {
	thresholds := make(map[NodeID]float64, len(g.nodes))
	for _, id := range g.Nodes() {
		if fixed > 0 {
			thresholds[id] = fixed
		} else {
			thresholds[id] = rng.Float64()
		}
	}
	influence := make(map[NodeID]float64, len(g.nodes))
	frontier := append([]NodeID(nil), seeds...)
	for len(frontier) > 0 {
		activated := ltpush(g, frontier, active, influence, thresholds)
		if len(activated) == 0 {
			return
		}
		sortNodes(activated)
		frontier = uniqueNodes(activated)
	}
}

// ltpush adds the outgoing weight of every newly active node to the
// accumulated influence of its neighbors and activates those crossing
// their sampled threshold.
func ltpush(g *Graph, frontier []NodeID, active map[NodeID]struct{}, influence, thresholds map[NodeID]float64) []NodeID {
	var newly []NodeID
	for _, u := range frontier {
		for _, e := range g.OutEdges(u) {
			if _, on := active[e.To]; on {
				continue
			}
			influence[e.To] += e.Weight
			if influence[e.To] >= thresholds[e.To] {
				active[e.To] = struct{}{}
				newly = append(newly, e.To)
			}
		}
	}
	return newly
}

func uniqueNodes(in []NodeID) []NodeID {
	seen := make(map[NodeID]struct{}, len(in))
	out := in[:0]
	for _, id := range in {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}
