package influence

import "context"

// SelectionConfig configures budget-constrained seed selection.
type SelectionConfig struct {
	Model      ModelConfig
	K          int
	Rounds     int
	BatchSize  int
	Seed       int64
	Confidence float64
	Candidates []NodeID
}

// SelectionResult is the selected seed set and its estimated spread.
type SelectionResult struct {
	Seeds         []NodeID  `json:"seeds"`
	MarginalGains []float64 `json:"marginal_gains"`
	Spread        float64   `json:"spread"`
	Rounds        int       `json:"rounds"`
}

// validateSelectionConfig validates a selection request against a snapshot.
func validateSelectionConfig(snap *GraphSnapshot, cfg SelectionConfig) error {
	if snap.NodeCount() == 0 {
		return ErrEmptyGraph
	}
	if cfg.K <= 0 {
		return &ValueError{Err: ErrInvalidSeedCount, Field: "k", Value: float64(cfg.K)}
	}
	if cfg.Rounds <= 0 {
		return &ValueError{Err: ErrInvalidRounds, Field: "rounds", Value: float64(cfg.Rounds)}
	}
	if cfg.BatchSize <= 0 {
		return &ValueError{Err: ErrInvalidBatchSize, Field: "batch_size", Value: float64(cfg.BatchSize)}
	}
	if cfg.Confidence <= 0 || cfg.Confidence >= 1 {
		return &ValueError{Err: ErrInvalidConfidence, Field: "confidence", Value: cfg.Confidence}
	}

	seen := make(map[NodeID]struct{}, len(cfg.Candidates))
	candidates := make([]NodeID, 0, len(cfg.Candidates))
	for _, c := range cfg.Candidates {
		if !snap.HasNode(c) {
			return &NodeError{Err: ErrSeedNotFound, Node: c}
		}
		if _, ok := seen[c]; ok {
			continue
		}
		seen[c] = struct{}{}
		candidates = append(candidates, c)
	}
	if len(candidates) == 0 {
		return ErrNoCandidates
	}
	if cfg.K > len(candidates) {
		return &ValueError{Err: ErrSeedCountExceedsNodes, Field: "k", Value: float64(cfg.K)}
	}
	return cfg.Model.validate(snap)
}

// runSelection selects K seeds with the CELF lazy-greedy algorithm, driven
// entirely by Monte Carlo estimates of the configured propagation model.
func runSelection(ctx context.Context, snap *GraphSnapshot, cfg SelectionConfig,
	completed []BatchStats, progress func(BatchStats), onComplete func()) (SelectionResult, error) {
	if err := validateSelectionConfig(snap, cfg); err != nil {
		return SelectionResult{}, err
	}
	if err := validateBatchBinding(completed, snap.Hash()); err != nil {
		return SelectionResult{}, err
	}

	candidates := normalizeCandidates(cfg.Candidates)
	sortNodeIDs(candidates)

	chosen := make([]NodeID, 0, cfg.K)
	gains := make([]float64, 0, cfg.K)

	// gain evaluates the spread of chosen+node by simulation and returns the
	// marginal gain over the spread of the current chosen set.
	currentSpread := 0.0
	reg := newBatchRegistry(snap, cfg.Seed)
	for _, b := range completed {
		reg.seed(b)
	}
	gain := func(node NodeID) (float64, error) {
		seeds := append(append([]NodeID(nil), chosen...), node)
		all, err := reg.evaluate(ctx, estimationSpec{model: cfg.Model, seeds: uniqueSortedSeeds(seeds)},
			0, batchesFor(cfg.Rounds, cfg.BatchSize), cfg.BatchSize, cfg.Rounds, progress, onComplete)
		if err != nil {
			return 0, err
		}
		res := aggregate(all, cfg.Confidence)
		return res.Mean - currentSpread, nil
	}

	// Initialise the lazy-greedy heap with every candidate evaluated once.
	heap := make([]heapEntry, 0, len(candidates))
	for _, node := range candidates {
		g, err := gain(node)
		if err != nil {
			return SelectionResult{}, err
		}
		heap = append(heap, heapEntry{node: node, gain: g, valid: true})
	}

	for step := 0; step < cfg.K; step++ {
		// CELF: the top entry is acceptable only when its stored gain is still
		// valid (it was computed for the current chosen set). Otherwise refresh
		// the best candidate and reheapify.
		for {
			maxIndex := 0
			for i := 1; i < len(heap); i++ {
				if lessHeapEntry(heap[maxIndex], heap[i]) {
					maxIndex = i
				}
			}
			if heap[maxIndex].valid {
				best := heap[maxIndex]
				heap = append(heap[:maxIndex], heap[maxIndex+1:]...)
				chosen = append(chosen, best.node)
				gains = append(gains, best.gain)
				currentSpread += best.gain
				// All remaining cached gains were computed for a smaller chosen
				// set and become stale upper bounds.
				for i := range heap {
					heap[i].valid = false
				}
				break
			}
			g, err := gain(heap[maxIndex].node)
			if err != nil {
				return SelectionResult{}, err
			}
			heap[maxIndex].gain = g
			heap[maxIndex].valid = true
		}
	}

	return SelectionResult{
		Seeds:         chosen,
		MarginalGains: gains,
		Spread:        currentSpread,
		Rounds:        cfg.Rounds,
	}, nil
}

type heapEntry struct {
	node  NodeID
	gain  float64
	valid bool
}

// lessHeapEntry orders entries by gain, then node ID, giving deterministic
// tie-breaking independent of map iteration.
func lessHeapEntry(a, b heapEntry) bool {
	if a.gain != b.gain {
		return a.gain < b.gain
	}
	return a.node < b.node
}

func normalizeCandidates(in []NodeID) []NodeID {
	seen := make(map[NodeID]struct{}, len(in))
	out := make([]NodeID, 0, len(in))
	for _, c := range in {
		if _, ok := seen[c]; ok {
			continue
		}
		seen[c] = struct{}{}
		out = append(out, c)
	}
	return out
}

func batchesFor(rounds, batchSize int) int {
	return (rounds + batchSize - 1) / batchSize
}
