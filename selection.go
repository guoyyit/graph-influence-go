package influence

import "sort"

// SelectRequest asks for a budget-sized seed set maximizing estimated
// expected spread. Selection is driven exclusively by simulation
// estimates; static graph metrics (degree, centrality, ...) are never
// used as the basis.
type SelectRequest struct {
	Graph     *Graph      `json:"-"`
	Config    ModelConfig `json:"config"`
	Batches   int         `json:"batches"`
	Rounds    int         `json:"rounds"`
	Budget    int         `json:"budget"`
	Candidate []NodeID
	// Confidence for each per-candidate estimate; 0 -> default.
	Confidence float64 `json:"confidence"`
	// Workers bounds concurrent batch execution within each estimate.
	Workers int `json:"workers"`
	// Store caches simulation batches across candidates and restarts.
	Store Store `json:"-"`
	// Cancel cooperatively aborts selection; no partial result is kept.
	Cancel *CancellationToken `json:"-"`
	// Progress reports (selected, budget) after each greedy round.
	Progress func(selected, budget int, pick NodeID, gain float64) `json:"-"`
}

// SelectResult lists chosen seeds and their final estimated spread.
type SelectResult struct {
	Seeds          []NodeID
	Gains          []float64
	ExpectedSpread float64
	GraphVersion   int64
}

// Select runs greedy marginal-gain selection. At every round each
// candidate's spread with the current seed set is estimated by Monte
// Carlo simulation, and the candidate with the largest marginal gain is
// chosen. Candidate iteration order is canonical, so ties are resolved
// deterministically regardless of concurrency or scheduling.
func Select(req SelectRequest) (SelectResult, error) {
	if err := req.Config.Validate(); err != nil {
		return SelectResult{}, err
	}
	if req.Graph == nil || len(req.Graph.nodes) == 0 {
		return SelectResult{}, newError(ErrEmptyGraph, "cannot select on an empty graph")
	}
	if req.Budget <= 0 {
		return SelectResult{}, newError(ErrInvalidBudget, "budget must be positive, got %d", req.Budget)
	}
	if req.Batches <= 0 {
		return SelectResult{}, newError(ErrInvalidBatches, "batches must be positive, got %d", req.Batches)
	}
	cfg := req.Config
	if req.Rounds > 0 {
		cfg.Rounds = req.Rounds
	}

	candidates := seedCanonical(req.Candidate)
	if len(candidates) == 0 {
		candidates = req.Graph.Nodes()
	} else {
		for _, id := range candidates {
			if !req.Graph.HasNode(id) {
				return SelectResult{}, newError(ErrUnknownNode, "candidate node %q is not in the graph", id)
			}
		}
		candidates = uniqueNodes(candidates)
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i] < candidates[j] })
	if req.Budget > len(candidates) {
		return SelectResult{}, newError(ErrInvalidBudget, "budget %d exceeds candidate count %d", req.Budget, len(candidates))
	}

	chosen := make([]NodeID, 0, req.Budget)
	gains := make([]float64, 0, req.Budget)
	chosenSet := make(map[NodeID]struct{}, req.Budget)
	currentSpread := 0.0

	for round := 0; round < req.Budget; round++ {
		if req.Cancel != nil && req.Cancel.Canceled() {
			return SelectResult{}, newError(ErrTaskCanceled, "selection canceled before round %d", round)
		}
		best := NodeID("")
		bestSpread := currentSpread
		bestGain := 0.0
		for _, cand := range candidates {
			if _, taken := chosenSet[cand]; taken {
				continue
			}
			seeds := append(append([]NodeID(nil), chosen...), cand)
			res, err := Estimate(EstimateRequest{
				Graph:      req.Graph,
				Seeds:      seeds,
				Config:     cfg,
				Batches:    req.Batches,
				Confidence: req.Confidence,
				Workers:    req.Workers,
				Store:      req.Store,
				Cancel:     req.Cancel,
			})
			if err != nil {
				return SelectResult{}, err
			}
			gain := res.Mean - currentSpread
			// Strictly greater keeps the lexicographically smallest
			// candidate on ties (candidates are sorted), giving stable
			// output across runs and concurrency levels.
			if gain > bestGain {
				bestGain = gain
				best = cand
				bestSpread = res.Mean
			}
		}
		if best == "" {
			break
		}
		chosen = append(chosen, best)
		gains = append(gains, bestGain)
		chosenSet[best] = struct{}{}
		currentSpread = bestSpread
		if req.Progress != nil {
			req.Progress(len(chosen), req.Budget, best, bestGain)
		}
	}

	return SelectResult{
		Seeds:          chosen,
		Gains:          gains,
		ExpectedSpread: currentSpread,
		GraphVersion:   req.Graph.version,
	}, nil
}

// CancellationToken allows cooperative cancellation of long tasks.
type CancellationToken struct {
	ch chan struct{}
}

func NewCancellationToken() *CancellationToken {
	return &CancellationToken{ch: make(chan struct{})}
}

// Cancel is idempotent and safe for concurrent use.
func (c *CancellationToken) Cancel() {
	if c == nil {
		return
	}
	select {
	case <-c.ch:
	default:
		close(c.ch)
	}
}

func (c *CancellationToken) Canceled() bool {
	if c == nil {
		return false
	}
	select {
	case <-c.ch:
		return true
	default:
		return false
	}
}
