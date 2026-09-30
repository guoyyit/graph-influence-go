package influence

import (
	"context"
	"math"
	"sort"
	"sync"
)

// DefaultConfidence is used when the caller does not request a level.
const DefaultConfidence = 0.95

// EstimateRequest asks for the expected influence of a fixed seed set.
type EstimateRequest struct {
	Graph  *Graph      `json:"-"`
	Seeds  []NodeID    `json:"seeds"`
	Config ModelConfig `json:"config"`
	// Batches is the number of Monte Carlo batches. Each batch runs
	// Config.Rounds independent simulations; batches are the unit of
	// caching, concurrency and crash recovery.
	Batches int
	// Confidence is in (0,1); zero means DefaultConfidence.
	Confidence float64 `json:"confidence"`
	// Workers bounds concurrent in-process batch execution. <=0 runs
	// batches serially, which keeps memory and CPU usage bounded.
	Workers int `json:"workers"`
	// Store optionally caches completed batches; equal BatchKey values
	// are never recomputed (also across restarts).
	Store Store `json:"-"`
	// Cancel optionally aborts a running estimate cooperatively.
	Cancel *CancellationToken `json:"-"`
	// Progress is invoked after each completed batch (best effort).
	Progress func(done, total int) `json:"-"`
}

// EstimateResult is the deterministic influence estimate.
type EstimateResult struct {
	Mean          float64
	StdDev        float64
	Confidence    float64
	MarginOfError float64
	Rounds        int
	Batches       int
	Samples       int
	GraphVersion  int64
}

// Estimate estimates expected influence purely from simulation results.
// Determinism holds across concurrency levels and process restarts:
// random streams are derived per (batch, round), and aggregate order is
// canonical, never map- or schedule-dependent.
func Estimate(req EstimateRequest) (EstimateResult, error) {
	if err := req.Config.Validate(); err != nil {
		return EstimateResult{}, err
	}
	if req.Graph == nil || len(req.Graph.nodes) == 0 {
		return EstimateResult{}, newError(ErrEmptyGraph, "cannot estimate on an empty graph")
	}
	if len(req.Seeds) == 0 {
		return EstimateResult{}, newError(ErrUnknownNode, "seed set must not be empty")
	}
	ordered := seedCanonical(req.Seeds)
	for _, id := range ordered {
		if !req.Graph.HasNode(id) {
			return EstimateResult{}, newError(ErrUnknownNode, "seed node %q is not in the graph", id)
		}
	}
	if req.Batches <= 0 {
		return EstimateResult{}, newError(ErrInvalidBatches, "batches must be positive, got %d", req.Batches)
	}
	confidence := req.Confidence
	if confidence == 0 {
		confidence = DefaultConfidence
	}
	if confidence <= 0 || confidence >= 1 {
		return EstimateResult{}, newError(ErrInvalidThreshold, "confidence %v out of range (0,1)", confidence)
	}

	seedKey := canonicalSeedKey(ordered)
	type batchWork struct{ index int }
	work := make([]batchWork, 0, req.Batches)
	stats := make([]BatchStats, req.Batches)
	for i := 0; i < req.Batches; i++ {
		key := BatchKey{
			GraphVersion:   req.Graph.version,
			Model:          req.Config.Model,
			Seed:           req.Config.Seed,
			Rounds:         req.Config.Rounds,
			BatchIndex:     i,
			Seeds:          seedKey,
			FixedThreshold: req.Config.FixedThreshold,
		}
		if req.Store != nil {
			if cached, ok, err := req.Store.LoadBatch(context.Background(), key); err != nil {
				return EstimateResult{}, err
			} else if ok {
				stats[i] = cached
				continue
			}
		}
		work = append(work, batchWork{i})
	}

	runOne := func(idx int) (BatchStats, error) {
		bs := BatchStats{Count: req.Config.Rounds}
		for r := 0; r < req.Config.Rounds; r++ {
			activated, err := simulate(req.Graph, ordered, req.Config, idx, r)
			if err != nil {
				return BatchStats{}, err
			}
			x := float64(len(activated))
			bs.Mean += x
			bs.SumSq += x * x
		}
		return bs, nil
	}

	var mu sync.Mutex
	var firstErr error
	var completed int
	setErr := func(err error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		mu.Unlock()
	}
	process := func(idx int, bs BatchStats) {
		mu.Lock()
		stats[idx] = bs
		completed++
		done, total := completed, req.Batches
		cb := req.Progress
		mu.Unlock()
		if cb != nil {
			cb(done, total)
		}
	}

	workers := req.Workers
	if workers <= 0 {
		workers = 1
	}
	if workers > len(work) {
		workers = len(work)
	}
	if workers <= 1 {
		for _, w := range work {
			if req.Cancel != nil && req.Cancel.Canceled() {
				return EstimateResult{}, newError(ErrTaskCanceled, "estimate canceled before batch %d", w.index)
			}
			bs, err := runOne(w.index)
			if err != nil {
				return EstimateResult{}, err
			}
			if err := saveBatchIf(req.Store, batchKeyFor(req, seedKey, w.index), bs); err != nil {
				return EstimateResult{}, err
			}
			process(w.index, bs)
		}
	} else {
		var wg sync.WaitGroup
		ch := make(chan int, len(work))
		for _, w := range work {
			ch <- w.index
		}
		close(ch)
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for idx := range ch {
					mu.Lock()
					stop := firstErr != nil
					mu.Unlock()
					if stop {
						return
					}
					if req.Cancel != nil && req.Cancel.Canceled() {
						setErr(newError(ErrTaskCanceled, "estimate canceled during batch %d", idx))
						return
					}
					bs, err := runOne(idx)
					if err != nil {
						setErr(err)
						return
					}
					if err := saveBatchIf(req.Store, batchKeyFor(req, seedKey, idx), bs); err != nil {
						setErr(err)
						return
					}
					process(idx, bs)
				}
			}()
		}
		wg.Wait()
		if firstErr != nil {
			return EstimateResult{}, firstErr
		}
	}

	totalN := req.Batches * req.Config.Rounds
	var grandSum, grandSumSq float64
	for _, bs := range stats {
		grandSum += bs.Mean
		grandSumSq += bs.SumSq
	}
	mean := grandSum / float64(totalN)
	variance := 0.0
	if totalN > 1 {
		variance = (grandSumSq - grandSum*grandSum/float64(totalN)) / float64(totalN-1)
		if variance < 0 {
			variance = 0
		}
	}
	stdDev := math.Sqrt(variance)
	se := 0.0
	if totalN > 1 {
		se = stdDev / math.Sqrt(float64(totalN))
	}
	margin := 0.0
	if totalN > 1 {
		margin = tCritical(confidence, totalN-1) * se
	}
	return EstimateResult{
		Mean:          mean,
		StdDev:        stdDev,
		Confidence:    confidence,
		MarginOfError: margin,
		Rounds:        req.Config.Rounds,
		Batches:       req.Batches,
		Samples:       totalN,
		GraphVersion:  req.Graph.version,
	}, nil
}

func batchKeyFor(req EstimateRequest, seedKey string, idx int) BatchKey {
	return BatchKey{
		GraphVersion:   req.Graph.version,
		Model:          req.Config.Model,
		Seed:           req.Config.Seed,
		Rounds:         req.Config.Rounds,
		BatchIndex:     idx,
		Seeds:          seedKey,
		FixedThreshold: req.Config.FixedThreshold,
	}
}

func saveBatchIf(s Store, key BatchKey, stats BatchStats) error {
	if s == nil {
		return nil
	}
	return s.SaveBatch(context.Background(), key, stats)
}

// canonicalSeedKey renders the seed set as a stable string: sorted,
// length-prefixed ids joined by '|'.
func canonicalSeedKey(seeds []NodeID) string {
	out := append([]NodeID(nil), seeds...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	var b []byte
	for i, id := range out {
		if i > 0 {
			b = append(b, '|')
		}
		b = append(b, []byte(id)...)
	}
	return string(b)
}
