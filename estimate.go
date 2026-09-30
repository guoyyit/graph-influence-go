package influence

import (
	"context"
	"math"
	"sort"
)

// EstimateConfig configures influence estimation for a fixed seed set.
type EstimateConfig struct {
	Model      ModelConfig
	Seeds      []NodeID
	Rounds     int
	BatchSize  int
	Seed       int64
	Confidence float64
}

// BatchStats is the aggregate statistics of one deterministic simulation
// batch. Only sums are retained so memory stays bounded.
type BatchStats struct {
	Index    int    `json:"index"`
	SpecKey  string `json:"spec_key"`
	Position int    `json:"position"`
	Rounds   int    `json:"rounds"`
	Sum      int64  `json:"sum"`
	SumSq    int64  `json:"sum_sq"`
}

// EstimateResult is the influence estimate with a confidence interval.
type EstimateResult struct {
	Mean             float64 `json:"mean"`
	StdDev           float64 `json:"std_dev"`
	MarginOfError    float64 `json:"margin_of_error"`
	Confidence       float64 `json:"confidence"`
	LowerBound       float64 `json:"lower_bound"`
	UpperBound       float64 `json:"upper_bound"`
	Rounds           int     `json:"rounds"`
	CompletedBatches int     `json:"completed_batches"`
	TotalBatches     int     `json:"total_batches"`
}

// batchRounds returns the number of rounds in batch position p.
func batchRounds(total, size, p int) int {
	start := p * size
	if start >= total {
		return 0
	}
	if r := total - start; r < size {
		return r
	}
	return size
}

// simulateBatch simulates rounds starting at global batch index with a fixed
// random stream bound only to (masterSeed, index).
func simulateBatch(snap *GraphSnapshot, model ModelConfig, seeds []NodeID,
	masterSeed int64, index, position, rounds int, specKey string) BatchStats {
	normalized := uniqueSortedSeeds(seeds)
	key := rngSeed(masterSeed, index)
	stats := BatchStats{Index: index, SpecKey: specKey, Position: position, Rounds: rounds}
	for r := 0; r < rounds; r++ {
		influenced := simulateRound(snap, model, normalized, key, r)
		n := int64(len(influenced))
		stats.Sum += n
		stats.SumSq += n * n
	}
	return stats
}

func uniqueSortedSeeds(seeds []NodeID) []NodeID {
	seen := make(map[NodeID]struct{}, len(seeds))
	out := make([]NodeID, 0, len(seeds))
	for _, s := range seeds {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	sortNodeIDs(out)
	return out
}

// aggregate combines completed batch statistics into an estimate.
func aggregate(stats []BatchStats, confidence float64) EstimateResult {
	stats = append([]BatchStats(nil), stats...)
	sort.Slice(stats, func(i, j int) bool {
		if stats[i].Position != stats[j].Position {
			return stats[i].Position < stats[j].Position
		}
		return stats[i].Index < stats[j].Index
	})
	res := EstimateResult{
		Confidence:       confidence,
		CompletedBatches: len(stats),
		TotalBatches:     len(stats),
	}
	var n int64
	var sum, sumSq int64
	for _, b := range stats {
		n += int64(b.Rounds)
		sum += b.Sum
		sumSq += b.SumSq
	}
	res.Rounds = int(n)
	if n == 0 {
		return res
	}
	mean := float64(sum) / float64(n)
	res.Mean = mean
	if n > 1 {
		variance := (float64(sumSq) - float64(sum)*float64(sum)/float64(n)) / float64(n-1)
		if variance < 0 {
			variance = 0
		}
		res.StdDev = math.Sqrt(variance)
	}
	// Normal-approximate confidence interval of the sample mean.
	z := normalQuantile(0.5 + confidence/2)
	margin := z * res.StdDev / math.Sqrt(float64(n))
	res.MarginOfError = margin
	res.LowerBound = mean - margin
	res.UpperBound = mean + margin
	return res
}

// runEstimation executes all pending batches until completion, cancellation,
// or failure. It reports completed batch statistics via progress.
func runEstimation(ctx context.Context, snap *GraphSnapshot, cfg EstimateConfig,
	completed []BatchStats, progress func(BatchStats)) (EstimateResult, error) {
	if err := validateEstimateConfig(snap, cfg); err != nil {
		return EstimateResult{}, err
	}
	total := cfg.totalBatches()
	reg := newBatchRegistry(snap, cfg.Seed)
	for _, b := range completed {
		reg.seed(b)
	}
	seeds := uniqueSortedSeeds(cfg.Seeds)
	all, err := reg.evaluate(ctx, estimationSpec{model: cfg.Model, seeds: seeds}, 0, total, cfg.BatchSize,
		cfg.Rounds, progress, nil)
	if err != nil {
		return EstimateResult{}, err
	}
	res := aggregate(all, cfg.Confidence)
	res.TotalBatches = total
	return res, nil
}

// totalBatches returns the number of batches covering Rounds simulations.
func (cfg EstimateConfig) totalBatches() int {
	return (cfg.Rounds + cfg.BatchSize - 1) / cfg.BatchSize
}

// validateEstimateConfig validates an estimation request against a snapshot.
func validateEstimateConfig(snap *GraphSnapshot, cfg EstimateConfig) error {
	if snap.NodeCount() == 0 {
		return ErrEmptyGraph
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
	if len(cfg.Seeds) == 0 {
		return ErrEmptySeedSet
	}
	for _, seed := range cfg.Seeds {
		if !snap.HasNode(seed) {
			return &NodeError{Err: ErrSeedNotFound, Node: seed}
		}
	}
	return cfg.Model.validate(snap)
}

// normalQuantile returns the inverse standard normal CDF (Acklam's algorithm).
func normalQuantile(p float64) float64 {
	a := []float64{
		-3.969683028665376e+01, 2.209460984245205e+02,
		-2.759285104469687e+02, 1.383577518672690e+02,
		-3.066479806614716e+01, 2.506628277459239e+00,
	}
	b := []float64{
		-5.447609879822406e+01, 1.615858368580409e+02,
		-1.556989798598866e+02, 6.680131188771972e+01,
		-1.328068155288572e+01,
	}
	c := []float64{
		-7.784894002430293e-03, -3.223964580411365e-01,
		-2.400758277161838e+00, -2.549732539343734e+00,
		4.374664141464968e+00, 2.938163982698783e+00,
	}
	d := []float64{
		7.784695709041462e-03, 3.224671290700398e-01,
		2.445134137142996e+00, 3.754408661907416e+00,
	}

	plow := 0.02425
	phigh := 1 - plow
	switch {
	case p < plow:
		q := math.Sqrt(-2 * math.Log(p))
		return (((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q + c[5]) /
			((((d[0]*q+d[1])*q+d[2])*q+d[3])*q + 1)
	case p <= phigh:
		q := p - 0.5
		r := q * q
		return (((((a[0]*r+a[1])*r+a[2])*r+a[3])*r+a[4])*r + a[5]) * q /
			(((((b[0]*r+b[1])*r+b[2])*r+b[3])*r+b[4])*r + 1)
	default:
		q := math.Sqrt(-2 * math.Log(1-p))
		return -(((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q + c[5]) /
			((((d[0]*q+d[1])*q+d[2])*q+d[3])*q + 1)
	}
}
