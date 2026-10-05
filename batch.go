package influence

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"sort"
	"strconv"
)

// estimationSpec identifies one Monte Carlo estimation: the propagation model
// parameters and the seed set (node order-independent).
type estimationSpec struct {
	model ModelConfig
	seeds []NodeID
}

// key returns the stable string key of the specification.
func (s estimationSpec) key() string {
	seeds := uniqueSortedSeeds(s.seeds)
	h := sha256.New()
	writeStr := func(prefix, v string) {
		h.Write([]byte(prefix))
		var b [8]byte
		binary.BigEndian.PutUint64(b[:], uint64(len(v)))
		h.Write(b[:])
		h.Write([]byte(v))
	}
	writeStr("m", string(s.model.Model))
	writeStr("u", strconv.FormatBool(s.model.UseEdgeWeights))
	writeStr("p", strconv.FormatFloat(s.model.DefaultProbability, 'g', -1, 64))
	// Deterministic threshold map ordering.
	nodes := make([]NodeID, 0, len(s.model.Thresholds))
	for n := range s.model.Thresholds {
		nodes = append(nodes, n)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i] < nodes[j] })
	for _, n := range nodes {
		writeStr("t:"+string(n), strconv.FormatFloat(s.model.Thresholds[n], 'g', -1, 64))
	}
	writeStr("k", strconv.Itoa(len(seeds)))
	for _, seed := range seeds {
		writeStr("s", string(seed))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// batchRegistry assigns globally unique batch indices inside one task run and
// memoizes (spec, batchPosition) results. Identical specifications reuse the
// same statistics (CELF evaluations), so work is never computed twice and
// restarted tasks resume without recomputing persisted batches.
type batchRegistry struct {
	snap       *GraphSnapshot
	masterSeed int64
	mu         struct {
		// Mutable registry state. Each task owns a private registry.
		counter int
		byIndex map[int]BatchStats
		byPos   map[string]map[int]int
	}
}

func newBatchRegistry(snap *GraphSnapshot, masterSeed int64) *batchRegistry {
	r := &batchRegistry{snap: snap, masterSeed: masterSeed}
	r.mu.byIndex = make(map[int]BatchStats)
	r.mu.byPos = make(map[string]map[int]int)
	return r
}

// seed loads a previously persisted batch without recomputation. A batch is
// reused only when it was provably simulated on the exact graph content this
// registry is bound to: its recorded graph hash must equal the bound
// snapshot's content hash. Anything else (foreign, legacy or provenance-free
// statistics) is rejected with ErrGraphStatsMismatch instead of being mixed
// in silently.
func (r *batchRegistry) seed(b BatchStats) error {
	if b.Rounds <= 0 {
		return nil
	}
	if b.GraphHash != r.snap.Hash() {
		return &BatchProvenanceError{Index: b.Index, Want: r.snap.Hash(), Got: b.GraphHash}
	}
	if _, exists := r.mu.byIndex[b.Index]; exists {
		return nil
	}
	r.mu.byIndex[b.Index] = b
	pos, ok := r.mu.byPos[b.SpecKey]
	if !ok {
		pos = make(map[int]int)
		r.mu.byPos[b.SpecKey] = pos
	}
	pos[b.Position] = b.Index
	if b.Index+1 > r.mu.counter {
		r.mu.counter = b.Index + 1
	}
	return nil
}

// evaluate returns all batch statistics for one specification, reusing cached
// and persisted batches and simulating missing ones.
func (r *batchRegistry) evaluate(ctx context.Context, spec estimationSpec,
	start, count, batchSize, rounds int, progress func(BatchStats), onComplete func()) ([]BatchStats, error) {
	key := spec.key()
	out := make([]BatchStats, 0, count)
	for p := start; p < start+count; p++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		n := batchRounds(rounds, batchSize, p)
		if n == 0 {
			continue
		}

		var b BatchStats
		if pos, ok := r.mu.byPos[key]; ok {
			if idx, hit := pos[p]; hit {
				b = r.mu.byIndex[idx]
			}
		}
		if b.Rounds == 0 {
			idx := r.nextIndex()
			b = simulateBatch(r.snap, spec.model, spec.seeds, r.masterSeed, idx, p, n, key)
			r.mu.byIndex[idx] = b
			pos, ok := r.mu.byPos[key]
			if !ok {
				pos = make(map[int]int)
				r.mu.byPos[key] = pos
			}
			pos[p] = idx
		}
		out = append(out, b)
		if progress != nil {
			progress(b)
		}
	}
	if onComplete != nil {
		onComplete()
	}
	return out, nil
}

// nextIndex hands out a fresh global batch index.
func (r *batchRegistry) nextIndex() int {
	idx := r.mu.counter
	r.mu.counter++
	return idx
}
