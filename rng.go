package influence

import (
	"crypto/sha256"
	"encoding/binary"
	"math/rand/v2"
)

const (
	streamDomainBatch = "graph-influence-go/v1/batch"
	streamDomainEdge  = "graph-influence-go/v1/edge"
	streamDomainNode  = "graph-influence-go/v1/node"
)

// rngSeed derives a deterministic per-batch key from the master random seed
// and the batch index. Identical configuration therefore yields identical
// batches regardless of goroutine scheduling, process restarts or retries.
// Batch indices are globally unique within one task run (see selection.go),
// so distinct estimation specifications never share a random stream.
func rngSeed(masterSeed int64, batchIndex int) [32]byte {
	h := sha256.New()
	h.Write([]byte(streamDomainBatch))
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(masterSeed))
	h.Write(buf[:])
	binary.BigEndian.PutUint64(buf[:], uint64(batchIndex))
	h.Write(buf[:])
	var key [32]byte
	copy(key[:], h.Sum(nil))
	return key
}

// keyUniform draws one deterministic uniform [0,1) value from a 32-byte key.
func keyUniform(key [32]byte) float64 {
	r := rand.New(rand.NewChaCha8(key))
	return r.Float64()
}

// edgeDecisionKey derives the key of one edge activation attempt. The key is
// bound to the edge endpoints, so the same edge always draws the same number
// within a round regardless of which other nodes are active.
func edgeDecisionKey(batchKey [32]byte, roundIndex int, from, to NodeID) [32]byte {
	h := sha256.New()
	h.Write([]byte(streamDomainEdge))
	h.Write(batchKey[:])
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(roundIndex))
	h.Write(buf[:])
	h.Write([]byte(from))
	h.Write([]byte{0})
	h.Write([]byte(to))
	var key [32]byte
	copy(key[:], h.Sum(nil))
	return key
}

// nodeDecisionKey derives the key of one node-level random decision (such as
// a linear threshold draw).
func nodeDecisionKey(batchKey [32]byte, roundIndex int, node NodeID) [32]byte {
	h := sha256.New()
	h.Write([]byte(streamDomainNode))
	h.Write(batchKey[:])
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(roundIndex))
	h.Write(buf[:])
	h.Write([]byte(node))
	var key [32]byte
	copy(key[:], h.Sum(nil))
	return key
}
