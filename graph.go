package influence

import (
	"crypto/sha256"
	"encoding/hex"
	"math"
	"sort"
	"strconv"
	"sync"
)

// NodeID identifies a graph node.
type NodeID string

// Edge is a directed edge with a model-dependent weight.
// For the independent cascade model the weight is the propagation probability.
// For the linear threshold model the weight is the influence weight.
type Edge struct {
	From   NodeID
	To     NodeID
	Weight float64
}

// Graph is a mutable weighted directed graph with monotonically increasing
// version numbers. Every successful mutation bumps the version.
type Graph struct {
	mu      sync.RWMutex
	version uint64
	out     map[NodeID]map[NodeID]float64
	in      map[NodeID]map[NodeID]float64
}

// NewGraph creates an empty graph at version 0.
func NewGraph() *Graph {
	return &Graph{
		out: make(map[NodeID]map[NodeID]float64),
		in:  make(map[NodeID]map[NodeID]float64),
	}
}

func validWeight(w float64) bool {
	return !math.IsNaN(w) && !math.IsInf(w, 0) && w >= 0
}

// AddNode inserts a node.
func (g *Graph) AddNode(id NodeID) error {
	if id == "" {
		return ErrEmptyNodeID
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.out[id]; ok {
		return &NodeError{Err: ErrNodeExists, Node: id}
	}
	g.out[id] = make(map[NodeID]float64)
	g.in[id] = make(map[NodeID]float64)
	g.version++
	return nil
}

// RemoveNode removes a node and every incident edge.
func (g *Graph) RemoveNode(id NodeID) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.out[id]; !ok {
		return &NodeError{Err: ErrNodeNotFound, Node: id}
	}
	for to := range g.out[id] {
		delete(g.in[to], id)
	}
	for from := range g.in[id] {
		delete(g.out[from], id)
	}
	delete(g.out, id)
	delete(g.in, id)
	g.version++
	return nil
}

// AddEdge inserts a directed weighted edge. The weight must be in [0,1]; for
// the independent cascade model it is interpreted as the propagation
// probability.
func (g *Graph) AddEdge(from, to NodeID, weight float64) error {
	if from == "" || to == "" {
		if from == "" {
			return &NodeError{Err: ErrEmptyNodeID, Node: from}
		}
		return &NodeError{Err: ErrEmptyNodeID, Node: to}
	}
	if from == to {
		return &EdgeError{Err: ErrSelfLoop, From: from, To: to}
	}
	if !validWeight(weight) {
		return &EdgeError{Err: &ValueError{Err: ErrWeightOutOfRange, Field: "weight", Value: weight}, From: from, To: to}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.out[from]; !ok {
		return &EdgeError{Err: &NodeError{Err: ErrNodeNotFound, Node: from}, From: from, To: to}
	}
	if _, ok := g.in[to]; !ok {
		return &EdgeError{Err: &NodeError{Err: ErrNodeNotFound, Node: to}, From: from, To: to}
	}
	if _, ok := g.out[from][to]; ok {
		return &EdgeError{Err: ErrDuplicateEdge, From: from, To: to}
	}
	g.out[from][to] = weight
	g.in[to][from] = weight
	g.version++
	return nil
}

// RemoveEdge removes a directed edge.
func (g *Graph) RemoveEdge(from, to NodeID) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.out[from][to]; !ok {
		return &EdgeError{Err: ErrEdgeNotFound, From: from, To: to}
	}
	delete(g.out[from], to)
	delete(g.in[to], from)
	g.version++
	return nil
}

// SetEdgeWeight changes the weight of an existing directed edge.
func (g *Graph) SetEdgeWeight(from, to NodeID, weight float64) error {
	if !validWeight(weight) {
		return &EdgeError{Err: &ValueError{Err: ErrWeightOutOfRange, Field: "weight", Value: weight}, From: from, To: to}
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.out[from][to]; !ok {
		return &EdgeError{Err: ErrEdgeNotFound, From: from, To: to}
	}
	if g.out[from][to] == weight {
		return nil
	}
	g.out[from][to] = weight
	g.in[to][from] = weight
	g.version++
	return nil
}

// Version returns the current graph version.
func (g *Graph) Version() uint64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.version
}

// Snapshot returns an immutable, independently owned snapshot of the graph.
// Concurrent graph mutations never affect a returned snapshot.
func (g *Graph) Snapshot() *GraphSnapshot {
	g.mu.RLock()
	defer g.mu.RUnlock()

	nodes := make([]NodeID, 0, len(g.out))
	for id := range g.out {
		nodes = append(nodes, id)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i] < nodes[j] })

	out := make(map[NodeID][]Edge, len(nodes))
	in := make(map[NodeID][]Edge, len(nodes))
	edgeCount := 0
	for _, from := range nodes {
		targets := make([]NodeID, 0, len(g.out[from]))
		for to := range g.out[from] {
			targets = append(targets, to)
		}
		sort.Slice(targets, func(i, j int) bool { return targets[i] < targets[j] })
		edges := make([]Edge, 0, len(targets))
		for _, to := range targets {
			edges = append(edges, Edge{From: from, To: to, Weight: g.out[from][to]})
		}
		out[from] = edges
		edgeCount += len(edges)

		sources := make([]NodeID, 0, len(g.in[from]))
		for src := range g.in[from] {
			sources = append(sources, src)
		}
		sort.Slice(sources, func(i, j int) bool { return sources[i] < sources[j] })
		edgesIn := make([]Edge, 0, len(sources))
		for _, src := range sources {
			edgesIn = append(edgesIn, Edge{From: src, To: from, Weight: g.in[from][src]})
		}
		in[from] = edgesIn
	}

	snap := &GraphSnapshot{
		version: g.version,
		nodes:   nodes,
		out:     out,
		in:      in,
	}
	snap.edgeCount = edgeCount
	snap.hash = computeHash(snap)
	return snap
}

// GraphSnapshot is an immutable view of the graph at one version. Tasks pin a
// snapshot so concurrent graph updates never perturb running simulations.
type GraphSnapshot struct {
	version   uint64
	hash      string
	nodes     []NodeID
	out       map[NodeID][]Edge
	in        map[NodeID][]Edge
	edgeCount int
}

// Version returns the graph version of this snapshot.
func (s *GraphSnapshot) Version() uint64 { return s.version }

// Hash returns the deterministic content hash of this snapshot. The hash
// covers only the graph content (nodes, edges, weights); two snapshots with
// identical content share the same hash even when their version counters
// differ, so content-addressed caches survive pure version bumps.
func (s *GraphSnapshot) Hash() string { return s.hash }

// NodeCount returns the number of nodes.
func (s *GraphSnapshot) NodeCount() int { return len(s.nodes) }

// EdgeCount returns the number of directed edges.
func (s *GraphSnapshot) EdgeCount() int { return s.edgeCount }

// HasNode reports whether the snapshot contains a node.
func (s *GraphSnapshot) HasNode(id NodeID) bool {
	_, ok := s.out[id]
	return ok
}

// Nodes returns a copy of all node IDs in sorted order.
func (s *GraphSnapshot) Nodes() []NodeID {
	out := make([]NodeID, len(s.nodes))
	copy(out, s.nodes)
	return out
}

// OutEdges returns a copy of the outgoing edges of a node sorted by target.
func (s *GraphSnapshot) OutEdges(from NodeID) []Edge {
	edges := s.out[from]
	out := make([]Edge, len(edges))
	copy(out, edges)
	return out
}

// InEdges returns a copy of the incoming edges of a node sorted by source.
func (s *GraphSnapshot) InEdges(to NodeID) []Edge {
	edges := s.in[to]
	out := make([]Edge, len(edges))
	copy(out, edges)
	return out
}

// Edge returns the edge from -> to and reports whether it exists.
func (s *GraphSnapshot) Edge(from, to NodeID) (Edge, bool) {
	for _, e := range s.out[from] {
		if e.To == to {
			return e, true
		}
	}
	return Edge{}, false
}

func computeHash(s *GraphSnapshot) string {
	h := sha256.New()
	var buf [16]byte
	putUint64 := func(v uint64) {
		for i := 7; i >= 0; i-- {
			buf[i] = byte(v)
			v >>= 8
		}
		h.Write(buf[:8])
	}
	// The version counter is deliberately excluded: the hash identifies graph
	// content, not its position in the mutation history.
	for _, from := range s.nodes {
		id := string(from)
		putUint64(uint64(len(id)))
		h.Write([]byte(id))
		for _, e := range s.out[from] {
			to := string(e.To)
			putUint64(uint64(len(to)))
			h.Write([]byte(to))
			h.Write([]byte(strconv.FormatFloat(e.Weight, 'g', -1, 64)))
			h.Write([]byte{';'})
		}
		h.Write([]byte{'\n'})
	}
	return hex.EncodeToString(h.Sum(nil))
}
