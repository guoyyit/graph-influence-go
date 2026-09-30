package influence

// NodeID identifies a vertex.
type NodeID string

// Edge is a directed weighted edge. Weight must be in [0,1]. In the
// independent cascade model it is the propagation probability; in the
// linear threshold model it contributes to a node's weighted in-degree.
type Edge struct {
	From   NodeID
	To     NodeID
	Weight float64
}

// directedAdj stores one adjacency list keyed by ordered endpoint pair.
type directedAdj struct {
	edges map[NodeID]map[NodeID]float64
}

func newDirectedAdj() *directedAdj {
	return &directedAdj{edges: make(map[NodeID]map[NodeID]float64)}
}

func (a *directedAdj) set(from, to NodeID, w float64) {
	m := a.edges[from]
	if m == nil {
		m = make(map[NodeID]float64)
		a.edges[from] = m
	}
	m[to] = w
}

func (a *directedAdj) delete(from, to NodeID) bool {
	m := a.edges[from]
	if _, ok := m[to]; !ok {
		return false
	}
	delete(m, to)
	if len(m) == 0 {
		delete(a.edges, from)
	}
	return true
}

// Graph is an immutable snapshot of a directed weighted graph. Mutating
// operations go through GraphBuilder and yield a new version, so tasks
// running against an older snapshot are never mutated underneath.
type Graph struct {
	version int64
	nodes   map[NodeID]struct{}
	out     *directedAdj
	in      *directedAdj
}

// NewGraph validates nodes and edges and returns a version-1 snapshot.
// It distinguishes empty graphs, self loops, out-of-range weights,
// missing endpoints and duplicate edges via GraphError codes.
func NewGraph(nodes []NodeID, edges []Edge) (*Graph, error) {
	if len(nodes) == 0 {
		return nil, newError(ErrEmptyGraph, "graph must contain at least one node")
	}
	g := &Graph{
		version: 1,
		nodes:   make(map[NodeID]struct{}, len(nodes)),
		out:     newDirectedAdj(),
		in:      newDirectedAdj(),
	}
	seen := make(map[NodeID]struct{}, len(nodes))
	for _, id := range nodes {
		if id == "" {
			return nil, newError(ErrUnknownNode, "node id must not be empty")
		}
		if _, dup := seen[id]; dup {
			return nil, newError(ErrDuplicateEdge, "duplicate node id %q", id)
		}
		seen[id] = struct{}{}
		g.nodes[id] = struct{}{}
	}
	for _, e := range edges {
		if err := validateEdge(e); err != nil {
			return nil, err
		}
		if !g.HasNode(e.From) || !g.HasNode(e.To) {
			return nil, newError(ErrMissingEndpoint, "edge %q->%q references a node not in the node set", e.From, e.To)
		}
		if _, exists := g.out.edges[e.From][e.To]; exists {
			return nil, newError(ErrDuplicateEdge, "duplicate edge %q->%q", e.From, e.To)
		}
		g.out.set(e.From, e.To, e.Weight)
		g.in.set(e.To, e.From, e.Weight)
	}
	return g, nil
}

func validateEdge(e Edge) error {
	if e.From == e.To {
		return newError(ErrSelfLoop, "self loops are not allowed: %q", e.From)
	}
	if e.Weight < 0 || e.Weight > 1 {
		return newError(ErrInvalidWeight, "edge %q->%q weight %v out of range [0,1]", e.From, e.To, e.Weight)
	}
	return nil
}

// Version returns the monotonically increasing snapshot version.
func (g *Graph) Version() int64 { return g.version }

// Nodes returns a stable-ordered copy of all node ids.
func (g *Graph) Nodes() []NodeID {
	if g == nil {
		return nil
	}
	out := make([]NodeID, 0, len(g.nodes))
	for id := range g.nodes {
		out = append(out, id)
	}
	sortNodes(out)
	return out
}

func (g *Graph) HasNode(id NodeID) bool {
	if g == nil {
		return false
	}
	_, ok := g.nodes[id]
	return ok
}

func (g *Graph) Edge(from, to NodeID) (Edge, bool) {
	if g == nil {
		return Edge{}, false
	}
	w, ok := g.out.edges[from][to]
	if !ok {
		return Edge{}, false
	}
	return Edge{From: from, To: to, Weight: w}, true
}

// OutEdges returns a deterministic-ordered copy of outgoing edges.
func (g *Graph) OutEdges(id NodeID) []Edge {
	if g == nil {
		return nil
	}
	return adjEdges(g.out, id)
}

// InEdges returns a deterministic-ordered copy of incoming edges.
func (g *Graph) InEdges(id NodeID) []Edge {
	if g == nil {
		return nil
	}
	m := g.in.edges[id]
	out := make([]Edge, 0, len(m))
	for from, w := range m {
		out = append(out, Edge{From: from, To: id, Weight: w})
	}
	sortEdges(out)
	return out
}

func adjEdges(a *directedAdj, id NodeID) []Edge {
	m := a.edges[id]
	out := make([]Edge, 0, len(m))
	for to, w := range m {
		out = append(out, Edge{From: id, To: to, Weight: w})
	}
	sortEdges(out)
	return out
}

// GraphBuilder performs online updates. Each mutating call clones the
// snapshot, bumps its version, and leaves the old snapshot untouched.
type GraphBuilder struct {
	current *Graph
}

func NewGraphBuilder(g *Graph) *GraphBuilder {
	return &GraphBuilder{current: g}
}

func (b *GraphBuilder) Current() *Graph { return b.current }

func (b *GraphBuilder) clone() *Graph {
	cur := b.current
	nodes := make(map[NodeID]struct{}, len(cur.nodes)+1)
	for id := range cur.nodes {
		nodes[id] = struct{}{}
	}
	out := newDirectedAdj()
	in := newDirectedAdj()
	for from, m := range cur.out.edges {
		for to, w := range m {
			out.set(from, to, w)
			in.set(to, from, w)
		}
	}
	return &Graph{version: cur.version + 1, nodes: nodes, out: out, in: in}
}

func (b *GraphBuilder) AddNode(id NodeID) (*Graph, error) {
	if id == "" {
		return nil, newError(ErrUnknownNode, "node id must not be empty")
	}
	if b.current.HasNode(id) {
		return nil, newError(ErrDuplicateEdge, "node %q already exists", id)
	}
	next := b.clone()
	next.nodes[id] = struct{}{}
	b.current = next
	return next, nil
}

func (b *GraphBuilder) RemoveNode(id NodeID) (*Graph, error) {
	if !b.current.HasNode(id) {
		return nil, newError(ErrUnknownNode, "cannot remove unknown node %q", id)
	}
	next := b.clone()
	delete(next.nodes, id)
	for to := range next.out.edges[id] {
		delete(next.in.edges[to], id)
		if len(next.in.edges[to]) == 0 {
			delete(next.in.edges, to)
		}
	}
	delete(next.out.edges, id)
	for from := range next.in.edges[id] {
		delete(next.out.edges[from], id)
		if len(next.out.edges[from]) == 0 {
			delete(next.out.edges, from)
		}
	}
	delete(next.in.edges, id)
	b.current = next
	return next, nil
}

func (b *GraphBuilder) AddEdge(e Edge) (*Graph, error) {
	if err := validateEdge(e); err != nil {
		return nil, err
	}
	if !b.current.HasNode(e.From) || !b.current.HasNode(e.To) {
		return nil, newError(ErrMissingEndpoint, "edge %q->%q references an unknown node", e.From, e.To)
	}
	if _, exists := b.current.out.edges[e.From][e.To]; exists {
		return nil, newError(ErrDuplicateEdge, "edge %q->%q already exists", e.From, e.To)
	}
	next := b.clone()
	next.out.set(e.From, e.To, e.Weight)
	next.in.set(e.To, e.From, e.Weight)
	b.current = next
	return next, nil
}

func (b *GraphBuilder) RemoveEdge(from, to NodeID) (*Graph, error) {
	if !b.current.HasNode(from) || !b.current.HasNode(to) {
		return nil, newError(ErrMissingEndpoint, "edge %q->%q references an unknown node", from, to)
	}
	if _, exists := b.current.out.edges[from][to]; !exists {
		return nil, newError(ErrUnknownNode, "edge %q->%q does not exist", from, to)
	}
	next := b.clone()
	next.out.delete(from, to)
	next.in.delete(to, from)
	b.current = next
	return next, nil
}

func (b *GraphBuilder) SetWeight(from, to NodeID, weight float64) (*Graph, error) {
	if weight < 0 || weight > 1 {
		return nil, newError(ErrInvalidWeight, "weight %v for edge %q->%q out of range [0,1]", weight, from, to)
	}
	if _, exists := b.current.out.edges[from][to]; !exists {
		return nil, newError(ErrUnknownNode, "edge %q->%q does not exist", from, to)
	}
	next := b.clone()
	next.out.set(from, to, weight)
	next.in.set(to, from, weight)
	b.current = next
	return next, nil
}
