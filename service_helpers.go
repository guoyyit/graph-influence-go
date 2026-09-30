package influence

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync/atomic"
	"time"
)

var taskSeq atomic.Uint64

func newTaskID(kind string, version int64) string {
	n := taskSeq.Add(1)
	raw := fmt.Sprintf("%s-%d-%d-%d", kind, version, n, time.Now().UnixNano())
	sum := sha256.Sum256([]byte(raw))
	return kind + "-" + hex.EncodeToString(sum[:8])
}

func graphToDoc(g *Graph) graphDoc {
	doc := graphDoc{Version: g.version}
	for _, id := range g.Nodes() {
		doc.Nodes = append(doc.Nodes, string(id))
	}
	for _, from := range g.Nodes() {
		for _, e := range g.OutEdges(from) {
			doc.Edges = append(doc.Edges, edgeDoc{From: string(e.From), To: string(e.To), Weight: e.Weight})
		}
	}
	return doc
}

func docToGraph(doc graphDoc) (*Graph, error) {
	nodes := make([]NodeID, len(doc.Nodes))
	for i, n := range doc.Nodes {
		nodes[i] = NodeID(n)
	}
	edges := make([]Edge, len(doc.Edges))
	for i, e := range doc.Edges {
		edges[i] = Edge{From: NodeID(e.From), To: NodeID(e.To), Weight: e.Weight}
	}
	g, err := NewGraph(nodes, edges)
	if err != nil {
		return nil, err
	}
	g.version = doc.Version
	return g, nil
}
