package influence

import "sort"

func sortNodes(ids []NodeID) {
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
}

func sortEdges(edges []Edge) {
	sort.Slice(edges, func(i, j int) bool {
		if edges[i].From != edges[j].From {
			return edges[i].From < edges[j].From
		}
		return edges[i].To < edges[j].To
	})
}
