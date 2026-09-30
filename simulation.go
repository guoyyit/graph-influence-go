package influence

// simulateRound executes one Monte Carlo propagation round on a snapshot and
// returns the influenced node IDs including the seeds. IDs are returned in
// sorted order. Every edge attempt and every node threshold is an independent
// deterministic draw keyed by (batch, round, endpoint), so the same graph
// decision consumes the same randomness even when different seed sets change
// the activation traversal. This keeps independent estimation tasks bit
// identical regardless of scheduling or restart.
func simulateRound(snap *GraphSnapshot, cfg ModelConfig, seeds []NodeID,
	batchKey [32]byte, roundIndex int) []NodeID {
	active := make(map[NodeID]struct{}, len(seeds))
	for _, seed := range seeds {
		active[seed] = struct{}{}
	}

	switch cfg.Model {
	case ModelIndependentCascade:
		// Frontier BFS: a node gets exactly one activation attempt at each of
		// its in-neighbours, made when that in-neighbour first activates.
		frontier := make([]NodeID, 0, len(seeds))
		frontier = append(frontier, seeds...)
		for len(frontier) > 0 {
			next := make([]NodeID, 0)
			for _, from := range frontier {
				for _, e := range snap.OutEdges(from) {
					if _, on := active[e.To]; on {
						continue
					}
					key := edgeDecisionKey(batchKey, roundIndex, e.From, e.To)
					if keyUniform(key) < cfg.edgeProbability(e) {
						active[e.To] = struct{}{}
						next = append(next, e.To)
					}
				}
			}
			frontier = next
		}
	case ModelLinearThreshold:
		// Synchronous rounds: thresholds are fixed once, then activation is
		// recomputed from all currently active in-neighbours until a fixed
		// point is reached.
		thresholds := make(map[NodeID]float64, snap.NodeCount())
		for _, node := range snap.nodes {
			if t, ok := cfg.Thresholds[node]; ok {
				thresholds[node] = t
			} else {
				thresholds[node] = keyUniform(nodeDecisionKey(batchKey, roundIndex, node))
			}
		}
		for {
			var newly []NodeID
			for _, node := range snap.nodes {
				if _, on := active[node]; on {
					continue
				}
				var sum float64
				for _, e := range snap.InEdges(node) {
					if _, on := active[e.From]; on {
						sum += e.Weight
					}
				}
				if sum >= thresholds[node] {
					newly = append(newly, node)
				}
			}
			if len(newly) == 0 {
				break
			}
			for _, node := range newly {
				active[node] = struct{}{}
			}
		}
	}

	out := make([]NodeID, 0, len(active))
	for node := range active {
		out = append(out, node)
	}
	// Active derives from the snapshot's sorted nodes and their sorted edges;
	// sort defensively so callers never observe map iteration order.
	sortNodeIDs(out)
	return out
}

// sortNodeIDs sorts node IDs lexicographically.
func sortNodeIDs(ids []NodeID) {
	for i := 1; i < len(ids); i++ {
		for j := i; j > 0 && ids[j-1] > ids[j]; j-- {
			ids[j-1], ids[j] = ids[j], ids[j-1]
		}
	}
}
