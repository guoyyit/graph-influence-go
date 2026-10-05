package influence

import "fmt"

// Sentinel errors. Callers can distinguish failure classes with errors.Is.
var (
	ErrEmptyGraph            = sentinelError("influence: graph is empty")
	ErrNodeNotFound          = sentinelError("influence: node not found")
	ErrNodeExists            = sentinelError("influence: node already exists")
	ErrEmptyNodeID           = sentinelError("influence: node id must not be empty")
	ErrSelfLoop              = sentinelError("influence: self loops are not allowed")
	ErrDuplicateEdge         = sentinelError("influence: edge already exists")
	ErrEdgeNotFound          = sentinelError("influence: edge not found")
	ErrWeightOutOfRange      = sentinelError("influence: edge weight out of range [0,1]")
	ErrProbabilityOutOfRange = sentinelError("influence: propagation probability out of range [0,1]")
	ErrThresholdOutOfRange   = sentinelError("influence: threshold out of range [0,1]")
	ErrUnknownModel          = sentinelError("influence: unknown propagation model")
	ErrInvalidRounds         = sentinelError("influence: rounds must be positive")
	ErrInvalidBatchSize      = sentinelError("influence: batch size must be positive")
	ErrInvalidConfidence     = sentinelError("influence: confidence must be within (0,1)")
	ErrInvalidSeedCount      = sentinelError("influence: seed count must be positive")
	ErrEmptySeedSet          = sentinelError("influence: seed set is empty")
	ErrSeedNotFound          = sentinelError("influence: seed references unknown node")
	ErrNoCandidates          = sentinelError("influence: no candidate nodes")
	ErrSeedCountExceedsNodes = sentinelError("influence: seed count exceeds number of nodes")
	ErrTaskNotFound          = sentinelError("influence: task not found")
	ErrTaskCancelled         = sentinelError("influence: task cancelled")
	ErrTaskTerminal          = sentinelError("influence: task already in terminal state")
	ErrServiceClosed         = sentinelError("influence: service closed")
	// ErrGraphBindingLost marks tasks whose bound graph content cannot be
	// restored at recovery, so a consistent result can no longer be
	// guaranteed. The task fails explicitly instead of silently reusing
	// statistics computed against a different graph.
	ErrGraphBindingLost = sentinelError("influence: bound graph content is no longer available")
	// ErrGraphStatsMixed marks tasks whose persisted simulation statistics
	// span more than one graph content. Such tasks are never completed.
	ErrGraphStatsMixed = sentinelError("influence: task statistics span different graph contents")
)

type sentinelError string

func (e sentinelError) Error() string { return string(e) }

// NodeError wraps an error with the offending node ID.
type NodeError struct {
	Err  error
	Node NodeID
}

func (e *NodeError) Error() string { return fmt.Sprintf("%s: node %q", e.Err, e.Node) }
func (e *NodeError) Unwrap() error { return e.Err }

// EdgeError wraps an error with the offending edge endpoints.
type EdgeError struct {
	Err  error
	From NodeID
	To   NodeID
}

func (e *EdgeError) Error() string {
	return fmt.Sprintf("%s: edge %q -> %q", e.Err, e.From, e.To)
}
func (e *EdgeError) Unwrap() error { return e.Err }

// ValueError wraps an error with the offending field name and value.
type ValueError struct {
	Err   error
	Field string
	Value float64
}

func (e *ValueError) Error() string {
	return fmt.Sprintf("%s: %s=%v", e.Err, e.Field, e.Value)
}
func (e *ValueError) Unwrap() error { return e.Err }
