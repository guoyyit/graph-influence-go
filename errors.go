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
	// ErrGraphSnapshotUnavailable reports that the graph content a task was
	// bound to at submission cannot be restored (the snapshot file is
	// missing or corrupt and the current graph has different content), so
	// the task cannot be resumed without risking silently mixed statistics.
	ErrGraphSnapshotUnavailable = sentinelError("influence: bound graph snapshot unavailable")
	// ErrGraphStatsMismatch reports that persisted simulation statistics
	// belong to graph content different from the one the task is bound to.
	// Such statistics are never reused silently; the task fails instead.
	ErrGraphStatsMismatch = sentinelError("influence: persisted statistics do not match the bound graph")
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

// GraphBindingError reports that a task's bound graph content could not be
// restored during recovery. It always unwraps to
// ErrGraphSnapshotUnavailable so callers can classify it with errors.Is.
type GraphBindingError struct {
	TaskID string
	Hash   string
	Err    error
}

func (e *GraphBindingError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: task %q bound to graph %s: %v", ErrGraphSnapshotUnavailable, e.TaskID, e.Hash, e.Err)
	}
	return fmt.Sprintf("%s: task %q bound to graph %s", ErrGraphSnapshotUnavailable, e.TaskID, e.Hash)
}
func (e *GraphBindingError) Unwrap() error { return ErrGraphSnapshotUnavailable }

// BatchProvenanceError reports a persisted simulation batch whose recorded
// graph hash does not match the graph content a task is bound to. It always
// unwraps to ErrGraphStatsMismatch so callers can classify it with errors.Is.
type BatchProvenanceError struct {
	Index int
	Want  string
	Got   string
}

func (e *BatchProvenanceError) Error() string {
	return fmt.Sprintf("%s: batch %d belongs to graph %q, task is bound to %q",
		ErrGraphStatsMismatch, e.Index, e.Got, e.Want)
}
func (e *BatchProvenanceError) Unwrap() error { return ErrGraphStatsMismatch }
