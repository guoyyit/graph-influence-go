package influence

import (
	"errors"
	"fmt"
)

// Error codes for distinguishable rejection of invalid input.
type ErrorCode string

const (
	ErrEmptyGraph       ErrorCode = "empty_graph"
	ErrSelfLoop         ErrorCode = "self_loop"
	ErrMissingEndpoint  ErrorCode = "missing_endpoint"
	ErrInvalidWeight    ErrorCode = "invalid_weight"
	ErrInvalidThreshold ErrorCode = "invalid_threshold"
	ErrInvalidRounds    ErrorCode = "invalid_rounds"
	ErrInvalidBatches   ErrorCode = "invalid_batches"
	ErrUnknownNode      ErrorCode = "unknown_node"
	ErrDuplicateEdge    ErrorCode = "duplicate_edge"
	ErrInvalidBudget    ErrorCode = "invalid_budget"
	ErrTaskNotFound     ErrorCode = "task_not_found"
	ErrTaskCanceled     ErrorCode = "task_canceled"
	ErrTaskNotRunning   ErrorCode = "task_not_running"
	ErrBatchNotFound    ErrorCode = "batch_not_found"
	ErrPersist          ErrorCode = "persist_failed"
	ErrUnknownModel     ErrorCode = "unknown_model"
)

// GraphError describes a rejected operation in a distinguishable way.
// Callers can use errors.As and inspect Code to distinguish empty graph,
// self loops, out-of-range weights, missing endpoints and other cases.
type GraphError struct {
	Code    ErrorCode
	Message string
}

func (e *GraphError) Error() string {
	return string(e.Code) + ": " + e.Message
}

func newError(code ErrorCode, format string, args ...any) *GraphError {
	return &GraphError{Code: code, Message: fmt.Sprintf(format, args...)}
}

// ErrorCodeOf returns the ErrorCode carried by err, or "" when err is
// nil or not a *GraphError.
func ErrorCodeOf(err error) ErrorCode {
	if err == nil {
		return ""
	}
	var ge *GraphError
	if errors.As(err, &ge) {
		return ge.Code
	}
	return ""
}
