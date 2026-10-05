// Package influence implements local influence propagation simulation and
// budget-constrained seed selection on weighted directed graphs.
//
// It supports the independent cascade and linear threshold propagation
// models, deterministic Monte Carlo estimation with confidence intervals,
// resumable batch persistence, online graph updates with snapshot semantics,
// task cancellation and fully local operation without external services.
//
// Every task is bound at submission to an immutable graph snapshot identified
// by a content hash; the binding is persisted and restored across restarts,
// so a task's result depends only on its bound graph content, never on later
// graph updates. Tasks whose bound content cannot be restored at recovery
// fail explicitly instead of reusing mismatched statistics.
package influence
