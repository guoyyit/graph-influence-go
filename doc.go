// Package influence implements local influence propagation simulation and
// budget-constrained seed selection on weighted directed graphs.
//
// It supports the independent cascade and linear threshold propagation
// models, deterministic Monte Carlo estimation with confidence intervals,
// resumable batch persistence, online graph updates with snapshot semantics,
// task cancellation and fully local operation without external services.
//
// Every task is bound at submission to one immutable graph snapshot,
// identified by a content hash. The bound snapshot is persisted in a
// content-addressed store, so a task interrupted and resumed later — even
// after the live graph was modified — always completes against exactly the
// graph content it was submitted with, bit-identical to an uninterrupted
// run. Persisted simulation batches carry the content hash of the graph they
// were simulated on and are reused only when it matches the task's binding;
// anything else fails the task explicitly instead of being mixed in.
package influence
