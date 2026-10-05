package influence

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

// crashAfterProgress waits until the task has persisted at least one batch
// and then closes the service, simulating a process crash mid-task. The
// condition (not the wall clock) drives the test, so it is stable and
// reproducible; the deadline only guards against a hung task.
func crashAfterProgress(t *testing.T, s *Service, id string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		st, err := s.GetTask(id)
		if err != nil {
			t.Fatal(err)
		}
		if isTerminal(st.Status) {
			t.Fatalf("task reached terminal state %s before the crash could be simulated", st.Status)
		}
		if st.Progress > 0 {
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("task made no progress before simulated crash")
		}
		time.Sleep(time.Millisecond)
	}
}

// longEstCfg returns an estimation config with enough batches that a
// simulated crash always lands mid-task while still finishing fast.
func longEstCfg() EstimateConfig {
	cfg := estCfg()
	cfg.Rounds = 800
	cfg.BatchSize = 8
	return cfg
}

// referenceEstimation runs the same graph construction and configuration
// uninterrupted in a fresh directory and returns the completed state.
func referenceEstimation(t *testing.T, cfg EstimateConfig) TaskState {
	t.Helper()
	s, err := NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	sampleGraph(t, s)
	id, err := s.SubmitEstimation(cfg)
	if err != nil {
		t.Fatal(err)
	}
	st := waitDone(t, s, id)
	if st.Status != StatusCompleted {
		t.Fatalf("reference status = %s err = %s", st.Status, st.Error)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	return st
}

func readTaskRecord(t *testing.T, dir, id string) taskRecord {
	t.Helper()
	st, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	rec, err := st.LoadTask(id)
	if err != nil {
		t.Fatal(err)
	}
	return rec
}

// assertBatchesPreserved verifies every batch persisted before the crash is
// still present, byte-for-byte identical, in the final record: persisted
// statistics were reused, never recomputed or rewritten.
func assertBatchesPreserved(t *testing.T, pre, final []BatchStats) {
	t.Helper()
	if len(pre) == 0 {
		t.Fatal("test setup: no batches were persisted before the crash")
	}
	byIndex := make(map[int]BatchStats, len(final))
	for _, b := range final {
		byIndex[b.Index] = b
	}
	for _, b := range pre {
		got, ok := byIndex[b.Index]
		if !ok {
			t.Fatalf("pre-crash batch %d missing after resume", b.Index)
		}
		if got != b {
			t.Fatalf("pre-crash batch %d changed across resume:\n%+v\n%+v", b.Index, b, got)
		}
	}
}

// TestResumeAfterGraphModificationBitIdentical is the reported bug: a task is
// interrupted, the graph is modified while the process is down, and the task
// is resumed. The result must be computed strictly on the graph content the
// task was submitted against and be bit-identical to an uninterrupted run.
func TestResumeAfterGraphModificationBitIdentical(t *testing.T) {
	cfg := longEstCfg()
	ref := referenceEstimation(t, cfg)

	dir := t.TempDir()
	s, err := NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	sampleGraph(t, s)
	id, err := s.SubmitEstimation(cfg)
	if err != nil {
		t.Fatal(err)
	}
	crashAfterProgress(t, s, id)
	preBatches := readTaskRecord(t, dir, id).Batches

	// Restart, then modify nodes, edges and weights before resuming.
	s2, err := NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s2.Close() })
	if err := s2.AddNode("z"); err != nil {
		t.Fatal(err)
	}
	if err := s2.AddEdge("a", "z", 0.9); err != nil {
		t.Fatal(err)
	}
	if err := s2.SetEdgeWeight("a", "b", 0.99); err != nil {
		t.Fatal(err)
	}
	if err := s2.RemoveEdge("c", "d"); err != nil {
		t.Fatal(err)
	}
	resumed := s2.ResumePending()
	if !containsID(resumed, id) {
		t.Fatalf("task %s not resumed: %v", id, resumed)
	}
	st := waitDone(t, s2, id)
	if st.Status != StatusCompleted {
		t.Fatalf("status = %s err = %s", st.Status, st.Error)
	}
	if st.Estimate == nil || *st.Estimate != *ref.Estimate {
		t.Fatalf("resumed estimate differs from uninterrupted reference:\n%+v\n%+v", st.Estimate, ref.Estimate)
	}
	if st.GraphHash != ref.GraphHash {
		t.Fatalf("graph hash = %s, want bound content hash %s", st.GraphHash, ref.GraphHash)
	}
	if st.GraphVersion != ref.GraphVersion {
		t.Fatalf("graph version = %d, want submit-time version %d", st.GraphVersion, ref.GraphVersion)
	}
	assertBatchesPreserved(t, preBatches, readTaskRecord(t, dir, id).Batches)
}

// TestResumeAfterContentPreservingVersionBump covers graph updates that only
// move the version counter (add followed by remove) while the process is
// down: identical content must keep the same graph identity, reuse every
// persisted batch and stay bit-identical.
func TestResumeAfterContentPreservingVersionBump(t *testing.T) {
	cfg := longEstCfg()
	ref := referenceEstimation(t, cfg)

	dir := t.TempDir()
	s, err := NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	sampleGraph(t, s)
	id, err := s.SubmitEstimation(cfg)
	if err != nil {
		t.Fatal(err)
	}
	crashAfterProgress(t, s, id)
	preBatches := readTaskRecord(t, dir, id).Batches

	s2, err := NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s2.Close() })
	if err := s2.AddNode("tmp"); err != nil {
		t.Fatal(err)
	}
	if err := s2.RemoveNode("tmp"); err != nil {
		t.Fatal(err)
	}
	if got, want := s2.GraphVersion(), ref.GraphVersion+2; got != want {
		t.Fatalf("version after no-op mutations = %d, want %d", got, want)
	}
	s2.ResumePending()
	st := waitDone(t, s2, id)
	if st.Status != StatusCompleted {
		t.Fatalf("status = %s err = %s", st.Status, st.Error)
	}
	if *st.Estimate != *ref.Estimate {
		t.Fatalf("estimate differs after version-only drift:\n%+v\n%+v", st.Estimate, ref.Estimate)
	}
	if st.GraphHash != ref.GraphHash {
		t.Fatalf("identical content changed graph identity: %s vs %s", st.GraphHash, ref.GraphHash)
	}
	if st.GraphVersion != ref.GraphVersion {
		t.Fatalf("binding moved with the version counter: %d vs %d", st.GraphVersion, ref.GraphVersion)
	}
	assertBatchesPreserved(t, preBatches, readTaskRecord(t, dir, id).Batches)
}

func containsID(ids []string, id string) bool {
	for _, x := range ids {
		if x == id {
			return true
		}
	}
	return false
}

// TestSelectionResumeAfterGraphModificationBitIdentical applies the same
// crash-modify-resume scenario to a CELF seed selection task: every marginal
// gain must come from simulations on the bound graph content only.
func TestSelectionResumeAfterGraphModificationBitIdentical(t *testing.T) {
	cfg := SelectionConfig{
		Model:      ModelConfig{Model: ModelIndependentCascade, UseEdgeWeights: true},
		K:          2,
		Rounds:     400,
		BatchSize:  8,
		Seed:       5,
		Confidence: 0.95,
		Candidates: []NodeID{"a", "b", "c", "d"},
	}
	ref := referenceSelection(t, cfg)

	dir := t.TempDir()
	s, err := NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	sampleGraph(t, s)
	id, err := s.SubmitSelection(cfg)
	if err != nil {
		t.Fatal(err)
	}
	crashAfterProgress(t, s, id)
	preBatches := readTaskRecord(t, dir, id).Batches

	s2, err := NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s2.Close() })
	if err := s2.AddNode("z"); err != nil {
		t.Fatal(err)
	}
	if err := s2.AddEdge("z", "a", 0.9); err != nil {
		t.Fatal(err)
	}
	if err := s2.SetEdgeWeight("b", "c", 0.01); err != nil {
		t.Fatal(err)
	}
	if !containsID(s2.ResumePending(), id) {
		t.Fatalf("task %s not resumed", id)
	}
	st := waitDone(t, s2, id)
	if st.Status != StatusCompleted {
		t.Fatalf("status = %s err = %s", st.Status, st.Error)
	}
	if !reflect.DeepEqual(st.Selection, ref.Selection) {
		t.Fatalf("resumed selection differs from uninterrupted reference:\n%+v\n%+v", st.Selection, ref.Selection)
	}
	if st.GraphHash != ref.GraphHash || st.GraphVersion != ref.GraphVersion {
		t.Fatalf("binding drifted: (%s,%d) vs (%s,%d)", st.GraphHash, st.GraphVersion, ref.GraphHash, ref.GraphVersion)
	}
	assertBatchesPreserved(t, preBatches, readTaskRecord(t, dir, id).Batches)
}

func referenceSelection(t *testing.T, cfg SelectionConfig) TaskState {
	t.Helper()
	s, err := NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	sampleGraph(t, s)
	id, err := s.SubmitSelection(cfg)
	if err != nil {
		t.Fatal(err)
	}
	st := waitDone(t, s, id)
	if st.Status != StatusCompleted {
		t.Fatalf("reference status = %s err = %s", st.Status, st.Error)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	return st
}

// TestConcurrentGraphUpdateDuringTaskDeterministic submits a task and updates
// the graph immediately afterwards: the binding is fixed at submission, so
// the result must be bit-identical to an uninterrupted run on the original
// graph regardless of when the update lands relative to task execution.
func TestConcurrentGraphUpdateDuringTaskDeterministic(t *testing.T) {
	cfg := longEstCfg()
	ref := referenceEstimation(t, cfg)

	s := newTestService(t)
	sampleGraph(t, s)
	id, err := s.SubmitEstimation(cfg)
	if err != nil {
		t.Fatal(err)
	}
	// The update races the running task; the binding was fixed at submit.
	if err := s.SetEdgeWeight("a", "b", 0.99); err != nil {
		t.Fatal(err)
	}
	if err := s.AddNode("e"); err != nil {
		t.Fatal(err)
	}
	if err := s.AddEdge("e", "a", 1); err != nil {
		t.Fatal(err)
	}
	st := waitDone(t, s, id)
	if st.Status != StatusCompleted {
		t.Fatalf("status = %s err = %s", st.Status, st.Error)
	}
	if *st.Estimate != *ref.Estimate {
		t.Fatalf("concurrent update changed the pinned result:\n%+v\n%+v", st.Estimate, ref.Estimate)
	}
	if st.GraphHash != ref.GraphHash || st.GraphVersion != ref.GraphVersion {
		t.Fatalf("binding drifted: (%s,%d) vs (%s,%d)", st.GraphHash, st.GraphVersion, ref.GraphHash, ref.GraphVersion)
	}

	// A task submitted after the update binds the new content and must not
	// reuse the old conclusion.
	id2, err := s.SubmitEstimation(cfg)
	if err != nil {
		t.Fatal(err)
	}
	st2 := waitDone(t, s, id2)
	if st2.Status != StatusCompleted {
		t.Fatalf("status = %s err = %s", st2.Status, st2.Error)
	}
	if st2.GraphHash == st.GraphHash {
		t.Fatal("post-update task kept the pre-update graph identity")
	}
	if *st2.Estimate == *st.Estimate {
		t.Fatal("post-update task silently reused the pre-update result")
	}
}

// TestSubmitUpdateRaceBindingIsStable hammers submissions concurrently with a
// graph update. Every task binds exactly one of the two graph contents and
// its result must be bit-identical to the reference of the content it bound.
func TestSubmitUpdateRaceBindingIsStable(t *testing.T) {
	s := newTestService(t)
	if err := s.AddNode("a"); err != nil {
		t.Fatal(err)
	}
	if err := s.AddNode("b"); err != nil {
		t.Fatal(err)
	}
	if err := s.AddEdge("a", "b", 0); err != nil {
		t.Fatal(err)
	}
	cfg := estCfg()
	cfg.Rounds = 64
	cfg.BatchSize = 8

	// References for the only two contents the graph can have: p=0 (seed
	// reaches itself) and p=1 (seed always reaches both nodes).
	before := referenceOnGraph(t, 0, cfg)
	after := referenceOnGraph(t, 1, cfg)

	const n = 16
	ids := make([]string, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id, err := s.SubmitEstimation(cfg)
			if err != nil {
				t.Error(err)
				return
			}
			ids[i] = id
		}(i)
	}
	// Race the single content-changing update with the submissions.
	if err := s.SetEdgeWeight("a", "b", 1); err != nil {
		t.Fatal(err)
	}
	wg.Wait()

	for _, id := range ids {
		st := waitDone(t, s, id)
		if st.Status != StatusCompleted {
			t.Fatalf("status = %s err = %s", st.Status, st.Error)
		}
		switch st.GraphHash {
		case before.GraphHash:
			if *st.Estimate != *before.Estimate {
				t.Fatalf("task bound to pre-update graph produced a foreign result")
			}
		case after.GraphHash:
			if *st.Estimate != *after.Estimate {
				t.Fatalf("task bound to post-update graph produced a foreign result")
			}
		default:
			t.Fatalf("task recorded an unknown graph identity %s", st.GraphHash)
		}
	}
}

// referenceOnGraph runs cfg uninterrupted on a two-node graph whose single
// edge has weight p, returning the completed state.
func referenceOnGraph(t *testing.T, p float64, cfg EstimateConfig) TaskState {
	t.Helper()
	s, err := NewService(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if err := s.AddNode("a"); err != nil {
		t.Fatal(err)
	}
	if err := s.AddNode("b"); err != nil {
		t.Fatal(err)
	}
	if err := s.AddEdge("a", "b", p); err != nil {
		t.Fatal(err)
	}
	id, err := s.SubmitEstimation(cfg)
	if err != nil {
		t.Fatal(err)
	}
	st := waitDone(t, s, id)
	if st.Status != StatusCompleted {
		t.Fatalf("reference status = %s err = %s", st.Status, st.Error)
	}
	return st
}

// TestUnrestorableBindingFailsExplicitly deletes the bound snapshot and
// changes the current graph while the process is down. Recovery cannot
// restore the bound content, so the task must fail explicitly with
// ErrGraphSnapshotUnavailable instead of silently running on the new graph.
func TestUnrestorableBindingFailsExplicitly(t *testing.T) {
	dir := t.TempDir()
	s, err := NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	sampleGraph(t, s)
	id, err := s.SubmitEstimation(longEstCfg())
	if err != nil {
		t.Fatal(err)
	}
	crashAfterProgress(t, s, id)

	// While the process is down: destroy the bound snapshots and leave the
	// current graph file pointing at content that will be changed next.
	if err := os.RemoveAll(filepath.Join(dir, "graphs", "snapshots")); err != nil {
		t.Fatal(err)
	}

	s2, err := NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s2.AddNode("z"); err != nil {
		t.Fatal(err)
	}
	resumed := s2.ResumePending()
	if containsID(resumed, id) {
		t.Fatal("task with unrestorable binding must not be resumed")
	}
	st, err := s2.GetTask(id)
	if err != nil {
		t.Fatal(err)
	}
	if st.Status != StatusFailed {
		t.Fatalf("status = %s, want failed", st.Status)
	}
	if !strings.Contains(st.Error, ErrGraphSnapshotUnavailable.Error()) {
		t.Fatalf("error = %q, want it to identify %q", st.Error, ErrGraphSnapshotUnavailable)
	}
	if st.Estimate != nil {
		t.Fatal("failed task must not carry an estimate")
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}

	// The explicit failure is persisted and survives another restart.
	s3, err := NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s3.Close() })
	st3, err := s3.GetTask(id)
	if err != nil {
		t.Fatal(err)
	}
	if st3.Status != StatusFailed {
		t.Fatalf("status after restart = %s, want failed", st3.Status)
	}
	if containsID(s3.ResumePending(), id) {
		t.Fatal("failed task must never be resumed")
	}
}

// TestMixedGraphBatchesRejected splices statistics simulated on a different
// graph content into a task record. Recovery must detect the provenance
// mismatch and fail the task instead of mixing the foreign statistics in.
func TestMixedGraphBatchesRejected(t *testing.T) {
	cfg := longEstCfg()
	dir := t.TempDir()
	s, err := NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	sampleGraph(t, s)
	id, err := s.SubmitEstimation(cfg)
	if err != nil {
		t.Fatal(err)
	}
	crashAfterProgress(t, s, id)

	// Simulate the same configuration on different graph content.
	g2 := NewGraph()
	for _, n := range []NodeID{"a", "b", "c", "d"} {
		_ = g2.AddNode(n)
	}
	_ = g2.AddEdge("a", "b", 0.1)
	_ = g2.AddEdge("b", "c", 0.1)
	_ = g2.AddEdge("c", "d", 0.1)
	_ = g2.AddEdge("a", "c", 0.1)
	snap2 := g2.Snapshot()
	var foreign []BatchStats
	if _, err := runEstimation(context.Background(), snap2, cfg, nil, func(b BatchStats) {
		foreign = append(foreign, b)
	}); err != nil {
		t.Fatal(err)
	}
	if len(foreign) == 0 || foreign[0].GraphHash == "" {
		t.Fatal("test setup: no foreign batches produced")
	}

	// Splice the foreign statistics into the crashed task's record.
	st, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	rec, err := st.LoadTask(id)
	if err != nil {
		t.Fatal(err)
	}
	rec.Batches = foreign[:2]
	if err := st.SaveTask(rec); err != nil {
		t.Fatal(err)
	}

	s2, err := NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s2.Close() })
	if !containsID(s2.ResumePending(), id) {
		t.Fatal("task with a restorable binding should be resumed")
	}
	st2 := waitDone(t, s2, id)
	if st2.Status != StatusFailed {
		t.Fatalf("status = %s, want failed: mixed statistics must never complete", st2.Status)
	}
	if !strings.Contains(st2.Error, ErrGraphStatsMismatch.Error()) {
		t.Fatalf("error = %q, want it to identify %q", st2.Error, ErrGraphStatsMismatch)
	}
	if st2.Estimate != nil {
		t.Fatal("task with mixed statistics must not carry an estimate")
	}
}

// TestLegacyRecordWithoutSnapshotFails simulates a task record written before
// bound snapshots existed: its recorded graph identity matches no restorable
// content. The record must still load without migration, and the task must
// fail explicitly rather than silently resume against the current graph.
func TestLegacyRecordWithoutSnapshotFails(t *testing.T) {
	dir := t.TempDir()
	s, err := NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	sampleGraph(t, s)
	id, err := s.SubmitEstimation(longEstCfg())
	if err != nil {
		t.Fatal(err)
	}
	crashAfterProgress(t, s, id)

	// Downgrade the record to pre-binding format: an opaque graph identity
	// and batches without provenance.
	st, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	rec, err := st.LoadTask(id)
	if err != nil {
		t.Fatal(err)
	}
	rec.State.GraphHash = strings.Repeat("0", 64)
	for i := range rec.Batches {
		rec.Batches[i].GraphHash = ""
	}
	if err := st.SaveTask(rec); err != nil {
		t.Fatal(err)
	}

	s2, err := NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s2.Close() })
	if got, want := s2.GraphVersion(), uint64(8); got != want {
		t.Fatalf("graph version = %d, want %d (graph data must recover unchanged)", got, want)
	}
	if containsID(s2.ResumePending(), id) {
		t.Fatal("task with an unverifiable binding must not be resumed")
	}
	st2, err := s2.GetTask(id)
	if err != nil {
		t.Fatal(err)
	}
	if st2.Status != StatusFailed {
		t.Fatalf("status = %s, want failed", st2.Status)
	}
	if !strings.Contains(st2.Error, ErrGraphSnapshotUnavailable.Error()) {
		t.Fatalf("error = %q, want it to identify %q", st2.Error, ErrGraphSnapshotUnavailable)
	}
}

// TestResumeFallsBackToCurrentGraphWhenSnapshotMissing covers the case where
// the snapshot files are lost but the current graph still has exactly the
// bound content: the task resumes, stays bit-identical, and the snapshot
// store is backfilled for future restarts.
func TestResumeFallsBackToCurrentGraphWhenSnapshotMissing(t *testing.T) {
	cfg := longEstCfg()
	ref := referenceEstimation(t, cfg)

	dir := t.TempDir()
	s, err := NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	sampleGraph(t, s)
	id, err := s.SubmitEstimation(cfg)
	if err != nil {
		t.Fatal(err)
	}
	crashAfterProgress(t, s, id)
	if err := os.RemoveAll(filepath.Join(dir, "graphs", "snapshots")); err != nil {
		t.Fatal(err)
	}

	s2, err := NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s2.Close() })
	if !containsID(s2.ResumePending(), id) {
		t.Fatal("task should resume when the current graph has the bound content")
	}
	st := waitDone(t, s2, id)
	if st.Status != StatusCompleted {
		t.Fatalf("status = %s err = %s", st.Status, st.Error)
	}
	if *st.Estimate != *ref.Estimate || st.GraphHash != ref.GraphHash {
		t.Fatalf("fallback resume changed the result:\n%+v\n%+v", st.Estimate, ref.Estimate)
	}
	if _, err := os.Stat(filepath.Join(dir, "graphs", "snapshots", ref.GraphHash+".json")); err != nil {
		t.Fatalf("snapshot store was not backfilled: %v", err)
	}
}

// TestGraphHashIgnoresVersionOnlyChanges pins the content-hash semantics:
// the version counter is not part of the graph identity.
func TestGraphHashIgnoresVersionOnlyChanges(t *testing.T) {
	g := NewGraph()
	_ = g.AddNode("a")
	_ = g.AddNode("b")
	_ = g.AddEdge("a", "b", 0.5)
	h1 := g.Snapshot().Hash()
	v1 := g.Version()

	_ = g.AddNode("tmp")
	_ = g.RemoveNode("tmp")
	if g.Version() != v1+2 {
		t.Fatalf("version = %d, want %d", g.Version(), v1+2)
	}
	if h2 := g.Snapshot().Hash(); h2 != h1 {
		t.Fatalf("identical content produced different hashes: %s vs %s", h1, h2)
	}
	if err := g.SetEdgeWeight("a", "b", 0.5); err != nil {
		t.Fatal(err)
	}
	if g.Version() != v1+2 {
		t.Fatal("no-op weight set must not bump the version")
	}
	if err := g.SetEdgeWeight("a", "b", 0.7); err != nil {
		t.Fatal(err)
	}
	if h3 := g.Snapshot().Hash(); h3 == h1 {
		t.Fatal("content change must change the hash")
	}
}

// TestSnapshotStoreRoundTrip covers persistence, integrity verification and
// idempotency of the content-addressed snapshot store.
func TestSnapshotStoreRoundTrip(t *testing.T) {
	dir := t.TempDir()
	st, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	g := NewGraph()
	_ = g.AddNode("a")
	_ = g.AddNode("b")
	_ = g.AddEdge("a", "b", 0.25)
	snap := g.Snapshot()

	if err := st.SaveSnapshot(snap); err != nil {
		t.Fatal(err)
	}
	if err := st.SaveSnapshot(snap); err != nil {
		t.Fatal("save must be idempotent")
	}
	got, err := st.LoadSnapshot(snap.Hash())
	if err != nil {
		t.Fatal(err)
	}
	if got.Hash() != snap.Hash() || got.NodeCount() != 2 || got.EdgeCount() != 1 {
		t.Fatalf("snapshot round trip mismatch: %+v", got)
	}
	if e, ok := got.Edge("a", "b"); !ok || e.Weight != 0.25 {
		t.Fatalf("edge content mismatch: %+v %v", e, ok)
	}

	if _, err := st.LoadSnapshot(strings.Repeat("1", 64)); err == nil {
		t.Fatal("unknown hash must fail")
	}
	if _, err := st.LoadSnapshot("not-a-hash"); err == nil {
		t.Fatal("malformed hash must fail")
	}

	// Tampered content must be detected by the integrity check.
	rel := filepath.Join("graphs", "snapshots", snap.Hash()+".json")
	if err := st.writeJSON(rel, snapshotData{Nodes: []NodeID{"x"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.LoadSnapshot(snap.Hash()); err == nil {
		t.Fatal("tampered snapshot must fail the integrity check")
	}
}
