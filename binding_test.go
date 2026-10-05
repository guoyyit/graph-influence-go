package influence

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// newServiceWithGraph opens a service on a fresh dir and builds the sample
// graph (4 nodes, 4 edges, version 8).
func newServiceWithGraph(t *testing.T, dir string) *Service {
	t.Helper()
	s, err := NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	sampleGraph(t, s)
	return s
}

// waitForBatches blocks until at least n batches of the task are persisted.
// The condition is monotonic and guaranteed to occur for a running task; the
// deadline is only a guard against a hung implementation.
func waitForBatches(t *testing.T, s *Service, id string, n int) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		rec, err := s.store.LoadTask(id)
		if err == nil && len(rec.Batches) >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d persisted batches of task %s", n, id)
}

// runReferenceEstimation runs cfg to completion on the sample graph in a
// fresh dir and returns the uninterrupted reference result.
func runReferenceEstimation(t *testing.T, cfg EstimateConfig) (EstimateResult, TaskState) {
	t.Helper()
	s := newServiceWithGraph(t, t.TempDir())
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
	return *st.Estimate, st
}

// TestResumeAfterGraphModifyBitIdentical interrupts an estimation, modifies
// the graph while the process is down, then resumes. The result must be
// bit-identical to an uninterrupted run on the original graph, and the task
// must keep the identity of the graph it was bound to at submission.
func TestResumeAfterGraphModifyBitIdentical(t *testing.T) {
	cfg := estCfg()
	cfg.Rounds = 10000
	cfg.BatchSize = 100

	ref, refState := runReferenceEstimation(t, cfg)

	dir := t.TempDir()
	s := newServiceWithGraph(t, dir)
	id, err := s.SubmitEstimation(cfg)
	if err != nil {
		t.Fatal(err)
	}
	waitForBatches(t, s, id, 1)
	if st, _ := s.GetTask(id); st.Status == StatusCompleted {
		t.Fatal("task completed before interruption; increase rounds")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Restart, modify nodes, edges and weights while the task is pending.
	s2, err := NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s2.SetEdgeWeight("a", "b", 0.01); err != nil {
		t.Fatal(err)
	}
	if err := s2.AddNode("e"); err != nil {
		t.Fatal(err)
	}
	if err := s2.AddEdge("e", "a", 0.5); err != nil {
		t.Fatal(err)
	}
	resumed := s2.ResumePending()
	if len(resumed) != 1 || resumed[0] != id {
		t.Fatalf("resumed = %v, want [%s]", resumed, id)
	}
	st := waitDone(t, s2, id)
	if st.Status != StatusCompleted {
		t.Fatalf("status = %s err = %s", st.Status, st.Error)
	}
	if *st.Estimate != ref {
		t.Fatalf("resumed estimate differs from uninterrupted reference:\n%+v\n%+v", *st.Estimate, ref)
	}
	if st.GraphHash != refState.GraphHash {
		t.Fatalf("graph hash = %s, want submission-time binding %s", st.GraphHash, refState.GraphHash)
	}
	if st.GraphVersion != refState.GraphVersion {
		t.Fatalf("graph version = %d, want %d", st.GraphVersion, refState.GraphVersion)
	}

	// A task submitted after the modification binds the new graph content.
	id2, err := s2.SubmitEstimation(estCfg())
	if err != nil {
		t.Fatal(err)
	}
	st2 := waitDone(t, s2, id2)
	if st2.Status != StatusCompleted {
		t.Fatalf("status = %s err = %s", st2.Status, st2.Error)
	}
	if st2.GraphHash == st.GraphHash {
		t.Fatal("new task must bind the modified graph content")
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestSelectionResumeAfterGraphModifyBitIdentical applies the same
// interrupt-modify-resume scenario to a seed selection task: every CELF
// evaluation must come from the graph content bound at submission.
func TestSelectionResumeAfterGraphModifyBitIdentical(t *testing.T) {
	cfg := SelectionConfig{
		Model:      ModelConfig{Model: ModelIndependentCascade, UseEdgeWeights: true},
		K:          2,
		Rounds:     10000,
		BatchSize:  200,
		Seed:       5,
		Confidence: 0.95,
		Candidates: []NodeID{"a", "b", "c", "d"},
	}

	// Uninterrupted reference on the original graph.
	rs := newServiceWithGraph(t, t.TempDir())
	refID, err := rs.SubmitSelection(cfg)
	if err != nil {
		t.Fatal(err)
	}
	refState := waitDone(t, rs, refID)
	if refState.Status != StatusCompleted {
		t.Fatalf("reference status = %s err = %s", refState.Status, refState.Error)
	}
	if err := rs.Close(); err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	s := newServiceWithGraph(t, dir)
	id, err := s.SubmitSelection(cfg)
	if err != nil {
		t.Fatal(err)
	}
	waitForBatches(t, s, id, 1)
	if st, _ := s.GetTask(id); st.Status == StatusCompleted {
		t.Fatal("task completed before interruption; increase rounds")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s2.RemoveEdge("a", "b"); err != nil {
		t.Fatal(err)
	}
	if err := s2.SetEdgeWeight("b", "c", 0.05); err != nil {
		t.Fatal(err)
	}
	s2.ResumePending()
	st := waitDone(t, s2, id)
	if st.Status != StatusCompleted {
		t.Fatalf("status = %s err = %s", st.Status, st.Error)
	}
	if !reflect.DeepEqual(st.Selection, refState.Selection) {
		t.Fatalf("resumed selection differs from uninterrupted reference:\n%+v\n%+v", st.Selection, refState.Selection)
	}
	if st.GraphHash != refState.GraphHash || st.GraphVersion != refState.GraphVersion {
		t.Fatalf("binding drifted: (%s,%d) vs (%s,%d)",
			st.GraphHash, st.GraphVersion, refState.GraphHash, refState.GraphVersion)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
}

// writeLegacyRecord persists a task record in the pre-binding format (no
// bound_snapshot flag, version-scoped hash) to simulate an upgrade from an
// older release.
func writeLegacyRecord(t *testing.T, s *Service, id string, version uint64, cfg EstimateConfig) {
	t.Helper()
	now := time.Now().UTC()
	rec := taskRecord{
		State: TaskState{
			ID:           id,
			Kind:         TaskEstimation,
			Status:       StatusRunning,
			GraphVersion: version,
			GraphHash:    "legacy-version-scoped-hash",
			CreatedAt:    now,
			UpdatedAt:    now,
		},
		EstimateCfg: &cfg,
	}
	if err := s.store.SaveTask(rec); err != nil {
		t.Fatal(err)
	}
}

// TestLegacyRecordGraphChangedFails: a pre-upgrade record whose recorded
// graph version no longer matches the persisted graph cannot be resumed
// consistently and must fail with a distinguishable reason.
func TestLegacyRecordGraphChangedFails(t *testing.T) {
	dir := t.TempDir()
	s := newServiceWithGraph(t, dir) // sample graph at version 8
	writeLegacyRecord(t, s, "legacy-task", 8, estCfg())
	// The graph is modified after the legacy record was written.
	if err := s.AddNode("e"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	st, err := s2.GetTask("legacy-task")
	if err != nil {
		t.Fatal(err)
	}
	if st.Status != StatusFailed {
		t.Fatalf("status = %s, want failed", st.Status)
	}
	if !strings.Contains(st.Error, ErrGraphBindingLost.Error()) {
		t.Fatalf("error = %q, want ErrGraphBindingLost", st.Error)
	}
	if ids := s2.ResumePending(); len(ids) != 0 {
		t.Fatalf("failed task must not be resumed: %v", ids)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestLegacyRecordGraphUnchangedResumes: a pre-upgrade record whose recorded
// version still matches the persisted graph is rebound to the current content
// (and its record upgraded) without manual migration; the result is
// bit-identical to a fresh run.
func TestLegacyRecordGraphUnchangedResumes(t *testing.T) {
	cfg := estCfg()
	ref, refState := runReferenceEstimation(t, cfg)

	dir := t.TempDir()
	s := newServiceWithGraph(t, dir)
	writeLegacyRecord(t, s, "legacy-ok", 8, cfg)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	rec, err := s2.store.LoadTask("legacy-ok")
	if err != nil {
		t.Fatal(err)
	}
	if !rec.BoundSnapshot {
		t.Fatal("legacy record was not upgraded to a bound snapshot")
	}
	if rec.State.GraphHash != refState.GraphHash {
		t.Fatalf("rebound hash = %s, want %s", rec.State.GraphHash, refState.GraphHash)
	}
	s2.ResumePending()
	st := waitDone(t, s2, "legacy-ok")
	if st.Status != StatusCompleted {
		t.Fatalf("status = %s err = %s", st.Status, st.Error)
	}
	if *st.Estimate != ref {
		t.Fatalf("resumed legacy estimate differs from reference:\n%+v\n%+v", *st.Estimate, ref)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestSnapshotMissingAndGraphChangedFails: when the bound snapshot file is
// lost and the persisted graph no longer has the bound content, recovery must
// fail the task explicitly.
func TestSnapshotMissingAndGraphChangedFails(t *testing.T) {
	dir := t.TempDir()
	s := newServiceWithGraph(t, dir)
	cfg := estCfg()
	cfg.Rounds = 10000
	cfg.BatchSize = 100
	id, err := s.SubmitEstimation(cfg)
	if err != nil {
		t.Fatal(err)
	}
	waitForBatches(t, s, id, 1)
	st0, err := s.GetTask(id)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Lose the bound snapshot and change the persisted graph content.
	if err := os.Remove(filepath.Join(dir, "graphs", "snapshots", st0.GraphHash+".json")); err != nil {
		t.Fatal(err)
	}
	st, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	g, err := st.LoadGraph()
	if err != nil {
		t.Fatal(err)
	}
	if err := g.AddNode("z"); err != nil {
		t.Fatal(err)
	}
	if err := st.SaveGraph(g.Snapshot()); err != nil {
		t.Fatal(err)
	}
	_ = st.Close()

	s2, err := NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	st2, err := s2.GetTask(id)
	if err != nil {
		t.Fatal(err)
	}
	if st2.Status != StatusFailed {
		t.Fatalf("status = %s, want failed", st2.Status)
	}
	if !strings.Contains(st2.Error, ErrGraphBindingLost.Error()) {
		t.Fatalf("error = %q, want ErrGraphBindingLost", st2.Error)
	}
	if ids := s2.ResumePending(); len(ids) != 0 {
		t.Fatalf("failed task must not be resumed: %v", ids)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestSnapshotMissingGraphUnchangedHeals: a lost snapshot file is repaired
// from the current graph when its content still matches the binding, and the
// task resumes with a bit-identical result.
func TestSnapshotMissingGraphUnchangedHeals(t *testing.T) {
	cfg := estCfg()
	cfg.Rounds = 10000
	cfg.BatchSize = 100
	ref, _ := runReferenceEstimation(t, cfg)

	dir := t.TempDir()
	s := newServiceWithGraph(t, dir)
	id, err := s.SubmitEstimation(cfg)
	if err != nil {
		t.Fatal(err)
	}
	waitForBatches(t, s, id, 1)
	st0, err := s.GetTask(id)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "graphs", "snapshots", st0.GraphHash+".json")); err != nil {
		t.Fatal(err)
	}

	s2, err := NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	s2.ResumePending()
	st := waitDone(t, s2, id)
	if st.Status != StatusCompleted {
		t.Fatalf("status = %s err = %s", st.Status, st.Error)
	}
	if *st.Estimate != ref {
		t.Fatalf("healed estimate differs from reference:\n%+v\n%+v", *st.Estimate, ref)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestSameContentVersionBumpResumes: mutations that restore identical content
// (only the version counter advances) must not invalidate the binding or the
// persisted batches; the resumed result stays bit-identical.
func TestSameContentVersionBumpResumes(t *testing.T) {
	cfg := estCfg()
	cfg.Rounds = 10000
	cfg.BatchSize = 100
	ref, refState := runReferenceEstimation(t, cfg)

	dir := t.TempDir()
	s := newServiceWithGraph(t, dir)
	id, err := s.SubmitEstimation(cfg)
	if err != nil {
		t.Fatal(err)
	}
	waitForBatches(t, s, id, 1)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Restart, then mutate and revert: version advances, content identical.
	s2, err := NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := s2.AddNode("tmp"); err != nil {
		t.Fatal(err)
	}
	if err := s2.RemoveNode("tmp"); err != nil {
		t.Fatal(err)
	}
	if v := s2.GraphVersion(); v != refState.GraphVersion+2 {
		t.Fatalf("version = %d, want %d", v, refState.GraphVersion+2)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}

	s3, err := NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	s3.ResumePending()
	st := waitDone(t, s3, id)
	if st.Status != StatusCompleted {
		t.Fatalf("status = %s err = %s", st.Status, st.Error)
	}
	if *st.Estimate != ref {
		t.Fatalf("estimate after pure version bump differs:\n%+v\n%+v", *st.Estimate, ref)
	}
	if st.GraphHash != refState.GraphHash || st.GraphVersion != refState.GraphVersion {
		t.Fatalf("binding drifted: (%s,%d) vs (%s,%d)",
			st.GraphHash, st.GraphVersion, refState.GraphHash, refState.GraphVersion)
	}
	if err := s3.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestMixedBatchStatsRejected: statistics computed against a different graph
// content are never silently reused; the task fails instead of completing.
func TestMixedBatchStatsRejected(t *testing.T) {
	// Unit level: runEstimation rejects foreign batches.
	g := NewGraph()
	for _, n := range []NodeID{"a", "b", "c", "d"} {
		_ = g.AddNode(n)
	}
	_ = g.AddEdge("a", "b", 0.7)
	snap := g.Snapshot()
	other := NewGraph()
	_ = other.AddNode("x")
	foreign := []BatchStats{{
		Index: 0, SpecKey: "k", Position: 0, Rounds: 16, Sum: 16, SumSq: 16,
		GraphHash: other.Snapshot().Hash(),
	}}
	if _, err := runEstimation(context.Background(), snap, estCfg(), foreign, nil); !errors.Is(err, ErrGraphStatsMixed) {
		t.Fatalf("err = %v, want ErrGraphStatsMixed", err)
	}

	// Service level: a recovered task with tampered persisted batches fails.
	dir := t.TempDir()
	s := newServiceWithGraph(t, dir)
	cfg := estCfg()
	cfg.Rounds = 10000
	cfg.BatchSize = 100
	id, err := s.SubmitEstimation(cfg)
	if err != nil {
		t.Fatal(err)
	}
	waitForBatches(t, s, id, 1)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	st, err := OpenStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	rec, err := st.LoadTask(id)
	if err != nil {
		t.Fatal(err)
	}
	rec.Batches[0].GraphHash = strings.Repeat("0", 64)
	if err := st.SaveTask(rec); err != nil {
		t.Fatal(err)
	}
	_ = st.Close()

	s2, err := NewService(dir)
	if err != nil {
		t.Fatal(err)
	}
	st2, err := s2.GetTask(id)
	if err != nil {
		t.Fatal(err)
	}
	if st2.Status != StatusFailed {
		t.Fatalf("status = %s, want failed", st2.Status)
	}
	if !strings.Contains(st2.Error, ErrGraphStatsMixed.Error()) {
		t.Fatalf("error = %q, want ErrGraphStatsMixed", st2.Error)
	}
	if ids := s2.ResumePending(); len(ids) != 0 {
		t.Fatalf("failed task must not be resumed: %v", ids)
	}
	if err := s2.Close(); err != nil {
		t.Fatal(err)
	}
}

// TestConcurrentSubmitAndGraphUpdate: submissions racing a graph update bind
// either the pre- or the post-update content; each result is bit-identical to
// the reference of its recorded binding, and tasks still running across the
// update keep their original binding.
func TestConcurrentSubmitAndGraphUpdate(t *testing.T) {
	cfg := estCfg()
	cfg.Rounds = 4000
	cfg.BatchSize = 100

	// Reference for graph A (sample graph).
	refA, stateA := runReferenceEstimation(t, cfg)

	// Reference for graph B (weight a->b lowered).
	sb := newServiceWithGraph(t, t.TempDir())
	if err := sb.SetEdgeWeight("a", "b", 0.01); err != nil {
		t.Fatal(err)
	}
	idB, err := sb.SubmitEstimation(cfg)
	if err != nil {
		t.Fatal(err)
	}
	stB := waitDone(t, sb, idB)
	if stB.Status != StatusCompleted {
		t.Fatalf("reference B status = %s err = %s", stB.Status, stB.Error)
	}
	refB := *stB.Estimate
	if err := sb.Close(); err != nil {
		t.Fatal(err)
	}
	if stB.GraphHash == stateA.GraphHash {
		t.Fatal("test requires distinct graph contents")
	}

	s := newServiceWithGraph(t, t.TempDir())
	defer func() { _ = s.Close() }()

	submit := func() (string, error) { return s.SubmitEstimation(cfg) }

	// Batch 1 binds graph A and is still running when the update lands.
	const n1, n2 = 4, 4
	ids1 := make([]string, n1)
	for i := range ids1 {
		ids1[i], err = submit()
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := s.SetEdgeWeight("a", "b", 0.01); err != nil {
		t.Fatal(err)
	}
	// Batch 2 binds graph B.
	ids2 := make([]string, n2)
	for i := range ids2 {
		ids2[i], err = submit()
		if err != nil {
			t.Fatal(err)
		}
	}

	for _, id := range ids1 {
		st := waitDone(t, s, id)
		if st.Status != StatusCompleted {
			t.Fatalf("status = %s err = %s", st.Status, st.Error)
		}
		if st.GraphHash != stateA.GraphHash || *st.Estimate != refA {
			t.Fatalf("pre-update task not bit-identical to graph A reference: %+v", st)
		}
	}
	for _, id := range ids2 {
		st := waitDone(t, s, id)
		if st.Status != StatusCompleted {
			t.Fatalf("status = %s err = %s", st.Status, st.Error)
		}
		if st.GraphHash != stB.GraphHash || *st.Estimate != refB {
			t.Fatalf("post-update task not bit-identical to graph B reference: %+v", st)
		}
	}
}
