package influence

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func countBatchFiles(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(dir, "batches"))
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".json" {
			n++
		}
	}
	return n
}

func waitStatus(t *testing.T, s *Service, id string, want TaskStatus) TaskState {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		st, err := s.GetTask(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if st.Status == want {
			return st
		}
		time.Sleep(2 * time.Millisecond)
	}
	st, _ := s.GetTask(context.Background(), id)
	t.Fatalf("task %s status = %s, want %s", id, st.Status, want)
	return st
}

func TestServiceEstimateEndToEnd(t *testing.T) {
	store, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	g := influenceTestGraph()
	ctx := context.Background()
	if err := store.SaveGraph(ctx, g); err != nil {
		t.Fatal(err)
	}
	svc := NewService(store, WithWorkers(2))
	if err := svc.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer svc.Close()

	id, err := svc.SubmitEstimate(ctx, EstimateRequest{
		Seeds:   []NodeID{"a"},
		Config:  ModelConfig{Model: IndependentCascade, Rounds: 10, Seed: 321},
		Batches: 4, Workers: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, svc, id, TaskCompleted)
	var res EstimateResult
	if err := svc.TaskResult(ctx, id, &res); err != nil {
		t.Fatal(err)
	}
	if res.Samples != 40 || res.Mean <= 1 {
		t.Fatalf("bad result: %+v", res)
	}

	// Determinism across restart: a second service on the same store
	// recomputes the same request and gets the identical result, with
	// every batch served from the persisted cache.
	svc.Close()
	store2, err := OpenFileStore(store.dir)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := Estimate(EstimateRequest{
		Graph:   g,
		Seeds:   []NodeID{"a"},
		Config:  ModelConfig{Model: IndependentCascade, Rounds: 10, Seed: 321},
		Batches: 4, Workers: 2, Store: store2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if restarted != res {
		t.Fatalf("post-restart estimate mismatch:\n%+v\n%+v", res, restarted)
	}
}

func TestServiceSelectionEndToEnd(t *testing.T) {
	store, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	g := influenceTestGraph()
	ctx := context.Background()
	_ = store.SaveGraph(ctx, g)
	svc := NewService(store)
	if err := svc.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer svc.Close()

	id, err := svc.SubmitSelection(ctx, SelectRequest{
		Config:  ModelConfig{Model: IndependentCascade, Rounds: 8, Seed: 42},
		Batches: 2, Budget: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, svc, id, TaskCompleted)
	var res SelectResult
	if err := svc.TaskResult(ctx, id, &res); err != nil {
		t.Fatal(err)
	}
	if len(res.Seeds) != 2 {
		t.Fatalf("want 2 seeds, got %v", res.Seeds)
	}
}

func TestServiceCrashResume(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	g := influenceTestGraph()
	ctx := context.Background()
	_ = store.SaveGraph(ctx, g)

	svc := NewService(store)
	if err := svc.Start(ctx); err != nil {
		t.Fatal(err)
	}
	id, err := svc.SubmitEstimate(ctx, EstimateRequest{
		Seeds:   []NodeID{"a"},
		Config:  ModelConfig{Model: IndependentCascade, Rounds: 5, Seed: 77},
		Batches: 6,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Simulate a hard crash: drop the service without Close and force
	// the persisted status back to "running".
	st, _, _ := store.LoadTask(ctx, id)
	for st.Status != TaskRunning {
		st, _, _ = store.LoadTask(ctx, id)
		time.Sleep(time.Millisecond)
	}
	// Wait until at least one completed batch is durable, then kill the
	// process: worker goroutines stop abruptly with status left
	// "running" on disk, exactly like a real crash.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if countBatchFiles(t, store.dir) > 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if countBatchFiles(t, store.dir) == 0 {
		t.Fatal("no batch became durable before crash")
	}
	close(svc.stop)
	svc.wg.Wait()

	// New process: the interrupted task must be recovered and finish,
	// reusing already completed batches.
	store2, err := OpenFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	svc2 := NewService(store2)
	if err := svc2.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer svc2.Close()
	waitStatus(t, svc2, id, TaskCompleted)
	var res EstimateResult
	if err := svc2.TaskResult(ctx, id, &res); err != nil {
		t.Fatal(err)
	}
	if res.Samples != 30 {
		t.Fatalf("samples = %d want 30", res.Samples)
	}
}

func TestServiceCancel(t *testing.T) {
	store, err := OpenFileStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	g := influenceTestGraph()
	ctx := context.Background()
	_ = store.SaveGraph(ctx, g)
	svc := NewService(store)
	_ = svc.Start(ctx)
	defer svc.Close()

	id, err := svc.SubmitEstimate(ctx, EstimateRequest{
		Seeds:   []NodeID{"a"},
		Config:  ModelConfig{Model: IndependentCascade, Rounds: 100000, Seed: 9},
		Batches: 100000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.CancelTask(ctx, id); err != nil {
		t.Fatal(err)
	}
	st := waitStatus(t, svc, id, TaskCanceled)
	if len(st.Result) != 0 {
		t.Fatal("canceled task must not carry a partial result")
	}
	// Resuming must not resurrect a canceled task.
	if err := svc.Resume(ctx); err != nil {
		t.Fatal(err)
	}
	st2, _ := svc.GetTask(ctx, id)
	if st2.Status != TaskCanceled {
		t.Fatalf("canceled task resurrected as %s", st2.Status)
	}
}

func TestServiceGraphUpdateInvalidates(t *testing.T) {
	dir := t.TempDir()
	store, _ := OpenFileStore(dir)
	g := influenceTestGraph()
	ctx := context.Background()
	_ = store.SaveGraph(ctx, g)
	svc := NewService(store)
	_ = svc.Start(ctx)
	defer svc.Close()

	id, err := svc.SubmitEstimate(ctx, EstimateRequest{
		Seeds:   []NodeID{"a"},
		Config:  ModelConfig{Model: IndependentCascade, Rounds: 6, Seed: 3},
		Batches: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, svc, id, TaskCompleted)

	// Online update: bump weight on a->b. The running task stays pinned
	// to the old version; new submissions use the new version and must
	// not reuse old-version cached batches.
	b := NewGraphBuilder(g)
	g2, err := b.SetWeight("a", "b", 0.05)
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.UpdateGraph(ctx, g2); err != nil {
		t.Fatal(err)
	}
	id2, err := svc.SubmitEstimate(ctx, EstimateRequest{
		Graph:   g2,
		Seeds:   []NodeID{"a"},
		Config:  ModelConfig{Model: IndependentCascade, Rounds: 6, Seed: 3},
		Batches: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, svc, id2, TaskCompleted)

	var r1, r2 EstimateResult
	if err := svc.TaskResult(ctx, id, &r1); err != nil {
		t.Fatal(err)
	}
	if err := svc.TaskResult(ctx, id2, &r2); err != nil {
		t.Fatal(err)
	}
	if r1.GraphVersion == r2.GraphVersion {
		t.Fatal("updated task must run on a new graph version")
	}
	if r1.Mean <= r2.Mean {
		t.Fatalf("weakening a->b should reduce spread: old=%.3f new=%.3f", r1.Mean, r2.Mean)
	}
}
