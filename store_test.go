package influence

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestFileStoreRoundTrip(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	g := influenceTestGraph()
	ctx := context.Background()
	if err := store.SaveGraph(ctx, g); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.LoadGraph(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Version() != g.Version() || len(loaded.Nodes()) != len(g.Nodes()) {
		t.Fatalf("graph round trip mismatch: %+v", loaded)
	}
	key := BatchKey{
		GraphVersion: g.Version(), Model: IndependentCascade, Seed: 7,
		Rounds: 3, BatchIndex: 2, Seeds: "a",
	}
	stats := BatchStats{Mean: 12.5, SumSq: 60, Count: 3}
	if err := store.SaveBatch(ctx, key, stats); err != nil {
		t.Fatal(err)
	}
	got, ok, err := store.LoadBatch(ctx, key)
	if err != nil || !ok || got != stats {
		t.Fatalf("batch round trip failed: ok=%v got=%+v err=%v", ok, got, err)
	}
	// Different graph version must miss (explicit invalidation).
	oldKey := key
	oldKey.GraphVersion++
	if _, ok, err := store.LoadBatch(ctx, oldKey); err != nil || ok {
		t.Fatalf("old-version batch must not be reused: ok=%v err=%v", ok, err)
	}
}

func TestFileStoreNoTornFilesAfterAtomicWrite(t *testing.T) {
	dir := t.TempDir()
	store, err := OpenFileStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	g := influenceTestGraph()
	if err := store.SaveGraph(context.Background(), g); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".tmp" {
			t.Fatalf("temp file left behind: %s", e.Name())
		}
	}
}
