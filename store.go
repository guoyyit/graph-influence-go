package influence

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// Store persists graph snapshots, simulation batches and task state on the
// local filesystem. All writes use temp-file + fsync + atomic rename, so a
// crash never leaves torn files: a completed batch or task is either fully
// visible or absent and is recomputed after restart.
type Store struct {
	dir string
	mu  sync.RWMutex
}

// OpenStore opens (or initialises) a persistence directory.
func OpenStore(dir string) (*Store, error) {
	if dir == "" {
		return nil, errors.New("influence: store directory must not be empty")
	}
	for _, sub := range []string{"", "graphs", "graphs/snapshots", "tasks", "batches"} {
		if err := os.MkdirAll(filepath.Join(dir, sub), 0o755); err != nil {
			return nil, err
		}
	}
	return &Store{dir: dir}, nil
}

// Close releases store resources.
func (s *Store) Close() error { return nil }

// graphData is the on-disk serialisation of a graph.
type graphData struct {
	Version uint64     `json:"version"`
	Nodes   []NodeID   `json:"nodes"`
	Edges   []edgeData `json:"edges"`
}

type edgeData struct {
	From   NodeID  `json:"from"`
	To     NodeID  `json:"to"`
	Weight float64 `json:"weight"`
}

// SaveGraph atomically persists the current graph snapshot.
func (s *Store) SaveGraph(snap *GraphSnapshot) error {
	data := graphData{Version: snap.version, Nodes: snap.Nodes()}
	for _, from := range snap.nodes {
		for _, e := range snap.out[from] {
			data.Edges = append(data.Edges, edgeData{From: e.From, To: e.To, Weight: e.Weight})
		}
	}
	return s.writeJSON(filepath.Join("graphs", "current.json"), data)
}

// LoadGraph loads the persisted graph. It returns nil when none exists yet.
func (s *Store) LoadGraph() (*Graph, error) {
	var data graphData
	if err := s.readJSON(filepath.Join("graphs", "current.json"), &data); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	g := NewGraph()
	for _, n := range data.Nodes {
		if err := g.AddNode(n); err != nil {
			return nil, fmt.Errorf("influence: corrupt graph: %w", err)
		}
	}
	for _, e := range data.Edges {
		if err := g.AddEdge(e.From, e.To, e.Weight); err != nil {
			return nil, fmt.Errorf("influence: corrupt graph: %w", err)
		}
	}
	// Rebuild to the exact recorded version: an empty saved graph still
	// carries a meaningful version after node removals.
	g.version = data.Version
	return g, nil
}

// snapshotData is the on-disk serialisation of one immutable, task-bound
// graph snapshot. Snapshots are content-addressed: the file name is the
// content hash, so identical graph content is stored exactly once no matter
// how the version counter moved. The version counter is intentionally not
// part of a snapshot's identity.
type snapshotData struct {
	Nodes []NodeID   `json:"nodes"`
	Edges []edgeData `json:"edges"`
}

// snapshotPath maps a content hash to its snapshot file. The hash must be a
// lowercase hex digest as produced by GraphSnapshot.Hash; anything else is
// rejected so a corrupt or hostile task record cannot escape the snapshots
// directory.
func snapshotPath(hash string) (string, error) {
	if len(hash) != 64 {
		return "", fmt.Errorf("influence: malformed graph hash %q", hash)
	}
	for _, c := range hash {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return "", fmt.Errorf("influence: malformed graph hash %q", hash)
		}
	}
	return filepath.Join("graphs", "snapshots", hash+".json"), nil
}

// SaveSnapshot atomically persists an immutable graph snapshot keyed by its
// content hash. It is idempotent: an existing file already holds exactly this
// content, so it is left untouched and no work is repeated.
func (s *Store) SaveSnapshot(snap *GraphSnapshot) error {
	rel, err := snapshotPath(snap.hash)
	if err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(s.dir, rel)); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	data := snapshotData{Nodes: snap.Nodes()}
	for _, from := range snap.nodes {
		for _, e := range snap.out[from] {
			data.Edges = append(data.Edges, edgeData{From: e.From, To: e.To, Weight: e.Weight})
		}
	}
	return s.writeJSON(rel, data)
}

// LoadSnapshot restores the immutable snapshot bound to a content hash. The
// reconstructed content is re-hashed and compared against the requested hash,
// so a corrupted or tampered file is reported instead of being silently used.
func (s *Store) LoadSnapshot(hash string) (*GraphSnapshot, error) {
	rel, err := snapshotPath(hash)
	if err != nil {
		return nil, err
	}
	var data snapshotData
	if err := s.readJSON(rel, &data); err != nil {
		return nil, err
	}
	g := NewGraph()
	for _, n := range data.Nodes {
		if err := g.AddNode(n); err != nil {
			return nil, fmt.Errorf("influence: corrupt snapshot %s: %w", hash, err)
		}
	}
	for _, e := range data.Edges {
		if err := g.AddEdge(e.From, e.To, e.Weight); err != nil {
			return nil, fmt.Errorf("influence: corrupt snapshot %s: %w", hash, err)
		}
	}
	snap := g.Snapshot()
	if snap.Hash() != hash {
		return nil, fmt.Errorf("influence: snapshot content does not match hash %s", hash)
	}
	return snap, nil
}

// taskRecord is the on-disk serialisation of a task and its progress.
type taskRecord struct {
	State       TaskState        `json:"state"`
	EstimateCfg *EstimateConfig  `json:"estimate_cfg,omitempty"`
	SelectCfg   *SelectionConfig `json:"select_cfg,omitempty"`
	Batches     []BatchStats     `json:"batches,omitempty"`
}

// SaveTask atomically persists a task record.
func (s *Store) SaveTask(rec taskRecord) error {
	sort.Slice(rec.Batches, func(i, j int) bool { return rec.Batches[i].Index < rec.Batches[j].Index })
	return s.writeJSON(filepath.Join("tasks", rec.State.ID+".json"), rec)
}

// LoadTask reads one task record.
func (s *Store) LoadTask(id string) (taskRecord, error) {
	var rec taskRecord
	if err := s.readJSON(filepath.Join("tasks", id+".json"), &rec); err != nil {
		return taskRecord{}, err
	}
	return rec, nil
}

// ListTasks reads all task records sorted by ID for deterministic recovery.
func (s *Store) ListTasks() ([]taskRecord, error) {
	entries, err := os.ReadDir(filepath.Join(s.dir, "tasks"))
	if err != nil {
		return nil, err
	}
	var recs []taskRecord
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		id := strings.TrimSuffix(entry.Name(), ".json")
		rec, err := s.LoadTask(id)
		if err != nil {
			return nil, err
		}
		recs = append(recs, rec)
	}
	sort.Slice(recs, func(i, j int) bool { return recs[i].State.ID < recs[j].State.ID })
	return recs, nil
}

// DeleteTask removes a task record.
func (s *Store) DeleteTask(id string) error {
	err := os.Remove(filepath.Join(s.dir, "tasks", id+".json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (s *Store) writeJSON(rel string, v any) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	final := filepath.Join(s.dir, rel)
	if err := os.MkdirAll(filepath.Dir(final), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(final), ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	enc := json.NewEncoder(tmp)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	return os.Rename(tmpName, final)
}

func (s *Store) readJSON(rel string, v any) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	f, err := os.Open(filepath.Join(s.dir, rel))
	if err != nil {
		return err
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}
