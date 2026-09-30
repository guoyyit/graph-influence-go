package influence

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Store persists graphs, simulation batch statistics and task state so
// that after a crash unfinished tasks resume and completed simulation
// batches are never recomputed.
type Store interface {
	LoadGraph(ctx context.Context) (*Graph, error)
	SaveGraph(ctx context.Context, g *Graph) error

	LoadBatch(ctx context.Context, key BatchKey) (BatchStats, bool, error)
	SaveBatch(ctx context.Context, key BatchKey, stats BatchStats) error

	LoadTask(ctx context.Context, id string) (TaskState, bool, error)
	SaveTask(ctx context.Context, state TaskState) error
	ListTasks(ctx context.Context) ([]TaskState, error)
}

// BatchKey uniquely identifies a deterministic Monte Carlo batch.
type BatchKey struct {
	GraphVersion   int64     `json:"v"`
	Model          ModelKind `json:"m"`
	Seed           int64     `json:"s"`
	Rounds         int       `json:"r"`
	BatchIndex     int       `json:"b"`
	Seeds          string    `json:"z"`
	FixedThreshold float64   `json:"ft,omitempty"`
}

// BatchStats holds the aggregate of one simulation batch.
type BatchStats struct {
	Mean  float64 `json:"mean"`
	SumSq float64 `json:"sumsq"`
	Count int     `json:"count"`
}

// TaskStatus enumerates persisted task lifecycle states.
type TaskStatus string

const (
	TaskPending   TaskStatus = "pending"
	TaskRunning   TaskStatus = "running"
	TaskCompleted TaskStatus = "completed"
	TaskCanceled  TaskStatus = "canceled"
	TaskFailed    TaskStatus = "failed"
)

// TaskState is the persisted resume point of an async job.
type TaskState struct {
	ID           string     `json:"id"`
	Kind         string     `json:"kind"`
	Status       TaskStatus `json:"status"`
	GraphVersion int64      `json:"graph_version"`
	DoneBatches  int        `json:"done_batches"`
	TotalBatches int        `json:"total_batches"`
	Payload      []byte     `json:"payload,omitempty"`
	Result       []byte     `json:"result,omitempty"`
	Error        string     `json:"error,omitempty"`
	UpdatedAt    int64      `json:"updated_at"`
}

// graphDoc is the on-disk graph representation.
type graphDoc struct {
	Version int64     `json:"version"`
	Nodes   []string  `json:"nodes"`
	Edges   []edgeDoc `json:"edges"`
}

type edgeDoc struct {
	From   string  `json:"from"`
	To     string  `json:"to"`
	Weight float64 `json:"weight"`
}

// FileStore is a local-directory, file-backed Store. Writes are atomic
// (temp file + rename) and guarded by a process-local mutex, so a crash
// leaves either the previous or the new complete record, never a torn
// one.
type FileStore struct {
	dir string
	mu  sync.Mutex
}

// OpenFileStore opens (creating if needed) a persistence directory.
func OpenFileStore(dir string) (*FileStore, error) {
	if dir == "" {
		return nil, newError(ErrPersist, "store directory must not be empty")
	}
	if err := os.MkdirAll(filepath.Join(dir, "batches"), 0o755); err != nil {
		return nil, newError(ErrPersist, "create store dir: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "tasks"), 0o755); err != nil {
		return nil, newError(ErrPersist, "create tasks dir: %v", err)
	}
	return &FileStore{dir: dir}, nil
}

func (s *FileStore) Close() error { return nil }

func atomicWriteJSON(path string, v any) (err error) {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return newError(ErrPersist, "marshal: %v", err)
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return newError(ErrPersist, "write %s: %v", path, err)
	}
	defer func() {
		if err != nil {
			_ = f.Close()
			_ = os.Remove(tmp)
		}
	}()
	if _, err = f.Write(data); err != nil {
		return newError(ErrPersist, "write %s: %v", path, err)
	}
	if err = f.Sync(); err != nil {
		return newError(ErrPersist, "sync %s: %v", path, err)
	}
	if err = f.Close(); err != nil {
		return newError(ErrPersist, "close %s: %v", path, err)
	}
	if err = os.Rename(tmp, path); err != nil {
		return newError(ErrPersist, "rename %s: %v", path, err)
	}
	return nil
}

func (s *FileStore) SaveGraph(ctx context.Context, g *Graph) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	doc := graphDoc{Version: g.version}
	for _, id := range g.Nodes() {
		doc.Nodes = append(doc.Nodes, string(id))
	}
	for _, from := range g.Nodes() {
		for _, e := range g.OutEdges(from) {
			doc.Edges = append(doc.Edges, edgeDoc{From: string(e.From), To: string(e.To), Weight: e.Weight})
		}
	}
	return atomicWriteJSON(filepath.Join(s.dir, "graph.json"), doc)
}

func (s *FileStore) LoadGraph(ctx context.Context) (*Graph, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	path := filepath.Join(s.dir, "graph.json")
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, newError(ErrPersist, "read graph: %v", err)
	}
	var doc graphDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, newError(ErrPersist, "decode graph: %v", err)
	}
	nodes := make([]NodeID, len(doc.Nodes))
	for i, n := range doc.Nodes {
		nodes[i] = NodeID(n)
	}
	edges := make([]Edge, len(doc.Edges))
	for i, e := range doc.Edges {
		edges[i] = Edge{From: NodeID(e.From), To: NodeID(e.To), Weight: e.Weight}
	}
	g, err := NewGraph(nodes, edges)
	if err != nil {
		return nil, err
	}
	g.version = doc.Version
	return g, nil
}

func batchFileName(key BatchKey) string {
	// Human-readable prefix for debuggability plus a hash of the seed
	// set, which may contain arbitrary characters.
	sum := sha256.Sum256([]byte(key.Seeds))
	th := ""
	if key.FixedThreshold != 0 {
		th = "_ft" + strconv.FormatFloat(key.FixedThreshold, 'g', -1, 64)
	}
	return fmt.Sprintf("v%d_%s_seed%d_r%d_b%d%s_%s.json",
		key.GraphVersion, key.Model, key.Seed, key.Rounds, key.BatchIndex,
		th, hex.EncodeToString(sum[:8]))
}

func (s *FileStore) SaveBatch(ctx context.Context, key BatchKey, stats BatchStats) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec := struct {
		Key   BatchKey   `json:"key"`
		Stats BatchStats `json:"stats"`
	}{Key: key, Stats: stats}
	path := filepath.Join(s.dir, "batches", batchFileName(key))
	return atomicWriteJSON(path, rec)
}

func (s *FileStore) LoadBatch(ctx context.Context, key BatchKey) (BatchStats, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	path := filepath.Join(s.dir, "batches", batchFileName(key))
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return BatchStats{}, false, nil
	}
	if err != nil {
		return BatchStats{}, false, newError(ErrPersist, "read batch: %v", err)
	}
	var rec struct {
		Key   BatchKey   `json:"key"`
		Stats BatchStats `json:"stats"`
	}
	if err := json.Unmarshal(data, &rec); err != nil {
		return BatchStats{}, false, newError(ErrPersist, "decode batch: %v", err)
	}
	if rec.Key != key {
		return BatchStats{}, false, nil
	}
	return rec.Stats, true, nil
}

func (s *FileStore) SaveTask(ctx context.Context, state TaskState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if state.UpdatedAt == 0 {
		state.UpdatedAt = time.Now().UnixNano()
	}
	return atomicWriteJSON(filepath.Join(s.dir, "tasks", safeName(state.ID)+".json"), state)
}

func (s *FileStore) LoadTask(ctx context.Context, id string) (TaskState, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loadTaskLocked(id)
}

func (s *FileStore) loadTaskLocked(id string) (TaskState, bool, error) {
	path := filepath.Join(s.dir, "tasks", safeName(id)+".json")
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return TaskState{}, false, nil
	}
	if err != nil {
		return TaskState{}, false, newError(ErrPersist, "read task: %v", err)
	}
	var st TaskState
	if err := json.Unmarshal(data, &st); err != nil {
		return TaskState{}, false, newError(ErrPersist, "decode task: %v", err)
	}
	return st, true, nil
}

func (s *FileStore) ListTasks(ctx context.Context) ([]TaskState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := os.ReadDir(filepath.Join(s.dir, "tasks"))
	if err != nil {
		return nil, newError(ErrPersist, "list tasks: %v", err)
	}
	var out []TaskState
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		id := strings.TrimSuffix(e.Name(), ".json")
		st, ok, err := s.loadTaskLocked(id)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, st)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func safeName(id string) string {
	var b strings.Builder
	for _, r := range id {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	if b.Len() == 0 {
		return "task"
	}
	return b.String()
}
