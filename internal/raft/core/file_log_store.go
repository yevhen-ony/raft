package core


import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

type LogCodec interface {
	Marshal([]LogEntry) ([]byte, error)
	Unmarshal([]byte) ([]LogEntry, error)
}

type JSONLogCodec struct{}

func (JSONLogCodec) Marshal(entries []LogEntry) ([]byte, error) {
	return json.Marshal(entries)
}

func (JSONLogCodec) Unmarshal(data []byte) ([]LogEntry, error) {
	var entries []LogEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, err
	}
	return entries, nil
}

type FileLogStore struct {
	mu    sync.Mutex
	path  string
	codec LogCodec
}

func NewFileLogStore(path string, codec LogCodec) *FileLogStore {
	if codec == nil {
		codec = JSONLogCodec{}
	}
	return &FileLogStore{
		path:  path,
		codec: codec,
	}
}

func (s *FileLogStore) zeroLog() []LogEntry {
	return []LogEntry{{LogID: ZeroLogID}}
}

func (s *FileLogStore) Load(ctx context.Context) ([]LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	entries, err := s.load()
	if errors.Is(err, os.ErrNotExist) {
		return s.zeroLog(), nil
	}
	if err != nil {
		return nil, err
	}
	return entries, nil
}

func (s *FileLogStore) Append(ctx context.Context, entries ...LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}

	current, err := s.load()
	if errors.Is(err, os.ErrNotExist) {
		current = []LogEntry{{LogID: ZeroLogID}}
	} else if err != nil {
		return err
	}

	current = append(current, entries...)
	return s.save(ctx, current)
}

func (s *FileLogStore) AppendAfter(ctx context.Context, prev LogID, entries ...LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}

	current, err := s.load()
	if errors.Is(err, os.ErrNotExist) {
		current = []LogEntry{{LogID: ZeroLogID}}
	} else if err != nil {
		return err
	}

	cut := sort.Search(len(current), func(i int) bool {
		return current[i].Index > prev.Index
	})

	current = append(current[:cut], entries...)
	return s.save(ctx, current)
}

func (s *FileLogStore) load() ([]LogEntry, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		return nil, err
	}

	entries, err := s.codec.Unmarshal(data)
	if err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return []LogEntry{{LogID: ZeroLogID}}, nil
	}
	return entries, nil
}

func (s *FileLogStore) save(ctx context.Context, entries []LogEntry) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	data, err := s.codec.Marshal(entries)
	if err != nil {
		return err
	}

	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	tmp, err := os.CreateTemp(dir, ".raft-log-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()

	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpPath)
		}
	}()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}

	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}

	if err := tmp.Close(); err != nil {
		return err
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	if err := os.Rename(tmpPath, s.path); err != nil {
		return err
	}

	cleanup = false
	return nil
}

var _ LogStore = (*FileLogStore)(nil)
var _ LogCodec = JSONLogCodec{}
