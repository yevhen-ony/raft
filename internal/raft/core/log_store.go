package core

import (
	"context"
	"sort"
)

type InMemLogStore struct {
	entries []LogEntry
}

func NewInMemLogStore() *InMemLogStore {
	zeroEntry := LogEntry{LogID: ZeroLogID, Command: []byte{} }
	return &InMemLogStore{
		entries: []LogEntry{zeroEntry},
	}
}

func (s *InMemLogStore) Load(context.Context) ([]LogEntry, error) {
	entries := append([]LogEntry(nil), s.entries...)
	return entries, nil
}

func (s *InMemLogStore) Append(_ context.Context, entries ...LogEntry) error {
	s.entries = append(s.entries, entries...)
	return nil
}

func (s *InMemLogStore) AppendAfter(_ context.Context, prev LogID, entries ...LogEntry) error {
	pos := sort.Search(len(s.entries), func(i int) bool {
		return s.entries[i].Index > prev.Index
	})

	s.entries = append(s.entries[:pos], entries...)
	return nil
}
