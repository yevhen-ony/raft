package core

import (
	"context"
	"errors"
	"fmt"
	"sort"
)

var ZeroLogID = LogID{}

type LogStore interface {
	Load(context.Context) ([]LogEntry, error)
	Append(context.Context, ...LogEntry) error
	AppendAfter(context.Context, LogID, ...LogEntry) error
}

type Log struct {
	entries []LogEntry
	store   LogStore
}

func NewLog(ctx context.Context, store LogStore) (*Log, error) {
	entries, err := store.Load(ctx)
	if err != nil {
		return nil, err
	}

	l := &Log{
		entries: entries,
		store: store,
	}
	return l, nil
}

func (l *Log) LastLogID() LogID {
	last := len(l.entries) - 1
	return l.entries[last].LogID
}

func (l *Log) PrevLogID(index Index) (LogID, error) {
	pos, err := l.searchExact(index)
	if err != nil {
		return LogID{}, err
	}
	if pos == 0 {
		return LogID{}, ErrNoPrevLog
	}
	prev := l.entries[pos-1]
	return prev.LogID, nil
}

func (l *Log) PrevIndex(index Index) (Index, error) {
	prev, err := l.PrevLogID(index)
	if err != nil {
		return 0, err
	}
	return prev.Index, nil
}

func (l *Log) Append(ctx context.Context, entries ...LogEntry) error {
	last := l.LastLogID()
	if err := l.validate(last, entries...); err != nil {
		return err
	}

	if err := l.store.Append(ctx, entries...); err != nil {
		return err
	}

	l.entries = append(l.entries, entries...)
	return nil
}

func (l *Log) EntriesAfter(target LogID) ([]LogEntry, error) {
	pos, err := l.searchAndMatch(target)
	if err != nil {
		return nil, err
	}
	entries := append([]LogEntry(nil), l.entries[pos+1:]...)
	return entries, nil
}

func (l *Log) Segment(rng LogRange) (LogSegment, error) {
	if rng.Last < rng.Prev {
		return LogSegment{}, ErrInvalidLogRange
	}

	prev, err := l.searchExact(rng.Prev)
	if err != nil {
		return LogSegment{}, fmt.Errorf("find prev: %w", err)
	}

	seg := LogSegment{Prev: l.entries[prev].LogID}

	if rng.Last == rng.Prev {
		return seg, nil
	}

	last, err := l.searchExact(rng.Last)
	if err != nil {
		return LogSegment{}, fmt.Errorf("find last: %w", err)
	}

	seg.Entries = append([]LogEntry(nil), l.entries[prev+1:last+1]...)
	return seg, nil
}

func (l *Log) AppendAfter(prev LogID, entries ...LogEntry) error {
	pos, err := l.searchAndMatch(prev)
	if err != nil {
		return err
	}
	if err := l.validate(prev, entries...); err != nil {
		return err
	}
	l.entries = append(l.entries[:pos+1], entries...)
	return nil
}

func (l *Log) Contains(target LogID) bool {
	if _, err := l.searchAndMatch(target); err != nil {
		return false
	}
	return true
}

func (l *Log) validate(prev LogID, entries ...LogEntry) error {
	for _, entry := range entries {
		if entry.Index != prev.Index+1 {
			return errors.New("invalid index")
		}
		if entry.Term < prev.Term {
			return errors.New("invalid term")
		}
		prev = entry.LogID
	}
	return nil
}

func (l *Log) search(index Index) (int, error) {
	pos := sort.Search(len(l.entries), func(i int) bool {
		return l.entries[i].Index >= index
	})
	if pos == len(l.entries) {
		return 0, ErrLogNotFound
	}
	return pos, nil
}

func (l *Log) match(i int, target LogID) error {
	if l.entries[i].LogID != target {
		return ErrLogMismatch
	}
	return nil
}

func (l *Log) searchAndMatch(target LogID) (int, error) {
	pos, err := l.search(target.Index)
	if err != nil {
		return 0, err
	}
	if err := l.match(pos, target); err != nil {
		return 0, err
	}
	return pos, nil
}

func (l *Log) searchExact(index Index) (int, error) {
	pos, err := l.search(index)
	if err != nil {
		return 0, err
	}
	if l.entries[pos].Index != index {
		return 0, ErrLogNotFound
	}
	return pos, nil
}

func (l *Log) IsUpToDate(target LogID) bool {
	last := l.LastLogID()
	if target.Term != last.Term {
		return target.Term > last.Term
	}
	return target.Index >= last.Index
}

func (l *Log) Entry(index Index) (LogEntry, error) {
	pos, err := l.searchExact(index)
	if err != nil {
		return LogEntry{}, err
	}
	return l.entries[pos], nil
}
