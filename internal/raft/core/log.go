package core

import (
	"errors"
	"sort"
)

var ZeroLogID = LogID{}

type Log struct {
	entries []LogEntry
}

func NewLog() *Log {
	return &Log{
		entries: []LogEntry{{LogID: ZeroLogID}},
	}
}

func (l *Log) LastLogID() LogID {
	last := len(l.entries) - 1
	return l.entries[last].LogID
}

func (l *Log) PrevLogID(target LogID) (LogID, error) {
	pos, err := l.search(target)
	if err != nil {
		return LogID{}, err
	}
	if pos == 0 {
		return LogID{}, ErrNoPrevLog
	}
	prev := l.entries[pos-1]	
	return prev.LogID, nil
}

func (l *Log) Append(entries ...LogEntry) error {
	last := l.LastLogID()
	if err := l.validate(last, entries...); err != nil {
		return err
	}

	l.entries = append(l.entries, entries...)
	return nil
}

func (l *Log) EntriesAfter(target LogID) ([]LogEntry, error) {
	pos, err := l.search(target)
	if err != nil {
		return nil, err
	}
	entries := append([]LogEntry(nil), l.entries[pos+1:]...)
	return entries, nil
}

func (l *Log) AppendAfter(prev LogID, entries ...LogEntry) error {
	pos, err := l.search(prev)
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
	_, err := l.search(target) 
	return err == nil
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

func (l *Log) search(target LogID) (int, error) {
	pos := sort.Search(len(l.entries), func(i int) bool {
		return l.entries[i].Index >= target.Index
	})
	if pos == len(l.entries) {
		return 0, ErrLogNotFound
	}
	if target != l.entries[pos].LogID {
		return 0, ErrLogMismatch
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
