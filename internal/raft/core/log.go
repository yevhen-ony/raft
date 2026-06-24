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
	pos, err := l.searchAndMatch(target)
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
	pos, err := l.searchAndMatch(target)
	if err != nil {
		return nil, err
	}
	entries := append([]LogEntry(nil), l.entries[pos+1:]...)
	return entries, nil
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


func (l *Log) IsUpToDate(target LogID) bool {
  	last := l.LastLogID()
  	if target.Term != last.Term {
  		return target.Term > last.Term
  	}
  	return target.Index >= last.Index
}

func (l *Log) GetEntry(index Index) (LogEntry, error) {
	pos, err := l.search(index) 
	if err != nil {
		return LogEntry{}, err
	}
	return l.entries[pos], nil
}
