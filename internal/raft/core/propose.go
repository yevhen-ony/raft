package core

import (
	"context"
	"fmt"
)

func (r *Raft) Propose(ctx context.Context, cmd []byte) error {
	r.mu.RLock()
	role := r.state.Role
	r.mu.RUnlock()
	if role != Leader {
		return ErrNotLeader
	}

	prev, err := r.appendToLog(cmd)
	if err != nil {
		return fmt.Errorf("append to log: %w", err)
	}

	if err := r.replicateLogTail(ctx, prev); err != nil {
		return fmt.Errorf("replicate log tail: %w", err)
	}

	return nil
}

func (r *Raft) appendToLog(commands ...[]byte) (LogID, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	prev := r.log.LastLogID()

	entries := r.makeEntries(prev.Index+1, commands...)
	if err := r.log.Append(entries...); err != nil {
		return LogID{}, err
	}
	return prev, nil
}

func (r *Raft) makeEntries(index Index, commands ...[]byte) []LogEntry {
	entries := make([]LogEntry, len(commands))
	for i, cmd := range commands {
		entry := LogEntry{
			LogID: LogID{
				Index: index,
				Term:  r.state.Term,
			},
			Command: cmd,
		}
		entries[i] = entry
		index++
	}
	return entries
}
