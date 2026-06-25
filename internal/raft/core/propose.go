package core

import (
	"context"
	"fmt"
)

func (r *Raft) Propose(ctx context.Context, cmd []byte) error {

	rng, err := r.appendToLog(cmd)
	if err != nil {
		return fmt.Errorf("append to log: %w", err)
	}

	if err := r.replicateLogRange(ctx, rng); err != nil {
		return fmt.Errorf("replicate log tail: %w", err)
	}

	r.mu.Lock()
	r.updateCommitIndex(rng.Last)
	r.mu.Unlock()

	return nil
}

func (r *Raft) appendToLog(commands ...[]byte) (LogRange, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	role := r.state.Role
	if role != Leader {
		return LogRange{}, ErrNotLeader
	}

	prev := r.log.LastLogID()

	entries := r.makeEntries(prev.Index+1, commands...)
	if err := r.log.Append(entries...); err != nil {
		return LogRange{}, err
	}

	res := LogRange{
		Prev: prev.Index,
		Last: r.log.LastLogID().Index,
	}
	return res, nil
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
