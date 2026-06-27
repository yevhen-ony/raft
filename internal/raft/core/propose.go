package core

import (
	"context"
	"fmt"
)

func (r *Raft) propose(ctx context.Context, cmd []byte) (Index, error) {
	term, rng, err := r.appendToLog(ctx, cmd)
	if err != nil {
		return 0, fmt.Errorf("append to log: %w", err)
	}

	if err := r.replicateLogRange(ctx, term, rng); err != nil {
		return 0, fmt.Errorf("replicate log range: %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if err := r.state.EnsureLeaderTerm(term); err != nil {
		return 0, err
	}

	r.updateCommitIndex(rng.Last)
	return rng.Last, nil
}

func (r *Raft) Propose(ctx context.Context, cmd []byte) (Index, error) {
	return r.propose(ctx, cmd)
}

func (r *Raft) ProposeAndWait(ctx context.Context, cmd []byte) (Index, error) {
	idx, err := r.propose(ctx, cmd)
	if err != nil {
		return 0, fmt.Errorf("propose: %w", err) 
	}
	if err := r.waitApplied(ctx, idx); err != nil {
		return 0, fmt.Errorf("wait applied: %w", err)
	}
	return idx, nil
}

func (r *Raft) appendToLog(ctx context.Context, commands ...[]byte) (Term, LogRange, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	term, err := r.state.EnsureLeader()
	if err != nil {
		return term, LogRange{}, err
	}

	prev := r.log.LastLogID()

	entries := r.makeEntries(prev.Index+1, commands...)
	if err := r.log.Append(ctx, entries...); err != nil {
		return term, LogRange{}, err
	}

	res := LogRange{
		Prev: prev.Index,
		Last: r.log.LastLogID().Index,
	}
	return term, res, nil
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
