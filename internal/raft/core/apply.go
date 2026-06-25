package core

import (
	"context"
	"fmt"
)

// caller must hold mu
func (r *Raft) updateCommitIndex(index Index) {

	index = min(index, r.log.LastLogID().Index)
	if index <= r.state.CommitIndex {
		return
	}

	r.state.CommitIndex = index

	select {
	case r.logCommitted <- struct{}{}:
	default:
	}
}

func (r *Raft) RunApplierLoop(ctx context.Context) error {
	for {
		if err := r.applyNextCommands(ctx); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.logCommitted:
		}
	}
}

func (r *Raft) applyNextCommands(ctx context.Context) error {
	r.mu.RLock()
	nextIndex := r.state.LastApplied + 1
	commitIndex := r.state.CommitIndex
	r.mu.RUnlock()

	applied := false

	for ; nextIndex <= commitIndex; nextIndex++ {
		r.mu.RLock()
		entry, err := r.log.Entry(nextIndex)
		r.mu.RUnlock()

		if err != nil {
			return fmt.Errorf("get entry at index %d: %w", nextIndex, err)
		}
		if err := r.commandApplier.Apply(ctx, entry.Command); err != nil {
			return fmt.Errorf("apply command at index %d: %w", nextIndex, err)
		}

		r.mu.Lock()
		r.state.LastApplied = entry.Index
		r.mu.Unlock()

		applied = true
	}
	if applied {
		r.commandApplied.Broadcast()
	}
	return nil
}

func (r *Raft) waitApplied(ctx context.Context, index Index) error {
	for {
		applied := r.commandApplied.Subscribe()

		r.mu.RLock()
		done := r.state.LastApplied >= index
		r.mu.RUnlock()

		if done {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-applied:
		}
	}
}
