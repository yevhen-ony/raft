package core

import (
	"context"
	"fmt"
	"log/slog"
)

// caller must hold mu
func (r *Raft) followCommit(ci Index) {
	newCommitIndex := min(ci, r.log.LastLogID().Index)
	if newCommitIndex < r.state.CommitIndex {
		return
	}
	r.updateCommitIndex(newCommitIndex)
}

func (r *Raft) updateCommitIndex(index Index) {
	r.state.CommitIndex = index
	if r.state.CommitIndex <= r.state.LastApplied {
		return
	}

	select {
	case r.commitChanged <- struct{}{}:
	default:
	}
}

func (r *Raft) RunApplierLoop(ctx context.Context) error {
	for {
		if err := r.applyNextCommands(ctx); err != nil {
			slog.ErrorContext(ctx, "apply command failed", "error", err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.commitChanged:
		}
	}
}

func (r *Raft) applyNextCommands(ctx context.Context) error {
	r.mu.RLock()
	nextIndex := r.state.LastApplied + 1
	commitIndex := r.state.CommitIndex
	r.mu.RUnlock()

	for ; nextIndex <= commitIndex; nextIndex++ {
		r.mu.RLock()
		entry, err := r.log.GetEntry(nextIndex)
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
	}
	return nil
}
