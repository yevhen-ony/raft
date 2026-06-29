package core

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"golang.org/x/sync/errgroup"
)

func (r *Raft) Run(ctx context.Context) error {
	eg, ctx := errgroup.WithContext(ctx)

	eg.Go(func() error { return r.RunApplierLoop(ctx) })
	eg.Go(func() error { return r.RunLifecycleLoop(ctx) })

	if err := eg.Wait(); err != nil {
		if !errors.Is(err, context.Canceled) {
			slog.ErrorContext(ctx, "raft node stopped", "error", err)
		}
		return err
	}
	return nil
}

func (r *Raft) RunLifecycleLoop(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		default:
		}

		r.mu.RLock()
		role := r.state.Role
		r.mu.RUnlock()

		switch role {
		case Follower:
			if err := r.RunElectionLoop(ctx); err != nil {
				slog.ErrorContext(ctx, "election loop", "error", err)
			}
		case Leader:
			if err := r.RunHeartbeatLoop(ctx); err != nil {
				slog.ErrorContext(ctx, "heartbeat loop", "error", err)
			}
		case Candidate:
			slog.ErrorContext(ctx, "unsuperwised Candidate state observed")
			select {
			case <-r.roleChanged:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

// the caller must hold the lock
func (r *Raft) changeRole(role Role) bool {
	if r.state.Role == role {
		return false
	}
	r.state.Role = role

	select {
	case r.roleChanged <- struct{}{}:
	default:
	}

	return true
}

func (r *Raft) becomeCandidate(ctx context.Context) error {
	if r.state.Role != Follower {
		return ErrNotFollower
	}
	if err := r.state.IncTerm(ctx, r.cluster.Self.ID); err != nil {
		return fmt.Errorf("inc term: %w", err)
	}
	r.changeRole(Candidate)
	return nil
}

func (r *Raft) becomeLeader(term Term) error {
	if r.state.Term != term {
		return ErrOutdatedTerm
	}
	if r.state.Role != Candidate {
		return ErrNotCandidate
	}
	r.changeRole(Leader)
	r.state.LeaderID = r.cluster.Self.ID
	slog.Info("became leader", "term", term)
	return nil
}

func (r *Raft) becomeFollower(ctx context.Context, term Term, leaderID NodeID) error {
	if term > r.state.Term {
		if err := r.state.SetTerm(ctx, term); err != nil {
			return fmt.Errorf("set term: %w", err)
		}
	}
	r.changeRole(Follower)
	r.state.LeaderID = leaderID
	return nil
}
