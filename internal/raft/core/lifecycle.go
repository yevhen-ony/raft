package core

import (
	"context"
	"log/slog"
)

func (r *Raft) Run(ctx context.Context) error {
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

func (r *Raft) notifyRoleChanged() {
	select {
	case r.roleChanged <- struct{}{}:
	default:
	}
}

// the caller must hold the lock
func (r *Raft) changeRole(role Role) bool {
	if r.state.Role == role {
		return false
	}
	r.state.Role = role
	r.notifyRoleChanged()
	return true
}

func (r *Raft) becomeCandidate() error {
	if r.state.Role != Follower {
		return ErrNotFollower
	}
	r.state.Term++
	r.state.VotedFor = r.cluster.Self.ID
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
	return nil
}

// become follower is unconditional: never fails
func (r *Raft) becomeFollower(term Term) {
	if term > r.state.Term {
		r.state.Term = term
		r.state.VotedFor = ""
	}
	r.changeRole(Follower)
}
