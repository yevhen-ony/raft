package core

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

func (r *Raft) RunHeartbeatLoop(ctx context.Context) error {
	ticker := time.NewTicker(r.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-ticker.C:
			if err := r.Heartbeat(ctx); err != nil {
				if errors.Is(err, ErrNotLeader) || errors.Is(err, ErrOutdatedTerm) {
					return nil
				}
				slog.WarnContext(ctx, "hearbeat failed", "error", err)
			}
		case <-r.roleChanged:
			return nil
		}
	}
}

func (r *Raft) Heartbeat(ctx context.Context) error {
	r.mu.RLock()
	role := r.state.Role
	prev := r.log.LastLogID()
	r.mu.RUnlock()

	if role != Leader {
		return ErrNotLeader
	}

	rng := LogRange{
		Prev: prev.Index,
		Last: prev.Index,
	}
	return r.replicateLogRange(ctx, rng)
}
