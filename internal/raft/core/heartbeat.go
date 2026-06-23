package core

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

func (r *Raft) RunHeartbeatLoop(ctx context.Context, interval time.Duration) error {
  	ticker := time.NewTicker(interval)
  	defer ticker.Stop()

  	for {
  		select {
  		case <-ctx.Done():
  			return ctx.Err()

  		case <-ticker.C:
  			if err := r.Heartbeat(ctx); err != nil {
  				if errors.Is(err, ErrNotLeader) {
  					return err
  				}
  				slog.WarnContext(ctx, "heartbeat failed", "error", err)
  			}
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

  	return r.replicateLogTail(ctx, prev)
}
