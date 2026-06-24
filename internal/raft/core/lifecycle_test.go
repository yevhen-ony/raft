package core

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRun_SupervisesFollowerToLeader(tt *testing.T) {
	c := setupCluster(tt)

	c.n1.state.Role = Follower
	c.n1.cfg.ElectionTimeoutMin = 1 * time.Millisecond
	c.n1.cfg.ElectionTimeoutMax = 1 * time.Millisecond
	c.n1.cfg.HeartbeatInterval = time.Hour

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- c.n1.Run(ctx)
	}()

	require.Eventually(tt, func() bool {
		c.n1.mu.RLock()
		defer c.n1.mu.RUnlock()

		return c.n1.state.Role == Leader
	}, time.Second, time.Millisecond)

	cancel()

	require.ErrorIs(tt, <-done, context.Canceled)
}

func TestRun_ContinuesAfterLeaderStepsDown(tt *testing.T) {
	c := setupCluster(tt)

	c.n1.cfg.HeartbeatInterval = time.Hour
	c.n1.cfg.ElectionTimeoutMin = time.Hour

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- c.n1.Run(ctx)
	}()

	c.n1.mu.Lock()
	c.n1.becomeFollower(c.n1.state.Term)
	c.n1.mu.Unlock()

	require.Eventually(tt, func() bool {
		c.n1.mu.RLock()
		defer c.n1.mu.RUnlock()

		return c.n1.state.Role == Follower
	}, time.Second, time.Millisecond)

	cancel()

	require.ErrorIs(tt, <-done, context.Canceled)
}

func TestRun_ContinuesAsFollowerAfterHigherTermHeartbeatResponse(tt *testing.T) {
	c := setupCluster(tt)

	c.n1.cfg.HeartbeatInterval = time.Millisecond
	c.n1.cfg.ElectionTimeoutMin = time.Hour
	c.n1.cfg.ElectionTimeoutMax = time.Hour

	c.transport.highTerm(c.node2.ID, Term(2))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- c.n1.Run(ctx)
	}()

	require.Eventually(tt, func() bool {
		c.n1.mu.RLock()
		defer c.n1.mu.RUnlock()

		return c.n1.state.Role == Follower && c.n1.state.Term == Term(2)
	}, time.Second, time.Millisecond)

	cancel()

	require.ErrorIs(tt, <-done, context.Canceled)
}
