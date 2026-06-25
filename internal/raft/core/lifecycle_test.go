package core

import (
	"context"
	"errors"
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

func TestRun_StopsWhenApplierFails(tt *testing.T) {
	ctx := context.Background()
	c := setupCluster(tt)
	c.n1.cfg.HeartbeatInterval = time.Millisecond

	applyErr := errors.New("apply failed")
	c.n1Applier.Fail(applyErr)

	require.NoError(tt, c.n1.log.Append(ctx,
		LogEntry{LogID: LogID{Index: 1, Term: 1}, Command: []byte("one")},
	))

	c.n1.mu.Lock()
	c.n1.updateCommitIndex(1)
	c.n1.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := c.n1.Run(ctx)

	require.ErrorIs(tt, err, applyErr)
	require.Equal(tt, Index(0), c.n1.state.LastApplied)
}
