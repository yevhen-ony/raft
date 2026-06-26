package core

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRaftCluster_HeartbeatDoesNotAppendOnUpToDateFollower(tt *testing.T) {
	ctx := context.Background()

	c := setupCluster(tt).WithLeader(tt, 1)
	leader, follower := c.n1, c.n2
	c.transport.unregister("n3")

	require.NoError(tt, leader.Propose(ctx, []byte("hello")))
	require.NoError(tt, leader.Heartbeat(ctx))

	entries, err := follower.log.EntriesAfter(ZeroLogID)
	require.NoError(tt, err)
	require.Len(tt, entries, 1)
	require.Equal(tt, "hello", string(entries[0].Command))
}

func TestRaftCluster_HeartbeatCatchesUpBehindFollower(tt *testing.T) {
	ctx := context.Background()

	c := setupCluster(tt).WithLeader(tt, 1)
	leader, follower := c.n1, c.n2
	c.transport.unregister("n3")

	require.NoError(tt, leader.log.Append(ctx,
		LogEntry{LogID: LogID{Index: 1, Term: 1}, Command: []byte("one")},
		LogEntry{LogID: LogID{Index: 2, Term: 1}, Command: []byte("two")},
	))

	require.NoError(tt, leader.Heartbeat(ctx))

	entries, err := follower.log.EntriesAfter(ZeroLogID)
	require.NoError(tt, err)
	require.Len(tt, entries, 2)
	require.Equal(tt, "one", string(entries[0].Command))
	require.Equal(tt, "two", string(entries[1].Command))
}

func TestRaftCluster_FollowerHeartbeatReturnsNotLeader(tt *testing.T) {
	c := setupCluster(tt)
	follower := c.n2

	err := follower.Heartbeat(context.Background())
	require.ErrorIs(tt, err, ErrNotLeader)
}

func TestRunHeartbeatLoop_ReturnsAfterRoleChange(tt *testing.T) {
	ctx := context.Background()
	c := setupCluster(tt).WithLeader(tt, 1)
	c.n1.cfg.HeartbeatInterval = time.Hour

	done := make(chan error, 1)
	go func() {
		done <- c.n1.RunHeartbeatLoop(context.Background())
	}()

	c.n1.mu.Lock()
	c.n1.becomeFollower(ctx, c.n1.state.Term)
	c.n1.mu.Unlock()

	require.NoError(tt, <-done)
	require.Equal(tt, Follower, c.n1.state.Role)
}

func TestRunHeartbeatLoop_ReplicatesPeriodically(tt *testing.T) {
	ctx := context.Background()
	c := setupCluster(tt).WithLeader(tt, 1)

	leader, follower := c.n1, c.n2
	c.transport.unregister(c.node3.ID)

	leader.cfg.HeartbeatInterval = time.Millisecond

	require.NoError(tt, leader.log.Append(ctx,
		LogEntry{LogID: LogID{Index: 1, Term: 1}, Command: []byte("one")},
	))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- leader.RunHeartbeatLoop(ctx)
	}()

	require.Eventually(tt, func() bool {
		entries, err := follower.log.EntriesAfter(ZeroLogID)
		return err == nil && len(entries) == 1
	}, time.Second, time.Millisecond)

	cancel()

	require.ErrorIs(tt, <-done, context.Canceled)
}

func TestRunHeartbeatLoop_StepsDownOnHigherTermResponse(tt *testing.T) {
	c := setupCluster(tt).WithLeader(tt, 1)

	c.n1.cfg.HeartbeatInterval = time.Millisecond
	c.transport.highTerm(c.node2.ID, Term(2))
	c.transport.highTerm(c.node3.ID, Term(2))

	done := make(chan error, 1)
	go func() {
		done <- c.n1.RunHeartbeatLoop(context.Background())
	}()

	require.Eventually(tt, func() bool {
		c.n1.mu.RLock()
		defer c.n1.mu.RUnlock()

		return c.n1.state.Role == Follower && c.n1.state.Term == Term(2)
	}, time.Second, time.Millisecond)

	require.NoError(tt, <-done)
}
