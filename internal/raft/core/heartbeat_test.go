package core

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRaftCluster_HeartbeatDoesNotAppendOnUpToDateFollower(tt *testing.T) {
	ctx := context.Background()

	c := setupCluster(tt)
	leader, follower := c.n1, c.n2

	require.NoError(tt, leader.Propose(ctx, []byte("hello")))
	require.NoError(tt, leader.Heartbeat(ctx))

	entries, err := follower.log.EntriesAfter(ZeroLogID)
	require.NoError(tt, err)
	require.Len(tt, entries, 1)
	require.Equal(tt, "hello", string(entries[0].Command))
}

func TestRaftCluster_HeartbeatCatchesUpBehindFollower(tt *testing.T) {
	ctx := context.Background()

	c := setupCluster(tt)
	leader, follower := c.n1, c.n2

	require.NoError(tt, leader.log.Append(
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
