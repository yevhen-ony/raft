package core

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReplication_ProposeReplicatesToFollower(tt *testing.T) {
	ctx := context.Background()

	c := setupCluster(tt)

	require.NoError(tt, c.n1.Propose(ctx, []byte("hello")))

	entries, err := c.n1.log.EntriesAfter(ZeroLogID)
	require.NoError(tt, err)
	require.Len(tt, entries, 1)

	entry := entries[0]
	require.Equal(tt, Index(1), entry.Index)
	require.Equal(tt, Term(1), entry.Term)
	require.Equal(tt, "hello", string(entry.Command))
}

func TestReplication_FollowerRejectsStaleEntry(tt *testing.T) {
	c := setupCluster(tt)

	rsp := c.n2.AppendEntries(context.Background(), AppendEntriesRequest{
		LeaderID:  "n1",
		Term:      0,
		PrevLogID: ZeroLogID,
	})

	require.False(tt, rsp.Success)
	require.Equal(tt, Term(1), rsp.Term)

	entries, err := c.n2.log.EntriesAfter(ZeroLogID)
	require.NoError(tt, err)
	require.Empty(tt, entries)
}

func TestReplication_FollowerRejectsMissingPrevLog(tt *testing.T) {
	c := setupCluster(tt)

	rsp := c.n2.AppendEntries(context.Background(), AppendEntriesRequest{
		LeaderID:  "n1",
		Term:      1,
		PrevLogID: LogID{Index: 10, Term: 1}, // not in log yet
		Entries: []LogEntry{
			{
				LogID:   LogID{Index: 11, Term: 1},
				Command: []byte("hello"),
			},
		},
	})

	require.False(tt, rsp.Success)
	require.Equal(tt, Term(1), rsp.Term)

	entries, err := c.n2.log.EntriesAfter(ZeroLogID)
	require.NoError(tt, err)
	require.Empty(tt, entries)
}

func TestReplication_LeaderStepsDownOnHigherTermAppendEntries(tt *testing.T) {
	c := setupCluster(tt)

	rsp := c.n1.AppendEntries(context.Background(), AppendEntriesRequest{
		LeaderID:  "n2",
		Term:      2,
		PrevLogID: ZeroLogID,
	})

	require.True(tt, rsp.Success)
	require.Equal(tt, Term(2), rsp.Term)
	require.Equal(tt, Term(2), c.n1.state.Term)
	require.Equal(tt, Follower, c.n1.state.Role)
}

func TestReplication_FollowerAppendsEntriesFromHigherTermLeader(tt *testing.T) {
	c := setupCluster(tt)

	rsp := c.n2.AppendEntries(context.Background(), AppendEntriesRequest{
		LeaderID:  "n1",
		Term:      2,
		PrevLogID: ZeroLogID,
		Entries: []LogEntry{
			{
				LogID:   LogID{Index: 1, Term: 2},
				Command: []byte("hello"),
			},
		},
	})

	require.True(tt, rsp.Success)
	require.Equal(tt, Term(2), rsp.Term)
	require.Equal(tt, Term(2), c.n2.state.Term)
	require.Equal(tt, Follower, c.n2.state.Role)

	entries, err := c.n2.log.EntriesAfter(ZeroLogID)
	require.NoError(tt, err)
	require.Len(tt, entries, 1)
	require.Equal(tt, Term(2), entries[0].Term)
	require.Equal(tt, "hello", string(entries[0].Command))
}

func TestReplication_ProposeReplicatesSequentialEntries(tt *testing.T) {
	ctx := context.Background()

	c := setupCluster(tt)
	c.transport.unregister("n3")

	require.NoError(tt, c.n1.Propose(ctx, []byte("one")))
	require.NoError(tt, c.n1.Propose(ctx, []byte("two")))
	require.NoError(tt, c.n1.Propose(ctx, []byte("three")))

	entries, err := c.n2.log.EntriesAfter(ZeroLogID)
	require.NoError(tt, err)
	require.Len(tt, entries, 3)

	require.Equal(tt, Index(1), entries[0].Index)
	require.Equal(tt, "one", string(entries[0].Command))

	require.Equal(tt, Index(2), entries[1].Index)
	require.Equal(tt, "two", string(entries[1].Command))

	require.Equal(tt, Index(3), entries[2].Index)
	require.Equal(tt, "three", string(entries[2].Command))
}

func TestRaftCluster_AppendEntriesReplacesFollowerConflict(tt *testing.T) {
	c := setupCluster(tt)

	follower := c.n2

	require.NoError(tt, follower.log.Append(
		LogEntry{
			LogID:   LogID{Index: 1, Term: 1},
			Command: []byte("old"),
		},
		LogEntry{
			LogID:   LogID{Index: 2, Term: 1},
			Command: []byte("conflict"),
		},
	))

	rsp := follower.AppendEntries(context.Background(), AppendEntriesRequest{
		LeaderID:  "n1",
		Term:      2,
		PrevLogID: LogID{Index: 1, Term: 1},
		Entries: []LogEntry{
			{
				LogID:   LogID{Index: 2, Term: 2},
				Command: []byte("new"),
			},
		},
	})

	require.True(tt, rsp.Success)
	require.Equal(tt, Term(2), rsp.Term)

	entries, err := follower.log.EntriesAfter(ZeroLogID)
	require.NoError(tt, err)
	require.Len(tt, entries, 2)

	require.Equal(tt, Index(1), entries[0].Index)
	require.Equal(tt, Term(1), entries[0].Term)
	require.Equal(tt, "old", string(entries[0].Command))

	require.Equal(tt, Index(2), entries[1].Index)
	require.Equal(tt, Term(2), entries[1].Term)
	require.Equal(tt, "new", string(entries[1].Command))
}

func TestReplication_LeaderStepsDownOnHigherTermReplicationResponse(tt *testing.T) {
	c := setupCluster(tt)

	c.transport.highTerm("n2", Term(2))
	c.transport.highTerm("n3", Term(2))

	leader := c.n1
	err := leader.Propose(context.Background(), []byte("hello"))

	require.ErrorIs(tt, err, ErrNotLeader)
	require.Equal(tt, Term(2), leader.state.Term)
	require.Equal(tt, Follower, leader.state.Role)
}

func TestReplication_LeaderBacktracksWhenFollowerIsBehind(tt *testing.T) {
	ctx := context.Background()

	c := setupCluster(tt)
	leader, follower := c.n1, c.n2
	c.transport.unregister("n3")

	require.NoError(tt, leader.log.Append(
		LogEntry{LogID: LogID{Index: 1, Term: 1}, Command: []byte("one")},
		LogEntry{LogID: LogID{Index: 2, Term: 1}, Command: []byte("two")},
	))

	require.NoError(tt, leader.Propose(ctx, []byte("three")))

	entries, err := follower.log.EntriesAfter(ZeroLogID)
	require.NoError(tt, err)
	require.Len(tt, entries, 3)

	require.Equal(tt, Index(1), entries[0].Index)
	require.Equal(tt, "one", string(entries[0].Command))

	require.Equal(tt, Index(2), entries[1].Index)
	require.Equal(tt, "two", string(entries[1].Command))

	require.Equal(tt, Index(3), entries[2].Index)
	require.Equal(tt, "three", string(entries[2].Command))
}

func TestReplication_LeaderBacktracksAndReplacesFollowerConflict(tt *testing.T) {
	ctx := context.Background()

	c := setupCluster(tt)
	leader, follower := c.n1, c.n2
	c.transport.unregister("n3")

	require.NoError(tt, leader.log.Append(
		LogEntry{LogID: LogID{Index: 1, Term: 1}, Command: []byte("one")},
		LogEntry{LogID: LogID{Index: 2, Term: 1}, Command: []byte("two")},
	))

	require.NoError(tt, follower.log.Append(
		LogEntry{LogID: LogID{Index: 1, Term: 1}, Command: []byte("one")},
		LogEntry{LogID: LogID{Index: 2, Term: 2}, Command: []byte("bad")},
	))

	require.NoError(tt, leader.Propose(ctx, []byte("three")))

	entries, err := follower.log.EntriesAfter(ZeroLogID)
	require.NoError(tt, err)
	require.Len(tt, entries, 3)

	require.Equal(tt, Index(1), entries[0].Index)
	require.Equal(tt, Term(1), entries[0].Term)
	require.Equal(tt, "one", string(entries[0].Command))

	require.Equal(tt, Index(2), entries[1].Index)
	require.Equal(tt, Term(1), entries[1].Term)
	require.Equal(tt, "two", string(entries[1].Command))

	require.Equal(tt, Index(3), entries[2].Index)
	require.Equal(tt, Term(1), entries[2].Term)
	require.Equal(tt, "three", string(entries[2].Command))
}

func TestReplication_ProposeSucceedsWithQuorum(tt *testing.T) {
	ctx := context.Background()

	c := setupCluster(tt)
	c.transport.unregister("n3")
	leader, follower := c.n1, c.n2

	require.NoError(tt, leader.Propose(ctx, []byte("hello")))

	entries, err := follower.log.EntriesAfter(ZeroLogID)
	require.NoError(tt, err)
	require.Len(tt, entries, 1)
	require.Equal(tt, "hello", string(entries[0].Command))
}

func TestReplication_ProposeFailsWithoutQuorum(tt *testing.T) {
	ctx := context.Background()

	c := setupCluster(tt)
	c.transport.unregister("n2")               // unreachable
	c.transport.fail("n3", errors.New("boom")) // failed

	leader := c.n1
	err := leader.Propose(ctx, []byte("hello"))
	require.ErrorIs(tt, err, ErrQuorumNotReached)
}

func TestReplication_ProposeAdvancesLeaderCommitIndex(tt *testing.T) {
  	ctx := context.Background()

  	c := setupCluster(tt)
  	leader := c.n1

  	require.NoError(tt, leader.Propose(ctx, []byte("hello")))

  	require.Equal(tt, Index(1), leader.state.CommitIndex)
}

func TestReplication_ProposeDoesNotCommitWithoutQuorum(tt *testing.T) {
  	ctx := context.Background()

  	c := setupCluster(tt)
  	c.transport.unregister("n2")
  	c.transport.fail("n3", errors.New("boom"))

  	leader := c.n1

  	err := leader.Propose(ctx, []byte("hello"))

  	require.ErrorIs(tt, err, ErrQuorumNotReached)
  	require.Equal(tt, Index(0), leader.state.CommitIndex)
}
