package core

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestElection_CandidateWinsElectinWithQuorum(tt *testing.T) {
	c := setupCluster(tt)

	c.n1.state.Role = Follower

	ok, err := c.n1.RunElection(context.Background())

	require.NoError(tt, err)
	require.True(tt, ok)
	require.Equal(tt, Leader, c.n1.state.Role)
	require.Equal(tt, Term(2), c.n1.state.Term)
	require.Equal(tt, c.node1.ID, c.n1.state.VotedFor)

	votes := 1
	if c.n2.state.VotedFor == c.node1.ID {
		votes++
	}
	if c.n3.state.VotedFor == c.node1.ID {
		votes++
	}
	require.GreaterOrEqual(tt, votes, c.n1.cluster.Quorum().Accept)
}

func TestVoting_LeaderDoesNotStartElection(tt *testing.T) {
	c := setupCluster(tt)

	ok, err := c.n1.RunElection(context.Background())

	require.ErrorIs(tt, err, ErrLeader)
	require.False(tt, ok)
	require.Equal(tt, Leader, c.n1.state.Role)
}

func TestElection_CandidateLosesElectionWithoutQuorum(tt *testing.T) {
	c := setupCluster(tt)

	c.n1.state.Role = Follower
	c.transport.unregister(c.node2.ID)
	c.transport.unregister(c.node3.ID)

	ok, err := c.n1.RunElection(context.Background())

	require.NoError(tt, err)
	require.False(tt, ok)
	require.Equal(tt, Follower, c.n1.state.Role)
	require.Equal(tt, Term(2), c.n1.state.Term)
	require.Equal(tt, c.node1.ID, c.n1.state.VotedFor)
}

func TestVote_RejectsOutdatedTerm(tt *testing.T) {
	c := setupCluster(tt)
	c.n2.state.Term = 1

	rsp := c.n2.Vote(context.Background(), VoteRequest{
		CandidateID: c.node1.ID,
		Term:        0,
		LastLogID:   ZeroLogID,
	})

	require.False(tt, rsp.Granted)
	require.Equal(tt, Term(1), rsp.Term)
	require.Equal(tt, Follower, c.n2.state.Role)
	require.Empty(tt, c.n2.state.VotedFor)
}

func TestVote_HigherTermMakesFollowerAndGrants(tt *testing.T) {
	c := setupCluster(tt)

	rsp := c.n1.Vote(context.Background(), VoteRequest{
		CandidateID: c.node2.ID,
		Term:        2,
		LastLogID:   ZeroLogID,
	})

	require.True(tt, rsp.Granted)
	require.Equal(tt, Term(2), rsp.Term)
	require.Equal(tt, Term(2), c.n1.state.Term)
	require.Equal(tt, Follower, c.n1.state.Role)
	require.Equal(tt, c.node2.ID, c.n1.state.VotedFor)
}

func TestVote_RejectsWhenAlreadyVotedForOtherCandidate(tt *testing.T) {
	c := setupCluster(tt)

	c.n2.state.Term = 2
	c.n2.state.VotedFor = c.node3.ID

	rsp := c.n2.Vote(context.Background(), VoteRequest{
		CandidateID: c.node1.ID,
		Term:        2,
		LastLogID:   ZeroLogID,
	})

	require.False(tt, rsp.Granted)
	require.Equal(tt, Term(2), rsp.Term)
	require.Equal(tt, c.node3.ID, c.n2.state.VotedFor)
	require.Equal(tt, Follower, c.n2.state.Role)
}

func TestVote_RejectsCandidateWithStaleLog(tt *testing.T) {
	ctx := context.Background()
	c := setupCluster(tt)

	require.NoError(tt, c.n2.log.Append(ctx,
		LogEntry{LogID: LogID{Index: 1, Term: 1}, Command: []byte("one")},
	))

	rsp := c.n2.Vote(context.Background(), VoteRequest{
		CandidateID: c.node1.ID,
		Term:        2,
		LastLogID:   ZeroLogID,
	})

	require.False(tt, rsp.Granted)
	require.Equal(tt, Term(2), rsp.Term)
	require.Equal(tt, Term(2), c.n2.state.Term)
	require.Equal(tt, Follower, c.n2.state.Role)
	require.Empty(tt, c.n2.state.VotedFor)
}

func TestVote_GrantsCandidateWithUpToDateLog(tt *testing.T) {
	ctx := context.Background()
	c := setupCluster(tt)

	require.NoError(tt, c.n2.log.Append(ctx,
		LogEntry{LogID: LogID{Index: 1, Term: 1}, Command: []byte("one")},
	))

	rsp := c.n2.Vote(context.Background(), VoteRequest{
		CandidateID: c.node1.ID,
		Term:        2,
		LastLogID:   LogID{Index: 1, Term: 1},
	})

	require.True(tt, rsp.Granted)
	require.Equal(tt, Term(2), rsp.Term)
	require.Equal(tt, Term(2), c.n2.state.Term)
	require.Equal(tt, Follower, c.n2.state.Role)
	require.Equal(tt, c.node1.ID, c.n2.state.VotedFor)
}

func TestVote_GrantsCandidateWithNewerLogTerm(tt *testing.T) {
	ctx := context.Background()
	c := setupCluster(tt)

	require.NoError(tt, c.n2.log.Append(ctx,
		LogEntry{LogID: LogID{Index: 1, Term: 1}, Command: []byte("one")},
		LogEntry{LogID: LogID{Index: 2, Term: 1}, Command: []byte("two")},
	))

	rsp := c.n2.Vote(context.Background(), VoteRequest{
		CandidateID: c.node1.ID,
		Term:        2,
		LastLogID:   LogID{Index: 1, Term: 2},
	})

	require.True(tt, rsp.Granted)
	require.Equal(tt, Term(2), rsp.Term)
	require.Equal(tt, c.node1.ID, c.n2.state.VotedFor)
}

func TestVote_RejectsCandidateWithSameTermShorterLog(tt *testing.T) {
	ctx := context.Background()
	c := setupCluster(tt)

	require.NoError(tt, c.n2.log.Append(ctx,
		LogEntry{LogID: LogID{Index: 1, Term: 1}, Command: []byte("one")},
		LogEntry{LogID: LogID{Index: 2, Term: 1}, Command: []byte("two")},
	))

	rsp := c.n2.Vote(context.Background(), VoteRequest{
		CandidateID: c.node1.ID,
		Term:        2,
		LastLogID:   LogID{Index: 1, Term: 1},
	})

	require.False(tt, rsp.Granted)
	require.Equal(tt, Term(2), rsp.Term)
	require.Empty(tt, c.n2.state.VotedFor)
}

func TestRunElectionLoop_StartsElectionAfterTimeout(tt *testing.T) {
	c := setupCluster(tt)

	c.n1.state.Role = Follower
	c.n1.cfg.ElectionTimeoutMin = 1 * time.Millisecond

	err := c.n1.RunElectionLoop(context.Background())

	require.NoError(tt, err)
	require.Equal(tt, Leader, c.n1.state.Role)
	require.Equal(tt, Term(2), c.n1.state.Term)
}

func TestRunElectionLoop_ReturnsAfterLostElection(tt *testing.T) {
	c := setupCluster(tt)

	c.n1.state.Role = Follower
	c.n1.cfg.ElectionTimeoutMin = 1 * time.Millisecond

	c.transport.unregister(c.node2.ID)
	c.transport.unregister(c.node3.ID)

	err := c.n1.RunElectionLoop(context.Background())

	require.NoError(tt, err)
	require.Equal(tt, Follower, c.n1.state.Role)
	require.Equal(tt, Term(2), c.n1.state.Term)
	require.Equal(tt, c.node1.ID, c.n1.state.VotedFor)
}

func TestRunElectionLoop_ResetsTimeoutOnLeaderSeen(tt *testing.T) {
	c := setupCluster(tt)

	c.n1.state.Role = Follower
	c.n1.cfg.ElectionTimeoutMin = 20 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- c.n1.RunElectionLoop(ctx)
	}()

	time.Sleep(10 * time.Millisecond)

	rsp := c.n1.AppendEntries(context.Background(), AppendEntriesRequest{
		LeaderID:  c.node2.ID,
		Term:      c.n1.state.Term,
		PrevLogID: ZeroLogID,
	})
	require.True(tt, rsp.Success)

	time.Sleep(15 * time.Millisecond)

	c.n1.mu.RLock()
	role := c.n1.state.Role
	c.n1.mu.RUnlock()

	require.Equal(tt, Follower, role)

	cancel()
	require.ErrorIs(tt, <-done, context.Canceled)
}
