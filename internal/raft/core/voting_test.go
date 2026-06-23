package core

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVoting_CandidateWinsElectionWithQuorum(tt *testing.T) {
  	c := setupCluster(tt)

  	c.n1.state.Role = Follower

  	err := c.n1.RunElection(context.Background())



	require.NoError(tt, err)
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

  	err := c.n1.RunElection(context.Background())

  	require.ErrorIs(tt, err, ErrLeader)
  	require.Equal(tt, Leader, c.n1.state.Role)
}


func TestVoting_CandidateLosesElectionWithoutQuorum(tt *testing.T) {
  	c := setupCluster(tt)

  	c.n1.state.Role = Follower
  	c.transport.unregister(c.node2.ID)
  	c.transport.unregister(c.node3.ID)

  	err := c.n1.RunElection(context.Background())

  	require.ErrorIs(tt, err, ErrElectionLost)
  	require.Equal(tt, Candidate, c.n1.state.Role)
  	require.Equal(tt, Term(2), c.n1.state.Term)
  	require.Equal(tt, c.node1.ID, c.n1.state.VotedFor)
}
