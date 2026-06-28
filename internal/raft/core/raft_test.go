package core

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRaft_RestoresStateAndLogFromStores(tt *testing.T) {
	ctx := context.Background()

	stateStore := NewInMemStateStore()
	logStore := NewInMemLogStore()

	state, err := NewState(ctx, stateStore)
	require.NoError(tt, err)
	require.NoError(tt, state.SetTerm(ctx, Term(3)))
	require.NoError(tt, state.SetVotedFor(ctx, NodeID("n2")))

	log, err := NewLog(ctx, logStore)
	require.NoError(tt, err)
	require.NoError(tt, log.Append(ctx,
		LogEntry{LogID: LogID{Index: 1, Term: 2}, Command: []byte("one")},
		LogEntry{LogID: LogID{Index: 2, Term: 3}, Command: []byte("two")},
	))

	restoredState, err := NewState(ctx, stateStore)
	require.NoError(tt, err)

	restoredLog, err := NewLog(ctx, logStore)
	require.NoError(tt, err)

	raft, err := NewRaft(RaftDeps{
		Config:    config(),
		State:     restoredState,
		Log:       restoredLog,
		Transport: newLocalTransport(),
	})
	require.NoError(tt, err)

	require.Equal(tt, Term(3), raft.state.Term)
	require.Equal(tt, NodeID("n2"), raft.state.VotedFor)
	require.Equal(tt, Follower, raft.state.Role)

	entries, err := raft.log.EntriesAfter(ZeroLogID)
	require.NoError(tt, err)
	require.Len(tt, entries, 2)
	require.Equal(tt, "one", string(entries[0].Command))
	require.Equal(tt, "two", string(entries[1].Command))
}

func TestRaft_PersistsGrantedVote(tt *testing.T) {
	ctx := context.Background()

	stateStore := NewInMemStateStore()
	state, err := NewState(ctx, stateStore)
	require.NoError(tt, err)
	require.NoError(tt, state.SetTerm(ctx, Term(1)))

	log, err := NewLog(ctx, NewInMemLogStore())
	require.NoError(tt, err)

	raft, err := NewRaft(RaftDeps{
		Config:    config(),
		State:     state,
		Log:       log,
		Transport: newLocalTransport(),
	})
	require.NoError(tt, err)

	rsp := raft.Vote(ctx, VoteRequest{
		CandidateID: "n2",
		Term:        1,
		LastLogID:   ZeroLogID,
	})

	require.True(tt, rsp.Granted)

	reloaded, err := NewState(ctx, stateStore)
	require.NoError(tt, err)

	require.Equal(tt, Term(1), reloaded.Term)
	require.Equal(tt, NodeID("n2"), reloaded.VotedFor)
}

func TestCluster_ElectsSingleLeader(tt *testing.T) {
	c := setupCluster(tt)

	for _, node := range c.Nodes() {
		node.cfg.ElectionTimeoutMin = 5 * time.Millisecond
		node.cfg.ElectionTimeoutMax = 25 * time.Millisecond
		node.cfg.HeartbeatInterval = 2 * time.Millisecond
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 3)
	for _, node := range []*Raft{c.n1, c.n2, c.n3} {
		go func(node *Raft) { done <- node.Run(ctx) }(node)
	}

	require.Eventually(tt, func() bool {
		leaders := 0
		for _, node := range []*Raft{c.n1, c.n2, c.n3} {
			node.mu.RLock()
			if node.state.Role == Leader {
				leaders++
			}
			node.mu.RUnlock()
		}
		return leaders == 1
	}, time.Second, time.Millisecond)

	cancel()

	for range 3 {
		require.ErrorIs(tt, <-done, context.Canceled)
	}
}

func TestCluster_ElectedLeaderReplicatesWithoutLeadershipChange(tt *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c := setupCluster(tt)
	done := c.Run(ctx)

	// for _, node := range c.Nodes() {
	// 	node.cfg.ElectionTimeoutMin = 30 * time.Millisecond
	// 	node.cfg.ElectionTimeoutMax = 60 * time.Millisecond
	// 	node.cfg.HeartbeatInterval = 5 * time.Millisecond
	// }
	//

	require.Eventually(tt, func() bool {
		return countLeaders(c) == 1
	}, time.Second, time.Millisecond)

	leader := c.Leader(tt)

	var err error

	_, err = leader.Propose(context.Background(), []byte("one"))
	require.NoError(tt, err)
	require.Same(tt, leader, c.Leader(tt))

	_, err = leader.Propose(context.Background(), []byte("two"))
	require.NoError(tt, err)
	require.Same(tt, leader, c.Leader(tt))

	_, err = leader.Propose(context.Background(), []byte("three"))
	require.NoError(tt, err)
	require.Same(tt, leader, c.Leader(tt))
	
	cancel()
	for range 3 {
		require.ErrorIs(tt, <-done, context.Canceled)
	}
}
