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

	cfg := &Config{
		Self: Node{ID: "n1"},
	}

	state, err := NewState(ctx, stateStore, cfg)
	require.NoError(tt, err)
	require.NoError(tt, state.SetTerm(ctx, Term(3)))
	require.NoError(tt, state.SetVotedFor(ctx, NodeID("n2")))

	log, err := NewLog(ctx, logStore)
	require.NoError(tt, err)
	require.NoError(tt, log.Append(ctx,
		LogEntry{LogID: LogID{Index: 1, Term: 2}, Command: []byte("one")},
		LogEntry{LogID: LogID{Index: 2, Term: 3}, Command: []byte("two")},
	))

	restoredState, err := NewState(ctx, stateStore, cfg)
	require.NoError(tt, err)

	restoredLog, err := NewLog(ctx, logStore)
	require.NoError(tt, err)

	raft, err := NewRaft(RaftDeps{
		Config:    cfg,
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
	state, err := NewState(ctx, stateStore, &Config{Self: Node{ID: "n1"}})
	require.NoError(tt, err)
	require.NoError(tt, state.SetTerm(ctx, Term(1)))

	log, err := NewLog(ctx, NewInMemLogStore())
	require.NoError(tt, err)

	raft, err := NewRaft(RaftDeps{
		Config:    &Config{Self: Node{ID: "n1"}},
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

	reloaded, err := NewState(ctx, stateStore, &Config{Self: Node{ID: "n1"}})
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
	c := setupCluster(tt)

	for _, node := range c.Nodes() {
		node.cfg.ElectionTimeoutMin = 30 * time.Millisecond
		node.cfg.ElectionTimeoutMax = 60 * time.Millisecond
		node.cfg.HeartbeatInterval = 5 * time.Millisecond
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := c.Run(ctx)

	require.Eventually(tt, func() bool {
		return countLeaders(c) == 1
	}, time.Second, time.Millisecond)

	leader := c.Leader(tt)

	leader.mu.RLock()
	leaderID := leader.cluster.Self.ID
	term := leader.state.Term
	leader.mu.RUnlock()


	var err error
	_, err = leader.Propose(context.Background(), []byte("one"))
	require.NoError(tt, err)
	_, err = leader.Propose(context.Background(), []byte("two"))
	require.NoError(tt, err)
 	_, err = leader.Propose(context.Background(), []byte("three"))
	require.NoError(tt, err)

	require.Eventually(tt, func() bool {
		for _, node := range c.Nodes() {
			entries, err := node.log.EntriesAfter(ZeroLogID)
			if err != nil || len(entries) != 3 {
				return false
			}
		}
		return true
	}, time.Second, time.Millisecond)

	require.Equal(tt, 1, countLeaders(c))

	nextLeader := c.Leader(tt)

	nextLeader.mu.RLock()
	defer nextLeader.mu.RUnlock()

	require.Equal(tt, leaderID, nextLeader.cluster.Self.ID)
	require.Equal(tt, term, nextLeader.state.Term)

	cancel()

	for range 3 {
		require.ErrorIs(tt, <-done, context.Canceled)
	}
}
