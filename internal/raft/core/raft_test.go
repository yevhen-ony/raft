package core

import (
	"context"
	"testing"

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
		Config:       cfg,
		State:        restoredState,
		Log:          restoredLog,
		LogTransport: newLocalTransport(),
		VoteTransport: newLocalTransport(),
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
  		Config:        &Config{Self: Node{ID: "n1"}},
  		State:         state,
  		Log:           log,
  		LogTransport:  newLocalTransport(),
  		VoteTransport: newLocalTransport(),
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

