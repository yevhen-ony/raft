package core

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)


func TestState_LoadsPersistentStateFromStore(tt *testing.T) {
  	ctx := context.Background()
  	store := NewInMemStateStore()

  	state, err := NewState(ctx, store, &Config{})
  	require.NoError(tt, err)

  	require.NoError(tt, state.SetTerm(ctx, Term(3)))
  	require.NoError(tt, state.SetVotedFor(ctx, NodeID("n2")))

  	reloaded, err := NewState(ctx, store, &Config{})
  	require.NoError(tt, err)

  	require.Equal(tt, Term(3), reloaded.Term)
  	require.Equal(tt, NodeID("n2"), reloaded.VotedFor)
  	require.Equal(tt, Follower, reloaded.Role)
  	require.Equal(tt, Index(0), reloaded.CommitIndex)
  	require.Equal(tt, Index(0), reloaded.LastApplied)
}


func TestState_SetTermClearsPersistedVote(tt *testing.T) {
  	ctx := context.Background()
  	store := NewInMemStateStore()

  	state, err := NewState(ctx, store, &Config{})
  	require.NoError(tt, err)

  	require.NoError(tt, state.SetVotedFor(ctx, NodeID("n1")))
  	require.NoError(tt, state.SetTerm(ctx, Term(2)))

  	reloaded, err := NewState(ctx, store, &Config{})
  	require.NoError(tt, err)

  	require.Equal(tt, Term(2), reloaded.Term)
  	require.Empty(tt, reloaded.VotedFor)
}
