package core

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunApplierLoop_AppliesCommittedEntriesInOrder(tt *testing.T) {
	ctx := context.Background()
	c := setupCluster(tt)
	leader := c.n1

	require.NoError(tt, leader.log.Append(ctx,
		LogEntry{LogID: LogID{Index: 1, Term: 1}, Command: []byte("one")},
		LogEntry{LogID: LogID{Index: 2, Term: 1}, Command: []byte("two")},
	))

	leader.mu.Lock()
	leader.updateCommitIndex(2)
	leader.mu.Unlock()

	require.NoError(tt, leader.applyNextCommands(context.Background()))

	require.Equal(tt, []string{"one", "two"}, c.n1Applier.Commands())
	require.Equal(tt, Index(2), leader.state.LastApplied)
}

func TestRunApplierLoop_ReturnsApplyError(tt *testing.T) {
	ctx := context.Background()
	c := setupCluster(tt)
	leader := c.n1

	applyErr := errors.New("apply failed")
	c.n1Applier.Fail(applyErr)

	require.NoError(tt, leader.log.Append(ctx,
		LogEntry{LogID: LogID{Index: 1, Term: 1}, Command: []byte("one")},
	))

	leader.mu.Lock()
	leader.updateCommitIndex(1)
	leader.mu.Unlock()

	err := leader.RunApplierLoop(context.Background())

	require.ErrorIs(tt, err, applyErr)
	require.Equal(tt, Index(0), leader.state.LastApplied)
	require.Empty(tt, c.n1Applier.Commands())
}
