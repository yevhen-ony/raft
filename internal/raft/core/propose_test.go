package core

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestProposeAndWait_ReturnsAfterCommandApplied(tt *testing.T) {
	c := setupCluster(tt)
	leader := c.n1
	c.transport.unregister(c.node3.ID)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- leader.RunApplierLoop(ctx)
	}()

	require.NoError(tt, leader.ProposeAndWait(context.Background(), []byte("hello")))

	require.Equal(tt, []string{"hello"}, c.n1Applier.Commands())
	require.Equal(tt, Index(1), leader.state.CommitIndex)
	require.Equal(tt, Index(1), leader.state.LastApplied)

	cancel()
	require.ErrorIs(tt, <-done, context.Canceled)
}

func TestWaitApplied_ReturnsDeadlineExceededWhenCommandNotApplied(tt *testing.T) {
	c := setupCluster(tt)
	leader := c.n1
	c.transport.unregister(c.node3.ID)

	require.NoError(tt, leader.Propose(context.Background(), []byte("hello")))

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()

	err := leader.waitApplied(ctx, 1)

	require.ErrorIs(tt, err, context.DeadlineExceeded)
	require.Equal(tt, Index(1), leader.state.CommitIndex)
	require.Equal(tt, Index(0), leader.state.LastApplied)
	require.Empty(tt, c.n1Applier.Commands())
}

func TestWaitApplied_WakesMultipleWaiters(tt *testing.T) {
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

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	wait1 := make(chan error, 1)
	wait2 := make(chan error, 1)

	go func() {
		wait1 <- leader.waitApplied(ctx, 1)
	}()
	go func() {
		wait2 <- leader.waitApplied(ctx, 2)
	}()

	require.NoError(tt, leader.applyNextCommands(context.Background()))

	require.NoError(tt, <-wait1)
	require.NoError(tt, <-wait2)
	require.Equal(tt, []string{"one", "two"}, c.n1Applier.Commands())
}
