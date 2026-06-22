package core

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRaftCluster_ProposeReplicatesToFollower(tt *testing.T) {
	ctx := context.Background()

	leader, follower := setupCluster(tt)

	require.NoError(tt, leader.Propose(ctx, []byte("hello")))

	entries, err := follower.log.EntriesAfter(ZeroLogID)
	require.NoError(tt, err)
	require.Len(tt, entries, 1)

	entry := entries[0]
	require.Equal(tt, Index(1), entry.Index)
	require.Equal(tt, Term(1), entry.Term)
	require.Equal(tt, "hello", string(entry.Command))
}

func TestRaftCluster_FollowerRejectsStaleEntry(tt *testing.T) {
	_, follower := setupCluster(tt)

	rsp := follower.AppendEntries(context.Background(), AppendEntriesRequest{
		LeaderID:  "n1",
		Term:      0,
		PrevLogID: ZeroLogID,
	})

	require.False(tt, rsp.Success)
	require.Equal(tt, Term(1), rsp.Term)

	entries, err := follower.log.EntriesAfter(ZeroLogID)
	require.NoError(tt, err)
	require.Empty(tt, entries)
}

func TestRaftCluster_FollowerRejectsMissingPrevLog(tt *testing.T) {
	_, follower := setupCluster(tt)

	rsp := follower.AppendEntries(context.Background(), AppendEntriesRequest{
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

	entries, err := follower.log.EntriesAfter(ZeroLogID)
	require.NoError(tt, err)
	require.Empty(tt, entries)
}

func TestRaftCluster_HeartbeatDoesNotTruncateFollowerLog(tt *testing.T) {
	ctx := context.Background()

	leader, follower := setupCluster(tt)

	require.NoError(tt, leader.Propose(ctx, []byte("hello")))

	rsp := follower.AppendEntries(ctx, AppendEntriesRequest{
		LeaderID:  "n1",
		Term:      1,
		PrevLogID: ZeroLogID,
		Entries:   nil, // empty entries -> heartbeat
	})

	require.True(tt, rsp.Success)

	entries, err := follower.log.EntriesAfter(ZeroLogID)
	require.NoError(tt, err)
	require.Len(tt, entries, 1)
	require.Equal(tt, "hello", string(entries[0].Command))
}

func TestRaftCluster_LeaderStepsDownOnHigherTermAppendEntries(tt *testing.T) {
	leader, _ := setupCluster(tt)

	rsp := leader.AppendEntries(context.Background(), AppendEntriesRequest{
		LeaderID:  "n2",
		Term:      2,
		PrevLogID: ZeroLogID,
	})

	require.True(tt, rsp.Success)
	require.Equal(tt, Term(2), rsp.Term)
	require.Equal(tt, Term(2), leader.state.Term)
	require.Equal(tt, Follower, leader.state.Role)
}

func TestRaftCluster_FollowerAppendsEntriesFromHigherTermLeader(tt *testing.T) {
	_, follower := setupCluster(tt)

	rsp := follower.AppendEntries(context.Background(), AppendEntriesRequest{
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
	require.Equal(tt, Term(2), follower.state.Term)
	require.Equal(tt, Follower, follower.state.Role)

	entries, err := follower.log.EntriesAfter(ZeroLogID)
	require.NoError(tt, err)
	require.Len(tt, entries, 1)
	require.Equal(tt, Term(2), entries[0].Term)
	require.Equal(tt, "hello", string(entries[0].Command))
}

func TestRaftCluster_ProposeReplicatesSequentialEntries(tt *testing.T) {
	ctx := context.Background()

	leader, follower := setupCluster(tt)

	require.NoError(tt, leader.Propose(ctx, []byte("one")))
	require.NoError(tt, leader.Propose(ctx, []byte("two")))
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

func TestRaftCluster_AppendEntriesReplacesFollowerConflict(tt *testing.T) {
	_, follower := setupCluster(tt)

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

func TestRaftCluster_LeaderStepsDownOnHigherTermReplicationResponse(tt *testing.T) {
	leader, err := NewRaft(RaftDeps{
		Config: &Config{
			Self:   Node{ID: "n1"},
			Peers:  []Node{{ID: "n2"}},
			Leader: true,
		},
		Transport: higherTermTransport{},
	})
	require.NoError(tt, err)

	err = leader.Propose(context.Background(), []byte("hello"))

	require.ErrorIs(tt, err, ErrNotLeader)
	require.Equal(tt, Term(2), leader.state.Term)
	require.Equal(tt, Follower, leader.state.Role)
}

type higherTermTransport struct{}

func (t higherTermTransport) AppendEntries(
	ctx context.Context,
	peer Node,
	req AppendEntriesRequest,
) (AppendEntriesResponse, error) {
	return AppendEntriesResponse{
		Term:    req.Term + 1,
		Success: false,
	}, nil
}

func setupCluster(tt *testing.T) (*Raft, *Raft) {
	tt.Helper()

	n1 := Node{ID: "n1"}
	n2 := Node{ID: "n2"}

	transport := newLocalTransport()

	leader, err := NewRaft(RaftDeps{
		Config: &Config{
			Self:   n1,
			Peers:  []Node{n2},
			Leader: true,
		},
		Transport: transport,
	})
	require.NoError(tt, err)

	follower, err := NewRaft(RaftDeps{
		Config: &Config{
			Self:  n2,
			Peers: []Node{n1},
		},
		Transport: transport,
	})
	require.NoError(tt, err)

	transport.register(n1.ID, leader)
	transport.register(n2.ID, follower)
	return leader, follower
}

type localTransport struct {
	mu    sync.RWMutex
	nodes map[NodeID]*Raft
}

func newLocalTransport() *localTransport {
	return &localTransport{nodes: make(map[NodeID]*Raft)}
}

func (t *localTransport) register(id NodeID, node *Raft) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nodes[id] = node
}

func (t *localTransport) AppendEntries(
	ctx context.Context,
	peer Node,
	req AppendEntriesRequest,
) (AppendEntriesResponse, error) {
	if err := ctx.Err(); err != nil {
		return AppendEntriesResponse{}, err
	}

	t.mu.RLock()
	node := t.nodes[peer.ID]
	t.mu.RUnlock()

	if node == nil {
		return AppendEntriesResponse{}, fmt.Errorf("unknown peer %s", peer.ID)
	}

	return node.AppendEntries(ctx, req), nil
}
