package core

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type clusterFixture struct {
	transport *localTransport

	n1 *Raft
	n2 *Raft
	n3 *Raft

	node1 Node
	node2 Node
	node3 Node

	n1Applier *recordingCommandApplier
	n2Applier *recordingCommandApplier
	n3Applier *recordingCommandApplier
}

func setupCluster(tt *testing.T) *clusterFixture {
	tt.Helper()

	f := &clusterFixture{
		transport: newLocalTransport(),
		node1:     Node{ID: "n1"},
		node2:     Node{ID: "n2"},
		node3:     Node{ID: "n3"},
	}

	cfg := config()

	cc1 := *newClusterConfig(f.node1, []Node{f.node2, f.node3})
	f.n1Applier = newRecordingCommandApplier()
	f.n1 = f.newRaft(tt, cfg.WithCluster(cc1), f.n1Applier)
	f.transport.register(f.node1.ID, f.n1)

	cc2 := *newClusterConfig(f.node2, []Node{f.node1, f.node3})
	f.n2Applier = newRecordingCommandApplier()
	f.n2 = f.newRaft(tt, cfg.WithCluster(cc2), f.n2Applier)
	f.transport.register(f.node2.ID, f.n2)

	cc3 := *newClusterConfig(f.node3, []Node{f.node1, f.node2})
	f.n3Applier = newRecordingCommandApplier()
	f.n3 = f.newRaft(tt, cfg.WithCluster(cc3), f.n3Applier)
	f.transport.register(f.node3.ID, f.n3)

	return f
}

func raftConfig() *RaftConfig {
	return &RaftConfig{
		HeartbeatInterval:  5 * time.Millisecond,
		ElectionTimeoutMin: 25 * time.Millisecond,
		ElectionTimeoutMax: 50 * time.Millisecond,
	}
}

func clusterConfig() *ClusterConfig {
	return &ClusterConfig{
		Self:  Node{ID: "n1"},
		Peers: []Node{{ID: "n2"}, {ID: "n3"}},
	}
}

func newClusterConfig(self Node, peers []Node) *ClusterConfig {
	return &ClusterConfig{Self: self, Peers: peers}
}

func config() *Config {
	return &Config{
		Raft:    *raftConfig(),
		Cluster: *clusterConfig(),
	}
}

func (f *clusterFixture) newRaft(
	tt *testing.T,
	cfg *Config,
	applier CommandApplier,
) *Raft {
	tt.Helper()

	ctx := context.Background()

	logStore := NewInMemLogStore()
	log, err := NewLog(ctx, logStore)
	require.NoError(tt, err)

	stateStore := NewInMemStateStore()
	state, err := NewState(ctx, stateStore)
	require.NoError(tt, err)

	r, err := NewRaft(RaftDeps{
		Log:            log,
		State:          state,
		Transport:      f.transport,
		CommandApplier: applier,
		Config:         cfg,
	})
	require.NoError(tt, err)

	return r
}

func (f *clusterFixture) Nodes() []*Raft {
	return []*Raft{f.n1, f.n2, f.n3}
}

func (f *clusterFixture) WithLeader(tt *testing.T, term Term) *clusterFixture {
	tt.Helper()
	ctx := context.Background()

	for _, node := range f.Nodes() {
		node.state.Role = Follower
		require.NoError(tt, node.state.SetTerm(ctx, term))
		require.NoError(tt, node.state.SetVotedFor(ctx, ""))
	}

	require.NoError(tt, f.n1.state.SetVotedFor(ctx, f.n1.cluster.Self.ID))
	f.n1.state.Role = Leader
	return f
}

func (f *clusterFixture) Run(ctx context.Context) <-chan error {
	done := make(chan error, 3)

	for _, node := range f.Nodes() {
		go func(node *Raft) {
			done <- node.Run(ctx)
		}(node)
	}

	return done
}

func countLeaders(c *clusterFixture) int {
	leaders := 0
	for _, node := range []*Raft{c.n1, c.n2, c.n3} {
		node.mu.RLock()
		if node.state.Role == Leader {
			leaders++
		}
		node.mu.RUnlock()
	}
	return leaders
}

func (c *clusterFixture) Leader(tt *testing.T) *Raft {
	for _, node := range []*Raft{c.n1, c.n2, c.n3} {
		node.mu.RLock()
		if node.state.Role == Leader {
			node.mu.RUnlock()
			return node
		}
		node.mu.RUnlock()
	}
	require.FailNow(tt, "leader not found")
	return nil
}

type localTransport struct {
	mu        sync.RWMutex
	nodes     map[NodeID]*Raft
	failures  map[NodeID]error
	highterms map[NodeID]Term
}

func newLocalTransport() *localTransport {
	return &localTransport{
		nodes:     make(map[NodeID]*Raft),
		failures:  make(map[NodeID]error),
		highterms: make(map[NodeID]Term),
	}
}

func (t *localTransport) register(id NodeID, node *Raft) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.nodes[id] = node
}

func (t *localTransport) unregister(id NodeID) {
	t.mu.Lock()
	defer t.mu.Unlock()

	delete(t.nodes, id)
}

func (t *localTransport) fail(id NodeID, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.failures[id] = err
}

func (t *localTransport) highTerm(id NodeID, term Term) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.highterms[id] = term
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
	err := t.failures[peer.ID]
	node := t.nodes[peer.ID]
	term := t.highterms[peer.ID]
	t.mu.RUnlock()

	if err != nil {
		return AppendEntriesResponse{}, err
	}
	if term != 0 {
		return AppendEntriesResponse{Term: term, Success: false}, nil
	}
	if node == nil {
		return AppendEntriesResponse{}, fmt.Errorf("unknown peer %s", peer.ID)
	}

	return node.AppendEntries(ctx, req), nil
}

func (t *localTransport) RequestVote(
	ctx context.Context,
	peer Node,
	req VoteRequest,
) (VoteResponse, error) {
	if err := ctx.Err(); err != nil {
		return VoteResponse{}, err
	}

	t.mu.RLock()
	err := t.failures[peer.ID]
	node := t.nodes[peer.ID]
	term := t.highterms[peer.ID]
	t.mu.RUnlock()

	if err != nil {
		return VoteResponse{}, err
	}
	if term != 0 {
		return VoteResponse{Term: term, Granted: false}, nil
	}
	if node == nil {
		return VoteResponse{}, fmt.Errorf("unknown peer %s", peer.ID)
	}

	return node.Vote(ctx, req), nil
}

var _ Transport = (*localTransport)(nil)

func requireEntries(tt *testing.T, node *Raft, expected ...string) {
	tt.Helper()

	entries, err := node.log.EntriesAfter(ZeroLogID)
	require.NoError(tt, err)
	require.Len(tt, entries, len(expected))

	for i, command := range expected {
		require.Equal(tt, Index(i+1), entries[i].Index)
		require.Equal(tt, command, string(entries[i].Command))
	}
}

type recordingCommandApplier struct {
	mu       sync.Mutex
	commands []string
	err      error
}

func newRecordingCommandApplier() *recordingCommandApplier {
	return &recordingCommandApplier{}
}

func (a *recordingCommandApplier) Apply(_ context.Context, command []byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.err != nil {
		return a.err
	}
	a.commands = append(a.commands, string(command))
	return nil
}

func (a *recordingCommandApplier) Commands() []string {
	a.mu.Lock()
	defer a.mu.Unlock()

	return append([]string(nil), a.commands...)
}

func (a *recordingCommandApplier) Fail(err error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.err = err
}
