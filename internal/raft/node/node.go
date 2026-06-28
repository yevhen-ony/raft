package node

import (
	"context"
	"errors"
	"fmt"

	api "raft/gen/proto/raft/v1"
	"raft/internal/raft/core"
	"raft/internal/raft/transport/rpc"

	"google.golang.org/grpc"
)

type NodeDeps struct {
	Config         *Config
	StateStore     core.StateStore
	LogStore       core.LogStore
	CommandApplier core.CommandApplier
}

type Node struct {
	log     *core.Log
	state   *core.State
	cluster *core.Cluster

	conns     *rpc.GRPCConnectionSource
	transport *rpc.GRPCPeerTransport
	server    *rpc.GRPCRaftPeerServer

	Raft *core.Raft

	config *Config
}

func NewNode(ctx context.Context, deps NodeDeps) (*Node, error) {
	if deps.Config == nil {
		return nil, errors.New("missing config")
	}
	if deps.StateStore == nil {
		return nil, errors.New("missing state store")
	}
	if deps.LogStore == nil {
		return nil, errors.New("missing log store")
	}
	if deps.CommandApplier == nil {
		return nil, fmt.Errorf("missing command applier")
	}

	state, err := core.NewState(ctx, deps.StateStore)
	if err != nil {
		return nil, fmt.Errorf("create state: %w", err)
	}

	log, err := core.NewLog(ctx, deps.LogStore)
	if err != nil {
		return nil, fmt.Errorf("create log: %w", err)
	}

	cfg := deps.Config

	cluster := core.NewCluster(&cfg.Cluster)
	connSource, err := rpc.NewGRPCConnectionSource(cfg.Cluster.Peers)
	if err != nil {
		return nil, fmt.Errorf("conn source: %w", err)
	}

	transport, err := rpc.NewGRPCPeerTransport(connSource, &cfg.Transport)
	if err != nil {
		return nil, fmt.Errorf("grpc transport: %w", err)
	}

	raft, err := core.NewRaft(core.RaftDeps{
		Log:            log,
		State:          state,
		Cluster:        cluster,
		Transport:      transport,
		CommandApplier: deps.CommandApplier,
		Config:         &cfg.Raft,
	})
	if err != nil {
		return nil, fmt.Errorf("create raft: %w", err)
	}

	server, err := rpc.NewGRPCRaftPeerServer(raft)
	if err != nil {
		return nil, fmt.Errorf("new peer server: %w", err)
	}

	node := &Node{
		log:     log,
		state:   state,
		cluster: cluster,

		conns:     connSource,
		transport: transport,
		server:    server,

		config: cfg,

		Raft: raft,
	}
	return node, nil
}

func (n *Node) Run(ctx context.Context) error {
	return n.Raft.Run(ctx)
}

func (n *Node) Register(registrar grpc.ServiceRegistrar) {
	api.RegisterRaftPeerServiceServer(registrar, n.server)
}

func (n *Node) Close() error {
	if n.conns != nil {
		return n.conns.Close()
	}
	return nil
}
