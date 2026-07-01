package node

import (
	"errors"
	"fmt"
	"raft/internal/kv"
	"raft/internal/kv/core"
	"raft/internal/kv/guard"
	"raft/internal/kv/rpc"
	api "raft/gen/proto/kv/api/v1"
	raftcore "raft/internal/raft/core"

	"google.golang.org/grpc"
)

type NodeDeps struct {
	State   *core.State
	Codec   core.Codec
	Applier core.CommandApplier
}

func NewNodeDeps(cfg *Config) (*NodeDeps, error) {
	if cfg == nil {
		return nil, errors.New("missing config")
	}

	state := core.NewState()
	codec := newCodec(cfg.Codec)

	applier, err := core.NewStateCommandApplier(state, codec)
	if err != nil {
		return nil, fmt.Errorf("applier: %w", err)
	}
	deps := &NodeDeps{
		State:   state,
		Codec:   codec,
		Applier: applier,
	}
	return deps, nil
}

type Node struct {
	service kv.KV
	server  *rpc.KVServer
}

func NewLocalNode(deps *NodeDeps) (*Node, error) {
	if deps == nil {
		return nil, errors.New("missing deps")
	}
	committer, err := core.NewLocalCommandCommitter(deps.Applier)
	if err != nil {
		return nil, fmt.Errorf("local committer: %w", err)
	}

	service, err := core.NewKVService(core.KVDeps{
		State:     deps.State,
		Codec:     deps.Codec,
		Committer: committer,
	})
	if err != nil {
		return nil, fmt.Errorf("local kv service: %w", err)
	}

	server, err := rpc.NewKVServer(service)
	if err != nil {
		return nil, fmt.Errorf("api server: %w", err)
	}

	node := &Node{
		service: service,
		server:  server,
	}

	return node, nil
}

func NewRaftNode(deps *NodeDeps, raft *raftcore.Raft) (*Node, error) {
	if deps == nil {
		return nil, errors.New("missing deps")
	}
	committer, err := core.NewRaftCommitter(raft)
	if err != nil {
		return nil, fmt.Errorf("raft committer: %w", err)
	}
	service, err := core.NewKVService(core.KVDeps{
		State:     deps.State,
		Codec:     deps.Codec,
		Committer: committer,
	})
	if err != nil {
		return nil, fmt.Errorf("raft kv service: %w", err)
	}
	guarded, err := guard.NewLeaderGuardedService(raft, service)
	if err != nil {
		return nil, fmt.Errorf("leader guard: %w", err)
	}

	server, err := rpc.NewKVServer(guarded)
	if err != nil {
		return nil, fmt.Errorf("api server: %w", err)
	}

	node := &Node{
		service: guarded,
		server:  server,
	}
	return node, nil
}

func (n *Node) Register(registrar grpc.ServiceRegistrar) {
	api.RegisterKVServiceServer(registrar, n.server)
}

func newCodec(label string) core.Codec {
	switch label {
	case "json":
		return core.NewJSONCodec()
	case "proto":
		return core.NewProtoCodec()
	default:
		return core.NewJSONCodec()
	}
}
