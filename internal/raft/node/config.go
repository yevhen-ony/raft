package node

import (
	"raft/internal/raft/core"
	"raft/internal/raft/transport/rpc"
)

type Config struct {
	Raft      core.RaftConfig
	Cluster   core.ClusterConfig
	Transport rpc.Config
}
