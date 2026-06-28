package core

import "time"

type ClusterConfig struct{
	Self  NodeRef
	Peers []NodeRef
}

type RaftConfig struct {
	HeartbeatInterval  time.Duration
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
}
