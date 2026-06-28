package core

import "time"

type Config struct {
	Cluster   ClusterConfig
	Raft      RaftConfig
	Transport TransportConfig
}

func (cfg *Config) WithCluster(cc ClusterConfig) *Config {
	copyCfg := *cfg
	copyCfg.Cluster = cc
	return &copyCfg 
}

type ClusterConfig struct {
	Self  Node
	Peers []Node
}

type RaftConfig struct {
	HeartbeatInterval  time.Duration
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
}

type TransportConfig struct {
	RequestTimeout time.Duration
}
