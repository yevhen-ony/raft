package main

import (
	"fmt"
	"os"
	"raft/internal/raft/core"
	"raft/internal/raft/node"
	"raft/internal/raft/transport/rpc"
	"time"

	yaml "gopkg.in/yaml.v3"
)

const (
	envSelfID                  = "RAFT_SELF_ID"
	envHeartbeatInterval       = "RAFT_HEARTBEAT_INTERVAL"
	envElectionTimeoutMin      = "RAFT_ELECTION_TIMEOUT_MIN"
	envElectionTimeoutMax      = "RAFT_ELECTION_TIMEOUT_MAX"
	envTransportRequestTimeout = "RAFT_TRANSPORT_REQUEST_TIMEOUT"
)

type Config struct {
	Listener  ListenerConfig  `yaml:"listener"`
	Cluster   ClusterConfig   `yaml:"cluster"`
	Transport TransportConfig `yaml:"transport"`
	Raft      RaftConfig      `yaml:"raft"`
}

type ListenerConfig struct {
	Addr string `yaml:"addr"`
}

func ConfigFromYAML(path string) (*Config, error) {
	cfg := &Config{}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	return cfg, nil
}

func (cfg *Config) OverrideWithEnv() {
	valueFromEnv(&cfg.Cluster.SelfID, envSelfID)
	valueFromEnv(&cfg.Raft.ElectionTimeoutMax, envElectionTimeoutMax)
	valueFromEnv(&cfg.Raft.ElectionTimeoutMin, envElectionTimeoutMin)
	valueFromEnv(&cfg.Raft.HeartbeatInterval, envHeartbeatInterval)
	valueFromEnv(&cfg.Transport.RequestTimeout, envTransportRequestTimeout)
}

func valueFromEnv(target *string, name string) {
	if value := os.Getenv(name); value != "" {
		*target = value
	}
}

func (cfg *Config) ToCore() (*node.Config, error) {
	cluster, err := cfg.Cluster.ToCore()
	if err != nil {
		return nil, fmt.Errorf("cluster: %w", err)
	}
	raft, err := cfg.Raft.ToCore()
	if err != nil {
		return nil, fmt.Errorf("raft: %w", err)
	}
	transport, err := cfg.Transport.ToCore()
	if err != nil {
		return nil, fmt.Errorf("transport: %w", err)
	}

	c := &node.Config{
		Raft:      *raft,
		Cluster:   *cluster,
		Transport: *transport,
	}
	return c, nil
}

type TransportConfig struct {
	RequestTimeout string `yaml:"request_timeout"`
}

func (cfg *TransportConfig) ToCore() (*rpc.Config, error) {
	reqTimeout, err := time.ParseDuration(cfg.RequestTimeout)
	if err != nil {
		return nil, fmt.Errorf("request timeout: %w", err)
	}
	corecfg := &rpc.Config{
		RequestTimeout: reqTimeout,
	}
	return corecfg, nil
}

type ClusterConfig struct {
	SelfID string    `yaml:"self_id"`
	Nodes  []NodeRef `yaml:"nodes"`
}

func (cfg ClusterConfig) ToCore() (*core.ClusterConfig, error) {
	var self core.NodeRef
	foundSelf := false

	peers := make([]core.NodeRef, 0, len(cfg.Nodes))

	for _, nodeCfg := range cfg.Nodes {
		node := core.NodeRef{
			ID:   core.NodeID(nodeCfg.ID),
			Addr: nodeCfg.Addr,
		}
		if nodeCfg.ID == cfg.SelfID {
			self = node
			foundSelf = true
			continue
		}
		peers = append(peers, node)
	}

	if !foundSelf {
		return nil, fmt.Errorf("self_id %q not found in cluster.nodes", cfg.SelfID)
	}

	corecfg := &core.ClusterConfig{
		Self:  self,
		Peers: peers,
	}

	return corecfg, nil
}

type RaftConfig struct {
	HeartbeatInterval  string `yaml:"heartbeat_interval"`
	ElectionTimeoutMin string `yaml:"election_timeout_min"`
	ElectionTimeoutMax string `yaml:"election_timeout_max"`
}

type NodeRef struct {
	ID   string `yaml:"id"`
	Addr string `yaml:"addr"`
}

func (cfg RaftConfig) ToCore() (*core.RaftConfig, error) {
	heartbeatInterval, err := time.ParseDuration(cfg.HeartbeatInterval)
	if err != nil {
		return nil, fmt.Errorf("heartbeat interval: %w", err)
	}

	electionTimeoutMin, err := time.ParseDuration(cfg.ElectionTimeoutMin)
	if err != nil {
		return nil, fmt.Errorf("election timeout min: %w", err)
	}

	electionTimeoutMax, err := time.ParseDuration(cfg.ElectionTimeoutMax)
	if err != nil {
		return nil, fmt.Errorf("election timeout max: %w", err)
	}

	corecfg := &core.RaftConfig{
		HeartbeatInterval:  heartbeatInterval,
		ElectionTimeoutMin: electionTimeoutMin,
		ElectionTimeoutMax: electionTimeoutMax,
	}
	return corecfg, nil
}
