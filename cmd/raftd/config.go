package main

import (
	"fmt"
	"os"
	"time"

	yaml "gopkg.in/yaml.v3"

	"raft/internal/raft/core"
)

const (
	envSelfID                  = "RAFT_SELF_ID"
	envHeartbeatInterval       = "RAFT_HEARTBEAT_INTERVAL"
	envElectionTimeoutMin      = "RAFT_ELECTION_TIMEOUT_MIN"
	envElectionTimeoutMax      = "RAFT_ELECTION_TIMEOUT_MAX"
	envTransportRequestTimeout = "RAFT_TRANSPORT_REQUEST_TIMEOUT"
)

type Config struct {
	Cluster   ClusterConfig   `yaml:"cluster"`
	Transport TransportConfig `yaml:"transport"`
	Raft      RaftConfig      `yaml:"raft"`
}

type TransportConfig struct {
	RequestTimeout string `yaml:"request_timeout"`
}

type RaftConfig struct {
	HeartbeatInterval  string `yaml:"heartbeat_interval"`
	ElectionTimeoutMin string `yaml:"election_timeout_min"`
	ElectionTimeoutMax string `yaml:"election_timeout_max"`
}

type ClusterConfig struct {
	SelfID string       `yaml:"self_id"`
	Nodes  []NodeConfig `yaml:"nodes"`
}

type NodeConfig struct {
	ID   string `yaml:"id"`
	Addr string `yaml:"addr"`
}

func configFromYAML(path string, cfg *Config) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read config: %w", err)
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return fmt.Errorf("parse config: %w", err)
	}

	return nil
}

func overrideWithEnv(cfg *Config) {
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

func (cfg ClusterConfig) ToCore() (*core.ClusterConfig, error) {
	var self core.Node
	foundSelf := false

	peers := make([]core.Node, 0, len(cfg.Nodes))

	for _, nodeCfg := range cfg.Nodes {
		node := core.Node{
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

	ccfg := &core.ClusterConfig{
		Self: self,
		Peers: peers,
	}

	return ccfg, nil
}


func (cfg Config) ToCore() (*core.Config, error) {
  	cluster, err := cfg.Cluster.ToCore()
  	if err != nil {
  		return nil, fmt.Errorf("cluster: %w", err)
  	}

  	heartbeatInterval, err := time.ParseDuration(cfg.Raft.HeartbeatInterval)
  	if err != nil {
		return nil, fmt.Errorf("heartbeat interval: %w", err)
  	}

  	electionTimeoutMin, err := time.ParseDuration(cfg.Raft.ElectionTimeoutMin)
  	if err != nil {
		return nil, fmt.Errorf("election timeout min: %w", err) 
  	}

  	electionTimeoutMax, err := time.ParseDuration(cfg.Raft.ElectionTimeoutMax)
  	if err != nil {
		return nil, fmt.Errorf("election timeout max: %w", err)
  	}

  	requestTimeout, err := time.ParseDuration(cfg.Transport.RequestTimeout)
  	if err != nil {
		return nil, fmt.Errorf("transport request timeout: %w", err)
  	}

  	return &core.Config{
  		Cluster: *cluster,
  		Raft: core.RaftConfig{
  			HeartbeatInterval:  heartbeatInterval,
  			ElectionTimeoutMin: electionTimeoutMin,
  			ElectionTimeoutMax: electionTimeoutMax,
  		},
  		Transport: core.TransportConfig{
  			RequestTimeout: requestTimeout,
  		},
  	}, nil
}
