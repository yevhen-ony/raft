package main

import (
	"fmt"
	"os"

	yaml "gopkg.in/yaml.v3"
	kvnode "raft/internal/kv/node"
)

const (
	envLoggerLevel  = "LOG_LEVEL"
	envListenerAddr = "LISTENER_ADDR"
)

type Config struct {
	Logger   LoggerConfig   `yaml:"logger"`
	KV       kvnode.Config  `yaml:"kv"`
	Listener ListenerConfig `yaml:"listener"`
	Raft     RaftConfig     `yaml:"raft"`
}

type ListenerConfig struct {
	Addr string `yaml:"addr"`
}

func (cfg *Config) OverwriteWithEnv() {
	valueFromEnv(&cfg.Logger.Level, envLoggerLevel)
	valueFromEnv(&cfg.Listener.Addr, envListenerAddr)
	cfg.Raft.OverwriteWithEnv()
}

func valueFromEnv(target *string, name string) {
	if value := os.Getenv(name); value != "" {
		*target = value
	}
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
