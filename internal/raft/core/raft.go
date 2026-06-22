package core

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type RaftDeps struct {
	Transport Transport
	Config    *Config
}

type Raft struct {
	cluster   *Cluster
	state     *State
	log       *Log
	transport Transport

	mu sync.RWMutex
}

func NewRaft(deps RaftDeps) (*Raft, error) {
	if deps.Transport == nil {
		return nil, errors.New("missing transport")
	}
	r := &Raft{
		cluster:   NewCluster(deps.Config),
		state:     NewState(deps.Config),
		log:       NewLog(),
		transport: deps.Transport,
	}
	return r, nil
}

func (r *Raft) Propose(ctx context.Context, cmd []byte) error {
	if err := r.ensureLeader(); err != nil {
		return err
	}

	prev, err := r.appendToLog(cmd)
	if err != nil {
		return fmt.Errorf("append to log: %w", err)
	}

	if err := r.replicateLogTail(ctx, prev); err != nil {
		return fmt.Errorf("replicate log tail: %w", err)
	}

	return nil
}

func (r *Raft) ensureLeader() error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.state.Role != Leader {
		return ErrNotLeader
	}
	return nil
}

func (r *Raft) appendToLog(commands ...[]byte) (LogID, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	prev := r.log.LastLogID()

	entries := r.makeEntries(prev.Index+1, commands...)
	if err := r.log.Append(entries...); err != nil {
		return LogID{}, err
	}
	return prev, nil
}

func (r *Raft) makeEntries(index Index, commands ...[]byte) []LogEntry {
	entries := make([]LogEntry, len(commands))
	for i, cmd := range commands {
		entry := LogEntry{
			LogID: LogID{
				Index: index,
				Term:  r.state.Term,
			},
			Command: cmd,
		}
		entries[i] = entry
		index++
	}
	return entries
}
