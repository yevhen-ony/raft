package core

import (
	"context"
	"errors"
	"sync"
)

type RaftDeps struct {
	Log            *Log
	State          *State
	Cluster        *Cluster
	Transport      Transport
	CommandApplier CommandApplier
	Config         *RaftConfig
}

type Raft struct {
	cluster *Cluster
	state   *State
	log     *Log

	commandApplier CommandApplier

	transport Transport

	// events
	leaderSeen     chan struct{}
	roleChanged    chan struct{}
	logCommitted   chan struct{}
	commandApplied Broadcaster

	mu  sync.RWMutex
	cfg *RaftConfig
}

func NewRaft(deps RaftDeps) (*Raft, error) {
	if deps.Transport == nil {
		return nil, errors.New("missing transport")
	}
	if deps.Config == nil {
		return nil, errors.New("missing config")
	}
	if deps.CommandApplier == nil {
		deps.CommandApplier = noopCommandHandler{}
	}
	if deps.Log == nil {
		return nil, errors.New("missing Log")
	}
	if deps.State == nil {
		return nil, errors.New("missing state")
	}
	if deps.Cluster == nil {
		return nil, errors.New("missing cluster")
	}

	r := &Raft{
		cluster: deps.Cluster,
		state:   deps.State,
		log:     deps.Log,

		commandApplier: deps.CommandApplier,

		transport: deps.Transport,

		leaderSeen:     make(chan struct{}, 1),
		roleChanged:    make(chan struct{}, 1),
		logCommitted:   make(chan struct{}, 1),
		commandApplied: *NewBroadcaster(),

		cfg: deps.Config,
	}
	return r, nil
}

func (r *Raft) IsLeader() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.state.Role == Leader
}

type noopCommandHandler struct{}

func (noopCommandHandler) Apply(context.Context, []byte) error { return nil }
