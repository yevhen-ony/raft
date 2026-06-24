package core

import (
	"context"
	"errors"
	"sync"
)

type RaftDeps struct {
	LogTransport   LogEntryTransport
	VoteTransport  VoteTransport
	CommandApplier CommandApplier
	Config         *Config
}

type Raft struct {
	cluster *Cluster
	state   *State
	log     *Log

	commandApplier CommandApplier

	logTransport  LogEntryTransport
	voteTransport VoteTransport

	leaderSeen    chan struct{}
	roleChanged   chan struct{}
	commitChanged chan struct{}

	mu  sync.RWMutex
	cfg *Config
}

func NewRaft(deps RaftDeps) (*Raft, error) {
	if deps.LogTransport == nil {
		return nil, errors.New("missing log transport")
	}
	if deps.Config == nil {
		return nil, errors.New("missing config")
	}
	if deps.CommandApplier == nil {
		deps.CommandApplier = noopCommandHandler{}
	}
	r := &Raft{
		cluster: NewCluster(deps.Config),
		state:   NewState(deps.Config),
		log:     NewLog(),

		commandApplier: deps.CommandApplier,

		logTransport:  deps.LogTransport,
		voteTransport: deps.VoteTransport,

		leaderSeen:    make(chan struct{}, 1),
		roleChanged:   make(chan struct{}, 1),
		commitChanged: make(chan struct{}, 1),

		cfg: deps.Config,
	}
	return r, nil
}

type noopCommandHandler struct{}

func (noopCommandHandler) Apply(context.Context, []byte) error { return nil }
