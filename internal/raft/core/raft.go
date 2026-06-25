package core

import (
	"context"
	"errors"
	"sync"
)

type RaftDeps struct {
	Log            *Log
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

	// events
	leaderSeen     chan struct{}
	roleChanged    chan struct{}
	logCommitted   chan struct{}
	commandApplied Broadcaster

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
	if deps.Log == nil {
		return nil, errors.New("missing Log")
	}

	r := &Raft{
		cluster: NewCluster(deps.Config),
		state:   NewState(deps.Config),
		log:     deps.Log,

		commandApplier: deps.CommandApplier,

		logTransport:  deps.LogTransport,
		voteTransport: deps.VoteTransport,

		leaderSeen:     make(chan struct{}, 1),
		roleChanged:    make(chan struct{}, 1),
		logCommitted:   make(chan struct{}, 1),
		commandApplied: *NewBroadcaster(),

		cfg: deps.Config,
	}
	return r, nil
}

type noopCommandHandler struct{}

func (noopCommandHandler) Apply(context.Context, []byte) error { return nil }
