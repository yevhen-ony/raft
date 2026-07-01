package core

import (
	"context"
	"errors"
	"fmt"

	"raft/internal/kv"
	raft "raft/internal/raft/core"
)

type CommandCommitter interface {
	Commit(context.Context, []byte) error
}

// Local committer

type LocalCommandCommitter struct {
	applier CommandApplier 
}

func NewLocalCommandCommitter(applier CommandApplier) (*LocalCommandCommitter, error) {
	if applier == nil {
		return nil, fmt.Errorf("missing applier")
	}
	return &LocalCommandCommitter{applier: applier}, nil
}

func (c *LocalCommandCommitter) Commit(ctx context.Context, command []byte) error {
	return c.applier.Apply(ctx, command)
}

// Raft committer

type RaftCommitter struct {
	raft *raft.Raft
}

func NewRaftCommitter(raft *raft.Raft) (*RaftCommitter, error) {
	if raft == nil {
		return nil, fmt.Errorf("missing raft")
	}
	return &RaftCommitter{raft: raft}, nil
}

func (c *RaftCommitter) Commit(ctx context.Context, command []byte) error {
	_, err := c.raft.ProposeAndWait(ctx, command)
	if errors.Is(err, raft.ErrNotLeader) {
		return kv.ErrNotLeader
	}
	return err
}

