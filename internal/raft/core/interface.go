package core

import (
	"context"
)

type CommandApplier interface {
	Apply(context.Context, []byte) error
}

type Transport interface {
	AppendEntries(context.Context, Node, AppendEntriesRequest) (AppendEntriesResponse, error)
	RequestVote(context.Context, Node, VoteRequest) (VoteResponse, error)
}
