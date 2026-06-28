package core

import (
	"context"
)

type CommandApplier interface {
	Apply(context.Context, []byte) error
}

type Transport interface {
	AppendEntries(context.Context, NodeRef, AppendEntriesRequest) (AppendEntriesResponse, error)
	RequestVote(context.Context, NodeRef, VoteRequest) (VoteResponse, error)
}
