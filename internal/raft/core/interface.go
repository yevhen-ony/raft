package core

import (
	"context"
)

type LogEntryTransport interface {
	AppendEntries(context.Context, Node, AppendEntriesRequest) (AppendEntriesResponse, error)
}

type VoteTransport interface {
	RequestVote(context.Context, Node, VoteRequest) (VoteResponse, error)
}

