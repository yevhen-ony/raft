package core

import (
	"context"
)

type Transport interface {
	AppendEntries(context.Context, Node, AppendEntriesRequest) (AppendEntriesResponse, error)
}

