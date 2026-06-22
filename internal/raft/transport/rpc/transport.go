package rpc

import (
	"context"
	"errors"
	"fmt"

	api "raft/gen/proto/raft/v1"
	c "raft/internal/raft/core"
)

type ClientSource interface {
	Client(nodeID c.NodeID) (api.RaftServiceClient, error)
}

type GRPCTransport struct {
	grpc ClientSource
}

func NewGRPCTransport(grpc ClientSource) (*GRPCTransport, error) {
	if grpc == nil {
		return nil, errors.New("missing grpc client source")
	}

	t := &GRPCTransport{
		grpc: grpc,
	}
	return t, nil
}

func (t *GRPCTransport) AppendEntries(
	ctx context.Context,
	peer c.Node,
	request c.AppendEntriesRequest,
) (c.AppendEntriesResponse, error) {

	client, err := t.grpc.Client(peer.ID)
	if err != nil {
		return c.AppendEntriesResponse{}, fmt.Errorf("get grpc client: %w", err)
	}

	req := AppendEntriesRequestToPB(request)
	rsp, err := client.AppendEntries(ctx, req)
	if err != nil {
		return c.AppendEntriesResponse{}, fmt.Errorf("rpc: %w", err)
	}
	response := AppendEntriesResponseFromPB(rsp)
	return response, nil
}
