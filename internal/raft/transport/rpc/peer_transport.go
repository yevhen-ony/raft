package rpc

import (
	"context"
	"errors"
	"fmt"

	api "raft/gen/proto/raft/v1"
	c "raft/internal/raft/core"
)


type GRPCRaftPeerTransport struct {
	grpc ConnectionSource
}

func NewGRPCRaftPeerTransport(grpc ConnectionSource) (*GRPCRaftPeerTransport, error) {
	if grpc == nil {
		return nil, errors.New("missing grpc client source")
	}

	t := &GRPCRaftPeerTransport{
		grpc: grpc,
	}
	return t, nil
}

func (t *GRPCRaftPeerTransport) client(nodeID c.NodeID) (api.RaftPeerServiceClient, error) {
	conn, err := t.grpc.Conn(nodeID)
	if err != nil {
		return nil, err
	}
	c := api.NewRaftPeerServiceClient(conn)
	return c, nil
}

func (t *GRPCRaftPeerTransport) AppendEntries(
	ctx context.Context,
	peer c.Node,
	request c.AppendEntriesRequest,
) (c.AppendEntriesResponse, error) {

	client, err := t.client(peer.ID)
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


func (t *GRPCRaftPeerTransport) RequestVote(
	ctx context.Context,
	peer c.Node,
	request c.VoteRequest,
) (c.VoteResponse, error) {
	client, err := t.client(peer.ID)
	if err != nil {
		return c.VoteResponse{}, fmt.Errorf("get grpc client: %w", err)
	}

	req := VoteRequestToPB(request)
	rsp, err := client.RequestVote(ctx, req)
	if err != nil {
		return c.VoteResponse{}, fmt.Errorf("rpc: %w", err)
	}

	response := VoteResponseFromPB(rsp)
	return response, nil
}

