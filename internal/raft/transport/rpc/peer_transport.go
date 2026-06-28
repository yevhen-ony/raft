package rpc

import (
	"context"
	"errors"
	"fmt"
	"time"

	api "raft/gen/proto/raft/v1"
	c "raft/internal/raft/core"
)

type Config struct {
	RequestTimeout time.Duration
}

type GRPCPeerTransport struct {
	grpc ConnectionSource
	cfg  *Config
}

func NewGRPCPeerTransport(grpc ConnectionSource, cfg *Config) (*GRPCPeerTransport, error) {
	if grpc == nil {
		return nil, errors.New("missing grpc client source")
	}
	if cfg == nil {
		return nil, errors.New("missing config")
	}

	t := &GRPCPeerTransport{grpc: grpc, cfg: cfg}
	return t, nil
}

func (t *GRPCPeerTransport) client(nodeID c.NodeID) (api.RaftPeerServiceClient, error) {
	conn, err := t.grpc.Conn(nodeID)
	if err != nil {
		return nil, err
	}
	c := api.NewRaftPeerServiceClient(conn)
	return c, nil
}

func (t *GRPCPeerTransport) AppendEntries(
	ctx context.Context,
	peer c.NodeRef,
	request c.AppendEntriesRequest,
) (c.AppendEntriesResponse, error) {

	ctx, cancel := context.WithTimeout(ctx, t.cfg.RequestTimeout)
	defer cancel()

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

func (t *GRPCPeerTransport) RequestVote(
	ctx context.Context,
	peer c.NodeRef,
	request c.VoteRequest,
) (c.VoteResponse, error) {

	ctx, cancel := context.WithTimeout(ctx, t.cfg.RequestTimeout)
	defer cancel()

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
