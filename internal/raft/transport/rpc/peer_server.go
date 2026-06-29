package rpc

import (
	"context"
	"errors"
	"log/slog"

	api "raft/gen/proto/raft/v1"
	c "raft/internal/raft/core"
)

type GRPCRaftPeerServer struct {
	api.UnimplementedRaftPeerServiceServer
	node *c.Raft
}

func NewGRPCRaftPeerServer(raft *c.Raft) (*GRPCRaftPeerServer, error) {
	if raft == nil {
		return nil, errors.New("missing raft")
	}
	srv := &GRPCRaftPeerServer{node: raft}
	return srv, nil
}

func (s *GRPCRaftPeerServer) AppendEntries(
	ctx context.Context,
	req *api.AppendEntriesRequest,
) (*api.AppendEntriesResponse, error) {

	slog.DebugContext(ctx, "append entries requested")

	request := AppendEntriesRequestFromPB(req)
	rsp := s.node.AppendEntries(ctx, request)

	return AppendEntriesResponseToPB(rsp), nil
}

func (s *GRPCRaftPeerServer) RequestVote(
	ctx context.Context,
	req *api.VoteRequest,
) (*api.VoteResponse, error) {
	
	slog.DebugContext(ctx, "vote requested")

	request := VoteRequestFromPB(req)
	response := s.node.Vote(ctx, request)

	rsp := VoteResponseToPB(response)
	return rsp, nil
}
