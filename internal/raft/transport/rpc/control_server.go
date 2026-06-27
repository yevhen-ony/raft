package rpc

import (
	"context"
	"errors"
	"log/slog"

	api "raft/gen/proto/raft/v1"
	c "raft/internal/raft/core"
)

type GRPCRaftControlServer struct {
	api.UnimplementedRaftControlServiceServer
	node *c.Raft
}

func NewGRPCRaftControlServer(raft *c.Raft) (*GRPCRaftControlServer, error) {
	if raft == nil {
		return nil, errors.New("missing raft")
	}
	srv := &GRPCRaftControlServer{node: raft}
	return srv, nil
}

func (s *GRPCRaftControlServer) ListNodes(
	ctx context.Context,
	req *api.ListNodesRequest,
) (*api.ListNodesResponse, error) {
	slog.DebugContext(ctx, "list nodes requested")

	nodes := s.node.Nodes()
	rsp := &api.ListNodesResponse{
		Nodes: mapSlice(nodes, NodeToPB),
	}
	return rsp, nil
}

func (s *GRPCRaftControlServer) Status(
	ctx context.Context,
	req *api.StatusRequest,
) (*api.StatusResponse, error) {
	slog.DebugContext(ctx, "status requested")

	status := s.node.Status()
	rsp := &api.StatusResponse{
		Status: RaftStatusToPB(status),
	}
	return rsp, nil
}

func (s *GRPCRaftControlServer) StepDown(
	ctx context.Context,
	req *api.StepDownRequest,
) (*api.StepDownResponse, error) {
	slog.DebugContext(ctx, "step down requested")

	if err := s.node.StepDown(ctx); err != nil {
		return nil, err
	}
	return &api.StepDownResponse{}, nil
}

func (s *GRPCRaftControlServer) Propose(
	ctx context.Context,
	req *api.ProposeRequest,
) (*api.ProposeResponse, error) {
	slog.DebugContext(ctx, "propose requested")

	index, err := s.node.ProposeAndWait(ctx, req.GetCommand())
  	if err != nil {
  		return nil, err
  	}
	rsp := &api.ProposeResponse{
  		Index: uint64(index),
  	}
	return rsp, nil
}
