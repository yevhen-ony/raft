package rpc

import (
	"context"
	"errors"
	"fmt"

	api "raft/gen/proto/raft/v1"
	c "raft/internal/raft/core"
)

type GRPCControlTransport struct {
	grpc ConnectionSource
}

func NewGRPCControlTransport(grpc ConnectionSource) (*GRPCControlTransport, error) {
	if grpc == nil {
		return nil, errors.New("missing grpc connection source")
	}

	cc := &GRPCControlTransport{
		grpc: grpc,
	}
	return cc, nil
}

func (cc *GRPCControlTransport) client(nodeID c.NodeID) (api.RaftControlServiceClient, error) {
	conn, err := cc.grpc.Conn(nodeID)
	if err != nil {
		return nil, err
	}
	return api.NewRaftControlServiceClient(conn), nil
}

func (cc *GRPCControlTransport) ListNodes(ctx context.Context, nodeID c.NodeID) ([]c.NodeRef, error) {
	client, err := cc.client(nodeID)
	if err != nil {
		return nil, fmt.Errorf("get grpc client: %w", err)
	}

	rsp, err := client.ListNodes(ctx, &api.ListNodesRequest{})
	if err != nil {
		return nil, fmt.Errorf("rpc: %w", err)
	}

	return mapSlice(rsp.GetNodes(), NodeRefFromPB), nil
}

func (cc *GRPCControlTransport) Status(ctx context.Context, nodeID c.NodeID) (c.RaftStatus, error) {
	client, err := cc.client(nodeID)
	if err != nil {
		return c.RaftStatus{}, fmt.Errorf("get grpc client: %w", err)
	}

	rsp, err := client.Status(ctx, &api.StatusRequest{})
	if err != nil {
		return c.RaftStatus{}, fmt.Errorf("rpc: %w", err)
	}

	return RaftStatusFromPB(rsp.GetStatus()), nil
}

func (cc *GRPCControlTransport) Propose(ctx context.Context, nodeID c.NodeID, command []byte) (c.Index, error) {
	client, err := cc.client(nodeID)
	if err != nil {
		return 0, fmt.Errorf("get grpc client: %w", err)
	}

	req := &api.ProposeRequest{
		Command: append([]byte(nil), command...),
	}

	rsp, err := client.Propose(ctx, req)
	if err != nil {
		return 0, fmt.Errorf("rpc: %w", err)
	}

	return c.Index(rsp.GetIndex()), nil
}

func (cc *GRPCControlTransport) StepDown(ctx context.Context, nodeID c.NodeID) error {
	client, err := cc.client(nodeID)
	if err != nil {
		return fmt.Errorf("get grpc client: %w", err)
	}

	if _, err := client.StepDown(ctx, &api.StepDownRequest{}); err != nil {
		return fmt.Errorf("rpc: %w", err)
	}
	return nil
}
