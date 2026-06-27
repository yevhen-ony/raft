package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"raft/internal/raft/core"
	"raft/internal/raft/transport/rpc"
)

const bootstrapNodeID = core.NodeID("bootstrap")

type Cluster struct {
	Leader core.Node
	Nodes  []core.Node

	Transport *rpc.GRPCRaftControlTransport

	close func() error
}

func NewCluster(ctx context.Context, bootstrapAddr string) (*Cluster, error) {
	nodes, err := bootstrapCluster(ctx, bootstrapAddr)
	if err != nil {
		return nil, fmt.Errorf("bootstrap cluster: %w", err)
	}
	if len(nodes) == 0 {
		return nil, fmt.Errorf("bootstrap cluster: no nodes found")
	}

	transport, close, err := newControlTransport(nodes)
	if err != nil {
		return nil, fmt.Errorf("new transport: %w", err)
	}
	cl := &Cluster{
		Nodes:     nodes,
		Transport: transport,
		close:     close,
	}

	if _, err := cl.GetLeader(ctx); err != nil {
		close()
		return nil, err
	}
	return cl, nil
}

func (cl *Cluster) GetLeader(ctx context.Context) (core.Node, error) {
	lastErr := errors.New("retry exhausted")

	for range 3 {
		for _, node := range cl.Nodes {
			status, err := cl.Transport.Status(ctx, node.ID)
			if err != nil {
				lastErr = fmt.Errorf("last status error: %w", err)
				continue
			}
			if status.Role == core.Leader {
				cl.Leader = node
				return cl.Leader, nil
			}
		}

		select {
		case <-ctx.Done():
			return core.Node{}, ctx.Err()
		case <-time.After(time.Second):
		}
	}

	return core.Node{}, fmt.Errorf("leader not found: %w", lastErr)
}

func bootstrapCluster(ctx context.Context, addr string) ([]core.Node, error) {
	bootstrap := core.Node{
		ID:   bootstrapNodeID,
		Addr: addr,
	}

	client, close, err := newControlTransport([]core.Node{bootstrap})
	if err != nil {
		return nil, err
	}
	defer close()

	return client.ListNodes(ctx, bootstrapNodeID)
}

func newControlTransport(nodes []core.Node) (*rpc.GRPCRaftControlTransport, func() error, error) {
	source, err := rpc.NewGRPCConnectionSource(nodes)
	if err != nil {
		return nil, nil, err
	}

	client, err := rpc.NewGRPCRaftControlTransport(source)
	if err != nil {
		_ = source.Close()
		return nil, nil, err
	}

	return client, source.Close, nil
}

func (cl *Cluster) Close() error {
	if cl.close == nil {
		return nil
	}
	return cl.close()
}
