package rpc

import (
	"fmt"
	c "raft/internal/raft/core"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ConnectionSource interface {
	Conn(nodeID c.NodeID) (*grpc.ClientConn, error)
}

type GRPCConnectionSource struct {
	conns map[c.NodeID]*grpc.ClientConn
}

func NewGRPCConnectionSource(peers []c.Node) (*GRPCConnectionSource, error) {
	conns := make(map[c.NodeID]*grpc.ClientConn, len(peers))
	for _, peer := range peers {
		conn, err := newConn(peer.Addr)
		if err != nil {
			closeConn(conns)
			return nil, err
		}
		conns[peer.ID] = conn
	}

	src := &GRPCConnectionSource{
		conns: conns,
	}
	return src, nil
}

func (src *GRPCConnectionSource) Conn(nodeID c.NodeID) (*grpc.ClientConn, error) {
	client, ok := src.conns[nodeID]
	if !ok {
		return nil, fmt.Errorf("unknown peer: id = %s", nodeID)
	}
	return client, nil
}

func (src *GRPCConnectionSource) Close() error {
	return closeConn(src.conns)
}

func newConn(addr string) (*grpc.ClientConn, error) {
	opts := grpc.WithTransportCredentials(insecure.NewCredentials())

	return grpc.NewClient(addr, opts)
}

// returns last non-nil error
func closeConn(conns map[c.NodeID]*grpc.ClientConn) error {
	var err error
	for _, conn := range conns {
		if closeErr := conn.Close(); closeErr != nil {
			err = closeErr
		}
	}
	return err
}
