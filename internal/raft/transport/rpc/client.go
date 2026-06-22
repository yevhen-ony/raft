package rpc

import (
	"fmt"
	api "raft/gen/proto/raft/v1"
	c "raft/internal/raft/core"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type GRPCClientSource struct {
	conns   []*grpc.ClientConn
	clients map[c.NodeID]api.RaftServiceClient
}

func NewGRPCClientSource(peers []c.Node) (*GRPCClientSource, error) {

	conns := make([]*grpc.ClientConn, 0, len(peers))
	clients := make(map[c.NodeID]api.RaftServiceClient, len(peers))

	for _, peer := range peers {
		conn, err := newConn(peer.Addr)
		if err != nil {
			closeConn(conns...)
			return nil, err
		}

		conns = append(conns, conn)
		clients[peer.ID] = api.NewRaftServiceClient(conn)
	}

	src := &GRPCClientSource{
		clients: clients,
		conns:   conns,
	}
	return src, nil
}

func (src *GRPCClientSource) Client(nodeID c.NodeID) (api.RaftServiceClient, error) {
	client, ok := src.clients[nodeID]
	if !ok {
		return nil, fmt.Errorf("unknown peer: id = %s", nodeID)
	}
	return client, nil
}

func (src *GRPCClientSource) Close() error {
	return closeConn(src.conns...)
}

func newConn(addr string) (*grpc.ClientConn, error) {
	opts := grpc.WithTransportCredentials(insecure.NewCredentials())

	return grpc.NewClient(addr, opts)
}

// returns last non-nil error
func closeConn(conns ...*grpc.ClientConn) error {
	var err error
	for _, conn := range conns {
		if closeErr := conn.Close(); closeErr != nil {
			err = closeErr
		}
	}
	return err
}
