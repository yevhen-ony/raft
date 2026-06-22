package rpc

import (
	"context"

	api "raft/gen/proto/raft/v1"
	c "raft/internal/raft/core"
)

type RaftServerGRPC struct {
  	api.UnimplementedRaftServiceServer
  	node *c.Raft
}

func (s *RaftServerGRPC) AppendEntries(
  	ctx context.Context,
  	req *api.AppendEntriesRequest,
) (*api.AppendEntriesResponse, error) {
	
	request := AppendEntriesRequestFromPB(req)
  	rsp := s.node.AppendEntries(ctx, request)

  	return AppendEntriesResponseToPB(rsp), nil
}
