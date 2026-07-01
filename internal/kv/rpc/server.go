package rpc

import (
	"context"
	"errors"
	"log/slog"

	api "raft/gen/proto/kv/api/v1"
	"raft/internal/kv"
)

type KVServer struct {
	api.UnimplementedKVServiceServer
	svc kv.KV
}

func NewKVServer(svc kv.KV) (*KVServer, error) {
	if svc == nil {
		return nil, errors.New("missing kv service")
	}
	s := &KVServer{svc: svc}
	return s, nil
}

func (s *KVServer) Get(ctx context.Context, req *api.GetRequest) (*api.GetResponse, error) {
	slog.DebugContext(ctx, "get requested")

	key := kv.Key(req.GetKey())
	val, err := s.svc.Get(ctx, key)

	if errors.Is(err, kv.ErrNotFound) {
		rsp := &api.GetResponse{Found: false}
		return rsp, nil
	}

	if err != nil {
		return nil, toRPCError(err)
	}

	rsp := &api.GetResponse{
		Found: true,
		Value: string(val),
	}
	return rsp, nil
}

func (s *KVServer) Put(ctx context.Context, req *api.PutRequest) (*api.PutResponse, error) {
	slog.DebugContext(ctx, "put requested")

	key := kv.Key(req.GetKey())
	val := kv.Value(req.GetValue())
	if err := s.svc.Put(ctx, key, val); err != nil {
		return nil, toRPCError(err)
	}
	return &api.PutResponse{}, nil
}

func (s *KVServer) Delete(ctx context.Context, req *api.DeleteRequest) (*api.DeleteResponse, error) {
	slog.DebugContext(ctx, "delete requested")

	key := kv.Key(req.GetKey())
	err := s.svc.Delete(ctx, key)

	if errors.Is(err, kv.ErrNotFound) {
		return &api.DeleteResponse{Deleted: false}, nil
	}

	if err != nil {
		return nil, toRPCError(err)
	}

	return &api.DeleteResponse{Deleted: true}, nil
}

func (s *KVServer) List(ctx context.Context, req *api.ListRequest) (*api.ListResponse, error) {
	slog.DebugContext(ctx, "list requested")

	items, err := s.svc.List(ctx)
	if err != nil {
		return nil, toRPCError(err)
	}
	rsp := &api.ListResponse{Items: Map(items, PairToPB)}
	return rsp, nil
}
