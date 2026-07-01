package rpc

import (
	"errors"
	"raft/internal/kv"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func toRPCError(err error) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, kv.ErrNotLeader) {
		return status.Error(codes.Unavailable, "not leader")
	}

	if errors.Is(err, kv.ErrInvalidKey) {
		return status.Error(codes.InvalidArgument, "invalid key")
	}

	if errors.Is(err, kv.ErrNotFound) {
		return status.Error(codes.NotFound, "not found")
	}

	return status.Error(codes.Internal, err.Error())
}

func fromRPCError(err error) error {
	if err == nil {
		return nil
	}

	switch status.Code(err) {
	case codes.Unavailable:
		return kv.ErrUnavailable
	case codes.InvalidArgument:
		return kv.ErrInvalidKey
	case codes.NotFound:
		return kv.ErrNotFound
	default:
		return err
	}
}
