package core

import (
	"context"
	"errors"
	"fmt"

	cmd "raft/gen/proto/kv/cmd/v1"
	"raft/internal/kv"
)

type KVDeps struct {
	State     *State
	Codec     Codec
	Committer Committer
}

type KVService struct {
	state     *State
	codec     Codec
	committer Committer
}

var _ kv.KV = (*KVService)(nil)

func NewKVService(deps KVDeps) (*KVService, error) {
	if deps.State == nil {
		return nil, errors.New("missing state")
	}
	if deps.Codec == nil {
		return nil, errors.New("missing codec")
	}
	if deps.Committer == nil {
		return nil, errors.New("missing committer")
	}
	kv := &KVService{
		state:     deps.State,
		codec:     deps.Codec,
		committer: deps.Committer,
	}
	return kv, nil
}

func (s *KVService) Get(_ context.Context, key kv.Key) (kv.Value, error) {
	return s.state.Get(key)
}

func (s *KVService) List(_ context.Context) ([]kv.Pair, error) {
	return s.state.List(), nil
}

func (s *KVService) Put(ctx context.Context, key kv.Key, value kv.Value) error {
	if key == kv.ZeroK {
		return kv.ErrInvalidKey
	}

	return s.commit(ctx, &cmd.Command{
		Operation: cmd.Operation_OPERATION_PUT,
		Key:       string(key),
		Value:     string(value),
	})
}

func (s *KVService) Delete(ctx context.Context, key kv.Key) error {
	if key == kv.ZeroK {
		return kv.ErrInvalidKey
	}

	return s.commit(ctx, &cmd.Command{
		Operation: cmd.Operation_OPERATION_DELETE,
		Key:       string(key),
	})
}

func (s *KVService) commit(ctx context.Context, command *cmd.Command) error {
	raw, err := s.codec.Marshal(command)
	if err != nil {
		return fmt.Errorf("marshal command: %w", err)
	}

	if err := s.committer.Commit(ctx, raw); err != nil {
		return fmt.Errorf("commit command: %w", err)
	}
	return nil
}
