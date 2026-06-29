package core

import (
	"context"
	"errors"
	"fmt"

	cmd "raft/gen/proto/kv/cmd/v1"
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

func (kv *KVService) Get(_ context.Context, key Key) (Value, error) {
	return kv.state.Get(key)
}

func (kv *KVService) List(_ context.Context) ([]Pair, error) {
	return kv.state.List(), nil
}

func (kv *KVService) Put(ctx context.Context, key Key, value Value) error {
	if key == zeroK {
		return ErrInvalidKey
	}

	return kv.commit(ctx, &cmd.Command{
		Operation: cmd.Operation_OPERATION_PUT,
		Key:       string(key),
		Value:     string(value),
	})
}

func (kv *KVService) Delete(ctx context.Context, key Key) error {
	if key == zeroK {
		return ErrInvalidKey
	}

	return kv.commit(ctx, &cmd.Command{
		Operation: cmd.Operation_OPERATION_DELETE,
		Key:       string(key),
	})
}

func (kv *KVService) commit(ctx context.Context, command *cmd.Command) error {
	raw, err := kv.codec.Marshal(command)
	if err != nil {
		return fmt.Errorf("marshal command: %w", err)
	}

	if err := kv.committer.Commit(ctx, raw); err != nil {
		return fmt.Errorf("commit command: %w", err)
	}
	return nil
}
