package core

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	cmd "raft/gen/proto/kv/cmd/v1"
	"raft/internal/kv"
)

type CommandApplier interface {
	Apply(context.Context, []byte) error
}

type StateCommandApplier struct {
	state *State
	codec Codec
}

func NewStateCommandApplier(state *State, codec Codec) (*StateCommandApplier, error) {
	if state == nil {
		return nil, errors.New("missing state")
	}
	if codec == nil {
		return nil, errors.New("missing codec")
	}
	applier := &StateCommandApplier{state: state, codec: codec}
	return applier, nil
}

func (ca *StateCommandApplier) Apply(ctx context.Context, raw []byte) error {

	command, err := ca.codec.Unmarshal(raw)
	if err != nil {
		return fmt.Errorf("command unmarshal failed: %w", err)
	}

	switch command.GetOperation() {
	case cmd.Operation_OPERATION_PUT:
		key := kv.Key(command.GetKey())
		value := kv.Value(command.GetValue())
		if err := ca.state.Set(key, value); err != nil {
			slog.ErrorContext(ctx, "set operation failed", "error", err)
		}
		return nil

	case cmd.Operation_OPERATION_DELETE:
		key := kv.Key(command.GetKey())
		if err := ca.state.Delete(key); err != nil {
			slog.ErrorContext(ctx, "delete operation failed", "error", err)
		}
		return nil

	default:
		return fmt.Errorf("unsupported operation applied: %d", command.GetOperation())
	}
}
