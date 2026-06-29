package core

import (
	"context"
	"fmt"
	"log/slog"

	cmd "raft/gen/proto/kv/cmd/v1"
)

type CommandApplier interface {
	Apply(context.Context, []byte) error
}

type StateCommandApplier struct {
	state *State
	codec Codec
}

func NewCommandApplier(state *State) *StateCommandApplier {
	return &StateCommandApplier{state: state}
}

func (ca *StateCommandApplier) Apply(ctx context.Context, raw []byte) error {

	command, err := ca.codec.Unmarshal(raw)
	if err != nil {
		// probably wrong codec was used
		return fmt.Errorf("command unmarshal failed: %w", err)
	}

	switch command.GetOperation() {
	case cmd.Operation_OPERATION_PUT:
		key := Key(command.GetKey())
		value := Value(command.GetValue())
		if err := ca.state.Set(key, value); err != nil {
			slog.ErrorContext(ctx, "set operation failed", "error", err)
		}
		return nil

	case cmd.Operation_OPERATION_DELETE:
		key := Key(command.GetKey())
		if err := ca.state.Delete(key); err != nil {
			slog.ErrorContext(ctx, "delete operation failed", "error", err)
		}
		return nil

	default:
		slog.ErrorContext(ctx, "unsupported operation applied", "operation", command.GetOperation())
		return nil
	}
}
