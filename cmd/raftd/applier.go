package main

import (
	"context"
	"log/slog"
)

type LogCommandApplier struct{}

func (LogCommandApplier) Apply(ctx context.Context, command []byte) error {
	slog.InfoContext(ctx, "command applied", "command", string(command))
	return nil
}

