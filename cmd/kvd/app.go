package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"

	kvnode "raft/internal/kv/node"
	raftcore "raft/internal/raft/core"
	raftnode "raft/internal/raft/node"

	"google.golang.org/grpc"
)

type App struct {
	KVNode   *kvnode.Node
	RaftNode *raftnode.Node
}

func NewApp(ctx context.Context, cfg *Config) (*App, error) {

	kvDeps, err := kvnode.NewNodeDeps(&cfg.KV)
	if err != nil {
		return nil, fmt.Errorf("kv deps: %w", err)
	}

	raftCfg, err := cfg.Raft.ToCore()
	if err != nil {
		return nil, fmt.Errorf("raft config: %w", err)
	}
	raftNode, err := raftnode.NewNode(ctx, raftnode.NodeDeps{
		Config:         raftCfg,
		LogStore:       raftcore.NewInMemLogStore(),
		StateStore:     raftcore.NewInMemStateStore(),
		CommandApplier: kvDeps.Applier,
	})
	if err != nil {
		return nil, fmt.Errorf("raft node: %w", err)
	}

	kvNode, err := kvnode.NewRaftNode(kvDeps, raftNode.Raft)
	if err != nil {
		return nil, fmt.Errorf("kv node: %w", err)
	}

	app := &App{
		RaftNode: raftNode,
		KVNode:   kvNode,
	}
	return app, nil
}

func (app *App) Close() error {
	if app.RaftNode != nil {
		return app.RaftNode.Close()
	}
	return nil
}

func (app *App) Run(ctx context.Context, cfg *ListenerConfig) error {

	slog.InfoContext(ctx, "starting listener", "addr", cfg.Addr)
	listener, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	defer listener.Close()

	grpcServer := grpc.NewServer()
	defer grpcServer.Stop()

	app.RaftNode.Register(grpcServer)
	app.KVNode.Register(grpcServer)

	errCh := make(chan error, 2)

	go func() {
		slog.InfoContext(ctx, "starting grpc server")
		errCh <- grpcServer.Serve(listener)
	}()
	go func() {
		slog.InfoContext(ctx, "starting raft node")
		errCh <- app.RaftNode.Run(ctx)
	}()

	select {
	case <-ctx.Done():
		grpcServer.Stop()
		return nil
	case err = <-errCh:
		grpcServer.Stop()
		return filterErrors(err)
	}
}

func filterErrors(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return nil
	}
	if errors.Is(err, grpc.ErrServerStopped) {
		return nil
	}
	return err
}
