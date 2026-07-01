package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"

	api "raft/gen/proto/raft/v1"
	"raft/internal/raft/core"
	"raft/internal/raft/node"
	"raft/internal/raft/transport/rpc"

	"google.golang.org/grpc"
)

type App struct {
	Config        *Config
	Node          *node.Node
	ControlServer *rpc.GRPCRaftControlServer
}

func NewApp(ctx context.Context, cfg *Config) (*App, error) {
	coreCfg, err := cfg.Raft.ToCore()
	if err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}

	node, err := node.NewNode(ctx, node.NodeDeps{
		Config:         coreCfg,
		StateStore:     core.NewInMemStateStore(),
		LogStore:       core.NewInMemLogStore(),
		CommandApplier: LogCommandApplier{},
	})
	if err != nil {
		return nil, fmt.Errorf("node: %w", err)
	}

	controlServer, err := rpc.NewGRPCRaftControlServer(node.Raft)
	if err != nil {
		return nil, fmt.Errorf("control server: %w", err)
	}

	app := &App{
		Config:        cfg,
		Node:          node,
		ControlServer: controlServer,
	}
	return app, nil
}

func (app *App) Close() error {
	if app.Node != nil {
		return app.Node.Close()
	}
	return nil
}

func (app *App) Run(ctx context.Context) error {

	addr := app.Config.Listener.Addr
	slog.InfoContext(ctx, "starting listener", "addr", addr)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	defer listener.Close()

	grpcServer := grpc.NewServer()
	defer grpcServer.Stop()

	app.Node.Register(grpcServer)
	api.RegisterRaftControlServiceServer(grpcServer, app.ControlServer)

	errCh := make(chan error, 2)

	go func() {
		slog.InfoContext(ctx, "starting grpc server")
		errCh <- grpcServer.Serve(listener) 
	}()
	go func() {
		slog.InfoContext(ctx, "starting raft node")
		errCh <- app.Node.Run(ctx) 
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
