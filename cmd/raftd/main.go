package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	p, err := parseParams(args)
	if err != nil {
		return fmt.Errorf("parse params: %w", err)
	}

	cfg, err := ConfigFromYAML(p.configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	cfg.OverrideWithEnv()

	SetupLogger(&cfg.Logger)	

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	initCtx, initCancel := context.WithTimeout(ctx, 5*time.Second)
	defer initCancel()

	app, err := NewApp(initCtx, cfg)
	if err != nil {
		return fmt.Errorf("create app: %w", err)
	}
	defer app.Close()

	if err := app.Run(ctx); err != nil {
		return fmt.Errorf("app run: %w", err)
	}
	return nil
}

type params struct {
	configPath string
}

func parseParams(args []string) (*params, error) {
	fs := flag.NewFlagSet("raftd", flag.ContinueOnError)
	configPath := fs.String("config", "./config.yml", "config path")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	p := &params{
		configPath: *configPath,
	}
	return p, nil
}
