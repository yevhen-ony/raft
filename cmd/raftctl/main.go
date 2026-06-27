package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		printUsage()
		return errors.New("missing command")
	}
	params, err := parseFlags(args[1:])
	if err != nil {
		return fmt.Errorf("parse flags: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), params.Timeout)
	defer cancel()

	cl, err := NewCluster(ctx, params.Addr)
	if err != nil {
		return fmt.Errorf("new cluster: %w", err) 
	}
	defer cl.Close()
	
	start := time.Now()
	res := newExec(cl, params).Exec(ctx,  args[0])
	elapsed := time.Since(start)

	if err := printResult(res, elapsed); err != nil {
		return fmt.Errorf("print result: %w", err)
	}

	return res.Error	
}

func printUsage() {
	fmt.Fprintln(os.Stderr, `usage:
  raftctl nodes     --addr 127.0.0.1:5001
  raftctl status    --addr 127.0.0.1:5001 [--target n2]
  raftctl leader    --addr 127.0.0.1:5001
  raftctl propose   --addr 127.0.0.1:5001 --command hello [--target n2]
  raftctl stepdown  --addr 127.0.0.1:5001 [--target n2]`)
}

