package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
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

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	transport, err := InitTransport()
	if err != nil {
		return fmt.Errorf("init transport: %w", err)
	}
	defer transport.Close()

	exec, err := NewExecutor(transport)
	if err != nil {
		return fmt.Errorf("create executor: %w", err)
	}

	res, execErr := exec.Exec(ctx, args[0], NewParams(args[1:]))
	if err := printResult(res); err != nil {
		return fmt.Errorf("print result: %w", err)
	}

	return execErr
}

func printResult(res Result) error {
	data, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		return err
	}

	fmt.Println(string(data))
	return nil
}

func printUsage() {
	fmt.Fprintln(os.Stderr, `
  usage:
    kvcli put KEY VALUE
    kvcli get KEY
    kvcli delete KEY
    kvcli list

  aliases:
    del     delete
    ls      list

  environment:
    KV_ADDRS     comma-separated server addresses, e.g. 127.0.0.1:6001,127.0.0.1:6002`)
}
