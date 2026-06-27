package main

import (
	"flag"
	"os"
	"time"
)

type cliParams struct {
	Addr    string
	Target  string
	Command string
	Timeout time.Duration
}

func defaultParams() cliParams {
	return cliParams{
		Addr:    defaultAddr(),
		Timeout: defaultTimeout(),
	}
}

func parseFlags(args []string) (cliParams, error) {
	params := defaultParams()

	fs := flag.NewFlagSet("raftctl", flag.ContinueOnError)
	fs.StringVar(&params.Addr, "addr", params.Addr, "bootstrap node control address")
	fs.StringVar(&params.Target, "target", params.Target, "target node id")
	fs.StringVar(&params.Command, "command", params.Command, "command to propose")
	fs.DurationVar(&params.Timeout, "timeout", params.Timeout, "request timeout")

	return params, fs.Parse(args)
}

func defaultAddr() string {
	addr := os.Getenv("RAFT_ADDR")
	if len(addr) == 0 {
		addr = "127.0.0.1:5001"
	}
	return addr
}

func defaultTimeout() time.Duration {
	raw := os.Getenv("RAFT_TIMEOUT")
	if len(raw) == 0 {
		return time.Second
	}
	timeout, err := time.ParseDuration(raw)
	if err != nil {
		return time.Second
	}
	return timeout
}
