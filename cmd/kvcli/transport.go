package main

import (
	"errors"
	"os"
	"raft/internal/kv/rpc"
	"strings"
)

const (
	envKVAddrs = "KVCLI_ADDRS"
)

func InitTransport() (*rpc.KVTransport, error) {
	addrs, err := getAddrs()
	if err != nil {
		return nil, err
	}

	return rpc.NewKVTransport(addrs)
}

func getAddrs() ([]string, error) {
	addrs := splitCSV(os.Getenv(envKVAddrs))
	if len(addrs) == 0 {
		return nil, errors.New("missing addrs: set env KV_ADDRS")
	}
	return addrs, nil
}

func splitCSV(raw string) []string {
	parts := strings.Split(raw, ",")
	res := make([]string, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			res = append(res, part)
		}
	}
	return res
}
