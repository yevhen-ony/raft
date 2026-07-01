package rpc

import (
	api "raft/gen/proto/kv/api/v1"
	"raft/internal/kv"
)

func PairFromPB(pair *api.Pair) kv.Pair {
	return kv.Pair{
		Key:   kv.Key(pair.GetKey()),
		Value: kv.Value(pair.GetValue()),
	}
}

func PairToPB(pair kv.Pair) *api.Pair {
	return &api.Pair{
		Key:   string(pair.Key),
		Value: string(pair.Value),
	}
}

func Map[T any, R any](items []T, fn func(T) R) []R {
	if items == nil {
		return nil
	}

	res := make([]R, len(items))
	for i, item := range items {
		res[i] = fn(item)
	}
	return res
}
