package kv

import (
	"context"
)

type KV interface {
	Get(context.Context, Key) (Value, error)
	List(context.Context) ([]Pair, error)
	Put(context.Context, Key, Value) error
	Delete(context.Context, Key) error
}
