package kv

import "errors"

var (
	ErrNotFound    = errors.New("not found")
	ErrInvalidKey  = errors.New("invalid key")
	ErrNotLeader   = errors.New("not a leader")
	ErrUnavailable = errors.New("unavailable")
)

type Key string
type Value string
type Pair struct {
	Key   Key
	Value Value
}

const ZeroK Key = ""
const ZeroV Value = ""
