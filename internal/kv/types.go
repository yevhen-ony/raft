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
	Key   Key   `json:"key"`
	Value Value `json:"val"`
}

const ZeroK Key = ""
const ZeroV Value = ""
