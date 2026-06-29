package core

import "errors"

var (
	ErrNotFound   = errors.New("not found")
	ErrInvalidKey = errors.New("invalid key")
	ErrNotLeader  = errors.New("not a leader")
)

type Key string
type Value string
type Pair struct {
	Key   Key
	Value Value
}

const zeroK Key = ""
const zeroV Value = ""
