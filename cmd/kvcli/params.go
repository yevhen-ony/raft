package main

import (
	"errors"
	"raft/internal/kv"
)

type Params struct {
	args []string
}

func NewParams(args []string) Params{
	return Params{args}
}

func (p Params) GetKey() (kv.Key, error) {
	if len(p.args) < 1 {
		return "", errors.New("missing key")
	}
	return kv.Key(p.args[0]), nil
}

func (p Params) GetValue() (kv.Value, error) {
	if len(p.args) < 2 {
		return "", errors.New("missing value")
	}
	return kv.Value(p.args[1]), nil
}
