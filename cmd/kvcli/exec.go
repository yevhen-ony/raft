package main

import (
	"context"
	"errors"
	"raft/internal/kv/rpc"
	"time"
)

type Executor struct {
	transport *rpc.KVTransport
}

func NewExecutor(t *rpc.KVTransport) (*Executor, error) {
	if t == nil {
		return nil, errors.New("missing transport")
	}
	return &Executor{transport: t}, nil
}

func (e *Executor) Exec(ctx context.Context, cmd string, params Params) (Result, error) {
	var res Result
	var err error

	start := time.Now()

	switch cmd {
	case "put":
		res, err = e.Put(ctx, params)
	case "get":
		res, err = e.Get(ctx, params)
	case "del", "delete":
		res, err = e.Delete(ctx, params)
	case "ls", "list":
		res, err = e.List(ctx, params)
	default:
		err = errors.New("unknown command")
		res = ErrResult(cmd, err)
	}

	res.ElapsedMs = time.Since(start).Milliseconds()
	return res, err
}

func (e *Executor) Put(ctx context.Context, params Params) (Result, error) {
	key, err := params.GetKey()
	if err != nil {
		return ErrResult("put", err), err
	}
	val, err := params.GetValue()
	if err != nil {
		return ErrResult("put", err), err
	}
	if err := e.transport.Put(ctx, key, val); err != nil {
		return ErrResult("put", err), err
	}
	return NewResult("put", "ok"), nil
}

func (e *Executor) Get(ctx context.Context, params Params) (Result, error) {
	key, err := params.GetKey()
	if err != nil {
		return ErrResult("get", err), err
	}
	val, err := e.transport.Get(ctx, key)
	if err != nil {
		return ErrResult("get", err), err
	}
	return NewResult("get", val), nil
}

func (e *Executor) Delete(ctx context.Context, params Params) (Result, error) {
	key, err := params.GetKey()
	if err != nil {
		return ErrResult("delete", err), err
	}
	err = e.transport.Delete(ctx, key)
	if err != nil {
		return ErrResult("delete", err), err
	}
	return NewResult("delete", "ok"), nil
}

func (e *Executor) List(ctx context.Context, params Params) (Result, error) {
	res, err := e.transport.List(ctx)
	if err != nil {
		return ErrResult("list", err), err
	}
	return NewResult("list", res), nil
}

type Result struct {
	Command   string `json:"command"`
	Result    any    `json:"result,omitempty"`
	Error     string `json:"error,omitempty"`
	ElapsedMs int64  `json:"elapsed_ms,omitempty"`
}

func ErrResult(cmd string, err error) Result {
	return Result{Command: cmd, Error: err.Error()}
}

func NewResult(cmd string, res any) Result {
	return Result{Command: cmd, Result: res}
}
