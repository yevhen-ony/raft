package main

import (
	"context"
	"errors"
	"raft/internal/raft/core"
)

type executor struct {
	cl     *Cluster
	params cliParams
}

func newExec(cl *Cluster, params cliParams) *executor {
	return &executor{cl: cl, params: params}
}

func (e *executor) Exec(ctx context.Context, cmd string) commandResult {

	target := core.NodeID(e.params.Target)
	if len(target) == 0 {
		target = e.cl.Leader.ID
	}

	r := commandResult{Action: cmd}
	switch cmd {
	case "nodes":
		r.Result, r.Error = e.cl.Transport.ListNodes(ctx, target)

	case "status":
		r.Result, r.Error = e.cl.Transport.Status(ctx, target)

	case "leader":
		r.Result, r.Error = e.cl.GetLeader(ctx)

	case "propose":
		command := []byte(e.params.Command)
		r.Result, r.Error = e.cl.Transport.Propose(ctx, target, command)

	case "stepdown":
		r.Error = e.cl.Transport.StepDown(ctx, target)

	default:
		r.Result, r.Error = nil, errors.New("unknown cmd")
	}

	return r
}
