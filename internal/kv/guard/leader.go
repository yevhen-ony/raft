package guard

import (
	"context"
	"errors"

	"raft/internal/kv"
)

type leadership interface {
	IsLeader() bool
}

type LeaderGuardedService struct {
	leader leadership
	kv     kv.KV
}

func NewLeaderGuardedService(leadership leadership, kv kv.KV) (*LeaderGuardedService, error) {
	if leadership == nil {
		return nil, errors.New("missing leadership")
	}
	if kv == nil {
		return nil, errors.New("missing kv")
	}
	s := &LeaderGuardedService{
		leader: leadership,
		kv:     kv,
	}
	return s, nil
}

func (g *LeaderGuardedService) Get(ctx context.Context, key kv.Key) (kv.Value, error) {
	if !g.leader.IsLeader() {
		return kv.ZeroV, kv.ErrNotLeader
	}
	return g.kv.Get(ctx, key)
}

func (g *LeaderGuardedService) List(ctx context.Context) ([]kv.Pair, error) {
	if !g.leader.IsLeader() {
		return nil, kv.ErrNotLeader
	}
	return g.kv.List(ctx)
}

func (g *LeaderGuardedService) Put(ctx context.Context, key kv.Key, val kv.Value) error {
	if !g.leader.IsLeader() {
		return kv.ErrNotLeader
	}
	return g.kv.Put(ctx, key, val)
}

func (g *LeaderGuardedService) Delete(ctx context.Context, key kv.Key) error {
	if !g.leader.IsLeader() {
		return kv.ErrNotLeader
	}
	return g.kv.Delete(ctx, key)
}
