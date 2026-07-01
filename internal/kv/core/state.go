package core

import (
	"raft/internal/kv"
	"sync"
)

type State struct {
	mu   sync.RWMutex
	data map[kv.Key]kv.Value
}

func NewState() *State {
	return &State{
		data: make(map[kv.Key]kv.Value),
	}
}

func (s *State) Get(key kv.Key) (kv.Value, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.data[key]
	if !ok {
		return kv.ZeroV, kv.ErrNotFound
	}
	return value, nil
}

func (s *State) Set(key kv.Key, value kv.Value) error {
	if key == kv.ZeroK {
		return kv.ErrInvalidKey
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[key] = value
	return nil
}

func (s *State) Delete(key kv.Key) error {
	if key == kv.ZeroK {
		return kv.ErrInvalidKey
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.data, key)
	return nil
}

func (s *State) List() []kv.Pair {
	s.mu.RLock()
	defer s.mu.RUnlock()

	res := make([]kv.Pair, 0, len(s.data))
	for k, v := range s.data {
		res = append(res, kv.Pair{Key: k, Value: v})
	}
	return res
}
