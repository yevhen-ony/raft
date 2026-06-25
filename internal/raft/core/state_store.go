package core

import (
	"context"
	"sync"
)

type InMemStateStore struct {
	mu    sync.Mutex
	state PersistentState
}

func NewInMemStateStore() *InMemStateStore {
	return &InMemStateStore{}
}

func (s *InMemStateStore) Load(context.Context) (PersistentState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.state, nil
}

func (s *InMemStateStore) Save(_ context.Context, state PersistentState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.state = state
	return nil
}

var _ StateStore = (*InMemStateStore)(nil)
