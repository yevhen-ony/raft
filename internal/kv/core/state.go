package core

import "sync"

type State struct {
	mu   sync.RWMutex
	data map[Key]Value
}

func NewState() *State {
	return &State{
		data: make(map[Key]Value),
	}
}

func (s *State) Get(key Key) (Value, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.data[key]
	if !ok {
		return zeroV, ErrNotFound
	}
	return value, nil
}

func (s *State) Set(key Key, value Value) error {
	if key == zeroK {
		return ErrInvalidKey
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[key] = value
	return nil
}

func (s *State) Delete(key Key) error {
	if key == zeroK {
		return ErrInvalidKey
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.data, key)
	return nil
}

func (s *State) List() []Pair {
	s.mu.RLock()
	defer s.mu.RUnlock()

	res := make([]Pair, 0, len(s.data))
	for k, v := range s.data {
		res = append(res, Pair{Key: k, Value: v})
	}
	return res
}
