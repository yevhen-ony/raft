package core

import (
	"context"
	"fmt"
)

type StateStore interface {
	Load(context.Context) (PersistentState, error)
	Save(context.Context, PersistentState) error
}

type PersistentState struct {
	Term     Term
	VotedFor NodeID
}

type State struct {
	PersistentState

	Role        Role
	CommitIndex Index
	LastApplied Index

	store StateStore
}

func NewState(ctx context.Context, store StateStore) (*State, error) {
	ps, err := store.Load(ctx)
	if err != nil {
		return nil, fmt.Errorf("load persistent state: %w", err)
	}

	s := &State{
		PersistentState: ps,
		Role:            Follower,
		CommitIndex:     0,
		LastApplied:     0,

		store: store,
	}
	return s, nil
}

func (s *State) EnsureLeader() (Term, error) {
	if s.Role != Leader {
		return 0, ErrNotLeader
	}
	return s.Term, nil
}

func (s *State) EnsureLeaderTerm(term Term) error {
	if s.Role != Leader {
		return ErrNotLeader
	}
	if s.Term != term {
		return ErrOutdatedTerm
	}
	return nil
}

func (s *State) save(ctx context.Context) error {
	return s.store.Save(ctx, s.PersistentState)
}

func (s *State) SetTerm(ctx context.Context, term Term) error {
	if term == s.Term {
		return nil
	}

	prev := s.PersistentState

	s.Term = term
	s.VotedFor = ""

	if err := s.save(ctx); err != nil {
		s.PersistentState = prev
		return err
	}
	return nil
}

func (s *State) IncTerm(ctx context.Context, votedFor NodeID) error {
	prev := s.PersistentState

	s.Term++
	s.VotedFor = votedFor

	if err := s.save(ctx); err != nil {
		s.PersistentState = prev
		return err
	}
	return nil
}

func (s *State) SetVotedFor(ctx context.Context, votedFor NodeID) error {
	if s.VotedFor == votedFor {
		return nil
	}

	prev := s.PersistentState
	s.VotedFor = votedFor

	if err := s.save(ctx); err != nil {
		s.PersistentState = prev
		return err
	}
	return nil
}
