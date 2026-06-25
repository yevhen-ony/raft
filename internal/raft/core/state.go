package core

type State struct {
	Term        Term
	Role        Role
	VotedFor    NodeID
	CommitIndex Index
	LastApplied Index
}

func NewState(config *Config) *State {
	role := Follower
	votedFor := NodeID("")
	if config.Leader {
		role = Leader
		votedFor = config.Self.ID
	}

	return &State{
		Term:        0,
		Role:        role,
		VotedFor:    votedFor,
		CommitIndex: 0,
		LastApplied: 0,
	}
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
