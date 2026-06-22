package core 

type State struct {
	Term Term
	Role Role
}

func NewState(config *Config) *State {
	role := Follower
	if config.Leader {
		role = Leader
	}

	return &State{Term: 1, Role: role}
}

func (s *State) StepDown(term Term) {
	if term > s.Term {
		s.Term = term
	}
	s.Role = Follower
}

func (s *State) Follow(term Term) error {
	if term < s.Term {
		return ErrOutdatedTerm
	}

	if s.Role == Leader && term == s.Term {
		return ErrLeader
	}

	s.StepDown(term)
	return nil
}
