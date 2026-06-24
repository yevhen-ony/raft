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
		Term: 0,
		Role: role,
		VotedFor: votedFor,
		CommitIndex: 0,
		LastApplied: 0,
	}
}
