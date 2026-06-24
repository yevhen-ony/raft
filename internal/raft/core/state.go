package core

type State struct {
	Term     Term
	Role     Role
	VotedFor NodeID
}

func NewState(config *Config) *State {
	role := Follower
	votedFor := NodeID("")
	if config.Leader {
		role = Leader
		votedFor = config.Self.ID
	}

	return &State{Term: 1, Role: role, VotedFor: votedFor}
}

