package core

import "context"

func (r *Raft) Status() RaftStatus {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return RaftStatus{
		NodeID:      r.cluster.Self.ID,
		Role:        r.state.Role,
		Term:        r.state.Term,
		VotedFor:    r.state.VotedFor,
		CommitIndex: r.state.CommitIndex,
		LastApplied: r.state.LastApplied,
		LastLogID:   r.log.LastLogID(),
		Leader:      r.cluster.Node(r.state.LeaderID),
	}
}

func (r *Raft) StepDown(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.becomeFollower(ctx, r.state.Term, "")
}

func (r *Raft) Nodes() []NodeRef {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.cluster.Nodes()
}
