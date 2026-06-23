package core

import (
	"context"
)

type election struct {
	req    VoteRequest
	peers  []Node
	quorum Quorum
}

func (r *Raft) startElection() (election, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	role := r.state.Role
	if role == Leader {
		return election{}, ErrLeader
	}

	r.state.Term++
	r.state.Role = Candidate
	r.state.VotedFor = r.cluster.Self.ID

	e := election{
		req: VoteRequest{
			CandidateID: r.cluster.Self.ID,
			Term:        r.state.Term,
			LastLogID:   r.log.LastLogID(),
		},
		peers:  append([]Node(nil), r.cluster.Peers...),
		quorum: r.cluster.Quorum(),
	}

	return e, nil
}

func (r *Raft) RunElection(ctx context.Context) error {

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	e, err := r.startElection()
	if err != nil {
		return err
	}

	granted, denied := 1, 0

	voteRes := make(chan VoteResult, len(e.peers))
	for _, peer := range e.peers {
		go r.requestVote(ctx, peer, e.req, voteRes)
	}

LOOP:
	for range len(e.peers) {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case res := <-voteRes:
			switch res.Outcome {
			case VoteGranted:
				granted++
				if granted >= e.quorum.Accept {
					break LOOP
				}
			case VoteFailed, VoteDenied:
				denied++
				if denied >= e.quorum.Reject {
					return ErrElectionLost
				}
			case VoteHigherTerm:
				r.mu.Lock()
				r.state.StepDown(res.Term)
				r.mu.Unlock()
				return ErrOutdatedTerm
			}
		}
	}

	if granted < e.quorum.Accept {
		return ErrElectionLost
	}
	if err := r.promoteToLeader(e.req.Term); err != nil {
		return err
	}
	return nil
}

type VoteOutcome int

const (
	VoteGranted VoteOutcome = iota
	VoteDenied
	VoteHigherTerm
	VoteFailed
)

type VoteResult struct {
	Peer    Node
	Outcome VoteOutcome
	Term    Term
	Error   error
}

func (r *Raft) requestVote(
	ctx context.Context,
	peer Node,
	req VoteRequest,
	results chan<- VoteResult,
) {
	if err := ctx.Err(); err != nil {
		results <- VoteResult{Peer: peer, Outcome: VoteFailed, Error: err}
		return
	}
	rsp, err := r.voteTransport.RequestVote(ctx, peer, req)
	if err != nil {
		results <- VoteResult{Peer: peer, Outcome: VoteFailed, Error: err}
		return
	}
	if rsp.Term > req.Term {
		results <- VoteResult{Peer: peer, Outcome: VoteHigherTerm, Term: rsp.Term}
		return
	}
	if rsp.Granted {
		results <- VoteResult{Peer: peer, Outcome: VoteGranted, Term: rsp.Term}
		return
	}
	results <- VoteResult{Peer: peer, Outcome: VoteDenied, Term: rsp.Term}
}

func (r *Raft) promoteToLeader(term Term) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state.Term != term || r.state.Role != Candidate {
		return ErrNotCandidate
	}

	r.state.Role = Leader
	return nil
}

func (r *Raft) Vote(_ context.Context, req VoteRequest) VoteResponse {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state.Term > req.Term {
		return VoteResponse{Term: r.state.Term, Granted: false}
	}

	if r.state.Term < req.Term {
		r.state.StepDown(req.Term)
		r.state.VotedFor = ""
	}

	if r.state.VotedFor != "" && r.state.VotedFor != req.CandidateID {
		return VoteResponse{Term: r.state.Term, Granted: false}
	}

	if !r.log.IsUpToDate(req.LastLogID) {
		return VoteResponse{Term: r.state.Term, Granted: false}
	}

	r.state.VotedFor = req.CandidateID
	return VoteResponse{Term: r.state.Term, Granted: true}
}

