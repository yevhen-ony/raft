package core

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"
)

type election struct {
	req    VoteRequest
	peers  []NodeRef
	quorum Quorum
}

func (r *Raft) startElection(ctx context.Context) (election, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	role := r.state.Role
	if role == Leader {
		return election{}, ErrLeader
	}

	if err := r.becomeCandidate(ctx); err != nil {
		return election{}, fmt.Errorf("become candidate: %w", err)
	}

	e := election{
		req: VoteRequest{
			CandidateID: r.cluster.Self.ID,
			Term:        r.state.Term,
			LastLogID:   r.log.LastLogID(),
		},
		peers:  append([]NodeRef(nil), r.cluster.Peers...),
		quorum: r.cluster.Quorum(),
	}

	return e, nil
}

func (r *Raft) RunElection(ctx context.Context) (bool, error) {

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	e, err := r.startElection(ctx)
	if err != nil {
		return false, err
	}

	granted, denied := 1, 0
	term := e.req.Term

	voteRes := make(chan VoteResult, len(e.peers))
	for _, peer := range e.peers {
		go r.requestVote(ctx, peer, e.req, voteRes)
	}

LOOP:
	for range len(e.peers) {
		select {
		case <-ctx.Done():
			return false, ctx.Err()

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
					break LOOP
				}
			case VoteHigherTerm:
				term = res.Term
				break LOOP
			}
		}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if term > e.req.Term {
		err := r.becomeFollower(ctx, term)
		return false, err
	}
	if granted < e.quorum.Accept {
		err = r.becomeFollower(ctx, e.req.Term)
		return false, err
	}
	if err := r.becomeLeader(e.req.Term); err != nil {
		err = r.becomeFollower(ctx, e.req.Term)
		return false, err
	}
	return true, nil
}

type VoteOutcome int

const (
	VoteGranted VoteOutcome = iota
	VoteDenied
	VoteHigherTerm
	VoteFailed
)

type VoteResult struct {
	Peer    NodeRef
	Outcome VoteOutcome
	Term    Term
	Error   error
}

func (r *Raft) requestVote(
	ctx context.Context,
	peer NodeRef,
	req VoteRequest,
	results chan<- VoteResult,
) {
	if err := ctx.Err(); err != nil {
		results <- VoteResult{Peer: peer, Outcome: VoteFailed, Error: err}
		return
	}
	rsp, err := r.transport.RequestVote(ctx, peer, req)
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

func (r *Raft) Vote(ctx context.Context, req VoteRequest) VoteResponse {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state.Term > req.Term {
		return VoteResponse{Term: r.state.Term, Granted: false}
	}

	if r.state.Term < req.Term {
		if err := r.becomeFollower(ctx, req.Term); err != nil {
			return VoteResponse{Term: r.state.Term, Granted: false}
		}
	}

	if r.state.VotedFor != "" && r.state.VotedFor != req.CandidateID {
		return VoteResponse{Term: r.state.Term, Granted: false}
	}

	if !r.log.IsUpToDate(req.LastLogID) {
		return VoteResponse{Term: r.state.Term, Granted: false}
	}

	if err := r.state.SetVotedFor(ctx, req.CandidateID); err != nil {
		return VoteResponse{Term: r.state.Term, Granted: false}
	}

	r.resetElection()
	return VoteResponse{Term: r.state.Term, Granted: true}
}

func (r *Raft) nextElectionTimeout() time.Duration {

	minDur := r.cfg.ElectionTimeoutMin
	maxDur := r.cfg.ElectionTimeoutMax
	if minDur >= maxDur {
		return minDur
	}

	steps := []int{0, 2, 4, 6, 8, 10}
	i := rand.IntN(len(steps))

	delta := maxDur - minDur
	return minDur + delta*time.Duration(steps[i])/10
}

func (r *Raft) observeLeader(ctx context.Context, term Term) error {
	if r.state.Term > term {
		return ErrOutdatedTerm
	}

	if err := r.becomeFollower(ctx, term); err != nil {
		return fmt.Errorf("become follower: %w", err)
	}

	r.resetElection()
	return nil
}

func (r *Raft) resetElection() {
	select {
	case r.leaderSeen <- struct{}{}:
	default: // non-blocking
	}
}

func (r *Raft) RunElectionLoop(ctx context.Context) error {
	timer := time.NewTimer(r.nextElectionTimeout())
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-r.leaderSeen:
			timer.Reset(r.nextElectionTimeout())

		case <-timer.C:
			_, err := r.RunElection(ctx)
			return err
		}
	}
}
