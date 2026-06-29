package core

import (
	"context"
	"fmt"
	"log/slog"
)

type replRound struct {
	Peers  []NodeRef
	Quorum Quorum
	Term   Term
}

func (r *Raft) startReplication(term Term) (*replRound, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if err := r.state.EnsureLeaderTerm(term); err != nil {
		return nil, err
	}
	rr := &replRound{
		Peers:  append([]NodeRef(nil), r.cluster.Peers...),
		Quorum: r.cluster.Quorum(),
		Term:   term,
	}
	return rr, nil
}

// leader only
func (r *Raft) replicateLogRange(ctx context.Context, term Term, rng LogRange) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	rr, err := r.startReplication(term)
	if err != nil {
		return err
	}

	replRes := make(chan ReplicationResult, len(rr.Peers))
	for _, peer := range rr.Peers {
		go r.replicateLogRangeTo(ctx, peer, term, rng, replRes)
	}

	accepted, rejected := 1, 0
LOOP:
	for range len(rr.Peers) {
		select {
		case <-ctx.Done():
			if accepted >= rr.Quorum.Accept {
				return nil
			}
			return ctx.Err()

		case res := <-replRes:
			switch res.Outcome {
			case ReplicateAccepted:
				accepted++
				if accepted >= rr.Quorum.Accept {
					break LOOP
				}

			case ReplicateFailed:
				slog.ErrorContext(ctx, "replication failed", "peer", res.Peer.ID, "error", res.Error)
				rejected++
				if rejected >= rr.Quorum.Reject {
					break LOOP
				}

			case ReplicateHigherTerm:
				r.mu.Lock()
				err = r.becomeFollower(ctx, res.Term)
				r.mu.Unlock()

				if err != nil {
					return fmt.Errorf("become follower: %w", err)
				}
				return ErrNotLeader
			}
		}
	}

	if accepted < rr.Quorum.Accept {
		return ErrQuorumNotReached
	}
	return nil
}

type ReplicationOutcome int

const (
	ReplicateAccepted ReplicationOutcome = iota
	ReplicateHigherTerm
	ReplicateFailed
)

type ReplicationResult struct {
	Peer    NodeRef
	Outcome ReplicationOutcome
	Term    Term
	Error   error
}

// send replication request to a peer
func (r *Raft) replicateLogRangeTo(
	ctx context.Context,
	peer NodeRef,
	term Term,
	rng LogRange,
	results chan<- ReplicationResult,
) {
	slog.DebugContext(ctx, "sending replication request", "term", term, "addr", peer.Addr)
	for {
		if err := ctx.Err(); err != nil {
			results <- ReplicationResult{Peer: peer, Outcome: ReplicateFailed, Error: err}
			return
		}
		req, err := r.makeAppendEntriesRequest(term, rng)
		if err != nil {
			results <- ReplicationResult{Peer: peer, Outcome: ReplicateFailed, Error: err}
			return
		}
		rsp, err := r.transport.AppendEntries(ctx, peer, req)
		if err != nil {
			results <- ReplicationResult{Peer: peer, Outcome: ReplicateFailed, Error: err}
			return
		}
		if rsp.Term > req.Term {
			results <- ReplicationResult{Peer: peer, Outcome: ReplicateHigherTerm, Term: rsp.Term}
			return
		}
		if rsp.Success {
			results <- ReplicationResult{Peer: peer, Outcome: ReplicateAccepted, Term: rsp.Term}
			return
		}
		// on reject

		r.mu.RLock()
		prev, err := r.log.PrevIndex(rng.Prev)
		r.mu.RUnlock()

		if err != nil {
			err = fmt.Errorf("access previous log: %w", err)
			results <- ReplicationResult{Peer: peer, Outcome: ReplicateFailed, Error: err}
			return
		}

		rng.Prev = prev
	}
}

func (r *Raft) makeAppendEntriesRequest(term Term, rng LogRange) (AppendEntriesRequest, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if err := r.state.EnsureLeaderTerm(term); err != nil {
		return AppendEntriesRequest{}, err
	}

	seg, err := r.log.Segment(rng)
	if err != nil {
		return AppendEntriesRequest{}, err
	}

	req := AppendEntriesRequest{
		LeaderID:    r.cluster.Self.ID,
		Term:        term,
		PrevLogID:   seg.Prev,
		Entries:     seg.Entries,
		CommitIndex: r.state.CommitIndex,
	}
	return req, nil
}
