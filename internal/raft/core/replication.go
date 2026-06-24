package core

import (
	"context"
	"fmt"
	"log/slog"
)

func (r *Raft) replicateLogTail(ctx context.Context, prev LogID) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	r.mu.RLock()
	peers := append([]Node(nil), r.cluster.Peers...)
	quorum := r.cluster.Quorum()
	r.mu.RUnlock()

	replRes := make(chan ReplicationResult, len(peers))
	for _, peer := range peers {
		go r.replicateLogTailTo(ctx, peer, prev, replRes)
	}

	accepted, rejected := 1, 0
LOOP:
	for range len(peers) {
		select {
		case <-ctx.Done():
			if accepted >= quorum.Accept {
				return nil
			}
			return ctx.Err()

		case res := <-replRes:
			switch res.Outcome {
			case ReplicateAccepted:
				accepted++
				if accepted >= quorum.Accept {
					break LOOP
				}

			case ReplicateFailed:
				slog.ErrorContext(ctx, "replication failed", "peer", res.Peer.ID, "error", res.Error)
				rejected++
				if rejected >= quorum.Reject {
					break LOOP
				}

			case ReplicateHigherTerm:
				r.mu.Lock()
				r.becomeFollower(res.Term)
				r.mu.Unlock()
				return ErrNotLeader
			}
		}
	}

	if accepted < quorum.Accept {
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
	Peer    Node
	Outcome ReplicationOutcome
	Term    Term
	Error   error
}

// send replication request to a peer
func (r *Raft) replicateLogTailTo(
	ctx context.Context,
	peer Node,
	prev LogID,
	results chan<- ReplicationResult,
) {
	for {
		if err := ctx.Err(); err != nil {
			results <- ReplicationResult{Peer: peer, Outcome: ReplicateFailed, Error: err}
			return
		}
		req, err := r.makeAppendEntriesRequest(prev)
		if err != nil {
			results <- ReplicationResult{Peer: peer, Outcome: ReplicateFailed, Error: err}
			return
		}
		rsp, err := r.logTransport.AppendEntries(ctx, peer, req)
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
		prev, err = r.log.PrevLogID(prev)
		r.mu.RUnlock()
		if err != nil {
			err = fmt.Errorf("access previous log: %w", err)
			results <- ReplicationResult{Peer: peer, Outcome: ReplicateFailed, Error: err}
			return
		}
	}
}

func (r *Raft) makeAppendEntriesRequest(prev LogID) (AppendEntriesRequest, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	entries, err := r.log.EntriesAfter(prev)
	if err != nil {
		return AppendEntriesRequest{}, err
	}

	req := AppendEntriesRequest{
		LeaderID:  r.cluster.Self.ID,
		Term:      r.state.Term,
		PrevLogID: prev,
		Entries:   entries,
	}
	return req, nil
}
