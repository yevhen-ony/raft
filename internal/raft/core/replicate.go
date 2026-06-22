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

	state := r.state
	self := r.cluster.Self.ID
	peers := append([]Node(nil), r.cluster.Peers...)
	entries, err := r.log.EntriesAfter(prev)

	r.mu.RUnlock()

	if err != nil {
		return fmt.Errorf("get log entries after: %w", err)
	}

	req := AppendEntriesRequest{
		LeaderID:  self,
		Term:      state.Term,
		PrevLogID: prev,
		Entries:   entries,
	}

	replRes := make(chan ReplicationResult, len(peers))
	for _, peer := range peers {
		go r.replicateLogTailTo(ctx, peer, req, replRes)
	}

	for range len(peers) {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case res := <-replRes:
			switch res.Outcome {
			case ReplicateAccepted:
				// all good

			case ReplicateRejected:
				slog.WarnContext(ctx, "replication rejected", "peer", res.Peer)
				return ErrPeerRejected

			case ReplicateTransportError:
				slog.ErrorContext(ctx, "transport", "peer", res.Peer, "error", res.Error)
				return res.Error

			case ReplicateHigherTerm:
				slog.WarnContext(ctx, "step down", "peer", res.Peer, "old_term", state.Term, "new_term", res.Term)
				r.mu.Lock()
				r.state.StepDown(res.Term)
				r.mu.Unlock()
				return ErrNotLeader
			}
		}
	}
	return nil
}

type ReplicationOutcome int

const (
	ReplicateAccepted ReplicationOutcome = iota
	ReplicateRejected
	ReplicateTransportError
	ReplicateHigherTerm
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
	req AppendEntriesRequest,
	results chan<- ReplicationResult,
) {
	rsp, err := r.transport.AppendEntries(ctx, peer, req)
	if err != nil {
		results <- ReplicationResult{
			Peer:    peer,
			Outcome: ReplicateTransportError,
			Error:   err,
		}
		return
	}
	if rsp.Term > req.Term {
		results <- ReplicationResult{
			Peer:    peer,
			Outcome: ReplicateHigherTerm,
			Term:    rsp.Term,
		}
		return
	}
	if !rsp.Success {
		results <- ReplicationResult{
			Peer:    peer,
			Outcome: ReplicateRejected,
			Term:    rsp.Term,
		}
		return
	}
	results <- ReplicationResult{
		Peer:    peer,
		Outcome: ReplicateAccepted,
		Term:    rsp.Term,
	}
}
