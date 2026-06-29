package core

import (
	"context"
	"log/slog"
)

func (r *Raft) AppendEntries(
	ctx context.Context,
	req AppendEntriesRequest,
) AppendEntriesResponse {

	r.mu.Lock()
	defer r.mu.Unlock()

	if err := r.observeLeader(ctx, req.Term, req.LeaderID); err != nil {
		slog.WarnContext(ctx, "observe leader failes", "term", req.Term, "error", err)
		return AppendEntriesResponse{Term: r.state.Term, Success: false}
	}
	if !r.log.Contains(req.PrevLogID) {
		slog.WarnContext(ctx, "missing prev log", "log_id", req.PrevLogID)
		return AppendEntriesResponse{Term: r.state.Term, Success: false}
	}
	if len(req.Entries) > 0 {
		if err := r.log.AppendAfter(ctx, req.PrevLogID, req.Entries...); err != nil {
			slog.WarnContext(ctx, "failed to append log", "error", err)
			return AppendEntriesResponse{Term: r.state.Term, Success: false}
		}
	}
	r.updateCommitIndex(req.CommitIndex)

	return AppendEntriesResponse{Term: r.state.Term, Success: true}
}
