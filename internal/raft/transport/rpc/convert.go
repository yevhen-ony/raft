package rpc

import (
	api "raft/gen/proto/raft/v1"
	c "raft/internal/raft/core"
)

func AppendEntriesRequestToPB(req c.AppendEntriesRequest) *api.AppendEntriesRequest {
	entries := make([]*api.LogEntry, len(req.Entries))
	for i, entry := range req.Entries {
		entries[i] = LogEntryToPB(entry)
	}

	return &api.AppendEntriesRequest{
		LeaderId:    string(req.LeaderID),
		Term:        uint64(req.Term),
		PrevLogId:   LogIDToPB(req.PrevLogID),
		CommitIndex: uint64(req.CommitIndex),
		Entries:     entries,
	}
}

func AppendEntriesRequestFromPB(req *api.AppendEntriesRequest) c.AppendEntriesRequest {
	entries := make([]c.LogEntry, len(req.Entries))
	for i, entry := range req.Entries {
		entries[i] = LogEntryFromPB(entry)
	}

	return c.AppendEntriesRequest{
		LeaderID:    c.NodeID(req.LeaderId),
		Term:        c.Term(req.Term),
		PrevLogID:   LogIDFromPB(req.GetPrevLogId()),
		CommitIndex: c.Index(req.CommitIndex),
		Entries:     entries,
	}
}

func AppendEntriesResponseToPB(rsp c.AppendEntriesResponse) *api.AppendEntriesResponse {
	return &api.AppendEntriesResponse{
		Term:    uint64(rsp.Term),
		Success: rsp.Success,
	}
}

func AppendEntriesResponseFromPB(rsp *api.AppendEntriesResponse) c.AppendEntriesResponse {
	return c.AppendEntriesResponse{
		Term:    c.Term(rsp.Term),
		Success: rsp.Success,
	}
}

func LogEntryToPB(entry c.LogEntry) *api.LogEntry {
	return &api.LogEntry{
		LogId:   LogIDToPB(entry.LogID),
		Command: append([]byte(nil), entry.Command...),
	}
}

func LogEntryFromPB(entry *api.LogEntry) c.LogEntry {
	return c.LogEntry{
		LogID:   LogIDFromPB(entry.GetLogId()),
		Command: append([]byte(nil), entry.Command...),
	}
}

func LogIDToPB(logID c.LogID) *api.LogID {
	return &api.LogID{
		Index: uint64(logID.Index),
		Term:  uint64(logID.Term),
	}
}

func LogIDFromPB(logID *api.LogID) c.LogID {
	return c.LogID{
		Index: c.Index(logID.GetIndex()),
		Term:  c.Term(logID.GetTerm()),
	}
}

func VoteRequestToPB(req c.VoteRequest) *api.VoteRequest {
	return &api.VoteRequest{
		CandidateId: string(req.CandidateID),
		Term:        uint64(req.Term),
		LastLogId:   LogIDToPB(req.LastLogID),
	}
}

func VoteRequestFromPB(req *api.VoteRequest) c.VoteRequest {
	return c.VoteRequest{
		CandidateID: c.NodeID(req.GetCandidateId()),
		Term:        c.Term(req.GetTerm()),
		LastLogID:   LogIDFromPB(req.GetLastLogId()),
	}
}

func VoteResponseToPB(rsp c.VoteResponse) *api.VoteResponse {
	return &api.VoteResponse{
		Term:    uint64(rsp.Term),
		Granted: rsp.Granted,
	}
}

func VoteResponseFromPB(rsp *api.VoteResponse) c.VoteResponse {
	return c.VoteResponse{
		Term:    c.Term(rsp.GetTerm()),
		Granted: rsp.GetGranted(),
	}
}

func NodeRefToPB(node c.NodeRef) *api.NodeRef {
	return &api.NodeRef{
		Id:   string(node.ID),
		Addr: node.Addr,
	}
}

func NodeRefFromPB(node *api.NodeRef) c.NodeRef {
	return c.NodeRef{
		ID:   c.NodeID(node.GetId()),
		Addr: node.GetAddr(),
	}
}

func RaftStatusToPB(status c.RaftStatus) *api.RaftStatus {
	return &api.RaftStatus{
		NodeId:      string(status.NodeID),
		Role:        RoleToPB(status.Role),
		Term:        uint64(status.Term),
		VotedFor:    string(status.VotedFor),
		CommitIndex: uint64(status.CommitIndex),
		LastApplied: uint64(status.LastApplied),
		LastLogId:   LogIDToPB(status.LastLogID),
		Leader:      NodeRefToPB(status.Leader),
	}
}

func RoleToPB(role c.Role) string {
	switch role {
	case c.Candidate:
		return "candidate"
	case c.Leader:
		return "leader"
	default:
		return "follower"
	}
}

func RaftStatusFromPB(status *api.RaftStatus) c.RaftStatus {
	return c.RaftStatus{
		NodeID:      c.NodeID(status.GetNodeId()),
		Role:        RoleFromPB(status.GetRole()),
		Term:        c.Term(status.GetTerm()),
		VotedFor:    c.NodeID(status.GetVotedFor()),
		CommitIndex: c.Index(status.GetCommitIndex()),
		LastApplied: c.Index(status.GetLastApplied()),
		LastLogID:   LogIDFromPB(status.GetLastLogId()),
		Leader:      NodeRefFromPB(status.GetLeader()),
	}
}

func RoleFromPB(role string) c.Role {
	switch role {
	case "leader":
		return c.Leader
	case "candidate":
		return c.Candidate
	default:
		return c.Follower
	}
}

func mapSlice[T any, R any](items []T, fn func(T) R) []R {
	res := make([]R, len(items))
	for i, item := range items {
		res[i] = fn(item)
	}
	return res
}
