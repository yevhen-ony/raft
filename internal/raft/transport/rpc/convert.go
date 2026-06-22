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
		LeaderId:     string(req.LeaderID),
		Term:         uint64(req.Term),
		PrevLogIndex: uint64(req.PrevLogID.Index),
		PrevLogTerm:  uint64(req.PrevLogID.Term),
		Entries:      entries,
	}
}

func AppendEntriesRequestFromPB(req *api.AppendEntriesRequest) c.AppendEntriesRequest {
	entries := make([]c.LogEntry, len(req.Entries))
	for i, entry := range req.Entries {
		entries[i] = LogEntryFromPB(entry)
	}

	return c.AppendEntriesRequest{
		LeaderID: c.NodeID(req.LeaderId),
		Term:     c.Term(req.Term),
		PrevLogID: c.LogID{
			Index: c.Index(req.PrevLogIndex),
			Term:  c.Term(req.PrevLogTerm),
		},
		Entries: entries,
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
		Index:   uint64(entry.Index),
		Term:    uint64(entry.Term),
		Command: append([]byte(nil), entry.Command...),
	}
}

func LogEntryFromPB(entry *api.LogEntry) c.LogEntry {
	return c.LogEntry{
		LogID: c.LogID{
			Index: c.Index(entry.Index),
			Term:  c.Term(entry.Term),
		},
		Command: append([]byte(nil), entry.Command...),
	}
}
