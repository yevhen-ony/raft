package core

type NodeID string

type Role int

type Term uint64

type Index uint64

const (
	Follower Role = iota
	Candidate
	Leader
)

type LogID struct {
	Index Index `json:"index"`
	Term  Term  `json:"term"`
}

type LogRange struct {
	Prev Index
	Last Index
}

type LogSegment struct {
	Prev    LogID
	Entries []LogEntry
}

type LogEntry struct {
	LogID
	Command []byte
}

type Node struct {
	ID   NodeID `json:"id"`
	Addr string `json:"addr"`
}

type AppendEntriesRequest struct {
	LeaderID    NodeID
	Term        Term
	PrevLogID   LogID
	Entries     []LogEntry
	CommitIndex Index
}

type AppendEntriesResponse struct {
	Term    Term
	Success bool
}

type VoteRequest struct {
	CandidateID NodeID
	Term        Term
	LastLogID   LogID
}

type VoteResponse struct {
	Term    Term
	Granted bool
}

type RaftStatus struct {
	NodeID      NodeID `json:"node_id"`
	Role        Role   `json:"role"`
	Term        Term   `json:"term"`
	VotedFor    NodeID `json:"voted_for"`
	CommitIndex Index  `json:"commit_index"`
	LastApplied Index  `json:"last_applied"`
	LastLogID   LogID  `json:"last_log_id"`
}
