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
	Index Index
	Term  Term
}

type LogEntry struct {
	LogID
	Command []byte
}

type Node struct {
	ID   NodeID
	Addr string
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
