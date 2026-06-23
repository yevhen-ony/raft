package core

import "errors"

var (
	ErrNotLeader        = errors.New("not a leader")
	ErrNotCandidate     = errors.New("not a candidate")
	ErrLeader           = errors.New("is leader")
	ErrPeerRejected     = errors.New("peer rejected")
	ErrOutdatedTerm     = errors.New("outdated term")
	ErrLogNotFound      = errors.New("log not found")
	ErrLogMismatch      = errors.New("log mismatch")
	ErrNoPrevLog        = errors.New("first log reached")
	ErrQuorumNotReached = errors.New("quorum not reached")
	ErrElectionLost     = errors.New("election lost")
)
