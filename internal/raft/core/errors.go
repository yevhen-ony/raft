package core

import "errors"

var (
	ErrNotLeader = errors.New("not a leader")
	ErrLeader = errors.New("is leader")
	ErrPeerRejected = errors.New("peer rejected")
	ErrOutdatedTerm = errors.New("outdated term")
	ErrLogNotFound = errors.New("log not found")
	ErrLogMismatch = errors.New("log mismtach")
)

