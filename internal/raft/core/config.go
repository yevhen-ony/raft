package core

import "time"

type Config struct {
	Self   Node
	Peers  []Node
	Leader bool

	HeartbeatInterval  time.Duration
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
}
