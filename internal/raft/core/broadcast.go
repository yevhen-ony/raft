package core

import "sync"

type Broadcaster struct {
	mu sync.Mutex
	ch chan struct{}
}

func NewBroadcaster() *Broadcaster {
	return &Broadcaster{
		ch: make(chan struct{}),
	}
}

func (b *Broadcaster) Broadcast() {
	b.mu.Lock()
	defer b.mu.Unlock()

	close(b.ch)
	b.ch = make(chan struct{})
}

func (b *Broadcaster) Subscribe() <-chan struct{} {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.ch
}
