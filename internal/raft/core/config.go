package core

type Config struct {
	Self   Node
	Peers  []Node
	Leader bool
}
