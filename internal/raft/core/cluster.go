package core

type Cluster struct {
	Self  Node
	Peers []Node
}

func NewCluster(config *Config) *Cluster {
	peers := make([]Node, 0, len(config.Peers))
	for _, peer := range config.Peers {
		if peer != config.Self {
			peers = append(peers, peer)
		}
	}
	cluster := Cluster{
		Self:  config.Self,
		Peers: peers,
	}
	return &cluster
}

func (cl *Cluster) Size() int {
	return len(cl.Peers) + 1
}

type Quorum struct {
	Accept int
	Reject int
}

func (cl *Cluster) Quorum() Quorum {
	size := cl.Size()
	accept := size/2 + 1

	return Quorum{
		Accept: accept,
		Reject: size - accept + 1,
	}
}

func (cl *Cluster) Nodes() []Node {
	return append([]Node{cl.Self}, cl.Peers...)
}
