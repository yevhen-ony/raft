package core

type Cluster struct {
	Self  NodeRef
	Peers []NodeRef
}

func NewCluster(config *ClusterConfig) *Cluster {
	peers := make([]NodeRef, 0, len(config.Peers))
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

func (cl *Cluster) Nodes() []NodeRef {
	return append([]NodeRef{cl.Self}, cl.Peers...)
}

func (cl *Cluster) Node(id NodeID) NodeRef {
	if id == cl.Self.ID {
		return cl.Self
	}
	for _, n := range cl.Peers {
		if n.ID == id {
			return n
		}
	}
	return NodeRef{}
}
