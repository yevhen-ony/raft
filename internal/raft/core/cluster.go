package core 


type Cluster struct {
	Self Node
	Peers []Node
}	

func NewCluster(config *Config) *Cluster {
	peers := make([]Node, 0, len(config.Peers))
	for _, peer := range config.Peers {
		if peer != config.Self  {
			peers = append(peers, peer)
		}
	}
	cluster := Cluster{
		Self: config.Self,
		Peers: peers,
	}
	return &cluster
}


