package raft

type ClientConfig struct {
	ClientID                string
	CoordIPPort             string
	LocalCoordIPPort        string
	LocalLeaderServerIPPort string
	ChCapacity              int
	TracingServerAddr       string
	Secret                  []byte
	TracingIdentity         string
}
