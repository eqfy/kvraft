package raft

type ClientConfig struct {
	ClientID              string
	CoordIPPort           string
	LocalCoordIPPort      string
	LocalHeadServerIPPort string
	ChCapacity            int
	TracingServerAddr     string
	Secret                []byte
	TracingIdentity       string
}
