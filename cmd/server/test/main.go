package main

import (
	"fmt"
	"os"
	"net/rpc"
	"cs.ubc.ca/cpsc416/kvraft/raft"
	"cs.ubc.ca/cpsc416/kvraft/util"
	"github.com/DistributedClocks/tracing"
)

type PutRequest struct {
	ClientId              string
	OpId                  uint32
	Key                   string
	Value                 string
	Token                 tracing.TracingToken
}

type PutResponse struct {
	ClientId string
	OpId     uint32
	Key      string
	Value    string
	Token    tracing.TracingToken
}

func main() {
	var config raft.ClientConfig
	err := util.ReadJSONConfig("config/client_config.json", &config)
	util.CheckErr(err, "Error reading client config: %v\n", err)
	var serverconfig raft.ServerConfig
	err = util.ReadJSONConfig("config/server_config1.json", &serverconfig)
	util.CheckErr(err, "Error reading server config: %v\n", err)

	ctracer := tracing.NewTracer(tracing.TracerConfig{
		ServerAddress:  config.TracingServerAddr,
		TracerIdentity: config.TracingIdentity,
		Secret:         config.Secret,
	})
	ctrace := ctracer.CreateTrace()
	leaderClientAddr := serverconfig.ClientListenAddr
	leaderConn, err := rpc.Dial("tcp", leaderClientAddr)
	if err != nil{
		fmt.Printf("Can't dial lead server %s\n", leaderClientAddr)
		os.Exit(1)
	}
	fmt.Printf("here")

	// Test 1: Add Key=1, Value=10
	args := &PutRequest{"1", 1, "1", "10", ctrace.GenerateToken()} 
	var reply PutResponse
	err = leaderConn.Call("Server.Put", args, &reply)
	if err != nil{
		fmt.Printf("Error %v", err)
	}

	// Test 2: Add Key=2, Value=20
	args = &PutRequest{"1", 1, "2", "20", ctrace.GenerateToken()} 
	err = leaderConn.Call("Server.Put", args, &reply)
	if err != nil{
		fmt.Printf("Error %v", err)
	}
	
}
