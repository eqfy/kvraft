package main

import (
	"fmt"
	"net/rpc"
	"os"
	"strconv"

	"cs.ubc.ca/cpsc416/kvraft/kvslib"
	"cs.ubc.ca/cpsc416/kvraft/raft"
	"cs.ubc.ca/cpsc416/kvraft/util"
	"github.com/DistributedClocks/tracing"
)

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
	if err != nil {
		fmt.Printf("Can't dial lead server %s\n", leaderClientAddr)
		os.Exit(1)
	}
	fmt.Printf("here")

	// Test 1 Put: Add Key=1 to 10, Value=10 to 100
	for i := 1; i < 11; i++ {
		args := &kvslib.PutRequest{
			ClientId: config.ClientID,
			OpId:     uint32(i),
			Key:      strconv.Itoa(i),
			Value:    strconv.Itoa(i) + "0",
			Token:    ctrace.GenerateToken()}
		var reply kvslib.PutResponse
		err = leaderConn.Call("Server.Put", args, &reply)
		if err != nil {
			util.PrintfRed("Test 1 Put Error: %v \n", err)
		}
		if args.Key != reply.Key {
			util.PrintfRed("Test 1 Put Error: Expected Key %s , Actual Key", args.Key, reply.Key)
		}
		if args.Value != reply.Value {
			util.PrintfRed("Test 1 Put Error: Expected Value %s , Actual Value", args.Value, reply.Value)
		}
	}

	// Test 1 Get: Get Key=1 to 10, verify values
	for i := 1; i < 11; i++ {
		args := &kvslib.GetRequest{
			ClientId: config.ClientID,
			OpId:     uint32(i + 10),
			Key:      strconv.Itoa(i),
			Token:    ctrace.GenerateToken(),
		}
		var reply kvslib.GetResponse
		err = leaderConn.Call("Server.Get", args, &reply)
		if err != nil {
			util.PrintfRed("Test 1 Get Error: %v \n", err)
		}
		if args.Key != reply.Key {
			util.PrintfRed("Test 1 Get Error: Expected Key %s , Actual Key", args.Key, reply.Key)
		}
		expectedVal := strconv.Itoa(i) + "0"
		if reply.Value != expectedVal {
			util.PrintfRed("Test 1 Get Error: Expected value is %s, actual value is %s\n", expectedVal, reply.Value)
		}
	}

	// Test 2: Sequential Get Put
	key := strconv.Itoa(1000)
	value := key + "0"

	opId := 21
	get_arg := &kvslib.GetRequest{
		ClientId: config.ClientID,
		OpId:     uint32(opId),
		Key:      key,
		Token:    ctrace.GenerateToken(),
	}
	var reply1 kvslib.GetResponse
	err = leaderConn.Call("Server.Get", get_arg, &reply1)
	if err != nil {
		util.PrintfRed("Test 2 Get1 Error: %v \n", err)
	}
	if reply1.Value != "" {
		util.PrintfRed("Test 2 Get1 Error: expected empty string, got %s", reply1.Value)
	}

	opId += 1
	put_arg := &kvslib.PutRequest{
		ClientId: config.ClientID,
		OpId:     uint32(opId),
		Key:      key,
		Value:    key + "0",
		Token:    ctrace.GenerateToken(),
	}
	var put_reply kvslib.PutResponse
	err = leaderConn.Call("Server.Put", put_arg, &put_reply)
	if err != nil {
		util.PrintfRed("Test 2 Put Error: %v \n", err)
	}

	opId += 1
	get_arg.OpId = uint32(opId)
	var reply2 kvslib.GetResponse
	err = leaderConn.Call("Server.Get", get_arg, &reply2)
	if err != nil {
		util.PrintfRed("Test 2 Get2 Error: %v \n", err)
	}
	if reply2.Value != value {
		util.PrintfRed("Test 2 Get2 Error: expected value: %s, got %s", value, reply1.Value)
	}

}
