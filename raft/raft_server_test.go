package raft

import (
	"net/rpc"
	"strconv"
	"testing"
	"time"

	"cs.ubc.ca/cpsc416/kvraft/kvslib"
	"cs.ubc.ca/cpsc416/kvraft/util"
	"github.com/DistributedClocks/tracing"
)

func readPeersConfigsHelper() []ServerConfig {
	peersConfig := make([]ServerConfig, 0)
	for i := 1; i <= 3; i++ {
		var sConfig ServerConfig
		err := util.ReadJSONConfig("../config/server_config"+strconv.Itoa(i)+".json", &sConfig)
		util.CheckErr(err, "Error reading server config: %v\n", err)
		peersConfig = append(peersConfig, sConfig)
	}
	return peersConfig
}

func readClientConfigHelper() (ClientConfig, *tracing.Trace) {
	var config ClientConfig
	err := util.ReadJSONConfig("../config/client_config.json", &config)
	util.CheckErr(err, "Error reading client config: %v\n", err)
	ctracer := tracing.NewTracer(tracing.TracerConfig{
		ServerAddress:  config.TracingServerAddr,
		TracerIdentity: config.TracingIdentity,
		Secret:         config.Secret,
	})
	ctrace := ctracer.CreateTrace()
	return config, ctrace
}

func TestPutAndGet(t *testing.T) {
	config, ctrace := readClientConfigHelper()
	peersConfig := readPeersConfigsHelper()

	leader := peersConfig[0]
	leaderClientAddr := leader.ClientListenAddr
	leaderConn, err := rpc.Dial("tcp", leaderClientAddr)
	if err != nil {
		t.Fatalf(`Can't dial lead server , %v`, err)
	}

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
			util.PrintfRed("Test 1 Put Error: Expected Key %s , Actual Key \n", args.Key, reply.Key)
		}
		if args.Value != reply.Value {
			util.PrintfRed("Test 1 Put Error: Expected Value %s , Actual Value \n", args.Value, reply.Value)
		} else {
			util.PrintfGreen("Test 1 Put Passed with key=%s, val=%s \n", strconv.Itoa(i), strconv.Itoa(i)+"0")
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
			util.PrintfRed("Test 1 Get Error: Expected Key %s , Actual Key \n", args.Key, reply.Key)
		}
		expectedVal := strconv.Itoa(i) + "0"
		if reply.Value != expectedVal {
			util.PrintfRed("Test 1 Get Error: Expected value is %s, actual value is %s \n", expectedVal, reply.Value)
		} else {
			util.PrintfGreen("Test 1 Get Passed: expected value: %s, got %s \n", expectedVal, reply.Value)
		}
	}
}

// Get(1000): expect ""
// Put (1000, 10,000): expect success
// Get(1000): expect 10,000
func TestSequentialPutGet(t *testing.T) {
	config, ctrace := readClientConfigHelper()
	peersConfig := readPeersConfigsHelper()

	leader := peersConfig[0]
	leaderClientAddr := leader.ClientListenAddr
	leaderConn, err := rpc.Dial("tcp", leaderClientAddr)
	if err != nil {
		t.Fatalf(`Can't dial lead server , %v`, err)
	}

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
		util.PrintfRed("Test 2 Get1 Error: expected empty string, got %s \n", reply1.Value)
	} else {
		util.PrintfGreen("Test 2 Get1 Passed, received empty string for non-added key\n")
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
		util.PrintfRed("Test 2 Get2 Error: expected value: %s, got %s \n", value, reply2.Value)
	} else {
		util.PrintfGreen("Test 2 Get2 Passed: expected value: %s, got %s \n", value, reply2.Value)
	}
}
func TestNetworkPartition(t *testing.T) {
	var startingOpId uint32
	startingOpId = 100
	var config ClientConfig
	err := util.ReadJSONConfig("../config/client_config.json", &config)
	util.CheckErr(err, "Error reading client config: %v\n", err)

	peersConfig := readPeersConfigsHelper()

	ctracer := tracing.NewTracer(tracing.TracerConfig{
		ServerAddress:  config.TracingServerAddr,
		TracerIdentity: config.TracingIdentity,
		Secret:         config.Secret,
	})
	ctrace := ctracer.CreateTrace()

	leader := peersConfig[0]
	leaderClientAddr := leader.ClientListenAddr
	leaderConn, err := rpc.Dial("tcp", leaderClientAddr)
	if err != nil {
		t.Fatalf(`Can't dial lead server , %v`, err)
	}

	/* Send Puts 1 - 10 to leader (Server 1) */
	for i := 1; i < 11; i++ {
		args := &kvslib.PutRequest{
			ClientId: config.ClientID,
			OpId:     startingOpId + uint32(i),
			Key:      strconv.Itoa(i),
			Value:    strconv.Itoa(i) + "0",
			Token:    ctrace.GenerateToken()}
		var reply kvslib.PutResponse
		err = leaderConn.Call("Server.Put", args, &reply)
		checkPutVal(t, args, reply, err)
	}

	/* Leader 1 is partitioned, should stop sending heartbeats to rest of cluster*/
	args := false
	var reply bool
	err = leaderConn.Call("Server.TestStopSendingLeaderHeartBeats", args, &reply)
	if err != nil {
		t.Fatalf(`Can't tell leader to stop sending hbeats, %v`, err)
	}

	/* Make node 2 the new leader, with cluster = [1,2,3] */
	util.PrintfCyan("Entering network partition...\n")
	peers := make([]ServerInfo, 0)
	node2 := peersConfig[1]
	for _, peer := range peersConfig {
		peers = append(peers, ServerInfo{ServerId: peer.ServerId,
			ServerAddr:       peer.ServerAddr,
			CoordListenAddr:  peer.CoordAddr,
			ServerListenAddr: peer.ServerListenAddr,
			ClientListenAddr: peer.ClientListenAddr,
			Token:            ctrace.GenerateToken()})
	}

	leaderFailOverMsg := LeaderFailOver{
		ServerId:       2,
		FailedLeaderId: 1,
		Peers:          peers,
		Term:           2,
		Token:          tracing.TracingToken{},
	}
	leaderFailOverack := LeaderFailOverAck{}
	node2ClientAddr := node2.ClientListenAddr
	node2Conn, err := rpc.Dial("tcp", node2ClientAddr)
	if err != nil {
		t.Fatalf(`Can't dial new leader, server 2, %v`, err)
	}
	err = node2Conn.Call("Server.NotifyFailOverLeader", leaderFailOverMsg, &leaderFailOverack)

	if err != nil {
		t.Fatalf(`Cannot notify new leader, server 2, %v`, err)
	}

	/* Put Key=11 to 20 to new leader (Server 2) */
	for i := 11; i < 21; i++ {
		args := &kvslib.PutRequest{
			ClientId: config.ClientID,
			OpId:     startingOpId + uint32(i),
			Key:      strconv.Itoa(i),
			Value:    strconv.Itoa(i) + "0",
			Token:    ctrace.GenerateToken()}
		var reply kvslib.PutResponse
		err = node2Conn.Call("Server.Put", args, &reply)
		checkPutVal(t, args, reply, err)
	}

	util.PrintfCyan("Waiting for network partition to heal\n")
	time.Sleep(10 * time.Second)

	/* Heal network partition */
	util.PrintfCyan("Network partition healed; verify server 1 (old leader) has new log entries 11-20 added during network partition\n")

}

func checkPutVal(t *testing.T, args *kvslib.PutRequest, reply kvslib.PutResponse, err error) {
	if err != nil {
		t.Fatalf(`Put Error: %v \n`, err)
	}
	if args.Key != reply.Key {
		util.PrintfRed("Put Error: Expected Key %s , Actual Key \n", args.Key, reply.Key)
	}
	if args.Value != reply.Value {
		util.PrintfRed("Put Error: Expected Value %s , Actual Value \n", args.Value, reply.Value)
	}
}
