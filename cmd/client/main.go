package main

import (
	"fmt"
	"os"

	"cs.ubc.ca/cpsc416/kvraft/kvslib"
	"cs.ubc.ca/cpsc416/kvraft/raft"
	"cs.ubc.ca/cpsc416/kvraft/util"
	"github.com/DistributedClocks/tracing"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Usage: client.go [clientId]")
		return
	}
	clientId := os.Args[1]

	var config raft.ClientConfig
	err := util.ReadJSONConfig("config/client_config"+clientId+".json", &config)
	util.CheckErr(err, "Error reading client config: %v\n", err)

	tracer := tracing.NewTracer(tracing.TracerConfig{
		ServerAddress:  config.TracingServerAddr,
		TracerIdentity: config.TracingIdentity,
		Secret:         config.Secret,
	})

	client := kvslib.NewKVS()
	notifCh, err := client.Start(tracer, config.ClientID, config.CoordIPPort, config.LocalCoordIPPort, config.LocalLeaderServerIPPort, config.ChCapacity)
	go receiveAndPrint(notifCh)

	util.CheckErr(err, "Error reading client config: %v\n", err)

	// time.Sleep(5 * time.Second)

	// Put a key-value pair
	op, err := client.Put(tracer, clientId, "key2", "value2")
	util.CheckErr(err, "Error putting value %v, opId: %v\b", err, op)

	op, err = client.Put(tracer, clientId, "key2", "value3")
	util.CheckErr(err, "Error putting value %v, opId: %v\b", err, op)

	op, err = client.Get(tracer, clientId, "key2")

	// // time.Sleep(5 * time.Second)

	// op, err = client.Put(tracer, clientId, "key2", "value1")
	// util.CheckErr(err, "Error putting value %v, opId: %v\b", err, op)

	// op, err = client.Get(tracer, clientId, "key1")

	// op, err = client.Get(tracer, clientId, "key2")

	for i := 0; i < 20; i++ {
		client.Put(tracer, clientId, fmt.Sprint("k", i), fmt.Sprint("val", i))
		client.Get(tracer, clientId, fmt.Sprint("k", i))
	}
	// for i := 0; i < 1000; i++ {
	// 	client.Get(tracer, clientId, fmt.Sprint("k", i))
	// }

	// Get a key's value
	// op, err = client.Get(tracer, "clientID1", "key1")
	// util.CheckErr(err, "Error getting value %v, opId: %v\b", err, op)

	client.Stop()
}

func receiveAndPrint(notifyCh kvslib.NotifyChannel) {
	for {
		result, ok := <-notifyCh
		if !ok {
			return
		}
		util.PrintfCyan("Result: %v\n", result)
	}
}
