package main

import (
	"cs.ubc.ca/cpsc416/kvraft/raft"
	"cs.ubc.ca/cpsc416/kvraft/util"
	"fmt"
	"github.com/DistributedClocks/tracing"
)

func main() {
	var config raft.CoordConfig
	util.ReadJSONConfig("config/coord_config.json", &config)
	ctracer := tracing.NewTracer(tracing.TracerConfig{
		ServerAddress:  config.TracingServerAddr,
		TracerIdentity: config.TracingIdentity,
		Secret:         config.Secret,
	})
	ctracer.SetShouldPrint(false)
	defer ctracer.Close()
	ctrace := ctracer.CreateTrace()

	coord := raft.NewCoord()
	coord.Trace = ctrace

	err := coord.Start(config.ClientAPIListenAddr, config.ServerAPIListenAddr, config.LostMsgsThresh, config.NumServers, ctracer)
	if err != nil {
		fmt.Println("Failure in coord.Start: ", err.Error())
	}

}
