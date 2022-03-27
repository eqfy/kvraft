package main

import (
	"fmt"
	"os"

	"cs.ubc.ca/cpsc416/a3/chainedkv"
	"cs.ubc.ca/cpsc416/a3/util"
	"github.com/DistributedClocks/tracing"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Usage: server.go [serverId]")
		return
	}
	serverId := os.Args[1]

	var config chainedkv.ServerConfig
	util.ReadJSONConfig("config/server_config"+serverId+".json", &config)
	stracer := tracing.NewTracer(tracing.TracerConfig{
		ServerAddress:  config.TracingServerAddr,
		TracerIdentity: config.TracingIdentity,
		Secret:         config.Secret,
	})
	server := chainedkv.NewServer()
	server.Start(config.ServerId, config.CoordAddr, config.ServerAddr, config.ServerServerAddr, config.ServerListenAddr, config.ClientListenAddr, stracer)
}
