package raft

import (
	"errors"
	"fmt"
	"math"
	"net"
	"net/rpc"
	"os"
	"sort"
	"sync"

	fchecker "cs.ubc.ca/cpsc416/kvraft/fcheck"
	"cs.ubc.ca/cpsc416/kvraft/kvslib"
	"github.com/DistributedClocks/tracing"
)

// Actions to be recorded by coord (as part of ctrace, ktrace, and strace):

type CoordStart struct {
}

type ServerFail struct {
	ServerId uint8
}

type ServerFailHandledRecvd struct {
	FailedServerId   uint8
	AdjacentServerId uint8
}

type NewChain struct {
	Chain []uint8
}

type AllServersJoined struct {
}

type HeadReqRecvd struct {
	ClientId string
}

type HeadRes struct {
	ClientId string
	ServerId uint8
}

type ServerJoiningRecvd struct {
	ServerId uint8
}

type ServerJoinedRecvd struct {
	ServerId uint8
}

type CoordConfig struct {
	ClientAPIListenAddr string
	ServerAPIListenAddr string
	LostMsgsThresh      uint8
	NumServers          uint8
	TracingServerAddr   string
	Secret              []byte
	TracingIdentity     string
}

type Coord struct {
	NumServers               uint8
	queuedServerJoinRequests []ServerInfo
	numJoinRequests          uint8
	ServerClusterView        map[uint8]ServerInfo
	BeginQueuedJoinReq       bool
	Trace                    *tracing.Trace
	Tracer                   *tracing.Tracer
	ClientContactAddr        string
	TermNumber               uint8
	Leader                   ServerInfo
	ClientIpList             []string
}

type ServerInfo struct {
	ServerId         uint8
	ServerAddr       string
	CoordListenAddr  string
	ServerListenAddr string
	ClientListenAddr string
	Token            tracing.TracingToken
}

type ServerJoinAck struct {
	ServerId uint8
	Token    tracing.TracingToken
}

type JoinResponse struct {
	ServerId uint8
	Leader   bool
	Peers    []ServerInfo
	Term     uint32
}

type ClientLearnServers struct {
	coord *Coord
}

type JoinRecvd struct {
	Token tracing.TracingToken
}

// ServerLogState Coord retrieves this info from each
// server during leader selection process to determine
// the rightful leader
type ServerLogState struct {
	ServerId uint8
	Term     uint32
	LogIdx   uint64
}

type ServerLogStateRequest struct {
	Token tracing.TracingToken
}

// LeaderFailOver Coord passes on this info to the
// new leader
type LeaderFailOver struct {
	// id of new leader
	ServerId       uint8
	FailedLeaderId uint8
	// perhaps the two above ids above
	// could be useful for tracing
	Peers []ServerInfo
	Term  uint8
	Token tracing.TracingToken
}

type LeaderFailOverAck struct {
	ServerId uint8
	Token    tracing.TracingToken
}

// TerminateNotification Let the servers know that kvs can
// no longer stay functional when consensus requirements
// are unmet
type TerminateNotification struct {
	Token tracing.TracingToken
}

var mu sync.Mutex
var clientMu sync.Mutex
var joinCompleted bool

func (c *ClientLearnServers) GetLeaderNode(request kvslib.CCoordGetLeaderNodeArg, reply *kvslib.CCoordGetLeaderNodeReply) error {
	clientMu.Lock()
	defer clientMu.Unlock()

	for {
		if !joinCompleted || c.coord.Leader.ServerId == 0 {
			continue
		} else {
			break
		}
	}

	ktracer := c.coord.Tracer.ReceiveToken(request.Token)
	ktracer.RecordAction(HeadReqRecvd{ClientId: request.ClientId})

	serverId := c.coord.Leader.ServerId
	ktracer.RecordAction(HeadRes{ClientId: request.ClientId, ServerId: serverId})

	*reply = kvslib.CCoordGetLeaderNodeReply{
		ServerId:     serverId,
		ServerIpPort: c.coord.Leader.ClientListenAddr,
		Token:        ktracer.GenerateToken(),
	}

	c.coord.ClientIpList = append(c.coord.ClientIpList, request.ClientInfo.CoordAPIListenAddr)
	return nil
}

func (c *Coord) RequestServerJoin(serverInfo ServerInfo, reply *JoinRecvd) error {
	mu.Lock()
	defer mu.Unlock()
	fmt.Printf("Received join request: %v\n", serverInfo)
	strace := c.Tracer.ReceiveToken(serverInfo.Token)
	strace.RecordAction(ServerJoiningRecvd{serverInfo.ServerId})
	// receivedServerId := serverInfo.ServerId
	c.numJoinRequests++
	if c.numJoinRequests > c.NumServers {
		return errors.New("coordinator cannot accept this server join request since max capacity of servers is reached")
	}
	reply.Token = strace.GenerateToken()
	fmt.Printf("( Inside RequestServerJoin() ) Queueing join request for server %d\n", serverInfo.ServerId)
	c.queuedServerJoinRequests = append(c.queuedServerJoinRequests, serverInfo)
	if c.numJoinRequests == c.NumServers {
		c.BeginQueuedJoinReq = true
	}
	return nil
}

func NewCoord() *Coord {
	return &Coord{}
}

func startListeningForServers(serverAPIListenAddr string, c *Coord) error {
	serverListenAddr, err := net.ResolveTCPAddr("tcp", serverAPIListenAddr)
	if err != nil {
		return errors.New("coordinator could not resolve serverAPIListener Addr: " + err.Error())
	}

	fmt.Println("listening for server msgs at: ", serverListenAddr.String())

	inboundServer, err := net.ListenTCP("tcp", serverListenAddr)
	if err != nil {
		return errors.New("unable to listen inbound server connections: " + err.Error())
	}

	err = rpc.Register(c)
	if err != nil {
		return errors.New("could not register Coord type for RPC: " + err.Error())
	}

	go func() {
		for {
			fmt.Println("waiting for incoming server connections")
			rpc.Accept(inboundServer)
			fmt.Println("go routine test test")
		}
	}()

	return nil
}

func startListeningForClient(clientAPIListenAddr string, c *ClientLearnServers) error {
	clientListenAddr, err := net.ResolveTCPAddr("tcp", clientAPIListenAddr)
	if err != nil {
		return errors.New("coordinator could not resolve clientAPIListener Addr: " + err.Error())
	}

	fmt.Println("listening for client msgs at: ", clientListenAddr.String())

	inbound, err := net.ListenTCP("tcp", clientListenAddr)
	if err != nil {
		return errors.New("unable to listen inbound client connections: " + err.Error())
	}

	err = rpc.Register(c)
	if err != nil {
		return errors.New("could not register ClientLearnServers type for RPC: " + err.Error())
	}

	go func() {
		for {
			fmt.Println("waiting for incoming client connections")
			rpc.Accept(inbound)
			fmt.Println("go routine test test")
		}
	}()

	return nil
}

func joinProtocolNotifyServer(c *Coord, server ServerInfo, joinResponse JoinResponse) {
	client, err := rpc.Dial("tcp", server.CoordListenAddr)
	if err != nil {
		fmt.Println("(MAIN) Could not contact server during join process. exiting")
		os.Exit(1)
	}

	serverAck := ServerJoinAck{}
	err = client.Call("Server.FindServerStateOnStartup", joinResponse, &serverAck)
	if err != nil {
		fmt.Printf("Error received when waiting for ack from server join protocol %v; Exiting", err.Error())
		os.Exit(1)
	}

	strace := c.Tracer.ReceiveToken(serverAck.Token)
	strace.RecordAction(ServerJoinedRecvd{serverAck.ServerId})
	err = client.Call("Server.JoinDoneReturnTokenTrace", TokenRequest{strace.GenerateToken()}, nil)
	if err != nil {
		fmt.Printf("Error received when sending token back to server from server join protocol%v; Exiting", err.Error())
		os.Exit(1)
	}
}

func (c *Coord) Start(clientAPIListenAddr string, serverAPIListenAddr string, lostMsgsThresh uint8, numServers uint8, ctracer *tracing.Tracer) error {
	c.Trace.RecordAction(CoordStart{})

	// configuring initial state of coord
	c.NumServers = numServers
	c.queuedServerJoinRequests = make([]ServerInfo, 0)
	c.ServerClusterView = make(map[uint8]ServerInfo)
	c.Tracer = ctracer
	c.TermNumber = 1
	c.ClientIpList = make([]string, 0)

	if err := startListeningForServers(serverAPIListenAddr, c); err != nil {
		return err
	}

	clientLearnServers := ClientLearnServers{coord: c}
	if err := startListeningForClient(clientAPIListenAddr, &clientLearnServers); err != nil {
		return err
	}

	for {
		if c.BeginQueuedJoinReq {
			fmt.Println("starting to resolve queued join requests")

			sort.Slice(c.queuedServerJoinRequests, func(i, j int) bool {
				return c.queuedServerJoinRequests[i].ServerId < c.queuedServerJoinRequests[j].ServerId
			})
			fmt.Printf("Queued requests: %v\n", c.queuedServerJoinRequests)

			c.Leader = c.queuedServerJoinRequests[0]

			for i := 1; i < len(c.queuedServerJoinRequests); i++ {
				v := c.queuedServerJoinRequests[i]
				followerJoinResponse := JoinResponse{
					ServerId: v.ServerId,
					Leader:   false,
					Peers:    c.queuedServerJoinRequests,
					Term:     uint32(c.TermNumber),
				}
				joinProtocolNotifyServer(c, v, followerJoinResponse)
				c.ServerClusterView[v.ServerId] = v
			}

			leaderJoinResponse := JoinResponse{
				ServerId: c.Leader.ServerId,
				Leader:   true,
				Peers:    c.queuedServerJoinRequests,
				Term:     uint32(c.TermNumber),
			}
			joinProtocolNotifyServer(c, c.Leader, leaderJoinResponse)
			c.ServerClusterView[c.Leader.ServerId] = c.Leader
			joinCompleted = true
			break
		}
	}

	// join process should be complete at this point
	if joinCompleted {
		c.Trace.RecordAction(AllServersJoined{})
		fmt.Printf("Join process completed; Leader is %v\n", c.Leader)
		printServerClusterView(c)
	}

	// server addresses at which fcheck is listening for heartbeats
	fcheckAddrMap := make(map[uint8]string)
	// coord addresses at which fcheck is running
	localFcheckAddrMap := make(map[uint8]string)

	setFcheckAddresses(c, fcheckAddrMap, localFcheckAddrMap, serverAPIListenAddr)

	fcheckNotifyCh, fcheckNotifyCh2, err := fchecker.StartMonitoringServers(fchecker.StartMonitoringConfig{
		RemoteServerAddr: fcheckAddrMap,
		LocalAddr:        localFcheckAddrMap,
		LostMsgThresh:    lostMsgsThresh,
	})
	if err != nil {
		fmt.Printf("Fchecker failed to start in coord: %v\n", err.Error())
		os.Exit(2)
	}

	serverFailures := make(map[uint8]bool)
	for {
		select {
		case notify := <-fcheckNotifyCh:
			// Begin Server Failure protocol
			fmt.Printf("server failure detected: %v\n", notify)
			failedServer := notify.ServerId

			if _, ok := serverFailures[failedServer]; ok {
				fmt.Println("Already marked this server as failed")
				break
			} else {
				serverFailures[failedServer] = true
			}

			if err := verifyQuorum(c, serverFailures); err != nil {
				os.Exit(2)
			}

			c.Trace.RecordAction(ServerFail{ServerId: failedServer})

			if c.Leader.ServerId == failedServer {
				if err := beginLeaderSelection(c, failedServer, serverFailures); err != nil {
					fmt.Printf("FATAL could not successfully complete the leader selection protocol: %v\nExiting", err.Error())
					os.Exit(2)
				}
				printServerClusterView(c)
				fmt.Printf("New leader: %v\n", c.Leader.ServerId)
			} else {
				delete(c.ServerClusterView, failedServer)
				printServerClusterView(c)
			}
		case serverId := <-fcheckNotifyCh2:
			fmt.Printf("\nServer %v that was initially unreachable is now responding!!\n", serverId)
			serverInfo := ServerInfo{}
			for _, v := range c.queuedServerJoinRequests {
				if v.ServerId == serverId {
					serverInfo = v
					break
				}
			}
			if serverInfo.ServerId == 0 {
				fmt.Printf("\n WARNING: unable to retrieve information for server %v which is now responsive!\n", serverId)
			} else {
				c.ServerClusterView[serverId] = serverInfo
				delete(serverFailures, serverId)
				printServerClusterView(c)
			}
		default:
			// do nothing
		}
	}
}

func setFcheckAddresses(c *Coord, fcheckAddrMap map[uint8]string, localFcheckAddrMap map[uint8]string, serverAPIListenAddr string) {
	for _, server := range c.ServerClusterView {
		client, err := rpc.Dial("tcp", server.CoordListenAddr)
		if err != nil {
			fmt.Println("(MAIN) Could not contact server during while trying to get fcheck addr. exiting")
			os.Exit(1)
		}
		serverFcheckAddr := ""
		err = client.Call("Server.GetFcheckerAddr", true, &serverFcheckAddr)
		if err != nil {
			fmt.Printf("Error received when trying to call rpc func to get fcheck addr %v; Exiting", err.Error())
			os.Exit(1)
		}
		fcheckAddrMap[server.ServerId] = serverFcheckAddr

		localAddr, err := net.ResolveUDPAddr("udp", serverAPIListenAddr)
		if err != nil {
			fmt.Printf("Could not start fcheck;failed to resolve local addr: %v\nExiting", err.Error())
			os.Exit(2)
		}
		localFcheckAddr, err := getFreePort(localAddr.IP.String())
		if err != nil {
			fmt.Printf("Could not get free ipPort for fchecker: %v\nExiting", err)
			os.Exit(2)
		}
		localFcheckAddrMap[server.ServerId] = localFcheckAddr
	}
	fmt.Println("remote fchecker addresses: ", fcheckAddrMap)
	fmt.Println("local fchecker addresses: ", localFcheckAddrMap)
}

func verifyQuorum(c *Coord, failedServersMap map[uint8]bool) error {
	quorum := int(math.Floor(float64(c.NumServers/2))) + 1
	if len(failedServersMap) >= quorum {
		fmt.Println("FATAL: QUORUM OF SERVERS UNAVAILABLE. SHUTTING DOWN.")

		for _, server := range c.ServerClusterView {
			if client, err := rpc.Dial("tcp", server.CoordListenAddr); err != nil {
				continue
			} else {
				terminateMsg := TerminateNotification{Token: c.Trace.GenerateToken()}
				var ack bool
				_ = client.Call("Server.Terminate", terminateMsg, &ack)
			}
		}
		return errors.New("terminate")
	}
	return nil
}

func findNewLeader(c *Coord, serverFailures map[uint8]bool) (ServerInfo, error) {
	availableServersLogState := make([]ServerLogState, 0)
	// get log state from each follower
	for _, server := range c.ServerClusterView {
		if server.ServerId != c.Leader.ServerId {
			client, err := rpc.Dial("tcp", server.CoordListenAddr)
			if err != nil {
				fmt.Printf("Could not contact server during server failure protocol: %v\n Marking this server as offline.\n", err.Error())
				if _, ok := serverFailures[server.ServerId]; !ok {
					c.Trace.RecordAction(ServerFail{ServerId: server.ServerId})
				}
				serverFailures[server.ServerId] = true
				if err := verifyQuorum(c, serverFailures); err != nil {
					os.Exit(2)
				}
				delete(c.ServerClusterView, server.ServerId)
				fmt.Println("Trying to get log state of next available server in the cluster.")
				printServerClusterView(c)
				continue
			}

			logStateRequest := ServerLogStateRequest{Token: c.Trace.GenerateToken()}
			serverLogState := ServerLogState{}
			err = client.Call("Server.GetLogState", logStateRequest, &serverLogState)
			if err != nil {
				fmt.Printf("Error received when waiting for logState from server during failure protocol: %v", err.Error())
				fmt.Println("Marking this server as offline")
				if _, ok := serverFailures[server.ServerId]; !ok {
					c.Trace.RecordAction(ServerFail{ServerId: server.ServerId})
				}
				serverFailures[server.ServerId] = true
				if err := verifyQuorum(c, serverFailures); err != nil {
					os.Exit(2)
				}
				delete(c.ServerClusterView, server.ServerId)
			} else {
				availableServersLogState = append(availableServersLogState, serverLogState)
			}
		}
	}

	// after receiving the log info from all available servers, select the next best leader
	sort.Slice(availableServersLogState, func(i, j int) bool {
		if availableServersLogState[i].Term > availableServersLogState[j].Term {
			return true
		}
		if availableServersLogState[i].Term < availableServersLogState[j].Term {
			return false
		}
		return availableServersLogState[i].LogIdx > availableServersLogState[j].LogIdx
	})

	if len(availableServersLogState) == 0 {
		return ServerInfo{}, errors.New("could not get log state from any servers")
	}

	newLeaderId := availableServersLogState[0].ServerId
	newLeader := c.ServerClusterView[newLeaderId]
	if newLeader.ServerId == 0 {
		return ServerInfo{}, errors.New("could not find newly selected leader in serverClusterView")
	}

	return newLeader, nil
}

func beginLeaderSelection(c *Coord, failedLeaderId uint8, serverFailures map[uint8]bool) error {
	delete(c.ServerClusterView, failedLeaderId)
	c.TermNumber = c.TermNumber + 1
	for len(c.ServerClusterView) > 0 {
		newLeader, err := findNewLeader(c, serverFailures)
		if err != nil {
			return err
		}
		client, err := rpc.Dial("tcp", newLeader.CoordListenAddr)
		if err != nil {
			fmt.Printf("Could not contact new leader during server failure protocol: %v\n Marking this server as offline.\n", err.Error())
			if _, ok := serverFailures[newLeader.ServerId]; !ok {
				c.Trace.RecordAction(ServerFail{ServerId: newLeader.ServerId})
			}
			serverFailures[newLeader.ServerId] = true
			if err := verifyQuorum(c, serverFailures); err != nil {
				os.Exit(2)
			}
			delete(c.ServerClusterView, newLeader.ServerId)
			fmt.Println("Retrying Leader Selection")
			continue
		}
		peers := make([]ServerInfo, 0)
		for _, server := range c.ServerClusterView {
			peers = append(peers, server)
		}
		leaderFailOverMsg := LeaderFailOver{
			ServerId:       newLeader.ServerId,
			FailedLeaderId: failedLeaderId,
			Peers:          peers,
			Term:           c.TermNumber,
			Token:          c.Trace.GenerateToken(),
		}
		leaderFailOverack := LeaderFailOverAck{}
		err = client.Call("Server.NotifyFailOverLeader", leaderFailOverMsg, &leaderFailOverack)
		if err != nil {
			fmt.Printf("Error received when waiting for leaderFailOverAck:: %v", err.Error())
			fmt.Println("Marking this server as offline")
			if _, ok := serverFailures[newLeader.ServerId]; !ok {
				c.Trace.RecordAction(ServerFail{ServerId: newLeader.ServerId})
			}
			serverFailures[newLeader.ServerId] = true
			if err := verifyQuorum(c, serverFailures); err != nil {
				os.Exit(2)
			}
			delete(c.ServerClusterView, newLeader.ServerId)
			fmt.Println("Retrying Leader Selection")
			continue
		} else {
			// TODO Handle tracing
			c.Leader = newLeader
			return nil
		}
	}
	return errors.New("leader selection failed")
}

func printServerClusterView(c *Coord) {
	fmt.Printf("ServerClusterView: ")
	for _, v := range c.ServerClusterView {
		fmt.Printf("%v, ", v.ServerId)
	}
	fmt.Println()
}
