package raft

import (
	"cs.ubc.ca/cpsc416/kvraft/kvslib"
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"sync"

	fchecker "cs.ubc.ca/cpsc416/kvraft/fcheck"
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

type TailReqRecvd struct {
	ClientId string
}

type TailRes struct {
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
	// Coord state may go here
	NumServers uint8
	// added for debugging purposes
	HeadServer ServerInfo
	// added for debugging purposes
	TailServer               ServerInfo
	queuedServerJoinRequests []ServerInfo
	numJoinRequests          uint8
	ServerClusterView        []ServerInfo
	BeginQueuedJoinReq       bool
	Trace                    *tracing.Trace
	Tracer                   *tracing.Tracer
	ClientContactAddr        string
	TermNumber               uint8
	Leader                   ServerInfo
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

type HeadTailServerRequest struct {
	ClientId string
	Type     string
	Token    tracing.TracingToken
}

type HeadTailServerReply struct {
	HeadServerAddress string
	TailServerAddress string
	ServerId          uint8
	Token             tracing.TracingToken
}

type ClientLearnServers struct {
	coord *Coord
}

type JoinRecvd struct {
	Token tracing.TracingToken
}

type NotifyServerFailure struct {
	FailedServerId []uint8
	NextServer     ServerInfo
	PrevServer     ServerInfo
	Token          tracing.TracingToken
}

type NotifyServerFailureAck struct {
	ServerId uint8
	Token    tracing.TracingToken
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
	// fmt.Println("Inside GetLeaderNode: LeaderAddr: ", c.coord.Leader.CoordListenAddr)
	// if !joinCompleted || c.coord.Leader.ServerId == 0 {
	//	return errors.New("leader is currently unavailable")
	//}
	ktracer := c.coord.Tracer.ReceiveToken(request.Token)
	ktracer.RecordAction(HeadReqRecvd{ClientId: request.ClientId})

	serverId := c.coord.Leader.ServerId
	ktracer.RecordAction(HeadRes{ClientId: request.ClientId, ServerId: serverId})

	*reply = kvslib.CCoordGetLeaderNodeReply{
		ServerId:     serverId,
		ServerIpPort: c.coord.Leader.ClientListenAddr,
		Token:        ktracer.GenerateToken(),
	}
	return nil
}

func (c *ClientLearnServers) GetHeadTailServer(request HeadTailServerRequest, reply *HeadTailServerReply) error {
	fmt.Println("Recevied headTailRequest from client")
	if !joinCompleted {
		return errors.New("waiting for server join process to finish")
	}
	if request.Type == "head" {
		ktracer := c.coord.Tracer.ReceiveToken(request.Token)
		ktracer.RecordAction(HeadReqRecvd{ClientId: request.ClientId})
		serverId := c.coord.ServerClusterView[0].ServerId

		ktracer.RecordAction(HeadRes{ClientId: request.ClientId, ServerId: serverId})
		*reply = HeadTailServerReply{HeadServerAddress: c.coord.ServerClusterView[0].ClientListenAddr,
			TailServerAddress: "", ServerId: serverId, Token: ktracer.GenerateToken()}
	} else if request.Type == "tail" {
		ktracer := c.coord.Tracer.ReceiveToken(request.Token)
		ktracer.RecordAction(TailReqRecvd{ClientId: request.ClientId})
		tailServerIdx := len(c.coord.ServerClusterView) - 1
		serverId := c.coord.ServerClusterView[tailServerIdx].ServerId

		ktracer.RecordAction(TailRes{ClientId: request.ClientId, ServerId: serverId})
		*reply = HeadTailServerReply{HeadServerAddress: "",
			TailServerAddress: c.coord.ServerClusterView[tailServerIdx].ClientListenAddr, ServerId: serverId, Token: ktracer.GenerateToken()}

	} else {
		return errors.New("could not recognize request type: " + request.Type + ". request type has to be head or tail")
	}
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

func (c *Coord) GetClientListenAddr(request string, reply *bool) error {
	c.ClientContactAddr = request
	fmt.Println("contact addr for client: ", c.ClientContactAddr)
	return nil
}

func joinProtocolNotifyServer(c *Coord, i int, joinResponse JoinResponse) {
	v := c.ServerClusterView[i]
	client, err := rpc.Dial("tcp", v.CoordListenAddr)
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
	c.ServerClusterView = make([]ServerInfo, 0)
	c.Tracer = ctracer

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

			c.ServerClusterView = c.queuedServerJoinRequests
			c.Leader = c.ServerClusterView[0]

			for i := 1; i < len(c.ServerClusterView); i++ {
				v := c.ServerClusterView[i]
				followerJoinResponse := JoinResponse{
					ServerId: v.ServerId,
					Leader:   false,
					Peers:    c.ServerClusterView,
					Term:     1,
				}
				joinProtocolNotifyServer(c, i, followerJoinResponse)
			}

			leaderJoinResponse := JoinResponse{
				ServerId: c.Leader.ServerId,
				Leader:   true,
				Peers:    c.ServerClusterView,
				Term:     1,
			}
			joinProtocolNotifyServer(c, 0, leaderJoinResponse)
			joinCompleted = true
			break
		}
	}

	// join process should be complete at this point
	if joinCompleted {
		c.Trace.RecordAction(AllServersJoined{})
		fmt.Printf("Join process completed; Leader is %v\n", c.Leader)
		fmt.Printf("serverClusterView: %v\n", c.ServerClusterView)
	}

	// server addresses at which fcheck is listening for heartbeats
	fcheckAddrMap := make(map[uint8]string)
	// coord addresses at which fcheck is running
	localFcheckAddrMap := make(map[uint8]string)

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

	fcheckNotifyCh, err := fchecker.StartMonitoringServers(fchecker.StartMonitoringConfig{
		RemoteServerAddr: fcheckAddrMap,
		LocalAddr:        localFcheckAddrMap,
		LostMsgThresh:    lostMsgsThresh,
	})
	if err != nil {
		fmt.Printf("Fchecker failed to start in coord: %v\n", err.Error())
		os.Exit(2)
	}

	concurrentServerFailures := make(map[uint8]bool)
	for {
		select {
		case notify := <-fcheckNotifyCh:
			// Begin Server Failure protocol
			fmt.Printf("server failure detected: %v\n", notify)
			failedServer := notify.ServerId

			if _, ok := concurrentServerFailures[failedServer]; ok {
				fmt.Println("Already marked this server as failed possibly because of concurrent failures")
				delete(concurrentServerFailures, failedServer)
				break
			}

			c.Trace.RecordAction(ServerFail{ServerId: failedServer})
			if c.HeadServer.ServerId == failedServer {
				errStr := ""
				retry := resolveHeadServerFailures(c, &errStr, concurrentServerFailures)
				for {
					if retry {
						fmt.Println("retrying to resolve HeadServerFailures...")
						errStr = ""
						retry = resolveHeadServerFailures(c, &errStr, concurrentServerFailures)
					} else {
						break
					}
				}
				if errStr != "" {
					fmt.Printf("Could not complete serverFailure Protocol: %v\nExiting", errStr)
					os.Exit(1)
				}
				// Chain should be properly updated now

			} else if c.TailServer.ServerId == failedServer {
				errStr := ""
				retry := resolveTailServerFailures(c, &errStr, failedServer, concurrentServerFailures)
				for {
					if retry {
						fmt.Println("retrying to resolve TailerverFailures...")
						errStr = ""
						retry = resolveTailServerFailures(c, &errStr, failedServer, concurrentServerFailures)
					} else {
						break
					}
				}
				if errStr != "" {
					fmt.Printf("Could not complete serverFailure Protocol: %v\nExiting", errStr)
					os.Exit(1)
				}
				// Chain should be properly updated now
			} else {
				// Dealing with intermediate server failures
				errStr := ""
				retry := resolveintermediateServerFailures(c, &errStr, failedServer, concurrentServerFailures)

				for {
					if retry {
						fmt.Println("retrying to resolve intermediate server failures...")
						errStr = ""
						retry = resolveintermediateServerFailures(c, &errStr, failedServer, concurrentServerFailures)
					} else {
						break
					}
				}
				if errStr != "" {
					fmt.Println("FATAL error during server failure protocol: ", errStr)
					fmt.Println("Exiting")
					os.Exit(1)
				}
				// Chain should be properly updated now
			}
		default:
			// do nothing
		}
	}
}

func resolveHeadServerFailures(c *Coord, errorStack *string, concurrentServerFailures map[uint8]bool) bool {
	nextHeadServer, multipleAdjServers := findNewNext(c, errorStack, 0, concurrentServerFailures)
	if *errorStack != "" {
		return false
	}

	// Notify newHead server
	NotifyMsgForNewHeadServer := NotifyServerFailure{
		FailedServerId: multipleAdjServers,
		NextServer:     ServerInfo{},
		PrevServer:     nextHeadServer,
		Token:          c.Trace.GenerateToken(),
	}
	var notifyServerFailureAck NotifyServerFailureAck

	if client, err := rpc.Dial("tcp", nextHeadServer.CoordListenAddr); err != nil {
		fmt.Printf("Could not dial nextHeadServer:  %v\n Retrying to find available servers\n", err.Error())
		*errorStack = ""
		// resolveHeadServerFailures(c, errorStack, concurrentServerFailures)
		return true
	} else {
		err = client.Call("Server.NotifyServerFailure", NotifyMsgForNewHeadServer, &notifyServerFailureAck)
		if err != nil {
			*errorStack = ""
			fmt.Printf("Error received while waiting for ack from server during failure protocol: %v\nRetrying to find available servers", err.Error())
			// resolveHeadServerFailures(c, errorStack, concurrentServerFailures)
			return true
		}
	}

	serverIdx := -1
	for i, v := range c.ServerClusterView {
		if v.ServerId == nextHeadServer.ServerId {
			serverIdx = i
			break
		}
	}
	c.ServerClusterView = c.ServerClusterView[serverIdx:]
	fmt.Println("updated serverChainView: ", c.ServerClusterView)
	c.HeadServer = c.ServerClusterView[0]
	c.TailServer = c.ServerClusterView[len(c.ServerClusterView)-1]

	// trace serverFailHandledReceived
	c.Trace = c.Tracer.ReceiveToken(notifyServerFailureAck.Token)
	for i := 0; i < len(multipleAdjServers); i++ {
		if i > 0 {
			c.Trace.RecordAction(ServerFailHandledRecvd{
				FailedServerId:   multipleAdjServers[i],
				AdjacentServerId: multipleAdjServers[i-1],
			})
		}
		if i != len(multipleAdjServers)-1 {
			c.Trace.RecordAction(ServerFailHandledRecvd{
				FailedServerId:   multipleAdjServers[i],
				AdjacentServerId: multipleAdjServers[i+1],
			})
		}
	}
	if multipleAdjServers[len(multipleAdjServers)-1] < c.HeadServer.ServerId {
		c.Trace.RecordAction(ServerFailHandledRecvd{
			FailedServerId:   multipleAdjServers[len(multipleAdjServers)-1],
			AdjacentServerId: c.HeadServer.ServerId,
		})
	}

	// trace the chain view
	newChainTrace := NewChain{}
	newChainTrace.Chain = make([]uint8, len(c.ServerClusterView))
	for i, v := range c.ServerClusterView {
		newChainTrace.Chain[i] = v.ServerId
	}
	c.Trace.RecordAction(newChainTrace)

	// notify client
	//if client, err := rpc.Dial("tcp", c.ClientContactAddr); err != nil {
	//	fmt.Println("Could not dial client to inform about head server failure: ", err.Error())
	//} else {
	//	var notifyClientAck bool
	//	err = client.Call("CoordNotification.NotifyClientForServerFailure", "head", &notifyClientAck)
	//	if err != nil {
	//		fmt.Println("Error received while waiting for ack from client during failure protocol: ", err.Error())
	//	}
	//}

	return false
}

func resolveTailServerFailures(c *Coord, errorStack *string, failedServerId uint8, concurrentServerFailures map[uint8]bool) bool {
	failedServerIdx := -1
	for i, v := range c.ServerClusterView {
		if v.ServerId == failedServerId {
			failedServerIdx = i
			break
		}
	}
	if failedServerIdx == -1 {
		*errorStack += "could not find server " + strconv.Itoa(int(failedServerId)) + " in serverChainView"
		return false
	}

	nextTailServer, multipleAdjFailures := findNewPrev(c, errorStack, failedServerIdx, concurrentServerFailures)
	if *errorStack != "" {
		return false
	}

	// Notify newTail server
	NotifyMsgForNewTailServer := NotifyServerFailure{
		FailedServerId: multipleAdjFailures,
		NextServer:     nextTailServer,
		PrevServer:     ServerInfo{},
		Token:          c.Trace.GenerateToken(),
	}
	var notifyServerFailureAck NotifyServerFailureAck

	if client, err := rpc.Dial("tcp", nextTailServer.CoordListenAddr); err != nil {
		fmt.Printf("Could not dial nextTailServer:  %v\n Retrying to find available servers\n", err.Error())
		*errorStack = ""
		// resolveHeadServerFailures(c, errorStack, concurrentServerFailures)
		return true
	} else {
		err = client.Call("Server.NotifyServerFailure", NotifyMsgForNewTailServer, &notifyServerFailureAck)
		if err != nil {
			*errorStack = ""
			fmt.Printf("Error received while waiting for ack from server during failure protocol: %v\nRetrying to find available servers", err.Error())
			// resolveHeadServerFailures(c, errorStack, concurrentServerFailures)
			return true
		}
	}

	serverIdx := -1
	for i, v := range c.ServerClusterView {
		if v.ServerId == nextTailServer.ServerId {
			serverIdx = i
			break
		}
	}
	c.ServerClusterView = c.ServerClusterView[:serverIdx+1]
	fmt.Println("updated serverChainView: ", c.ServerClusterView)
	c.HeadServer = c.ServerClusterView[0]
	c.TailServer = c.ServerClusterView[len(c.ServerClusterView)-1]

	// trace the ServerFailHandledReceived
	c.Trace = c.Tracer.ReceiveToken(notifyServerFailureAck.Token)
	if multipleAdjFailures[0] > c.TailServer.ServerId {
		c.Trace.RecordAction(ServerFailHandledRecvd{
			FailedServerId:   multipleAdjFailures[0],
			AdjacentServerId: c.TailServer.ServerId,
		})
	}
	for i := 0; i < len(multipleAdjFailures); i++ {
		if i > 0 {
			c.Trace.RecordAction(ServerFailHandledRecvd{
				FailedServerId:   multipleAdjFailures[i],
				AdjacentServerId: multipleAdjFailures[i-1],
			})
		}
		if i != len(multipleAdjFailures)-1 {
			c.Trace.RecordAction(ServerFailHandledRecvd{
				FailedServerId:   multipleAdjFailures[i],
				AdjacentServerId: multipleAdjFailures[i+1],
			})
		}
	}

	// trace the chain view
	newChainTrace := NewChain{}
	newChainTrace.Chain = make([]uint8, len(c.ServerClusterView))
	for i, v := range c.ServerClusterView {
		newChainTrace.Chain[i] = v.ServerId
	}
	c.Trace.RecordAction(newChainTrace)

	/*if client, err := rpc.Dial("tcp", c.ClientContactAddr); err != nil {
		fmt.Println("Could not dial client to inform about tail server failure: ", err.Error())
	} else {
		var notifyClientAck bool
		err = client.Call("CoordNotification.NotifyClientForServerFailure", "tail", &notifyClientAck)
		if err != nil {
			fmt.Println("Error received while waiting for ack from client during failure protocol: ", err.Error())
		}
	}*/

	return false
}

func resolveintermediateServerFailures(c *Coord, errorStack *string, failedServerId uint8, concurrentServerFailures map[uint8]bool) bool {
	failedServerIdx := -1
	for i, v := range c.ServerClusterView {
		if v.ServerId == failedServerId {
			failedServerIdx = i
			break
		}
	}
	if failedServerIdx == -1 {
		*errorStack += "could not find server " + strconv.Itoa(int(failedServerId)) + " in serverChainView"
		return false
	}

	leftAdjacentServer, multipleAdjFailuresLeft := findNewPrev(c, errorStack, failedServerIdx, concurrentServerFailures)

	rightAdjacentServer, multipleAdjFailuresRight := findNewNext(c, errorStack, failedServerIdx, concurrentServerFailures)

	if leftAdjacentServer.ServerId == 0 && rightAdjacentServer.ServerId == 0 {
		*errorStack += "No online servers found in the server chain"
		return false
	}

	// allAdjFailures := make([]uint8, 0)
	allAdjFailures := multipleAdjFailuresLeft[:len(multipleAdjFailuresLeft)]
	if len(multipleAdjFailuresRight) > 1 {
		allAdjFailures = append(allAdjFailures, multipleAdjFailuresRight[1:]...)
	}

	var notifyServerFailureAckLeft NotifyServerFailureAck
	var notifyServerFailureAckRight NotifyServerFailureAck

	if leftAdjacentServer.ServerId != 0 {
		// Notify leftAdjacent server
		NotifyMsgForLeftAdjServer := NotifyServerFailure{
			FailedServerId: allAdjFailures,
			NextServer:     rightAdjacentServer,
			PrevServer:     ServerInfo{},
			Token:          c.Trace.GenerateToken(),
		}
		if rightAdjacentServer.ServerId == 0 {
			// no servers are online towards the right, then the left adjacent server has to be the tail
			NotifyMsgForLeftAdjServer.NextServer = leftAdjacentServer
		}
		if client, err := rpc.Dial("tcp", leftAdjacentServer.CoordListenAddr); err != nil {
			fmt.Printf("Could not dial leftAdjacentServer:  %v\n Retrying to find available servers\n", err.Error())
			*errorStack = ""
			// resolveintermediateServerFailures(c, errorStack, failedServerId, concurrentServerFailures)
			return true
		} else {
			err = client.Call("Server.NotifyServerFailure", NotifyMsgForLeftAdjServer, &notifyServerFailureAckLeft)
			if err != nil {
				*errorStack = ""
				fmt.Printf("Error received while waiting for ack from server during failure protocol: %v\nRetrying to find available servers", err.Error())
				// resolveintermediateServerFailures(c, errorStack, failedServerId, concurrentServerFailures)
				return true
			}
		}
	}
	if rightAdjacentServer.ServerId != 0 {
		// Notify rightAdjacent server
		NotifyMsgForRightAdjServer := NotifyServerFailure{
			FailedServerId: allAdjFailures,
			NextServer:     ServerInfo{},
			PrevServer:     leftAdjacentServer,
			Token:          c.Trace.GenerateToken(),
		}
		if leftAdjacentServer.ServerId == 0 {
			NotifyMsgForRightAdjServer.PrevServer = rightAdjacentServer
		}
		if client, err := rpc.Dial("tcp", rightAdjacentServer.CoordListenAddr); err != nil {
			fmt.Printf("Could not dial rightAdjacentServer:  %v\n Retrying to find available servers\n", err.Error())
			*errorStack = ""
			// resolveintermediateServerFailures(c, errorStack, failedServerId, concurrentServerFailures)
			return true
		} else {
			err = client.Call("Server.NotifyServerFailure", NotifyMsgForRightAdjServer, &notifyServerFailureAckRight)
			if err != nil {
				*errorStack = ""
				fmt.Printf("Error received while waiting for ack from server during failure protocol: %v\nRetrying to find available servers", err.Error())
				// resolveintermediateServerFailures(c, errorStack, failedServerId, concurrentServerFailures)
				return true
			}
			c.Trace = c.Tracer.ReceiveToken(notifyServerFailureAckRight.Token)
		}
	}

	// received ack from both servers; update serverChainView
	lidx := -1
	ridx := -1
	for i, v := range c.ServerClusterView {
		if v.ServerId == leftAdjacentServer.ServerId {
			lidx = i
			break
		}
	}
	for i, v := range c.ServerClusterView {
		if v.ServerId == rightAdjacentServer.ServerId {
			ridx = i
			break
		}
	}
	if leftAdjacentServer.ServerId != 0 && rightAdjacentServer.ServerId != 0 {
		c.ServerClusterView = append(c.ServerClusterView[:lidx+1], c.ServerClusterView[ridx:]...)
	} else if leftAdjacentServer.ServerId == 0 {
		c.ServerClusterView = c.ServerClusterView[ridx:]
	} else {
		c.ServerClusterView = c.ServerClusterView[:lidx+1]
	}
	fmt.Println("updated serverChainView: ", c.ServerClusterView)
	c.HeadServer = c.ServerClusterView[0]
	c.TailServer = c.ServerClusterView[len(c.ServerClusterView)-1]

	// trace the ServerFailHandledReceived
	for i := 0; i < len(allAdjFailures); i++ {
		if i > 0 {
			c.Trace.RecordAction(ServerFailHandledRecvd{
				FailedServerId:   allAdjFailures[i],
				AdjacentServerId: allAdjFailures[i-1],
			})
		}
		if i != len(allAdjFailures)-1 {
			c.Trace.RecordAction(ServerFailHandledRecvd{
				FailedServerId:   allAdjFailures[i],
				AdjacentServerId: allAdjFailures[i+1],
			})
		}
	}

	if allAdjFailures[0] > c.TailServer.ServerId {
		c.Trace.RecordAction(ServerFailHandledRecvd{
			FailedServerId:   allAdjFailures[0],
			AdjacentServerId: c.TailServer.ServerId,
		})
	} else if allAdjFailures[len(allAdjFailures)-1] < c.HeadServer.ServerId {
		c.Trace.RecordAction(ServerFailHandledRecvd{
			FailedServerId:   allAdjFailures[len(allAdjFailures)-1],
			AdjacentServerId: c.HeadServer.ServerId,
		})
	} else if len(allAdjFailures) == 1 {
		c.Trace.RecordAction(ServerFailHandledRecvd{
			FailedServerId:   failedServerId,
			AdjacentServerId: leftAdjacentServer.ServerId,
		})
		c.Trace.RecordAction(ServerFailHandledRecvd{
			FailedServerId:   failedServerId,
			AdjacentServerId: rightAdjacentServer.ServerId,
		})
	} else if len(allAdjFailures) > 1 {
		c.Trace.RecordAction(ServerFailHandledRecvd{
			FailedServerId:   allAdjFailures[0],
			AdjacentServerId: leftAdjacentServer.ServerId,
		})
		c.Trace.RecordAction(ServerFailHandledRecvd{
			FailedServerId:   allAdjFailures[len(allAdjFailures)-1],
			AdjacentServerId: rightAdjacentServer.ServerId,
		})
	}

	// trace the chain view
	newChainTrace := NewChain{}
	newChainTrace.Chain = make([]uint8, len(c.ServerClusterView))
	for i, v := range c.ServerClusterView {
		newChainTrace.Chain[i] = v.ServerId
	}
	c.Trace.RecordAction(newChainTrace)

	*errorStack = ""
	return false
}

func findNewPrev(c *Coord, errorStack *string, failedServerIdx int, concurrentServerFailures map[uint8]bool) (ServerInfo, []uint8) {
	availableServer := ServerInfo{}
	multipleAdjFailures := make(map[uint8]bool)
	multipleAdjFailures[c.ServerClusterView[failedServerIdx].ServerId] = true

	for i := failedServerIdx - 1; i >= 0; i-- {
		serverToContact := c.ServerClusterView[i]

		if _, err := rpc.Dial("tcp", serverToContact.CoordListenAddr); err != nil {
			fmt.Printf("Could not contact server during server failure protocol: %v\n Marking this server as offline.\n", err.Error())
			if _, ok := concurrentServerFailures[serverToContact.ServerId]; !ok {
				c.Trace.RecordAction(ServerFail{ServerId: serverToContact.ServerId})
				multipleAdjFailures[serverToContact.ServerId] = true
			}
			concurrentServerFailures[serverToContact.ServerId] = true
			fmt.Println("Trying to look for next available server in the chain: ", c.ServerClusterView)
			continue
		} else {
			// server seems to be online. Send failure notification
			availableServer = serverToContact
			break
		}
	}

	if availableServer.ServerId == 0 {
		*errorStack += "could not find any online server to the left of server " + strconv.Itoa(int(c.ServerClusterView[failedServerIdx].ServerId))
	}

	keys := make([]uint8, len(multipleAdjFailures))
	i := 0
	for k := range multipleAdjFailures {
		keys[i] = k
		i++
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	return availableServer, keys
}

func findNewNext(c *Coord, errorStack *string, failedServerIdx int, concurrentServerFailures map[uint8]bool) (ServerInfo, []uint8) {
	availableServer := ServerInfo{}
	multipleAdjFailures := make(map[uint8]bool)
	multipleAdjFailures[c.ServerClusterView[failedServerIdx].ServerId] = true

	for i := failedServerIdx + 1; i < len(c.ServerClusterView); i++ {
		serverToContact := c.ServerClusterView[i]

		if _, err := rpc.Dial("tcp", serverToContact.CoordListenAddr); err != nil {
			fmt.Printf("Could not contact server during server failure protocol: %v\n Marking this server as offline.\n", err.Error())
			if _, ok := concurrentServerFailures[serverToContact.ServerId]; !ok {
				c.Trace.RecordAction(ServerFail{ServerId: serverToContact.ServerId})
				multipleAdjFailures[serverToContact.ServerId] = true
			}
			concurrentServerFailures[serverToContact.ServerId] = true
			fmt.Println("Trying to look for next available server in the chain: ", c.ServerClusterView)
			continue
		} else {
			// server seems to be online. Send failure notification
			availableServer = serverToContact
			break
		}
	}
	if availableServer.ServerId == 0 {
		*errorStack += "could not find any online server to the right of server " + strconv.Itoa(int(c.ServerClusterView[failedServerIdx].ServerId))
	}

	keys := make([]uint8, len(multipleAdjFailures))
	i := 0
	for k := range multipleAdjFailures {
		keys[i] = k
		i++
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return availableServer, keys
}
