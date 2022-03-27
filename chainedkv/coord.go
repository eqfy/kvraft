package chainedkv

import (
	"fmt"
	"math/rand"
	"net"
	"net/rpc"
	"strings"
	"sync"

	"cs.ubc.ca/cpsc416/a3/fcheck"
	"cs.ubc.ca/cpsc416/a3/util"
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
	// below are not populated in Start()
	TracingServerAddr string
	Secret            []byte
	TracingIdentity   string
}

type Coord struct {
	// Coord state may go here
	config *CoordConfig
	ctrace *tracing.Trace
	strace *tracing.Trace
	ktrace *tracing.Trace
	tracer *tracing.Tracer
	// Server
	headServerId  uint8
	tailServerId  uint8
	serverInfoMap [17]ServerInfo
	serverChain   []uint8
	// Client
	clientInfoMap []ClientInfo
	// Sync
	stateLock         sync.Mutex
	serverAllJoinedWg sync.WaitGroup
}

/** Custom definitions**/
// RPC Structs
type SCoord struct {
	shared *Coord
	// Server coord specific state
	// Server coord sync primitives
	serverJoinDone    [17]chan bool
	serverJoinedCount chan uint8
}
type CCoord struct {
	shared *Coord
	// Client coord specific state
	// Client coord sync primitives
}

// TODO update the RPC args after team discussion
// make sure that the server have separate rpc addrs for the other listening to client and coord
type SCoordJoinArg struct {
	ServerInfo
	Token tracing.TracingToken
}
type SCoordJoinReply struct {
	PrevServerId     uint8
	PrevServerIpPort string
	// Next   uint8 // NOTE Servers should be able to figure out who their next server is
	IsHead bool
	IsTail bool
	Token  tracing.TracingToken
}

type SCoordJoinedArg struct {
	ServerId uint8
	Token    tracing.TracingToken
}
type SCoordJoinedReply struct {
	ServerId        uint8
	AllServerJoined bool // Doesn't seem to be used
	Token           tracing.TracingToken
}

type CCoordGetHeadServerArg struct {
	ClientInfo
	Token tracing.TracingToken
}
type CCoordGetHeadServerReply struct {
	ServerId     uint8
	ServerIpPort string
	Token        tracing.TracingToken
}

type CCoordGetTailServerArg struct {
	ClientInfo
	Token tracing.TracingToken
}
type CCoordGetTailServerReply struct {
	ServerId     uint8
	ServerIpPort string
	Token        tracing.TracingToken
}

type ServerInfo struct {
	ServerId            uint8
	ServerAPIListenAddr string
	CoordAPIListenAddr  string
	ClientAPIListenAddr string
	HBeatAckAddr        string
}

type ClientInfo struct {
	ClientId           string
	CoordAPIListenAddr string
}

// Server RPC
type ReplacePrevServerArg struct {
	ServerId            uint8
	FailedServerIds     []uint8
	NewPrevServerId     uint8
	NewPrevServerIpPort string
	Token               tracing.TracingToken
}

type ReplacePrevServerReply struct {
	HandledFailServerIds []uint8
	LatestProcessedGId   uint64
	Token                tracing.TracingToken
}
type ReplaceNextServerArg struct {
	ServerId            uint8
	FailedServerIds     []uint8
	NewNextServerId     uint8
	NewNextServerIpPort string
	LatestProcessedGId  uint64 // The latested processed GId by the next server
	Token               tracing.TracingToken
}

type ReplaceNextServerReply struct {
	HandledFailServerIds []uint8
	Token                tracing.TracingToken
}

// Kvslib RPC

func NewCoord() *Coord {
	return &Coord{}
}

func (c *Coord) Start(clientAPIListenAddr string, serverAPIListenAddr string, lostMsgsThresh uint8, numServers uint8, ctrace *tracing.Tracer) error {
	// Initialize Coord
	c = &Coord{
		config: &CoordConfig{
			ClientAPIListenAddr: clientAPIListenAddr,
			ServerAPIListenAddr: serverAPIListenAddr,
			LostMsgsThresh:      lostMsgsThresh,
			NumServers:          numServers,
		},
		ctrace:            ctrace.CreateTrace(),
		tracer:            ctrace, // ctrace is a confusing name, see https://piazza.com/class/kxjnnflz1dn3p1?cid=555
		serverAllJoinedWg: sync.WaitGroup{},
	}
	c.serverAllJoinedWg.Add(int(numServers))

	// Record start
	c.ctrace.RecordAction(CoordStart{})

	// Start 2 RPC listners for server and client
	_, err := c.setupServerAPI()
	if err != nil {
		return err
	}

	_, err = c.setupClientAPI()
	if err != nil {
		return err
	}

	// Wait for all servers to have called SCoord.Joined
	// Once c.serverAllJoinedWg.Wait() unblocks, all CCoord functions are ready
	c.serverAllJoinedWg.Wait()
	c.headServerId = 1
	c.tailServerId = numServers
	fmt.Println("all server joined")

	// Start monitoring servers
	serverFailure, err := c.startFCheck()
	if err != nil {
		util.PrintfRed("%v %v\n", "coord: should not terminate with fcheck - ", err)
		return err
	}

	err = c.handleServerFailure(serverFailure)
	if err != nil {
		util.PrintfRed("%v\n", "coord: should not terminate with handle server failure")
		return err
	}

	util.PrintfRed("%v\n", "coord: should not terminate")
	return fmt.Errorf("coord: should not terminate")

}

func (c *Coord) setupServerAPI() (*SCoord, error) {
	fmt.Println("listening on serverAPIListenAddr", c.config.ServerAPIListenAddr)
	laddr, err := net.ResolveTCPAddr("tcp", c.config.ServerAPIListenAddr)
	if err != nil {
		printfErr(err, "unable to resolve serverAPIListenAddr")
		return nil, err
	}

	inbound, err := net.ListenTCP("tcp", laddr)
	if err != nil {
		printfErr(err, "unable to listen to serverAPIListenAddr")
		return nil, err
	}

	sc := &SCoord{
		shared:            c,
		serverJoinedCount: make(chan uint8, 1),
	}
	for i := uint8(0); i < c.config.NumServers+1; i++ {
		sc.serverJoinDone[i] = make(chan bool, 1)
	}

	sc.serverJoinDone[0] <- true // always done for first server
	sc.serverJoinedCount <- 0
	sc.shared.serverChain = append(sc.shared.serverChain, 0)

	serverRpcListener := rpc.NewServer()
	serverRpcListener.Register(sc)
	go serverRpcListener.Accept(inbound)

	return sc, nil
}

func (c *Coord) setupClientAPI() (*CCoord, error) {
	fmt.Println("listening on clientAPIListenAddr", c.config.ClientAPIListenAddr)
	laddr, err := net.ResolveTCPAddr("tcp", c.config.ClientAPIListenAddr)
	if err != nil {
		printfErr(err, "unable to resolve clientAPIListenAddr")
		return nil, err
	}

	inbound, err := net.ListenTCP("tcp", laddr)
	if err != nil {
		printfErr(err, "unable to listen to clientPIListenAddr")
		return nil, err
	}

	cc := &CCoord{
		shared: c,
	}
	clientRpcListener := rpc.NewServer()
	clientRpcListener.Register(cc)
	go clientRpcListener.Accept(inbound)

	return cc, nil
}

func (c *Coord) startFCheck() (<-chan fcheck.FailureDetected, error) {
	var fcheckStructs []fcheck.StartStruct
	for i := uint8(1); i <= c.config.NumServers; i++ {
		serverInfo := c.serverInfoMap[i]
		fcheckStructs = append(fcheckStructs, fcheck.StartStruct{
			EpochNonce:                   rand.Uint64(),
			HBeatLocalIPHBeatLocalPort:   "",
			HBeatRemoteIPHBeatRemotePort: serverInfo.HBeatAckAddr,
			LostMsgThresh:                c.config.LostMsgsThresh,
		})
	}
	return fcheck.StartMultiMonitor(fcheckStructs)
}

func (c *Coord) handleServerFailure(serverFailure <-chan fcheck.FailureDetected) error {
	// Verify this case: Handle case where multiple consecutive servers failed and replacePrev/NextServer returns an error
	//      ex. s2, s3 failed, s1, s4 are alive and we process s2 before s3.
	//          ServerFail{s2}
	//          s1 replace next s3 fail - ServerFailRecvd{s2} err
	//          s3 replace prev s1 fail - err
	//          NewChain{[s1, s3, s4]}
	//          ServerFail{s3}
	//          s1 replace next s4 success - ServerFailRecvd{s3} NewFailoverSuccessor{s4} ServerFailHandled{s3}
	//          s4 replace prev s1 success - ServerFailRecvd{s3} NewFailoverPredecessor{s4} ServerFailHandled{s3}
	//          NewChain{[s1, s4]}
	// Caution! it may be possible for a server to be dead and returns an error to kvslib but not detected by coord (yet)
	// Here are some other ideas regarding server failure
	// If middle server failed, kvslib can block until server finishes reconfigure, processes the command last sent
	//      and acks the command has been processed
	// If head server fails
	//   - kvslib will immediately get an error for put. kvslib should block until receiving a changeHeadServer call from coord to RESEND the put.
	//   - kvslib does not get an error for put, kvslib is ignorant of the headserver fail and can continue sending puts. But, the second put will go into case 1.
	//   - gets should work as normal
	// If tail server fails
	//   - put will block until chain reconfigured so that tail can send back the ack
	//   - kvslib will immediately get an error for get. kvslib should block until receiving a changeTaileServer call from coord to RESEND the get.
	var failedServerIds []uint8
	for {
		// handle one server failure at a time, only notifies client once all failures have been handled
		failure := <-serverFailure
		for _, serverInfo := range c.serverInfoMap {
			if serverInfo.HBeatAckAddr == failure.UDPIpPort {
				var changeHeadserver bool = false
				var changeTailserver bool = false
				failedServerId := serverInfo.ServerId
				chainIdx := -1
				for i, serverIdInChain := range c.serverChain {
					if failedServerId == serverIdInChain {
						chainIdx = i
					}
				}

				c.ctrace.RecordAction(ServerFail{
					ServerId: failedServerId,
				})

				var prevServerId, nextServerId uint8
				failedIsHead := failedServerId == c.headServerId
				failedIsTail := failedServerId == c.tailServerId

				fmt.Println("coord failure failedServerId:", failedServerId, " headServerId: ", c.headServerId, " tailServerId: ", c.tailServerId, " serverChain: ", c.serverChain)
				if !failedIsHead {
					prevServerId = c.serverChain[chainIdx-1]
				}
				if !failedIsTail {
					nextServerId = c.serverChain[chainIdx+1]
				}

				// From assumption that there will always be 1 healthy server, it is impossible for a failed server to be both head and tail server
				allFailed := false
				failedServerIds = append(failedServerIds, failedServerId)
				if failedIsHead { // failed server is a head node
					_, err := c.replacePrevServer(nextServerId, failedServerIds, 0)
					if err != nil {
						allFailed = true
					} else {
						changeHeadserver = true
					}
				} else if failedIsTail { // failed server is a tail node
					err := c.replaceNextServer(prevServerId, failedServerIds, 0, 0)
					if err != nil {
						allFailed = true
					} else {
						changeTailserver = true
					}
				} else { // failed server is a middle node
					replacePrevServerReply, errReplacePrev := c.replacePrevServer(nextServerId, failedServerIds, prevServerId)
					if errReplacePrev == nil && replacePrevServerReply != nil {
						c.replaceNextServer(prevServerId, failedServerIds, nextServerId, replacePrevServerReply.LatestProcessedGId)

						// if error = "Server contact prev server failed": allFailed = false, otherwise allFailed = true
					} else if fmt.Sprint(errReplacePrev) != "Server contact prev server failed" {
						allFailed = true
					}
				}

				if !allFailed {
					// We know all failedServerIds are processed by 1 OR 2 servers so we can clear it now
					failedServerIds = []uint8{}
				}

				// Reconfigure the chain regardless of the replaceNext/PrevServer's success
				c.stateLock.Lock()
				c.serverChain = append(c.serverChain[:chainIdx], c.serverChain[chainIdx+1:]...)
				c.headServerId = c.serverChain[1]
				c.tailServerId = c.serverChain[len(c.serverChain)-1]
				c.stateLock.Unlock()
				if changeHeadserver || len(c.serverChain) == 2 {
					err := c.changeHeadServer()
					if err != nil {
						util.PrintfRed("changeHeadServer failed with err %v\n", err)
					}
					util.PrintfYellow("Changed Head Server to %v\n", c.headServerId)
				}
				if changeTailserver || len(c.serverChain) == 2 {
					err := c.changeTailServer()
					if err != nil {
						util.PrintfRed("changeTailServer failed with err %v\n", err)
					}
					util.PrintfYellow("Changed Tail Server to %v\n", c.tailServerId)
				}
				c.ctrace.RecordAction(NewChain{
					Chain: c.serverChain[1:],
				})
				break
			}
		}
	}
}

func (sc *SCoord) Join(arg SCoordJoinArg, reply *SCoordJoinReply) error {
	serverId := arg.ServerId
	sc.shared.stateLock.Lock()
	sc.shared.strace = sc.shared.tracer.ReceiveToken(arg.Token)
	sc.shared.stateLock.Unlock()
	sc.shared.strace.RecordAction(ServerJoiningRecvd{
		ServerId: serverId,
	})
	sc.shared.serverInfoMap[serverId] = arg.ServerInfo

	// Wait for previous server to finish joining
	<-sc.serverJoinDone[serverId-1]

	// returns and tells the new server who the old tail is (diagram 3.2)
	// once server is done configuring, server will call SCoord.Joined
	if serverId == 1 {
		reply.IsHead = true
	}
	if serverId == sc.shared.config.NumServers {
		reply.IsTail = true
	}
	reply.PrevServerId = sc.shared.serverInfoMap[serverId-1].ServerId
	reply.PrevServerIpPort = sc.shared.serverInfoMap[serverId-1].ServerAPIListenAddr
	reply.Token = sc.shared.strace.GenerateToken()

	fmt.Printf("SCoord.Join(%v)\n", arg)
	return nil
}

func (sc *SCoord) Joined(arg SCoordJoinedArg, reply *SCoordJoinedReply) error {
	serverId := arg.ServerId

	sc.shared.stateLock.Lock()
	sc.shared.strace = sc.shared.tracer.ReceiveToken(arg.Token)
	sc.shared.serverChain = append(sc.shared.serverChain, serverId)
	sc.shared.stateLock.Unlock()

	sc.shared.strace.RecordAction(ServerJoinedRecvd{
		ServerId: serverId,
	})
	sc.shared.strace.RecordAction(NewChain{
		sc.shared.serverChain[1:],
	})

	sc.serverJoinDone[serverId] <- true
	sc.shared.serverAllJoinedWg.Done()
	reply.ServerId = serverId

	// FIXME currServerCount may be redundant
	currServerCount := <-sc.serverJoinedCount + 1
	// Checks if all the servers have Joined
	if currServerCount == sc.shared.config.NumServers {
		sc.shared.strace.RecordAction(AllServersJoined{})
		reply.AllServerJoined = true
	} else {
		sc.serverJoinedCount <- currServerCount
	}
	reply.Token = sc.shared.strace.GenerateToken()

	fmt.Printf("SCoord.Joined(%v) - curr joined server count: %v\n", arg, currServerCount)
	return nil
}

func (cc *CCoord) GetHeadServer(arg CCoordGetHeadServerArg, reply *CCoordGetHeadServerReply) error {
	cc.shared.serverAllJoinedWg.Wait()
	cc.shared.stateLock.Lock()
	cc.shared.ktrace = cc.shared.tracer.ReceiveToken(arg.Token)
	// Each client will call GetHeadServer once so we register the client's info here
	cc.shared.clientInfoMap = append(cc.shared.clientInfoMap, arg.ClientInfo)
	cc.shared.stateLock.Unlock()

	cc.shared.ktrace.RecordAction(HeadReqRecvd{
		ClientId: arg.ClientId,
	})

	headServerId := cc.shared.headServerId
	reply.ServerId = headServerId
	reply.ServerIpPort = cc.shared.serverInfoMap[headServerId].ClientAPIListenAddr

	cc.shared.ktrace.RecordAction(HeadRes{
		ClientId: arg.ClientId,
		ServerId: headServerId,
	})

	reply.Token = cc.shared.ktrace.GenerateToken()
	return nil
}

func (cc *CCoord) GetTailServer(arg CCoordGetTailServerArg, reply *CCoordGetTailServerReply) error {
	cc.shared.serverAllJoinedWg.Wait()
	cc.shared.stateLock.Lock()
	cc.shared.ktrace = cc.shared.tracer.ReceiveToken(arg.Token)
	cc.shared.stateLock.Unlock()

	cc.shared.ktrace.RecordAction(TailReqRecvd{
		ClientId: arg.ClientId,
	})

	tailServerId := cc.shared.tailServerId
	reply.ServerId = tailServerId
	reply.ServerIpPort = cc.shared.serverInfoMap[tailServerId].ClientAPIListenAddr

	cc.shared.ktrace.RecordAction(TailRes{
		ClientId: arg.ClientId,
		ServerId: tailServerId,
	})

	reply.Token = cc.shared.ktrace.GenerateToken()
	return nil
}

func (c *Coord) changeHeadServer() error {
	// TODO can potentially maintain a list of rpc client instead of dialing each time
	for _, clientInfo := range c.clientInfoMap {
		client, err := rpc.Dial("tcp", clientInfo.CoordAPIListenAddr)
		if err != nil {
			printfErr(err, "error dialing client rpc for %v\n", clientInfo)
			return err
		}
		defer client.Close()

		var reply bool // TODO not used
		err = client.Call("CoordListener.ChangeHeadServer", c.serverInfoMap[c.headServerId].ClientAPIListenAddr, &reply)
		if err != nil {
			printfErr(err, "CoordListener.ChangeHeadServer call failed")
			return err
		}
	}
	return nil
}

func (c *Coord) changeTailServer() error {
	for _, clientInfo := range c.clientInfoMap {
		client, err := rpc.Dial("tcp", clientInfo.CoordAPIListenAddr)
		if err != nil {
			printfErr(err, "error dialing client rpc for %v\n", clientInfo)
			return err
		}
		defer client.Close()

		var reply bool // TODO not used
		err = client.Call("CoordListener.ChangeTailServer", c.serverInfoMap[c.tailServerId].ClientAPIListenAddr, &reply)
		if err != nil {
			printfErr(err, "CoordListener.ChangeTailServer call failed")
			return err
		}
	}
	return nil
}

func (c *Coord) replacePrevServer(serverId uint8, failedServerIds []uint8, prevServerId uint8) (*ReplacePrevServerReply, error) {
	client, err := rpc.Dial("tcp", c.serverInfoMap[serverId].CoordAPIListenAddr)
	if err != nil {
		printfErr(err, "error dialing server rpc to replace %v's previous server to %v\n", serverId, prevServerId)
		return nil, err
	}
	defer client.Close()

	req := ReplacePrevServerArg{
		ServerId:            serverId,
		FailedServerIds:     failedServerIds,
		NewPrevServerId:     prevServerId,
		NewPrevServerIpPort: c.serverInfoMap[prevServerId].ServerAPIListenAddr,
		Token:               c.ctrace.GenerateToken(),
	}
	var reply = &ReplacePrevServerReply{}
	err = client.Call("ServerListener.ReplacePrev", req, reply)
	if err != nil {
		printfErr(err, "error calling server rpc to replace %v's previous server to %v\n", serverId, prevServerId)
		return nil, err
	}

	c.stateLock.Lock()
	c.ctrace = c.ctrace.Tracer.ReceiveToken(reply.Token)
	c.stateLock.Unlock()
	for _, handledFailServerId := range reply.HandledFailServerIds {
		c.ctrace.RecordAction(ServerFailHandledRecvd{
			AdjacentServerId: serverId,
			FailedServerId:   handledFailServerId,
		})
	}
	return reply, nil
}

func (c *Coord) replaceNextServer(serverId uint8, failedServerIds []uint8, nextServerId uint8, latestProcessedGId uint64) error {
	if serverId == 0 {
		return nil
	}

	client, err := rpc.Dial("tcp", c.serverInfoMap[serverId].CoordAPIListenAddr)
	if err != nil {
		printfErr(err, "error dialing server rpc to replace %v's next server to %v\n", serverId, nextServerId)
		return err
	}
	defer client.Close()

	req := ReplaceNextServerArg{
		ServerId:            serverId,
		FailedServerIds:     failedServerIds,
		NewNextServerId:     nextServerId,
		NewNextServerIpPort: c.serverInfoMap[nextServerId].ServerAPIListenAddr,
		LatestProcessedGId:  latestProcessedGId,
		Token:               c.ctrace.GenerateToken(),
	}
	var reply = ReplaceNextServerReply{}
	err = client.Call("ServerListener.ReplaceNext", req, &reply)
	if err != nil {
		printfErr(err, "error calling server rpc to replace %v's next server to %v\n", serverId, nextServerId)
		return err
	}

	c.stateLock.Lock()
	c.ctrace = c.ctrace.Tracer.ReceiveToken(reply.Token)
	c.stateLock.Unlock()
	for _, handledFailServerId := range reply.HandledFailServerIds {
		c.ctrace.RecordAction(ServerFailHandledRecvd{
			AdjacentServerId: serverId,
			FailedServerId:   handledFailServerId,
		})
	}
	return nil
}

func printfErr(err error, errMsg string, args ...interface{}) {
	if !strings.HasSuffix(errMsg, "\n") {
		errMsg += "\n"
	}
	util.PrintfRed("coord: "+err.Error()+" -- "+errMsg, args...)
}
