package chainedkv

import (
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"strings"
	"sync"
	"time"

	"cs.ubc.ca/cpsc416/a3/fcheck"
	"cs.ubc.ca/cpsc416/a3/kvslib"
	"cs.ubc.ca/cpsc416/a3/util"
	"github.com/DistributedClocks/tracing"
)

type ServerStart struct {
	ServerId uint8
}

type ServerJoining struct {
	ServerId uint8
}

type NextServerJoining struct {
	NextServerId uint8
}

type NewJoinedSuccessor struct {
	NextServerId uint8
}

type ServerJoined struct {
	ServerId uint8
}

type ServerFailRecvd struct {
	FailedServerId uint8
}

type NewFailoverSuccessor struct {
	NewNextServerId uint8
}

type NewFailoverPredecessor struct {
	NewPrevServerId uint8
}

type ServerFailHandled struct {
	FailedServerId uint8
}

type PutRecvd struct {
	ClientId string
	OpId     uint32
	Key      string
	Value    string
}

type PutOrdered struct {
	ClientId string
	OpId     uint32
	GId      uint64
	Key      string
	Value    string
}

type PutFwd struct {
	ClientId string
	OpId     uint32
	GId      uint64
	Key      string
	Value    string
}

type PutFwdRecvd struct {
	ClientId string
	OpId     uint32
	GId      uint64
	Key      string
	Value    string
}

type PutResult struct {
	ClientId string
	OpId     uint32
	GId      uint64
	Key      string
	Value    string
}

type GetRecvd struct {
	ClientId string
	OpId     uint32
	Key      string
}

type GetOrdered struct {
	ClientId string
	OpId     uint32
	GId      uint64
	Key      string
}

type GetResult struct {
	ClientId string
	OpId     uint32
	GId      uint64
	Key      string
	Value    string
}

type ServerConfig struct {
	ServerId          uint8
	CoordAddr         string
	ServerAddr        string
	ServerServerAddr  string
	ServerListenAddr  string
	ClientListenAddr  string
	TracingServerAddr string
	Secret            []byte
	TracingIdentity   string
}

/** Custom Definitions **/

const numClients = 256

type NextServer struct {
	NextServerId     uint8
	NextServerIpPort string
}

type PrevServer struct {
	PrevServerId     uint8
	PrevServerIpPort string
}

type KVState map[string]string

type ClientAndOpId struct {
	ClientId string
	OpId     uint32
}

type KeyValGId struct {
	Key   string
	Value string
	GId   uint64
}

type Server struct {
	// Server state may go here
	serverId          uint8
	coordAddr         string
	serverAddr        string
	serverListenAddr  string
	clientListenAdr   string
	strace            *tracing.Trace
	ktrace            *tracing.Trace
	ctrace            *tracing.Trace
	tracer            *tracing.Tracer
	KVState           KVState
	PrevServer        *PrevServer
	NextServer        *NextServer
	IsHead            bool
	IsTail            bool
	currPutGId        uint64
	prevPutGId        uint64
	currGetGId        uint64
	getGIdCount       uint64
	CoordRPC          *rpc.Client
	PrevServerRPC     *rpc.Client
	NextServerRPC     *rpc.Client
	KVSlibRPC         *rpc.Client
	OpMap             map[ClientAndOpId]KeyValGId
	putListenerRPCMap map[string]*rpc.Client
	keepAlive         chan bool

	pendingPutFwdArgs []PutFwdArg // List of PutFwds that are sent but not yet acked by the server

	getQueue chan InternalGet
	putQueue chan InternalPut
	L        sync.RWMutex

	pausePut           sync.WaitGroup // Used to pause processing puts if there is a replace next call
	waitForReplaceNext sync.Cond
}

type InternalGet struct {
	getReq kvslib.GetReq
	done   chan InternalGetRes
}

type InternalPut struct {
	putReq kvslib.PutReq
	done   chan InternalPutRes
}

type InternalGetRes struct {
	getRes kvslib.GetRes
	err    error
}

type InternalPutRes struct {
	putRes kvslib.PutRes
	err    error
}

// RPC struct for listening to kvslib
type Request struct {
	server *Server
}

// RPC struct for listening to adjacent servers
type ServerListener struct {
	server *Server
}

// Server Request and Reply structs
type GetKVStateArg struct {
	ServerId uint8
	Token    tracing.TracingToken
}

type GetKVStateReply struct {
	KVState KVState
	Token   tracing.TracingToken
}

type NewTailArg struct {
	TailServerId     uint8
	TailServerIpPort string
	Token            tracing.TracingToken
}

type NewTailReply struct {
	Token tracing.TracingToken
}

type PutFwdArg struct {
	ClientId        string
	OpId            uint32
	GId             uint64
	Key             string
	Value           string
	PutListenerAddr string
	newGetGId       uint64
	Token           tracing.TracingToken
}

type PutFwdReply struct {
	Gid   uint64
	Token tracing.TracingToken
}

type PutSuccessArg struct {
	PutResult PutResult
	Token     tracing.TracingToken
}

type GetFwdArg struct {
	getGidCount uint64
}
type GetFwdReply struct{}

func NewServer() *Server {
	return &Server{}
}

func (s *Server) Start(serverId uint8, coordAddr string, serverAddr string, serverServerAddr string, serverListenAddr string, clientListenAddr string, strace *tracing.Tracer) error {
	serverIpPort := strings.Split(serverAddr, ":")
	hBeatAckAddr, err := fcheck.StartOnlyAck(serverIpPort[0])
	if err != nil {
		err = errors.New("Error server calling StartOnlyAck failed: " + err.Error())
		util.PrintfRed("%s%s\n", "SHOULD NOT HAPPEN: ", err.Error())
		return err
	}

	// Init fields
	s.KVState = make(KVState)
	s.OpMap = make(map[ClientAndOpId]KeyValGId)
	s.getQueue = make(chan InternalGet, 1024) // potentially be smaller like 256 for the number of clients?
	s.putQueue = make(chan InternalPut, 1024)
	s.putListenerRPCMap = make(map[string]*rpc.Client)
	s.waitForReplaceNext = *sync.NewCond(&sync.Mutex{})

	///////////////////////////////////////////////////////////////////////
	// Part 1: Join the system (chain of servers)

	// trace: ServerStart, ServerJoining
	s.serverId = serverId
	s.coordAddr = coordAddr
	s.serverListenAddr = serverListenAddr
	s.clientListenAdr = clientListenAddr
	s.tracer = strace
	s.strace = strace.CreateTrace()
	s.strace.RecordAction(ServerStart{ServerId: serverId})
	s.strace.RecordAction(ServerJoining{ServerId: serverId})

	// Establish RPC client and listen connection with Coord
	var coordConnErr error
	s.CoordRPC, coordConnErr = rpc.Dial("tcp", coordAddr)
	if coordConnErr != nil {
		coordConnErr = errors.New("Error server " + string(serverId) + " connecting to coord: " + coordConnErr.Error())
		util.PrintfRed("%s%s\n", "SHOULD NOT HAPPEN: ", coordConnErr.Error())
		return coordConnErr
	}

	// Trace ServerJoining
	// Call SCoord.Join()
	// Receive tokens and configure the response values to the server
	var joinReq SCoordJoinArg = SCoordJoinArg{
		ServerInfo: ServerInfo{
			ServerId:            serverId,
			ServerAPIListenAddr: serverListenAddr,
			CoordAPIListenAddr:  serverListenAddr,
			ClientAPIListenAddr: clientListenAddr,
			HBeatAckAddr:        hBeatAckAddr,
		},
		Token: s.strace.GenerateToken(),
	}
	var joinRes SCoordJoinReply
	joinReqErr := s.CoordRPC.Call("SCoord.Join", joinReq, &joinRes)
	if joinReqErr != nil {
		joinReqErr = errors.New("Error server " + string(serverId) + " calling SCoord.Join: " + joinReqErr.Error())
		util.PrintfRed("%s%s\n", "SHOULD NOT HAPPEN: ", joinReqErr.Error())
		return joinReqErr
	}
	s.strace = s.tracer.ReceiveToken(joinRes.Token)

	setupServerListenerErr := s.setupServerListener()
	if setupServerListenerErr != nil {
		return setupServerListenerErr
	}

	if joinRes.PrevServerId == 0 { // Is a head server
		s.PrevServer = nil
	} else {
		s.PrevServer = &PrevServer{PrevServerId: joinRes.PrevServerId, PrevServerIpPort: joinRes.PrevServerIpPort}

		// Establish RPC client and listen connection with the PrevServer
		setupRPCWithPrevServerErr := s.setupRPCWithPrevServer()
		if setupRPCWithPrevServerErr != nil {
			return setupRPCWithPrevServerErr
		}

		//	Write to the PrevServer to declare as NewTail
		//  and wait for and receive an ACK from PrevServer
		var newTailReq NewTailArg = NewTailArg{TailServerId: serverId, TailServerIpPort: serverListenAddr, Token: s.strace.GenerateToken()}
		var newTailRes NewTailReply
		newTailErr := s.PrevServerRPC.Call("ServerListener.NewTail", newTailReq, &newTailRes)
		if newTailErr != nil {
			newTailErr = errors.New("Error server " + string(serverId) + " calling ServerListener.NewTail: " + newTailErr.Error())
			util.PrintfRed("%s%s\n", "SHOULD NOT HAPPEN: ", newTailErr.Error())
			return newTailErr
		}
		s.strace = s.tracer.ReceiveToken(newTailRes.Token)
	}
	s.IsHead = joinRes.IsHead
	s.IsTail = joinRes.IsTail

	// trace ServerJoined(s)
	s.strace.RecordAction(ServerJoined{ServerId: serverId})

	// Call Sccord.Joined()
	var joinedReq SCoordJoinedArg = SCoordJoinedArg{ServerId: serverId, Token: s.strace.GenerateToken()}
	var joinedRes SCoordJoinedReply
	joinedReqErr := s.CoordRPC.Call("SCoord.Joined", joinedReq, &joinedRes)
	if joinedReqErr != nil {
		joinedReqErr = errors.New("Error server " + string(serverId) + " calling SCoord.Joined: " + joinedReqErr.Error())
		util.PrintfRed("%s%s\n", "SHOULD NOT HAPPEN: Scoord.Joined failed ", joinedReqErr.Error())
		return joinedReqErr
	}
	s.strace = s.tracer.ReceiveToken(joinedRes.Token)

	///////////////////////////////////////////////////////////////////////
	// Part 2: Start listening to the client

	// Establish listening connection to Client KVSLib
	setupKVSlibListenerErr := s.setupClientListener()
	if setupKVSlibListenerErr != nil {
		return setupKVSlibListenerErr
	}

	go s.getProcessor()
	go s.putProcessor()

	<-s.keepAlive
	return nil
}

func (s *Server) setupServerListener() error {
	fmt.Println("listening on Server Listen address", s.serverListenAddr)
	laddr, err := net.ResolveTCPAddr("tcp", s.serverListenAddr)
	if err != nil {
		util.PrintfRed("%s%s\n", "Error unable to resolve Server Listen address", err.Error())
		return err
	}

	inbound, err := net.ListenTCP("tcp", laddr)
	if err != nil {
		util.PrintfRed("%s%s\n", "Error unable to listen to Server Listen address", err.Error())
		return err
	}

	sl := &ServerListener{server: s}

	serverRPCListener := rpc.NewServer()
	serverRPCListener.Register(sl)
	go serverRPCListener.Accept(inbound)

	return err
}

func (s *Server) setupClientListener() error {
	fmt.Println("listening on Client address", s.clientListenAdr)
	laddr, err := net.ResolveTCPAddr("tcp", s.clientListenAdr)
	if err != nil {
		util.PrintfRed("%s%s\n", "Error unable to resolve Client address", err.Error())
		return err
	}

	inbound, err := net.ListenTCP("tcp", laddr)
	if err != nil {
		util.PrintfRed("%s%s\n", "Error unable to listen to Client address", err.Error())
		return err
	}

	r := &Request{server: s}

	clientRPCListener := rpc.NewServer()
	clientRPCListener.Register(r)
	go clientRPCListener.Accept(inbound)

	return err
}

// If we receive a NextServer through NewTail request:
//  - trace NextServerJoining(NextServer)
// 	- Update the NextServer
// 	- Send an NewJoinedSuccessorACK to the NextServer (reply with the token is enough?)
//  - Estblish connection with the NextServer ??
func (sl *ServerListener) NewTail(arg NewTailArg, reply *NewTailReply) error {
	sl.server.strace = sl.server.tracer.ReceiveToken(arg.Token)
	newTailServerId := arg.TailServerId
	newTailServerIpPort := arg.TailServerIpPort
	sl.server.strace.RecordAction(NextServerJoining{NextServerId: newTailServerId})
	sl.server.NextServer = &NextServer{
		NextServerId:     newTailServerId,
		NextServerIpPort: newTailServerIpPort,
	}
	reply.Token = sl.server.strace.GenerateToken()
	fmt.Printf("ServerListener.NewTail (%v)", arg)

	// Establish client and listen connection with NextServer
	setupRPCWithNextServerErr := sl.server.setupRPCWithNextServer()
	if setupRPCWithNextServerErr != nil {
		return setupRPCWithNextServerErr
	}

	return nil
}

func (s *Server) setupRPCWithPrevServer() error {
	var prevServerConnErr error
	if s.PrevServer == nil {
		return nil
	}
	s.PrevServerRPC, prevServerConnErr = rpc.Dial("tcp", s.PrevServer.PrevServerIpPort)
	if prevServerConnErr != nil {
		prevServerConnErr = errors.New("Error server " + string(s.serverId) + "connecting to PrevServer " + string(s.PrevServer.PrevServerId) + "resulting :" + prevServerConnErr.Error())
		util.PrintfRed("%s%s\n", "SHOULD NOT HAPPEN: ", prevServerConnErr.Error())
		return prevServerConnErr
	}

	return nil
}

func (s *Server) setupRPCWithNextServer() error {
	if s.NextServer == nil {
		return nil
	}

	var nextServerConnErr error
	s.NextServerRPC, nextServerConnErr = rpc.Dial("tcp", s.NextServer.NextServerIpPort)
	if nextServerConnErr != nil {
		nextServerConnErr = errors.New("Error server " + string(s.serverId) + "connecting to NextServer " + string(s.NextServer.NextServerId) + "resulting :" + nextServerConnErr.Error())
		util.PrintfRed("%s%s\n", "SHOULD NOT HAPPEN: ", nextServerConnErr.Error())
		return nextServerConnErr
	}
	return nil
}

// If we receive PrevServerFailed:
// 	- trace ServerFailRecv(PrevServer)
// 	- Set failedServer = PrevServer and PrevServer = the new prevserver coord sent
//  - Estbablish connection with the updated PrevServer
//  - Ask PrevServer to send over its KVState, and update its KVStates ??
//  - trace: NewFailoverPredecessor(PrevServer), ServerFailHandled(failedServer)
//  - ACK to coord as reconfig done
func (cl *ServerListener) ReplacePrev(arg ReplacePrevServerArg, reply *ReplacePrevServerReply) error {
	cl.server.ctrace = cl.server.tracer.ReceiveToken(arg.Token)
	// Note: this implementation relies on Coord sending the prevserver Id as part of the failedServerIds
	var FailedPrevServerId uint8
	for _, FailedServerId := range arg.FailedServerIds {
		cl.server.ctrace.RecordAction(ServerFailRecvd{FailedServerId})

		// if it's current failed server id == prev server
		if FailedServerId == cl.server.PrevServer.PrevServerId {
			FailedPrevServerId = FailedServerId
		}
	}

	cl.server.L.Lock()
	if arg.NewPrevServerId != 0 {
		cl.server.PrevServer = &PrevServer{
			PrevServerId:     arg.NewPrevServerId,
			PrevServerIpPort: arg.NewPrevServerIpPort,
		}
		setupRPCWithPrevServerErr := cl.server.setupRPCWithPrevServer()
		if setupRPCWithPrevServerErr != nil {
			cl.server.L.Unlock()
			return fmt.Errorf("Server contact prev server failed")
		}

		cl.server.ctrace.RecordAction(NewFailoverPredecessor{NewPrevServerId: arg.NewPrevServerId})
	} else {
		cl.server.PrevServer = nil
	}
	cl.server.L.Unlock()

	cl.server.ctrace.RecordAction(ServerFailHandled{FailedServerId: FailedPrevServerId})

	reply.LatestProcessedGId = max(cl.server.currGetGId, cl.server.currPutGId)
	reply.HandledFailServerIds = append(reply.HandledFailServerIds, FailedPrevServerId)
	reply.Token = cl.server.ctrace.GenerateToken()

	return nil
}

// If we receive NextServerFailed:
//	- trace ServerFailRecvd(NextServer)
// 	- Set failedServer = NextServer and  NextServer = the server coord sent (no need?)
//  - Establish connection with the updated NextServer
//  - trace: NewFailoverSuccessor(NextServer), ServerFailHandled(failedServer)
//  - ACK to coord as reconfig done
//  - Respond to putrequests that are unresponded (does server worry about ths)??

// S1 S2 S3
// S3 fails
// S2's pendingPutFwdArgs may not be empty
// S2's replace next should try to empty pendingPutFwdArgs
// Send trace putResult, and call the relavant client's PutSuccess

// S1 S2 S3 S4
// S3 fails
// S2's pendingPutFwdArgs may not be empty
// S2's replace next should try to empty pendingPutFwdArgs first by sending putFwd to S4
// Send trace putResult, and call the relavant client's PutSuccess
func (cl *ServerListener) ReplaceNext(arg ReplaceNextServerArg, reply *ReplaceNextServerReply) error {
	cl.server.pausePut.Add(1)

	util.PrintfGreen("Replace next started\n")
	cl.server.L.Lock()
	cl.server.ctrace = cl.server.tracer.ReceiveToken(arg.Token)
	// Note: this implementation relies on Coord sending the nextserver Id as part of the failedServerIds
	var FailedNextServerId uint8
	for _, FailedServerId := range arg.FailedServerIds {
		cl.server.ctrace.RecordAction(ServerFailRecvd{FailedServerId})

		// if it's current failed server id == prev server
		if FailedServerId == cl.server.NextServer.NextServerId {
			FailedNextServerId = FailedServerId
		}
	}

	if arg.NewNextServerId != 0 {
		cl.server.NextServer = &NextServer{
			NextServerId:     arg.NewNextServerId,
			NextServerIpPort: arg.NewNextServerIpPort,
		}
		setupRPCWithNextServerErr := cl.server.setupRPCWithNextServer()
		if setupRPCWithNextServerErr != nil {
			return setupRPCWithNextServerErr
		}
		cl.server.ctrace.RecordAction(NewFailoverSuccessor{NewNextServerId: arg.NewNextServerId})

		for _, putFwdReq := range cl.server.pendingPutFwdArgs {
			if putFwdReq.GId <= arg.LatestProcessedGId {
				continue
			}
			var putFwdReply PutFwdReply
			putFwdErr := cl.server.NextServerRPC.Call("ServerListener.PutForward", putFwdReq, &putFwdReply)
			if putFwdErr != nil {
				util.PrintfRed("Put fwd error %v\n", putFwdErr)
				cl.server.L.Unlock()
				return putFwdErr
			}

			// Is this necessary? maybe a timeout at the previous server to resend putFwd is sufficient??
			if cl.server.PrevServer != nil {
				putFwdAckErr := cl.server.PrevServerRPC.Call("ServerListener.PutForwardAck", putFwdReply.Gid, nil)
				if putFwdAckErr != nil {
					util.PrintfRed("Put fwd ack error %v\n", putFwdErr)
					cl.server.L.Unlock()
					return putFwdAckErr
				}
			}
		}
		cl.server.pendingPutFwdArgs = []PutFwdArg{}
	} else { // tail server failure, curr server is a new tail server
		cl.server.NextServer = nil
		// All unacked pending put fwds are now known to have succeeded. Call the relavant client and report put success
		for _, putFwdReq := range cl.server.pendingPutFwdArgs {
			if putFwdReq.GId <= arg.LatestProcessedGId {
				continue
			}

			util.PrintfGreen("Replace next: pendingPutFwd arg sent as a put %v\n", putFwdReq)
			ptrace := cl.server.tracer.ReceiveToken(putFwdReq.Token)
			putRes := PutResult{
				ClientId: putFwdReq.ClientId,
				OpId:     putFwdReq.OpId,
				GId:      putFwdReq.GId,
				Key:      putFwdReq.Key,
				Value:    putFwdReq.Value,
			}

			putListenerRPC, putListenerConnErr := rpc.Dial("tcp", putFwdReq.PutListenerAddr)

			if putListenerConnErr != nil {
				putListenerConnErr = errors.New("Error occurred connecting to PutListener: " + putListenerConnErr.Error())
				util.PrintfRed("%s%s\n", "SHOULD NOT HAPPEN: ", putListenerConnErr.Error())
				cl.server.L.Unlock()
				return putListenerConnErr
			}
			ptrace.RecordAction(putRes)
			putResKVS := kvslib.PutRes{
				OpId:  putFwdReq.OpId,
				GId:   putFwdReq.GId,
				Key:   putFwdReq.Key,
				Token: ptrace.GenerateToken(),
			}
			err := putListenerRPC.Call("ServerListener.PutSuccess", putResKVS, nil)
			util.PrintfGreen("ReplaceNext PutSuccess error: %v\n", err)
		}
		cl.server.pendingPutFwdArgs = []PutFwdArg{}
	}
	cl.server.L.Unlock()
	cl.server.pausePut.Done()
	// cl.server.waitForReplaceNext.Broadcast()
	cl.server.ctrace.RecordAction(ServerFailHandled{FailedServerId: FailedNextServerId})
	reply.HandledFailServerIds = append(reply.HandledFailServerIds, FailedNextServerId)
	reply.Token = cl.server.ctrace.GenerateToken()
	util.PrintfGreen("Replace next ended\n")

	return nil
}

// When it's a Put Operation:
//	If this server is not head server, return error that KVSlib sent put to a non-head server
//	- PutRecvd,
//  - Add it to the server's OpMap
//  - update the KVState
//  - Update GId
//  - (PutOrdered(gid)), PutFwd(gid)
// 	- forward this put request and current GId and GIds to the next server
func (r *Request) Put(arg kvslib.PutReq, reply *kvslib.PutRes) error {
	if r.server.PrevServer != nil {
		putReqErr := errors.New("Server " + string(r.server.serverId) + "which is not head server, received a put request")
		util.PrintfRed("%s%s\n", "SHOULD NOT HAPPEN: ", putReqErr.Error())
		return putReqErr
	}

	done := make(chan InternalPutRes, 1)
	r.server.putQueue <- InternalPut{putReq: arg, done: done}
	<-done
	return nil
}

func (s *Server) putProcessor() error {
	for {
		util.PrintfGreen("waiting for replace next to finish\n")
		s.pausePut.Wait() // Wait until ReplaceNext is finishes handling remaining puts
		util.PrintfGreen("done waiting for replace next to finish\n")

		internalPutReq := <-s.putQueue

		putReq := internalPutReq.putReq
		var internalPutRes InternalPutRes

		util.PrintfGreen("Put got %v\n", putReq) // TODO Remove

		ptrace := s.tracer.ReceiveToken(putReq.Token)
		ptrace.RecordAction(PutRecvd{
			ClientId: putReq.ClientId,
			OpId:     putReq.OpId,
			Key:      putReq.Key,
			Value:    putReq.Value,
		})

		// Handle duplicate requests
		clientAndOpId := ClientAndOpId{ClientId: putReq.ClientId, OpId: putReq.OpId}
		_, existsInMap := s.OpMap[clientAndOpId]
		var GId uint64
		if !existsInMap {
			util.PrintfGreen("LOCK Processing new put lock 1\n")
			s.L.Lock()
			util.PrintfGreen("LOCK Processing new put lock 2\n")
			s.KVState[putReq.Key] = putReq.Value

			s.prevPutGId = s.currPutGId
			s.currPutGId = (max(putReq.NewGetGId, s.prevPutGId)/0x1000 + 1) * 0x1000
			GId = s.currPutGId
			s.OpMap[clientAndOpId] = KeyValGId{Key: putReq.Key, Value: putReq.Value, GId: s.currPutGId}
			s.L.Unlock()
			util.PrintfGreen("Processing new put unlock 1\n")
			ptrace.RecordAction(PutOrdered{
				ClientId: putReq.ClientId,
				OpId:     putReq.OpId,
				GId:      GId,
				Key:      putReq.Key,
				Value:    putReq.Value,
			})
		} else {
			GId = s.OpMap[clientAndOpId].GId
			// FIXME maybe we can just continue here if the put is not new
			// internalPutReq.done <- internalPutRes
			// continue
		}

		// don't really need following varible
		// since the nextserver doesn't really reply to prevserver on putfwd
		var putFwdReply PutFwdReply
		if s.NextServer != nil { // is middle server or head server if more than 1 server
			ptrace.RecordAction(PutFwd{
				ClientId: putReq.ClientId,
				OpId:     putReq.OpId,
				GId:      GId,
				Key:      putReq.Key,
				Value:    putReq.Value,
			})

			putFwdReq := PutFwdArg{
				ClientId:        putReq.ClientId,
				OpId:            putReq.OpId,
				GId:             GId,
				Key:             putReq.Key,
				Value:           putReq.Value,
				PutListenerAddr: putReq.PutListenerAddr,
				newGetGId:       putReq.NewGetGId,
				Token:           ptrace.GenerateToken(),
			}

			// Check if putFwd already exists in pendingPutFwdArgs, if it doesn't exist, add it
			alreadyExists := false
			util.PrintfGreen("%s\n", "LOCK Locking in putProcessor to send PutForWard 1")
			s.L.Lock()
			util.PrintfGreen("%s\n", "LOCK Locking in putProcessor to send PutForWard 2")
			for _, putFwdArgInQueue := range s.pendingPutFwdArgs {
				if putFwdArgInQueue.GId == putFwdReq.GId {
					alreadyExists = true
					break
				}
			}
			if !alreadyExists {
				s.pendingPutFwdArgs = append(s.pendingPutFwdArgs, putFwdReq)
			}

			util.PrintfGreen("PutProcessor Sending put forward\n")
			putFwdErr := s.NextServerRPC.Call("ServerListener.PutForward", putFwdReq, &putFwdReply)
			util.PrintfGreen("%s\n", "PutProcessor sent putForward\n")
			if putFwdErr != nil {
				util.PrintfGreen("%s\n", "PutProcessor sent putForward err 1\n")
				s.L.Unlock()
				util.PrintfGreen("%s\n", "PutProcessor sent putForward err 2\n")
				// internalPutRes.err = putFwdErr
				// internalPutReq.done <- internalPutRes
				// util.PrintfGreen("%s\n", "PutProcessor sent putForward err 3\n")
				continue
			}
			// PutForward succeeded
			i := 0
			for i = 0; i < len(s.pendingPutFwdArgs); i++ {
				if s.pendingPutFwdArgs[i].GId == putFwdReply.Gid {
					break
				}
			}
			if i == 0 {
				s.pendingPutFwdArgs = s.pendingPutFwdArgs[1:]
			} else {
				util.PrintfRed("Out of order put forward reply\n")
				if i == len(s.pendingPutFwdArgs) {
					s.pendingPutFwdArgs = s.pendingPutFwdArgs[:i]
				} else {
					s.pendingPutFwdArgs = append(s.pendingPutFwdArgs[:i], s.pendingPutFwdArgs[i+1:]...)
				}
			}
			util.PrintfGreen("%s\n", "Unlocking in putProcessor to send PutForWard 1")
			s.L.Unlock()
			util.PrintfGreen("%s\n", "Unlocking in putProcessor to send PutForWard 2")

		} else { // is tail or tail+head server
			putRes := PutResult{
				ClientId: putReq.ClientId,
				OpId:     putReq.OpId,
				GId:      GId,
				Key:      putReq.Key,
				Value:    putReq.Value,
			}
			putListenerRPC, ok := s.putListenerRPCMap[putReq.ClientId]
			if !ok {
				var putListenerConnErr error
				putListenerRPC, putListenerConnErr = rpc.Dial("tcp", putReq.PutListenerAddr)
				if putListenerConnErr != nil {
					putListenerConnErr = errors.New("Error occurred connecting to PutListener: " + putListenerConnErr.Error())
					util.PrintfRed("%s%s\n", "SHOULD NOT HAPPEN: ", putListenerConnErr.Error())
					return putListenerConnErr
				}

			}
			ptrace.RecordAction(putRes)
			putResKVS := kvslib.PutRes{
				OpId:  putReq.OpId,
				GId:   GId,
				Key:   putReq.Key,
				Token: ptrace.GenerateToken(),
			}
			putListenerRPC.Call("ServerListener.PutSuccess", putResKVS, nil)
		}
		internalPutReq.done <- internalPutRes
		util.PrintfGreen("Put done for %v\n", putReq.OpId)
	}
}

// When it's a Put Forward  Operation:
//  - update the KVState
//	- Update GId
//  - PutFwdRecv(gid)
// 	If this is the tailServer (doesn't have nextServer):
// 		- PutResult(gid)
// 		- send ack message to the source kvslib
//	else (middle server):
// 		- PutFwd(gid)
//		- forward this put to the next server
func (sl ServerListener) PutForward(arg PutFwdArg, reply *PutFwdReply) error {
	util.PrintfGreen("PutFoward got %v\n", arg) // TODO Remove

	sl.server.pausePut.Wait() // Wait until ReplaceNext is finishes handling remaining put forwards

	ptrace := sl.server.tracer.ReceiveToken(arg.Token)

	util.PrintfGreen("LOCK forward put with gid %v, 1\n", arg.GId)
	sl.server.L.Lock()
	util.PrintfGreen("LOCK forward put with gid %v, 2\n", arg.GId)
	sl.server.KVState[arg.Key] = arg.Value

	sl.server.prevPutGId = sl.server.currPutGId
	sl.server.currPutGId = arg.GId

	ptrace.RecordAction(PutFwdRecvd{
		ClientId: arg.ClientId,
		OpId:     arg.OpId,
		GId:      arg.GId,
		Key:      arg.Key,
		Value:    arg.Value,
	})
	clientAndOpId := ClientAndOpId{ClientId: arg.ClientId, OpId: arg.OpId}
	_, existsInMap := sl.server.OpMap[clientAndOpId]

	if sl.server.NextServer == nil {
		if !existsInMap {
			sl.server.OpMap[clientAndOpId] = KeyValGId{Key: arg.Key, Value: arg.Value, GId: arg.GId}
		} else {
			// ignoring duplicate put requests in tail server, since it would've already been ACKed
			return nil
		}
		sl.server.getGIdCount = 1
		sl.server.currGetGId = sl.server.currPutGId + sl.server.getGIdCount

		putRes := PutResult{
			ClientId: arg.ClientId,
			OpId:     arg.OpId,
			GId:      arg.GId,
			Key:      arg.Key,
			Value:    arg.Value,
		}
		ptrace.RecordAction(putRes)

		putListenerRPC, ok := sl.server.putListenerRPCMap[arg.ClientId]
		if !ok {
			var putListenerConnErr error
			putListenerRPC, putListenerConnErr = rpc.Dial("tcp", arg.PutListenerAddr)
			if putListenerConnErr != nil {
				putListenerConnErr = errors.New("Error occurred connecting to PutListener: " + putListenerConnErr.Error())
				util.PrintfRed("%s%s\n", "SHOULD NOT HAPPEN: ", putListenerConnErr.Error())
				return putListenerConnErr
			}

		}
		putListenerRPC.Call("ServerListener.PutSuccess", kvslib.PutRes{
			OpId:  putRes.OpId,
			GId:   putRes.GId,
			Key:   putRes.Key,
			Token: ptrace.GenerateToken(),
		}, nil)
	} else {
		if !existsInMap {
			ptrace.RecordAction(PutFwd{
				ClientId: arg.ClientId,
				OpId:     arg.OpId,
				GId:      arg.GId,
				Key:      arg.Key,
				Value:    arg.Value,
			})

			sl.server.OpMap[clientAndOpId] = KeyValGId{Key: arg.Key, Value: arg.Value, GId: arg.GId}
		}

		putFwdReq := PutFwdArg{
			ClientId:        arg.ClientId,
			OpId:            arg.OpId,
			GId:             arg.GId,
			Key:             arg.Key,
			Value:           arg.Value,
			PutListenerAddr: arg.PutListenerAddr,
			Token:           ptrace.GenerateToken(),
		}

		util.PrintfGreen("putForward pendingPutFwdArgs 1%v\n", sl.server.pendingPutFwdArgs) // TODO Remove

		// Check if putFwd already exists in pendingPutFwdArgs, if it doesn't exist, add it
		alreadyExists := false
		for _, putFwdArgInQueue := range sl.server.pendingPutFwdArgs {
			if putFwdArgInQueue.GId == putFwdReq.GId {
				alreadyExists = true
				break
			}
		}
		if !alreadyExists {
			sl.server.pendingPutFwdArgs = append(sl.server.pendingPutFwdArgs, putFwdReq)
		}

		util.PrintfGreen("putForward pendingPutFwdArgs 2%v\n", sl.server.pendingPutFwdArgs) // TODO Remove

		var putFwdReply PutFwdReply
		putFwdErr := sl.server.NextServerRPC.Call("ServerListener.PutForward", putFwdReq, &putFwdReply)
		if putFwdErr != nil {
			// TODO we should wait for replaceNext to succeed before continuing

			sl.server.L.Unlock()
			return putFwdErr
			// FIXME Not USED
			sl.server.waitForReplaceNext.L.Lock()
			sl.server.waitForReplaceNext.Wait()
			sl.server.waitForReplaceNext.L.Unlock()
		}

		// PutForward succeeded, remove the put from the list of pending puts not processed by server
		i := 0
		for i = 0; i < len(sl.server.pendingPutFwdArgs); i++ {
			if sl.server.pendingPutFwdArgs[i].GId == putFwdReply.Gid {
				break
			}
		}
		if i == 0 {
			sl.server.pendingPutFwdArgs = sl.server.pendingPutFwdArgs[1:]
		} else {
			util.PrintfRed("Out of order put forward reply waiting %v got gid%v\n", arg, putFwdReply.Gid) // TODO Remove
			if i == len(sl.server.pendingPutFwdArgs) {
				sl.server.pendingPutFwdArgs = sl.server.pendingPutFwdArgs[:i]
			} else {
				sl.server.pendingPutFwdArgs = append(sl.server.pendingPutFwdArgs[:i], sl.server.pendingPutFwdArgs[i+1:]...)
			}

		}
		util.PrintfGreen("putForward pendingPutFwdArgs 3%v\n", sl.server.pendingPutFwdArgs) // TODO Remove
	}

	sl.server.L.Unlock()
	util.PrintfGreen("Finished put forward with gid %v\n", arg.GId)

	reply.Gid = arg.GId
	return nil
}

// If it's a Get:
//  - check if server is tail server (no NextServer), otherwise return error (KVSlib contacted non-tail server)
// 	- trace: GetRecvd,
//  - GIdCount += 1; GId = GIdCount
//  - trace: GetOrdered(gid), GetResult(gid)
//  - send the value of the key requested to the source kvslib
func (r *Request) Get(arg kvslib.GetReq, reply *kvslib.GetRes) error {
	done := make(chan InternalGetRes, 1)
	r.server.getQueue <- InternalGet{getReq: arg, done: done}
	doneStruct := <-done
	*reply = doneStruct.getRes
	if doneStruct.err != nil {
		return doneStruct.err
	}
	return nil
}

func (s *Server) getProcessor() {
	for {
		internalGetReq := <-s.getQueue
		getReq := internalGetReq.getReq
		var internalGetRes InternalGetRes

		util.PrintfGreen("%s\n", "in Request.Get")
		if s.NextServer != nil {
			getReqErr := errors.New("Server " + string(s.serverId) + "which is not tail server, received a Get request")
			util.PrintfRed("%s%s\n", "SHOULD NOT HAPPEN: ", getReqErr.Error())
			internalGetRes.err = getReqErr
			internalGetReq.done <- internalGetRes
			continue
		}

		// return nil on duplicates
		clientAndOpId := ClientAndOpId{ClientId: getReq.ClientId, OpId: getReq.OpId}
		_, existsInMap := s.OpMap[clientAndOpId]
		if existsInMap {
			util.PrintfGreen("%s\n", "in Request.Get of server, existsInMap")
			internalGetReq.done <- internalGetRes
			continue
		}

		gtrace := s.tracer.ReceiveToken(getReq.Token)
		gtrace.RecordAction(GetRecvd{
			ClientId: getReq.ClientId,
			OpId:     getReq.OpId,
			Key:      getReq.Key,
		})

		util.PrintfGreen("LOCK Get lock for %v, 1\n", getReq)
		s.L.Lock()
		util.PrintfGreen("LOCK Get lock for %v, 2\n", getReq)
		s.getGIdCount += 1
		s.currGetGId = s.currPutGId + (s.getGIdCount/0x1000)*0x1000 + (s.getGIdCount % 0x1000)
		currGetGId := s.currGetGId
		s.OpMap[clientAndOpId] = KeyValGId{Key: getReq.Key, Value: "", GId: s.currGetGId}
		// fwd the get up the chain
		if s.PrevServer != nil && s.PrevServerRPC != nil {
			go s.getForwardSender()
		}
		s.L.Unlock()
		util.PrintfGreen("Get lock done for %v\n", getReq)

		gtrace.RecordAction(GetOrdered{
			ClientId: getReq.ClientId,
			OpId:     getReq.OpId,
			GId:      currGetGId,
			Key:      getReq.Key,
		})
		gtrace.RecordAction(GetResult{
			ClientId: getReq.ClientId,
			OpId:     getReq.OpId,
			GId:      currGetGId,
			Key:      getReq.Key,
			Value:    s.KVState[getReq.Key],
		})

		internalGetRes.getRes.OpId = getReq.OpId
		internalGetRes.getRes.GId = currGetGId
		internalGetRes.getRes.Key = getReq.Key
		if val, existsInMap := s.KVState[getReq.Key]; existsInMap {
			internalGetRes.getRes.Value = val
		} else {
			internalGetRes.getRes.Value = ""
		}
		internalGetRes.getRes.Token = gtrace.GenerateToken()

		internalGetRes.err = nil
		internalGetReq.done <- internalGetRes
	}
}

func (s *Server) getForwardSender() {
	for {
		timer := time.NewTimer(1 * time.Second)
		if s.PrevServerRPC == nil {
			return
		}
		fwdGetRPC := s.PrevServerRPC.Go("ServerListener.GetForward", GetFwdArg{s.getGIdCount}, nil, nil)
		select {
		case <-fwdGetRPC.Done:
			if fwdGetRPC.Error == nil {
				return
			}
			<-timer.C // wait and resend
			continue
		case <-timer.C:
			continue
		}
	}
}

func (sl *ServerListener) GetForward(arg GetFwdArg, reply *GetFwdReply) error {
	util.PrintfGreen("LOCK %s\n", "Locking in GetForward")
	sl.server.L.Lock()
	sl.server.currGetGId = arg.getGidCount
	if sl.server.PrevServer != nil { // curr server is not a head server
		go sl.server.getForwardSender()
	}
	sl.server.L.Unlock()
	util.PrintfGreen("LOCK %s\n", "Unlocked in GetForward")
	return nil
}

type PutForwardAckReply struct{}

func (sl *ServerListener) PutForwardAck(arg uint64, reply *PutForwardAckReply) error {
	// arg is the gid of the putFwdAck
	// PutForward succeeded, remove the put from the list of pending puts not processed by server
	util.PrintfGreen("LOCK %s\n", "Locking in PutForwardAck")
	sl.server.L.Lock()
	i := 0
	for i = 0; i < len(sl.server.pendingPutFwdArgs); i++ {
		if sl.server.pendingPutFwdArgs[i].GId == arg {
			break
		}
	}
	if i == 0 {
		sl.server.pendingPutFwdArgs = sl.server.pendingPutFwdArgs[1:]
	} else {
		util.PrintfRed("Out of order put forward reply\n")
		if i == len(sl.server.pendingPutFwdArgs) {
			sl.server.pendingPutFwdArgs = sl.server.pendingPutFwdArgs[:i]
		} else {
			sl.server.pendingPutFwdArgs = append(sl.server.pendingPutFwdArgs[:i], sl.server.pendingPutFwdArgs[i+1:]...)
		}
	}
	sl.server.L.Unlock()
	util.PrintfGreen("%s\n", "Unlocked in PutForwardAck")
	return nil
}

func max(n1 uint64, n2 uint64) uint64 {
	if n1 < n2 {
		return n2
	} else {
		return n1
	}
}
