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
	"time"

	fchecker "cs.ubc.ca/cpsc416/kvraft/fcheck"
	"cs.ubc.ca/cpsc416/kvraft/util"
	"github.com/DistributedClocks/tracing"
)

// Tracing

type ServerStart struct {
	ServerId uint8
}

type ServerJoining struct {
	ServerId uint8
}

type ServerJoined struct {
	ServerId uint8
}

type ServerFailRecvd struct {
	FailedServerId uint8
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

type PutCommitted struct {
	ClientId string
	OpId     uint32
	Key      string
	Value    string
}

type PutRequest struct {
	ClientId string
	OpId     uint32
	Key      string
	Value    string
	Token    tracing.TracingToken
}

type PutResponse struct {
	ClientId string
	OpId     uint32
	Key      string
	Value    string
	Token    tracing.TracingToken
}

type GetRecvd struct {
	ClientId string
	OpId     uint32
	Key      string
}

type GetCommitted struct {
	ClientId string
	OpId     uint32
	Key      string
	Value    string
}

type AppendEntriesRequestSent AppendEntriesArg
type AppendEntriesRequestRecvd AppendEntriesArg
type AppendEntriesResponseSent AppendEntriesReply
type AppendEntriesResponseRecvd AppendEntriesReply

type ForceFollowerLog struct {
	serverIndex uint8
	nextIndex   uint64
	matchIndex  uint64
}

type BecameLeader struct {
	ServerId uint8
}
type BecameFollower struct {
	ServerId uint8
}

// State and RPC

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

type Addr struct {
	CoordAddr        string
	ServerAddr       string
	ServerListenAddr string
	ClientListenAddr string
}

type Server struct {
	// Server state may go here
	ServerId         uint8
	Peers            []ServerInfo
	ClusterSize      uint8 /* original cluster size*/
	Majority         uint8
	Config           Addr
	trace            *tracing.Trace
	tracer           *tracing.Tracer
	fcheckAddr       string
	HeartbeatTimeout uint32

	isLeader bool

	// Raft Paper
	// Persistent state
	currentTerm uint32
	votedFor    uint8

	logMu sync.RWMutex //protects following
	log   []Entry

	kvMu sync.RWMutex // protects following
	kv   map[string]string

	// Volatile state
	commitIndex uint64
	lastApplied uint64
	// Volatile state on leader
	nextIndex  []uint64
	matchIndex []uint64

	// all servers
	commitIndexUpdated               chan bool
	rpcGotTermGreaterThanCurrentTerm chan uint32

	// follower
	appendEntriesCanRespond chan bool
	appendEntriesDone       chan bool
	runFollower             chan bool

	// candidate
	runCandidate chan bool

	// leader
	commandFromClient           chan ClientCommand
	triggerAllFollowerLogUpdate chan bool
	existsInterestingN          chan bool
	runLeader                   chan bool
}

// AppendEntries RPC
type AppendEntriesArg struct {
	Term         uint32
	LeaderId     uint8
	PrevLogIndex uint64
	PrevLogTerm  uint32
	Entries      []Entry
	LeaderCommit uint64

	Token tracing.TracingToken
}

type AppendEntriesReply struct {
	Term    uint32
	Success bool

	Token tracing.TracingToken
}

// Log entry definition
type Entry struct {
	Term    uint32
	Command Command
	Index   uint64
}

type ClientCommand struct {
	command Command
	done    chan Command
}

type Command struct {
	Kind CommandKind
	Key  string
	Val  string
}

type CommandKind int

const (
	Put CommandKind = iota
	Get
)

// Join procedure
type JoinRequest struct {
	ServerId         uint8
	ServerAddr       string
	ServerListenAddr string
}

// Client RPC Calls
type GetRequest struct {
	ClientId string
	OpId     uint32
	Key      string
	Token    tracing.TracingToken
}

type GetResponse struct {
	ClientId string
	OpId     uint32
	Key      string
	Value    string
	Token    tracing.TracingToken
}

type TokenRequest struct {
	Token tracing.TracingToken
}

var joinComplete chan bool

func NewServer() *Server {
	return &Server{}
}

func (s *Server) Start(serverId uint8, coordAddr string, serverAddr string, serverServerAddr string, serverListenAddr string, clientListenAddr string, strace *tracing.Tracer) error {
	s.initServerState(serverId, coordAddr, serverAddr, serverServerAddr, serverListenAddr, clientListenAddr, strace)
	s.trace.RecordAction(ServerStart{serverId})

	go startFcheck(serverAddr, int(serverId), s)
	go listenForCoord(s, serverAddr)
	go listenForServs(s, serverListenAddr)

	joinOnStartup(s)
	<-joinComplete

	/* Join complete, ready to take client requests*/
	go s.runRaft()

	// try to mock a network partition by
	// having a subset of nodes not respond
	// to coord's heartbeats
	// simulateNetworkPartition(s)

	listenForClient(s)

	return nil
}

/* initalize server state*/
func (s *Server) initServerState(serverId uint8, coordAddr string, serverAddr string, serverServerAddr string, serverListenAddr string, clientListenAddr string, strace *tracing.Tracer) {
	s.ServerId = serverId
	s.Config = Addr{CoordAddr: coordAddr, ServerAddr: serverAddr, ServerListenAddr: serverListenAddr, ClientListenAddr: clientListenAddr}
	s.trace = strace.CreateTrace()
	s.tracer = strace

	// values
	s.commitIndex = 0
	s.lastApplied = 0
	s.HeartbeatTimeout = 10

	// raft persistent state
	s.kv = make(map[string]string)
	s.log = make([]Entry, 0)
	s.log = append(s.log, Entry{}) // log is 1-indexed

	// join sync
	joinComplete = make(chan bool)

	// all server sync
	s.commitIndexUpdated = make(chan bool)
	s.rpcGotTermGreaterThanCurrentTerm = make(chan uint32)

	// follower sync
	s.appendEntriesCanRespond = make(chan bool)
	s.appendEntriesDone = make(chan bool)
	s.runFollower = make(chan bool, 1)

	// candidate sync
	s.runCandidate = make(chan bool, 1)

	// leader sync
	s.commandFromClient = make(chan ClientCommand, 1) //question: what should be the capacity of this channel?
	s.runLeader = make(chan bool, 1)
	s.triggerAllFollowerLogUpdate = make(chan bool, 1)

}

func joinOnStartup(s *Server) {
	/* send a request to join to coord*/
	coord, err := rpc.Dial("tcp", s.Config.CoordAddr)
	defer coord.Close()
	res := JoinRecvd{}

	s.trace.RecordAction(ServerJoining{s.ServerId})
	request := ServerInfo{s.ServerId, s.Config.ServerAddr, s.Config.ServerAddr, s.Config.ServerListenAddr, s.Config.ClientListenAddr, s.trace.GenerateToken()}

	err = coord.Call("Coord.RequestServerJoin", request, &res)
	util.CheckErr(err, "Can't connect to coord: ")

	s.tracer.ReceiveToken(res.Token)
	return
}

func (s *Server) FindServerStateOnStartup(coordReply JoinResponse, reply *ServerJoinAck) error {
	if coordReply.Leader {
		fmt.Println("I'm a leader.")
		s.isLeader = true
		s.runLeader <- true
	} else {
		fmt.Println("I'm a follower.")
		s.isLeader = false
		s.runFollower <- true
	}
	s.Peers = coordReply.Peers
	s.ClusterSize = uint8(len(coordReply.Peers))
	s.currentTerm = coordReply.Term
	s.Majority = uint8(math.Ceil(float64(s.ClusterSize) / float64(2)))
	fmt.Printf("Peers: %v\n TermNumber: %v\n", coordReply.Peers, coordReply.Term)

	s.trace.RecordAction(ServerJoined{s.ServerId})
	reply.ServerId = s.ServerId
	reply.Token = s.trace.GenerateToken()
	joinComplete <- true
	return nil
}

func (s *Server) JoinDoneReturnTokenTrace(req TokenRequest, res *bool) (err error) {
	s.tracer.ReceiveToken(req.Token)
	return
}

/* listen for AppendEntries from leader*/
func listenForServs(s *Server, serverListenAddr string) {
	rpc.Register(s)

	// Create a TCP listener that will listen on `Port`
	tcpAddr, err := net.ResolveTCPAddr("tcp", serverListenAddr)
	util.CheckErr(err, "Can't resolve address %s", serverListenAddr)

	// Close the listener whenever we stop
	listener, err := net.ListenTCP("tcp", tcpAddr)
	util.CheckErr(err, "Can't listen on address %s", serverListenAddr)
	fmt.Printf("Waiting for incoming servers...")
	s.Accept(listener) /* Serve each server in new go routinue*/
}

func listenForCoord(s *Server, serverAddr string) {
	rpc.Register(s)

	// Create a TCP listener that will listen on `Port`
	tcpAddr, err := net.ResolveTCPAddr("tcp", serverAddr)
	util.CheckErr(err, "Can't resolve address %s", serverAddr)

	// Close the listener whenever we stop
	listener, err := net.ListenTCP("tcp", tcpAddr)
	util.CheckErr(err, "Can't listen on address %s", serverAddr)

	rpc.Accept(listener)
}

func listenForClient(s *Server) {
	rpc.Register(s)

	// Create a TCP listener that will listen on `Port`
	tcpAddr, err := net.ResolveTCPAddr("tcp", s.Config.ClientListenAddr)
	util.CheckErr(err, "Can't resolve address %s", s.Config.ClientListenAddr)

	// Close the listener whenever we stop
	listener, err := net.ListenTCP("tcp", tcpAddr)
	util.CheckErr(err, "Can't listen on address %s", s.Config.ClientListenAddr)

	s.Accept(listener) /* Serve each client in new go routinue*/
}

func (server *Server) Accept(lis net.Listener) {
	for {
		conn, err := lis.Accept()
		if err != nil {
			fmt.Printf("Error in rpc.Serve: accept; continuing to next accept. %v\n", err.Error())
			continue
		}
		go rpc.ServeConn(conn)
	}
}

func getFreePort(ip string) (string, error) {
	ip = ip + ":0"
	addr, err := net.ResolveUDPAddr("udp", ip)
	if err != nil {
		return "", err
	}

	l, err := net.ListenUDP("udp", addr)
	if err != nil {
		return "", err
	}
	defer l.Close()
	return l.LocalAddr().String(), nil
	// return l.Addr().(*net.TCPAddr).Port, nil
}

func startFcheck(serverAddr string, servId int, s *Server) {
	localAddr, err := net.ResolveUDPAddr("udp", serverAddr)
	if err != nil {
		fmt.Printf("Could not start fcheck;failed to resolve udp addr: %v\nExiting", err.Error())
		os.Exit(2)
	}
	localIp := localAddr.IP.String()
	ipPort, err := getFreePort(localIp)
	if err != nil {
		fmt.Printf("Could not get free ipPort for fchecker: %v\nExiting", err)
		os.Exit(2)
	}
	notifyCh, err := fchecker.Start(fchecker.StartStruct{ipPort, uint64(servId),
		"", "", 0})
	if err != nil {
		fmt.Printf("fchecker could not start: %v\n", err)
		os.Exit(2)
	}

	fmt.Println("FCHECKER: Listening for heartbeats at address: ", ipPort)
	s.fcheckAddr = ipPort

	for {
		select {
		case notify := <-notifyCh:
			fmt.Printf("fchecker recevied a failure from the notify channel: %v\n", notify)
			// How to handle this???
			// fmt.Printf("Exiting")
			// os.Exit(2)
		}
	}
}

// GetFcheckerAddr Coord calls this to find out the address at which fcheck is listening for heartbeats
func (s *Server) GetFcheckerAddr(request bool, reply *string) error {
	if s.fcheckAddr == "" {
		return errors.New("could not get address at which fchecker is running")
	}
	*reply = s.fcheckAddr
	return nil
}

// NotifyFailOverLeader TEMPLATE Server learns it's the new leader when coord calls this
func (s *Server) NotifyFailOverLeader(notification LeaderFailOver, reply *LeaderFailOverAck) error {
	fmt.Println("Received leader failure notification. I'm the new leader.")
	if notification.ServerId != s.ServerId {
		util.PrintlnRed("NotifyFailOverLeader: ServerId mismatch, received Id %d and actual Id is %d ", notification.ServerId, s.ServerId)
		return errors.New("NotifyFailOverLeader: ServerId mismatch")
	}

	// end follower loop, start server loop, forces log on other follower servers
	s.runFollower <- false

	cTrace := s.tracer.ReceiveToken(notification.Token)
	cTrace.RecordAction(ServerFailRecvd{FailedServerId: notification.FailedLeaderId})
	s.Peers = notification.Peers
	s.currentTerm = uint32(notification.Term)
	s.isLeader = true
	s.runLeader <- true
	s.triggerAllFollowerLogUpdate <- true

	reply.ServerId = s.ServerId
	reply.Token = cTrace.GenerateToken()
	return nil
}

// GetLogState TEMPLATE coord calls this during leader selection to determine the next
// best leader
func (s *Server) GetLogState(request ServerLogStateRequest, reply *ServerLogState) error {
	prevLogEntry := s.log[len(s.log)-1]
	*reply = ServerLogState{
		ServerId: s.ServerId,
		Term:     prevLogEntry.Term,
		LogIdx:   prevLogEntry.Index,
	}
	return nil
}

// Terminate If coord detects majority failures, system shuts down
func (s *Server) Terminate(notification TerminateNotification, reply *bool) error {
	fmt.Println("FATAL Quorum of nodes unavailable. Shutting down")
	os.Exit(2)
	return nil
}

func (s *Server) Get(arg GetRequest, resp *GetResponse) error {
	gtrace := s.tracer.ReceiveToken(arg.Token)
	gtrace.RecordAction(GetRecvd{
		ClientId: arg.ClientId,
		OpId:     arg.OpId,
		Key:      arg.Key,
	})

	done := make(chan Command, 1)

	clientCommand := ClientCommand{
		command: Command{
			Kind: Get,
			Key:  arg.Key,
		},
		done: done,
	}

	fmt.Printf("(Leader Get): received client command=%v\n", clientCommand)

	s.commandFromClient <- clientCommand
	// Once done is received, that means the command has been committed
	command := <-clientCommand.done
	if command.Key != arg.Key {
		util.PrintfRed("Get Request: Applied Command key: %s and Client Command key: %s mismatch", command.Key, arg.Key)
		return errors.New("get request: applied command key and client command key mismatch")
	}
	gtrace.RecordAction(GetCommitted{
		ClientId: arg.ClientId,
		OpId:     arg.OpId,
		Key:      command.Key,
		Value:    command.Val,
	})

	resp.ClientId = arg.ClientId
	resp.OpId = arg.OpId
	resp.Key = command.Key
	resp.Value = command.Val
	resp.Token = gtrace.GenerateToken()
	return nil
}

func (s *Server) Put(arg PutRequest, resp *PutResponse) (err error) {
	ptrace := s.tracer.ReceiveToken(arg.Token)
	ptrace.RecordAction(PutRecvd{
		ClientId: arg.ClientId,
		OpId:     arg.OpId,
		Key:      arg.Key,
		Value:    arg.Value,
	})

	done := make(chan Command, 1)

	clientCommand := ClientCommand{
		command: Command{
			Kind: Put,
			Key:  arg.Key,
			Val:  arg.Value,
		},
		done: done,
	}

	fmt.Printf("(Leader Put): received client command=%v\n", clientCommand)

	s.commandFromClient <- clientCommand
	// Once done is received, that means the command has been committed
	command := <-clientCommand.done
	if command.Key != arg.Key {
		util.PrintfRed("Put Request: Applied Command key: %s and Client Command key: %s mismatch", command.Key, arg.Key)
		return errors.New("put request: applied command key and client command key mismatch")
	}
	if command.Val != arg.Value {
		util.PrintfRed("Put Request: Applied Command Value: %s and Client Command Value: %s mismatch", command.Val, arg.Value)
		return errors.New("Put request: applied command value and client command value mismatch")
	}
	ptrace.RecordAction(PutCommitted{
		ClientId: arg.ClientId,
		OpId:     arg.OpId,
		Key:      command.Key,
		Value:    command.Val,
	})

	resp.ClientId = arg.ClientId
	resp.OpId = arg.OpId
	resp.Key = command.Key
	resp.Value = command.Val
	resp.Token = ptrace.GenerateToken()
	return nil
}

func (s *Server) runRaft() {
	serverErrorChan := make(chan error)
	go s.raftFollower(serverErrorChan)
	// go s.raftCandidate(serverErrorChan)
	go s.raftLeader(serverErrorChan)

	// testing leaderSelection by mocking
	// incosistent logs amongnst nodes
	// mockLogs(s)

	for {
		err := <-serverErrorChan
		util.PrintlnRed("error running raft protocal %v", err)
	}
}

func (s *Server) raftFollower(errorChan chan<- error) {
	for {
		canRunFollower := <-s.runFollower
		s.trace.RecordAction(BecameFollower{s.ServerId})

		// init volatile state
		s.commitIndex = 0
		s.lastApplied = 0
		if canRunFollower {
			s.raftFollowerLoop(errorChan)
		}
	}

}

func (s *Server) raftFollowerLoop(errorChan chan<- error) {
	/*
		- Respond to RPCs from candidates and leaders
		- If election timeout elapses without receiving AppendEntries
		RPC from current leader or granting vote to candidate:
		convert to candidate (ELECTION)
	*/

	// var err error
	s.appendEntriesCanRespond <- true
	for {
		select {
		case canRunFollower := <-s.runFollower:
			if !canRunFollower {
				return
			}
		case <-s.commitIndexUpdated:
			s.doCommit(errorChan)
		case <-s.appendEntriesDone:
			s.appendEntriesCanRespond <- true
		}

	}
}

func (s *Server) raftLeader(errorChan chan<- error) {
	for {
		canRunLeader := <-s.runLeader
		s.trace.RecordAction(BecameLeader{s.ServerId})

		// init volatile state
		s.commitIndex = 0
		s.lastApplied = 0
		s.initLeaderVolatileState()

		if canRunLeader {
			s.raftLeaderLoop(errorChan)
		}
	}
}

func (s *Server) raftLeaderLoop(errorChan chan<- error) {
	/*
		- If command received from client: append entry to local log,
		respond after entry applied to state machine (§5.3)
		- If last log index ≥ nextIndex for a follower: send
		AppendEntries RPC with log entries starting at nextIndex
			- If successful: update nextIndex and matchIndex for
			follower (§5.3)
			- If AppendEntries fails because of log inconsistency:
			decrement nextIndex and retry (§5.3)
		- If there exists an N such that N > commitIndex, a majority
		of matchIndex[i] ≥ N, and log[N].term == currentTerm:
		set commitIndex = N (§5.3, §5.4)
	*/
	stopHeartbeats := make(chan bool)
	go s.sendHeartbeats(stopHeartbeats)
	// var err error
	for {
		select {
		case canRunLeader := <-s.runLeader:
			if !canRunLeader {
				stopHeartbeats <- true
				return
			}
		case <-s.commitIndexUpdated:
			s.doCommit(errorChan)
		case newTerm := <-s.rpcGotTermGreaterThanCurrentTerm:
			s.currentTerm = newTerm
			s.runFollower <- true
			s.runLeader <- false
			return
		case clientCommand := <-s.commandFromClient:
			/* need to run in go rountine, or <-s.commitIndexUpdated will block indefinitely...*/
			go s.leaderHandleCommand(clientCommand, errorChan)
		// case <-s.triggerAllFollowerLogUpdate:  // TODO fully delete this case
		// 	s.forceUpdateAllFollowerLog()
		case <-s.existsInterestingN:
			// TODO
		}
	}

}

func (s *Server) initLeaderVolatileState() {
	s.nextIndex = make([]uint64, len(s.Peers)+1)
	s.matchIndex = make([]uint64, len(s.Peers)+1)
	for i := 0; i < len(s.Peers); i++ {
		s.nextIndex[i] = uint64(len(s.log))
		s.matchIndex[i] = 0
	}
}

func (s *Server) sendHeartbeats(stopHeartbeats chan bool) {
	ticker := time.NewTicker(time.Duration(s.HeartbeatTimeout) * time.Second)

	go func() {
		for {
			select {
			case <-ticker.C:
				prevLogEntry := s.log[len(s.log)-1]
				appendEntryArg := AppendEntriesArg{
					Term:         s.currentTerm,
					LeaderId:     s.ServerId,
					PrevLogIndex: prevLogEntry.Index,
					PrevLogTerm:  prevLogEntry.Term,
					Entries:      nil,
					LeaderCommit: s.commitIndex}
				for _, peer := range s.Peers {
					if peer.ServerId == s.ServerId {
						continue
					}
					peerReplies := make(chan bool, len(s.Peers)-1)
					go s.sendAppendEntry(peer, &appendEntryArg, peerReplies, s.tracer.CreateTrace())
				}

			case <-stopHeartbeats:
				fmt.Println("(Leader heartbeats) Received stop heart beats; stopping...")
				return
			}
		}
	}()
}

func (s *Server) doCommit(errorChan chan<- error) {
	s.kvMu.Lock()
	defer s.kvMu.Unlock()
	for s.commitIndex > s.lastApplied {
		s.lastApplied++
		_, err := s.applyEntry(s.log[s.lastApplied])
		if err != nil {
			// question: should we do a s.lastApplied-- here?
			errorChan <- err
			fmt.Printf("error %v\n", err)
		}
	}
}

func (s *Server) leaderHandleCommand(clientCommand ClientCommand, errorChan chan<- error) {
	/*  - Send AppendEntries in parallel to all peers.
	- Once majority acks, return clientCommand.done <- clientCommand.command */
	newEntry := Entry{
		Term:    s.currentTerm,
		Command: clientCommand.command,
		Index:   uint64(len(s.log)),
	}
	prevLogEntry := s.log[len(s.log)-1]
	appendEntryArg := AppendEntriesArg{s.currentTerm, s.ServerId, prevLogEntry.Index, prevLogEntry.Term, []Entry{newEntry}, s.commitIndex, tracing.TracingToken{}}

	/* Append to my log*/
	s.log = append(s.log, newEntry)

	currPeers := len(s.Peers) - 1
	peerReplies := make(chan bool, currPeers) // buffer channel
	majorityReplied := make(chan bool)

	/* listen for async AppendEntry responses*/
	go s.countReplies(currPeers, peerReplies, majorityReplied)

	fmt.Printf("(Leader AppendEntries): Sending AppendEntries=%v to followers.\n", appendEntryArg)
	for _, peer := range s.Peers {
		if peer.ServerId == s.ServerId {
			continue
		}
		go s.sendAppendEntry(peer, &appendEntryArg, peerReplies, s.tracer.CreateTrace())
	}
	/* Can return once we have a majority*/
	<-majorityReplied

	/* Mark entry as committed, and applyEntry to update kv state*/
	s.commitIndex += 1
	command, err := s.applyEntry(newEntry)
	if err != nil {
		errorChan <- err
		util.PrintfRed("error %v\n", err)
	}
	s.lastApplied++
	s.commitIndexUpdated <- true
	fmt.Printf("(Leader AppendEntries): Successfully replicated entry=%v on majority, commitIndex updated to be %d\n", newEntry, s.commitIndex)
	clientCommand.done <- command
}

func (s *Server) countReplies(currPeers int, peerReplies chan bool, majorityReplied chan bool) {
	majority := s.Majority - 1 /* subtract itself*/
	count := 0

	for count < currPeers {
		replySuccess := <-peerReplies // wait for one task to complete
		if !replySuccess {            // FIXME can probably remove, will never go into this case
			continue
		}
		count++
		if uint8(count) >= majority {
			fmt.Println("(Leader RECEIVE REPLIES): Majority reached")
			majorityReplied <- true
		}
	}

	fmt.Println("(Leader RECEIVE REPLIES): All followers acked.")
}

/* If followers crash or run slowly, leader retries indefinitely, even if it has reponded to client, until all followers store log entry*/
func (s *Server) sendAppendEntry(peer ServerInfo, args *AppendEntriesArg, peerReplies chan bool, trace *tracing.Trace) error {
	trace.RecordAction(AppendEntriesRequestSent(*args))
	args.Token = trace.GenerateToken()

	// If a connection cannot be established or the rpc call fails due to network issues, just return an error
	// Once the follower becomes reachable again, the new appendEntry calls and if that fails, the forceUpdateFollowerLog calls
	// should force the follower's log to be consistent (TODO verify this)
	peerConn, err := rpc.Dial("tcp", peer.ServerListenAddr)
	if err != nil {
		fmt.Printf("Can't connect to peer id=%d, address=%s, error=%v\n", peer.ServerId, peer.ServerListenAddr, err)
		return err
	}
	defer peerConn.Close()

	var reply AppendEntriesReply
	done := make(chan *rpc.Call, 1)
	for {
		peerConn.Go("Server.AppendEntries", args, &reply, done)
		select {
		/* leader retries indefinitely if can't reach follower*/
		case <-time.After(20 * time.Second):
			continue
		case call := <-done:
			if call.Error == nil {
				trace = s.tracer.ReceiveToken(reply.Token)
				trace.RecordAction(AppendEntriesResponseRecvd(reply))
				res := call.Reply.(*AppendEntriesReply)
				// fmt.Printf("(Leader AppendEntries): Received AppendEntries result=%v from serverId=%d\n", res, peer.ServerId)
				if !res.Success {
					if res.Term > args.Term {
						// change server to follower
						s.runLeader <- false
						s.runFollower <- true
						fmt.Printf("Server %v downgraded to follower\n", s.ServerId)
						return nil
					}

					// Follower log is inconsistent with leader, neet to force and wait for follower to update log
					// After follower log has updated, the current entry will be also be added to the follower log.
					// This is because the current entry has already been added to the leader's log and
					// followers' AppendEntries will eventually accept everything on leader's log
					forceUpdateDone := make(chan bool, 1)
					s.forceUpdateFollowerLog(peer.ServerId, forceUpdateDone, trace.Tracer.ReceiveToken(res.Token))
					<-forceUpdateDone

					peerReplies <- true
					return nil
				} else {
					if len(args.Entries) > 0 {
						lastArgEntrySentIndex := args.Entries[len(args.Entries)-1].Index
						if s.matchIndex[peer.ServerId] < lastArgEntrySentIndex {
							s.matchIndex[peer.ServerId] = lastArgEntrySentIndex
						}
						if s.nextIndex[peer.ServerId] < lastArgEntrySentIndex+1 {
							s.nextIndex[peer.ServerId] = lastArgEntrySentIndex + 1
						}
					}
					peerReplies <- true
					return nil
				}
			} else {
				fmt.Printf("(Leader AppendEntries): Received AppendEntries Error=%v from serverId=%d\n", call.Error, peer.ServerId)
				return call.Error // what to do when AppendEntry returns error?
			}
		}
	}
}

// A synchronous appendEntry that does not retry, used only for forceUpdateFollowerLog
func (s *Server) sendAppendEntrySync(peer ServerInfo, arg *AppendEntriesArg, trace *tracing.Trace) (success bool, err error) {
	trace.RecordAction(AppendEntriesRequestSent(*arg))
	arg.Token = trace.GenerateToken()

	peerConn, err := rpc.Dial("tcp", peer.ServerListenAddr)
	if err != nil {
		fmt.Printf("Can't connect to peer id=%d, address=%s, error=%v\n", peer.ServerId, peer.ServerListenAddr, err)
		return false, err
	}
	defer peerConn.Close()

	var reply AppendEntriesReply
	err = peerConn.Call("Server.AppendEntries", arg, &reply)
	if err != nil {
		fmt.Printf("(Leader AppendEntries): Received AppendEntries Error=%v from serverId=%d\n", err, peer.ServerId)
	} else {
		fmt.Printf("(Leader AppendEntries): Received AppendEntries result=%v from serverId=%d\n", reply, peer.ServerId)
	}
	trace = s.tracer.ReceiveToken(reply.Token)
	trace.RecordAction(AppendEntriesResponseRecvd(reply))
	return reply.Success, err
}

// Force all known peers to adopt consistent logs
// func (s *Server) forceUpdateAllFollowerLog() {
// 	forceUpdateWg := sync.WaitGroup{}
// 	for _, peer := range s.Peers {
// 		peerId := peer.ServerId
// 		if peerId == s.ServerId {
// 			continue
// 		}
// 		if s.matchIndex[peerId] >= s.nextIndex[peerId] {
// 			go s.forceUpdateFollowerLog(peerId, forceUpdateWg)
// 		}
// 	}
// 	forceUpdateWg.Wait()
// }

// Force update the log of a single follower, uses a waitgroup for notifying success
// Succeeds or retries indefinitely
func (s *Server) forceUpdateFollowerLog(peerIndex uint8, done chan<- bool, trace *tracing.Trace) {
	trace.RecordAction(ForceFollowerLog{
		serverIndex: peerIndex,
		nextIndex:   s.nextIndex[peerIndex],
		matchIndex:  s.matchIndex[peerIndex],
	})

	lastLogEntry := s.log[s.nextIndex[peerIndex]-1]
	prevLogEntry := s.log[s.nextIndex[peerIndex]-1]
	peer := s.Peers[peerIndex]
	for {
		// retry until success or failure
		appendEntryArg := AppendEntriesArg{s.currentTerm, s.ServerId, prevLogEntry.Index, prevLogEntry.Term, s.log[s.nextIndex[peerIndex]:], s.commitIndex, tracing.TracingToken{}}
		success, err := s.sendAppendEntrySync(peer, &appendEntryArg, trace)
		if err != nil {
			<-time.After(10 * time.Second)
			continue
		}
		if success {
			s.nextIndex[peerIndex] = lastLogEntry.Index + 1
			s.matchIndex[peerIndex] = lastLogEntry.Index
			break
		} else {
			s.nextIndex[peerIndex] -= 1
			prevLogEntry = s.log[s.nextIndex[peerIndex]-1]
		}
	}
	done <- true
}

func (s *Server) applyEntry(entry Entry) (Command, error) {

	switch entry.Command.Kind {
	case Put:
		// TODO lock this and return result of put
		s.kv[entry.Command.Key] = entry.Command.Val
		newVal := s.kv[entry.Command.Key]
		fmt.Printf("(KV STATE): %v\n", s.kv)
		fmt.Printf("(KV STATE): Applied Entry, Key: %s, Val: %s\n", entry.Command.Key, newVal)
		return Command{Kind: Get, Key: entry.Command.Key, Val: newVal}, nil
	case Get:
		// TODO return result of get
		command := Command{Kind: Get, Key: entry.Command.Key}
		command.Val = s.kv[entry.Command.Key]
		return command, nil
	default:
		return Command{}, fmt.Errorf("unable to apply entry %v", entry)
	}
}

func (s *Server) AppendEntries(arg AppendEntriesArg, reply *AppendEntriesReply) error {
	// FIXME maybe instead of doing this, we can just look at s.isLeader
	<-s.appendEntriesCanRespond

	trace := s.trace.Tracer.ReceiveToken(arg.Token)
	trace.RecordAction(AppendEntriesRequestRecvd(arg))

	s.logMu.Lock()
	defer s.logMu.Unlock()

	if arg.Entries != nil {
		fmt.Printf("(Follower AppendEntries) Received AppendEntry=%v\n", arg)
	} else {
		util.PrintfGreen("(Follower AppendEntries) Received Heartbeat=%v\n", arg)
	}

	reply.Term = s.currentTerm

	// Ensure that the arg entries are in ASC order according to index
	sort.Slice(arg.Entries, func(i, j int) bool {
		return arg.Entries[i].Index < arg.Entries[j].Index
	})
	// Ensure that the arg entries are consecutive
	for i := 1; i < len(arg.Entries); i++ {
		if arg.Entries[i-1].Index+1 != arg.Entries[i].Index {
			reply.Success = false
			trace.RecordAction(AppendEntriesResponseSent(*reply))
			reply.Token = trace.GenerateToken()
			return fmt.Errorf("arg entries should be consecutive, %v", arg.Entries)
		}
	}

	// 1. Reply false if term < currentTerm (§5.1)
	if arg.Term < s.currentTerm {
		reply.Success = false
		trace.RecordAction(AppendEntriesResponseSent(*reply))
		reply.Token = trace.GenerateToken()
		return nil
	}
	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if int(arg.PrevLogIndex) >= len(s.log) {
		// FIXME might not be correct handling of this case
		reply.Success = false
		trace.RecordAction(AppendEntriesResponseSent(*reply))
		reply.Token = trace.GenerateToken()
		fmt.Printf("(Follower AppendEntries) error: saw an out of bounds prevLogIndex %v", arg.PrevLogIndex)
		return nil
	}

	if s.log[arg.PrevLogIndex].Term != arg.PrevLogTerm {
		reply.Success = false
		trace.RecordAction(AppendEntriesResponseSent(*reply))
		reply.Token = trace.GenerateToken()
		trace.RecordAction(AppendEntriesResponseSent(*reply))
		return nil
	}

	// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
	deleteStartIndex := len(s.log)
	appendStartIndex := len(arg.Entries)
	for i := 0; i < len(arg.Entries); i++ {
		currEntry := arg.Entries[i]
		if int(currEntry.Index) >= len(s.log) {
			if i < appendStartIndex {
				appendStartIndex = i
			}
			break
		} else if currEntry.Term != s.log[currEntry.Index].Term ||
			currEntry.Command != s.log[currEntry.Index].Command {
			deleteStartIndex = int(currEntry.Index)
			appendStartIndex = i
			break
		}
	}
	s.log = s.log[:deleteStartIndex]

	// 4. Append any new entries not already in the log
	s.log = append(s.log, arg.Entries[appendStartIndex:]...)

	if arg.Entries != nil {
		PrintLog(s.log)
	}
	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if arg.LeaderCommit > s.commitIndex {
		if arg.LeaderCommit < s.log[len(s.log)-1].Index {
			s.commitIndex = arg.LeaderCommit
		} else {
			s.commitIndex = s.log[len(s.log)-1].Index
		}
		s.commitIndexUpdated <- true
	}
	reply.Success = true
	trace.RecordAction(AppendEntriesResponseSent(*reply))
	reply.Token = trace.GenerateToken()
	s.appendEntriesDone <- true
	return nil
}

func (e Command) String() string {
	switch e.Kind {
	case Get:
		return fmt.Sprintf("Get(%s)", e.Key)
	case Put:
		return fmt.Sprintf("Put(%s,%s)", e.Key, e.Val)
	default:
		return fmt.Sprintf("Unrecognized command")
	}
}

func PrintLog(log []Entry) {
	for i, entry := range log {
		s := fmt.Sprintf("%d. Term=%d, Index=%d, Entry=%s\n", i, entry.Term, entry.Index, entry.Command)
		util.PrintfBlue(s)
	}
}

func mockLogs(s *Server) {
	if s.ServerId == 1 || s.ServerId == 3 || s.ServerId == 5 {
		for i := 1; i < 10; i++ {
			term := 1
			if i > 5 {
				term = 2
			}
			entry := Entry{
				Term:    uint32(term),
				Command: Command{},
				Index:   uint64(i),
			}
			s.log = append(s.log, entry)
		}
	}
	if s.ServerId == 2 || s.ServerId == 4 {
		for i := 1; i < 13; i++ {
			term := 1
			if i > 5 {
				term = 2
			}
			if i > 10 && s.ServerId == 4 {
				term = 5
			}
			entry := Entry{
				Term:    uint32(term),
				Command: Command{},
				Index:   uint64(i),
			}
			s.log = append(s.log, entry)
		}
	}
}

func simulateNetworkPartition(s *Server) {
	if s.ServerId == 1 || s.ServerId == 5 {
		select {
		case <-time.After(time.Second * 30):
			fmt.Printf("\nbeginning to simulate network partition. Won't respond to heartbeats for a while\n")
			fchecker.SimulateNetworkPartition = true
		}
	}
}
