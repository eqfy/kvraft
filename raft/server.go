package raft

import (
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"sort"
	"sync"

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

type PutFwd struct {
	ClientId string
	OpId     uint32
	GId      uint64
	Key      string
	Value    string
}

type InternalPutResponse struct {
	Token tracing.TracingToken
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

type PutRequest struct {
	ClientId              string
	OpId                  uint32
	Key                   string
	Value                 string
	LocalTailServerIPPort string
	Token                 tracing.TracingToken
}

type PutResponse struct {
	ClientId string
	OpId     uint32
	GId      uint64
	Key      string
	Value    string
	Token    tracing.TracingToken
}

type GetRecvd struct {
	ClientId string
	OpId     uint32
	Key      string
}

type GetResult struct {
	ClientId string
	OpId     uint32
	GId      uint64
	Key      string
	Value    string
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
	ServerId   uint8
	Peers      []ServerInfo
	Config     Addr
	trace      *tracing.Trace
	tracer     *tracing.Tracer
	fcheckAddr string

	isLeader bool

	// Raft Paper
	// Persistent state
	currentTerm uint32 // Might actually be unused
	votedFor    uint8
	log         []Entry
	kv          map[string]string
	// Volatile state
	commitIndex uint64
	lastApplied uint64
	// Volatile state on leader
	nextIndex  []uint64
	matchIndex []uint64

	// all servers
	commitIndexUpdated               chan uint64
	lastAppliedUpdated               chan uint64
	rpcGotTermGreaterThanCurrentTerm chan uint32

	// follower
	appendEntriesCanRespond chan bool
	appendEntriesDone       chan bool
	runFollower             chan bool

	// candidate
	runCandidate chan bool

	// leader
	commandFromClient                    chan ClientCommand
	followerLogIndexGreaterThanNextIndex chan bool // probably follower info
	existsInterestingN                   chan bool
	runLeader                            chan bool

	// Lock
	L sync.RWMutex
}

// AppendEntries RPC
type AppendEntriesArg struct {
	term         uint32
	leaderId     uint8
	prevLogIndex uint64
	prevLogTerm  uint32
	entries      []Entry
	leaderCommit uint64
}

type AppendEntriesReply struct {
	term    uint32
	success bool
}

// RequestVote RPC (Unused)
type RequestVoteArg struct {
	term         uint32
	candidateId  uint8
	lastLogIndex uint64
	lastLogTerm  uint64
}

type RequestVoteReply struct {
	term        uint32
	voteGranted bool
	lastLogTerm uint32
}

// Log entry definition
type Entry struct {
	term    uint32
	command Command
	index   uint64
}

type ClientCommand struct {
	command Command
	done    chan bool
}

type Command struct {
	kind CommandKind
	key  string
	val  string
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
	GId      uint64
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

	joinOnStartup(s)
	<-joinComplete

	/* Join complete, ready to take client requests*/
	go s.runRaft()
	listenForClient(s)

	return nil
}

/* initalize server state*/
func (s *Server) initServerState(serverId uint8, coordAddr string, serverAddr string, serverServerAddr string, serverListenAddr string, clientListenAddr string, strace *tracing.Tracer) {
	s.ServerId = serverId
	s.Config = Addr{CoordAddr: coordAddr, ServerAddr: serverAddr, ServerListenAddr: serverListenAddr, ClientListenAddr: clientListenAddr}
	s.trace = strace.CreateTrace()
	s.tracer = strace

	// raft persistent state
	s.kv = make(map[string]string)

	// join sync
	joinComplete = make(chan bool)

	// all server sync
	s.commitIndexUpdated = make(chan uint64)
	s.lastAppliedUpdated = make(chan uint64)
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

// NotifyServerFailure TEMPLATE FOR SERVER FAILURE PROTOCOL BETWEEN COORD-SERVER
// Added this to check if rpc calls are behaving as intended
func (s *Server) NotifyServerFailure(notification NotifyServerFailure, reply *NotifyServerFailureAck) error {
	fmt.Println("Received server failure notification for server with id: ", notification.FailedServerId)
	// // trace action
	// cTrace := s.tracer.ReceiveToken(notification.Token)
	// for _, servId := range notification.FailedServerId {
	// 	cTrace.RecordAction(ServerFailRecvd{FailedServerId: servId})
	// }
	return errors.New("invalid notification msg")
}

// TODO
func (s *Server) Get(arg GetRequest, resp GetResponse) {
	gtrace := s.tracer.ReceiveToken(arg.Token)
	gtrace.RecordAction(GetRecvd{
		ClientId: arg.ClientId,
		OpId:     arg.OpId,
		Key:      arg.Key,
	})

	clientCommand := ClientCommand{
		command: Command{
			kind: Get,
			key:  arg.Key,
		},
		done: make(chan bool, 1),
	}
	s.commandFromClient <- clientCommand
	<-clientCommand.done

	resp.ClientId = arg.ClientId
	resp.OpId = arg.OpId
	resp.Key = arg.Key
	resp.Value = s.kv[arg.Key]
	resp.Token = gtrace.GenerateToken()

}

// TODO
func (s *Server) Put(arg PutRequest, resp PutResponse) {
	ptrace := s.tracer.ReceiveToken(arg.Token)
	ptrace.RecordAction(PutRecvd{
		ClientId: arg.ClientId,
		OpId:     arg.OpId,
		Key:      arg.Key,
		Value:    arg.Value,
	})

	clientCommand := ClientCommand{
		command: Command{
			kind: Put,
			key:  arg.Key,
			val:  arg.Value,
		},
		done: make(chan bool, 1),
	}
	s.commandFromClient <- clientCommand
	<-clientCommand.done

	s.L.Lock()
	s.kv[arg.Key] = arg.Value
	s.L.Unlock()

	resp.ClientId = arg.ClientId
	resp.OpId = arg.OpId
	resp.Key = arg.Key
	resp.Value = arg.Value
	resp.Token = ptrace.GenerateToken()
}

func (s *Server) runRaft() {
	serverErrorChan := make(chan error)
	go s.raftFollower(serverErrorChan)
	// go s.raftCandidate(serverErrorChan)
	go s.raftLeader(serverErrorChan)

	for {
		err := <-serverErrorChan
		util.PrintlnRed("error running raft protocal %v", err)
	}
}

func (s *Server) raftFollower(errorChan chan<- error) {
	/*
		- Respond to RPCs from candidates and leaders
		- If election timeout elapses without receiving AppendEntries
		RPC from current leader or granting vote to candidate:
		convert to candidate (ELECTION)
	*/

	for {
		canRunFollower := <-s.runFollower
		if canRunFollower {
			s.raftFollowerLoop(errorChan)
		}
	}

}

func (s *Server) raftFollowerLoop(errorChan chan<- error) {
	var err error

	s.appendEntriesCanRespond <- true
	for {
		select {
		case canRunFollower := <-s.runFollower:
			if !canRunFollower {
				return
			}
		case commitIndex := <-s.commitIndexUpdated:
			if commitIndex > s.lastApplied {
				err = s.applyEntry(s.log[s.lastApplied])
				if err != nil {
					errorChan <- err
				}
			}
		case lastApplied := <-s.lastAppliedUpdated:
			if s.commitIndex > lastApplied {
				err = s.applyEntry(s.log[s.lastApplied])
				if err != nil {
					errorChan <- err
				}
			}
		case <-s.appendEntriesDone:
			s.appendEntriesCanRespond <- true
		}

	}
}

func (s *Server) raftCandidate(errorChan chan<- error) {}

func (s *Server) raftCandidateLoop(errorChan chan<- error) {
	/*
		(ELECTION)
		- On conversion to candidate, start election:
		- Increment currentTerm
		   - Vote for self
		   - Reset election timer
		   - Send RequestVote RPCs to all other servers
		- If votes received from majority of servers: become leader
		- If AppendEntries RPC received from new leader: convert to follower
		- If election timeout elapses: start new election
	*/
}

func (s *Server) raftLeader(errorChan chan<- error) {
	for {
		canRunLeader := <-s.runLeader
		if canRunLeader {
			s.raftLeaderLoop(errorChan)
		}
	}
}

func (s *Server) raftLeaderLoop(errorChan chan<- error) {
	/*
		- Upon election: send initial empty AppendEntries RPCs
		(heartbeat) to each server; repeat during idle periods to
		prevent election timeouts (§5.2) (ELECTION)
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

	var err error
	for {
		select {
		case canRunLeader := <-s.runLeader:
			if !canRunLeader {
				return
			}
		case commitIndex := <-s.commitIndexUpdated:
			if commitIndex > s.lastApplied {
				err = s.applyEntry(s.log[s.lastApplied])
				if err != nil {
					errorChan <- err
				}
			}
		case lastApplied := <-s.lastAppliedUpdated:
			if s.commitIndex > lastApplied {
				err = s.applyEntry(s.log[s.lastApplied])
				if err != nil {
					errorChan <- err
				}
			}
		case newTerm := <-s.rpcGotTermGreaterThanCurrentTerm:
			s.currentTerm = newTerm
			s.runFollower <- true
			s.runLeader <- false
			return
		case clientCommand := <-s.commandFromClient:
			s.log = append(s.log, Entry{
				term:    s.currentTerm,
				command: clientCommand.command,
				index:   uint64(len(s.log)),
			})
			// TODO send appendEntries to all followers
			// Commit the entry once most followers responded?
			clientCommand.done <- true
		case <-s.followerLogIndexGreaterThanNextIndex:
			// TODO
		case <-s.existsInterestingN:
			// TODO
		}
	}

}

func (s *Server) applyEntry(entry Entry) error {
	switch entry.command.kind {
	case Put:
		// TODO lock this and return result of put
		s.L.lock()
		s.kv[entry.command.key] = entry.command.val
	case Get:
		// TODO return result of get
	default:
		return fmt.Errorf("unable to apply entry %v", entry)
	}
	return nil
}

func (s *Server) AppendEntries(arg AppendEntriesArg, reply *AppendEntriesReply) error {
	// FIXME maybe instead of doing this, we can just look at s.isLeader
	<-s.appendEntriesCanRespond

	// Ensure that the arg entries are in ASC order according to index
	sort.Slice(arg.entries, func(i, j int) bool {
		return arg.entries[i].index < arg.entries[j].index
	})

	// Ensure that the arg entries are consecutive
	for i := 1; i < len(arg.entries); i++ {
		if arg.entries[i-1].index+1 != arg.entries[i].index {
			reply.success = false
			return fmt.Errorf("arg entries should be consecutive, %v", arg.entries)
		}
	}

	// 1. Reply false if term < currentTerm (§5.1)
	if arg.term < s.currentTerm {
		reply.success = false
		return nil
	}

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if int(arg.prevLogIndex) >= len(s.log) {
		// FIXME might not be correct handling of this case
		reply.success = false
		return fmt.Errorf("saw an out of bounds prevLogIndex %v", arg.prevLogIndex)
	}
	if s.log[arg.prevLogIndex].term != arg.prevLogTerm {
		reply.success = false
		return nil
	}

	// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
	deleteStartIndex := len(s.log)
	appendStartIndex := len(arg.entries)
	for i := 0; i < len(arg.entries); i++ {
		currEntry := arg.entries[i]
		if int(currEntry.index) >= len(s.log) {
			if i < appendStartIndex {
				appendStartIndex = i
			}
			break
		} else if currEntry.term != s.log[currEntry.index].term ||
			currEntry.command != s.log[currEntry.index].command {
			deleteStartIndex = int(currEntry.index)
			appendStartIndex = i
			break
		}
	}
	s.log = s.log[:deleteStartIndex]

	// 4. Append any new entries not already in the log
	s.log = append(s.log, arg.entries[appendStartIndex:]...)

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if arg.leaderCommit > s.commitIndex {
		if arg.leaderCommit < s.log[len(s.log)-1].index {
			s.commitIndex = arg.leaderCommit
		} else {
			s.commitIndex = s.log[len(s.log)-1].index
		}
	}
	reply.success = true
	reply.term = arg.term
	s.appendEntriesDone <- true
	return nil
}
