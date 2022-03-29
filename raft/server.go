package raft

import (
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"os"

	fchecker "cs.ubc.ca/cpsc416/kvraft/fcheck"
	"cs.ubc.ca/cpsc416/kvraft/util"
	"github.com/DistributedClocks/tracing"
	// "cs.ubc.ca/cpsc416/a3/kvslib"
	// "time"
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
	/* initalize global server state*/
	s.ServerId = serverId
	s.Config = Addr{CoordAddr: coordAddr, ServerAddr: serverAddr, ServerListenAddr: serverListenAddr, ClientListenAddr: clientListenAddr}
	s.trace = strace.CreateTrace()
	s.tracer = strace
	joinComplete = make(chan bool)
	s.trace.RecordAction(ServerStart{serverId})

	go startFcheck(serverAddr, int(serverId), s)
	go listenForCoord(s, serverAddr)

	joinOnStartup(s)
	<-joinComplete

	/* Join complete, ready to take client requests*/
	listenForClient(s)

	return nil
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
	} else {
		fmt.Println("I'm a follower.")
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

func (s *Server) AppendEntries(arg AppendEntriesArg, reply AppendEntriesReply) error {
	if arg.term < s.currentTerm {
		reply.success = false
		return nil
	}
	if s.log[arg.prevLogIndex].term != arg.prevLogTerm {
		reply.success = false
		return nil
	}

	// for i := len(s.log); i >= 0; i-- {
	// 	for j := len(arg.entries); j >= 0; j-- {
	// 		if s.log[i].index == arg.entries[j].index {
	// 			if s.log[i].term != arg.entries[j].term {

	// 			}
	// 		}
	// 	}
	// }
	return nil
}
