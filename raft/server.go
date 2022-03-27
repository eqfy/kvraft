package raft

import (
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"sync"

	fchecker "cs.ubc.ca/cpsc416/kvraft/fcheck"
	"cs.ubc.ca/cpsc416/kvraft/util"
	"github.com/DistributedClocks/tracing"

	// "cs.ubc.ca/cpsc416/a3/kvslib"
	"os"
	// "time"
	"reflect"
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

type InternalPutFwd struct {
	ClientId              string
	OpId                  uint32
	GId                   uint64
	Key                   string
	Value                 string
	Token                 tracing.TracingToken
	LocalTailServerIPPort string
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

type Server struct {
	// Server state may go here
	ServerId uint8
	PrevServ string
	NextServ string

	mapMutex sync.Mutex // protects following
	Map      map[string]string

	Config           Addr
	trace            *tracing.Trace
	tracer           *tracing.Tracer
	gIdMutex         sync.Mutex // protects following
	LocalGId         uint64
	HeadGId_K        uint64 // amount to increment GId at head by (for put requests)
	TailFailureGId_K uint64 //amount to increment new tail's gid by for tail failures
	fcheckAddr       string
	putsMu           sync.Mutex       // protects following
	cachedPuts       []InternalPutFwd // cache N internal put forwards for failure recovery. Map {client_id}:{opId} to InternalPutFwd
	cachedPutsSize   uint64
}

type Addr struct {
	CoordAddr        string
	ServerAddr       string
	ServerListenAddr string
	ClientListenAddr string
}

func NewServer() *Server {
	return &Server{}
}

type InternalJoinRequest struct {
	ServerId         uint8
	ServerListenAddr string
	Token            tracing.TracingToken
}

type InternalJoinResponse struct {
	Token tracing.TracingToken
}

type JoinRequest struct {
	ServerId         uint8
	ServerAddr       string
	ServerListenAddr string
}

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

type BackwardsPropRequest struct {
	GId uint64
}
type TokenRequest struct {
	Token tracing.TracingToken
}

type SendServStateResponse struct {
	diff map[string]string
	GId  uint64
}

var joinComplete chan bool

func (s *Server) Register(req InternalJoinRequest, res *InternalJoinResponse) (err error) {
	strace := s.tracer.ReceiveToken(req.Token)
	strace.RecordAction(NextServerJoining{req.ServerId})

	s.NextServ = req.ServerListenAddr
	// fmt.Printf("New node joined as tail, setting my next to be %s\n", s.NextServ)
	strace.RecordAction(NewJoinedSuccessor{req.ServerId})
	res.Token = strace.GenerateToken()
	return
}

func (s *Server) JoinDoneReturnTokenTrace(req TokenRequest, res *bool) (err error) {
	s.tracer.ReceiveToken(req.Token)
	return
}

func (s *Server) FindServerStateOnStartup(coordReply JoinResponse, reply *ServerJoinAck) error {
	if coordReply.Leader {
		fmt.Println("I'm a leader.")
	} else {
		fmt.Println("I'm a follower.")
	}
	fmt.Printf("Peers: %v\n TermNumber: %v\n", coordReply.Peers, coordReply.Term)

	s.trace.RecordAction(ServerJoined{s.ServerId})
	reply.ServerId = s.ServerId
	reply.Token = s.trace.GenerateToken()
	joinComplete <- true
	return nil
}

//func (s *Server) FindTailServer(coordReply JoinResponse, reply *ServerJoinAck) error {
//	/* receive tail server from coord node*/
//
//	/* check if tail is itself...*/
//	tailServAddr := coordReply.TailServer.ServerListenAddr
//	if coordReply.TailServer.ServerId != s.ServerId {
//		/* send my own listening address to tail server*/
//		request := InternalJoinRequest{s.ServerId, s.Config.ServerListenAddr, s.trace.GenerateToken()}
//		// fmt.Printf("Sending my own listen address to tail server %s\n", tailServAddr)
//		tailServ, err := rpc.Dial("tcp", tailServAddr)
//		fmt.Println("Error when dialing tail servers: ", err.Error())
//		util.CheckErr(err, "Can't connect to tail server: ")
//
//		var res *InternalJoinResponse
//		err = tailServ.Call("Server.Register", request, &res)
//		util.CheckErr(err, "Error from tail server connection: ")
//
//		s.tracer.ReceiveToken(res.Token)
//		/* Tail server acks, so set prev server to Tail*/
//		s.PrevServ = tailServAddr
//		fmt.Printf("I'm now the tail, and previous server is %s\n", s.PrevServ)
//		tailServ.Close()
//	}
//
//	s.trace.RecordAction(ServerJoined{s.ServerId})
//	reply.ServerId = s.ServerId
//	reply.Token = s.trace.GenerateToken()
//	joinComplete <- true
//	// joinComplete = true
//	return nil
//}

func (s *Server) UpdateGIdBackwardsProp(req BackwardsPropRequest, res *bool) (err error) {
	/* set my gid to be requests' gid*/
	s.gIdMutex.Lock()
	s.LocalGId = req.GId
	s.gIdMutex.Unlock()

	if s.PrevServ != "" {
		fmt.Printf("(Backwards Prop GId) Received tail's GId=%d, propagating upwards\n", s.LocalGId)
		go BackwardsPropGIdCallPrevServer(s.PrevServ, req)
	} else {
		fmt.Printf("(Backwards Prop GId) Head Server: Propagation complete, received tail's GId=%d\n", s.LocalGId)
	}
	return
}

/* Just ignore errors in backwards prop, because not essential service...*/
func BackwardsPropGIdCallPrevServer(prevServ string, req BackwardsPropRequest) {
	conn, err := rpc.Dial("tcp", prevServ)
	if err != nil {
		fmt.Printf("(Backwards Prop GId) Can't connect to previous server. Terminating propagation. " + err.Error())
		return
	}
	res := false
	fmt.Printf("Calling previous server %s and sending request %v\n", prevServ, req)
	err = conn.Call("Server.UpdateGIdBackwardsProp", req, &res)
	if err != nil {
		fmt.Printf("(Backwards Prop GId) Error from sending previous server backwards prop request. Terminating propagation. " + err.Error())
		return
	}
	conn.Close()
}

func (s *Server) Get(req GetRequest, res *GetResponse) (err error) {
	/*generate gid*/
	s.gIdMutex.Lock()
	gid_tail := s.LocalGId + 1
	s.LocalGId++
	s.gIdMutex.Unlock()

	getTrace := s.tracer.ReceiveToken(req.Token)
	getTrace.RecordAction(GetRecvd{req.ClientId, req.OpId, req.Key})

	var retVal string
	s.mapMutex.Lock()
	if _, ok := s.Map[req.Key]; ok {
		retVal = s.Map[req.Key]
	} else {
		fmt.Printf("(Get Request) key=%s was not found\n", req.Key)
		retVal = ""
	}
	s.mapMutex.Unlock()

	getTrace.RecordAction(GetOrdered{req.ClientId, req.OpId, gid_tail, req.Key})
	getTrace.RecordAction(GetResult{req.ClientId, req.OpId, gid_tail, req.Key, retVal})

	/* for tail failures: propagate current GId backwards for every K requests*/
	if gid_tail%s.TailFailureGId_K == 0 {
		fmt.Printf("(Backwards Prop GId) Begin propagating tail's GId=%d upwards\n", gid_tail)
		/* dial prevServer and propagate GId backwards*/
		go BackwardsPropGIdCallPrevServer(s.PrevServ, BackwardsPropRequest{gid_tail})
	}

	*res = GetResponse{req.ClientId, req.OpId, gid_tail, req.Key, retVal, getTrace.GenerateToken()}
	return
}

func (s *Server) Put(req PutRequest, res *bool) (err error) {

	/* generate gid*/
	s.gIdMutex.Lock()
	gid_head := s.LocalGId + s.HeadGId_K
	s.LocalGId += s.HeadGId_K
	s.gIdMutex.Unlock()

	/* set key, val*/
	s.mapMutex.Lock()
	s.Map[req.Key] = req.Value
	s.mapMutex.Unlock()

	putTrace := s.tracer.ReceiveToken(req.Token)
	putTrace.RecordAction(PutRecvd{ClientId: req.ClientId, OpId: req.OpId, Key: req.Key, Value: req.Value})
	/* just use dummy gid for now...*/

	putTrace.RecordAction(PutOrdered{ClientId: req.ClientId, OpId: req.OpId, GId: gid_head, Key: req.Key, Value: req.Value})
	putFwd := PutFwd{ClientId: req.ClientId, OpId: req.OpId, GId: gid_head, Key: req.Key, Value: req.Value}

	if s.NextServ == "" {
		putTrace.RecordAction(PutResult(putFwd))

		/* Call method in tail server with response*/
		clientConn, err := rpc.Dial("tcp", req.LocalTailServerIPPort)
		if err != nil {
			return errors.New("(PUT): Can't connect back to client to send put result" + err.Error())
		}

		fmt.Printf("(HEAD) Sending respones back to %s\n", req.LocalTailServerIPPort)
		clientRes := false
		putRes := PutResponse{req.ClientId, req.OpId, gid_head, req.Key, req.Value, putTrace.GenerateToken()}
		err = clientConn.Call("KVS.ReceivePutResult", putRes, &clientRes)
		if err != nil {
			return errors.New("(PUT): Can't call RPC method ReceivePutResult on client" + err.Error())
		}
		clientConn.Close()
		return nil
	}

	/* call internal Put Asynchronously*/
	putTrace.RecordAction(putFwd)
	nextServRes := new(bool)
	nextServReq := InternalPutFwd{ClientId: req.ClientId, OpId: req.OpId, GId: gid_head, Key: req.Key, Value: req.Value, Token: putTrace.GenerateToken(),
		LocalTailServerIPPort: req.LocalTailServerIPPort}

	addToCachedPuts(s, nextServReq)

	nextServ, err := rpc.Dial("tcp", s.NextServ)
	if err != nil {
		/* If next server fails, just reply as normal, and NotifyServerFailure will handle this (re-send request)*/
		fmt.Printf("(PUT): from Head Serv - Can't connect to next server: " + err.Error() + " .Returning as normal, and waiting for Failure Notification")
		*res = true
		return
	}

	putCall := nextServ.Go("Server.InternalPut", nextServReq, nextServRes, nil)
	go closeOnDone(putCall, nextServ)
	*res = true

	return
}

func closeOnDone(putCall *rpc.Call, nextServConn *rpc.Client) {
	<-putCall.Done
	nextServConn.Close()
}

func (s *Server) InternalPut(req InternalPutFwd, res *bool) (err error) {

	/* set key, val*/
	s.gIdMutex.Lock()
	if req.GId > s.LocalGId {
		s.LocalGId = req.GId /* ensure servers in the chain have same gid*/
	}
	s.gIdMutex.Unlock()

	s.mapMutex.Lock()
	s.Map[req.Key] = req.Value
	s.mapMutex.Unlock()

	putTrace := s.tracer.ReceiveToken(req.Token)
	putTrace.RecordAction(PutFwdRecvd{ClientId: req.ClientId, OpId: req.OpId, GId: req.GId, Key: req.Key, Value: req.Value})
	putFwd := PutFwd{ClientId: req.ClientId, OpId: req.OpId, GId: req.GId, Key: req.Key, Value: req.Value}

	/* just cache the put anyways (even for tail*/
	nextServReq := InternalPutFwd{ClientId: req.ClientId, OpId: req.OpId, GId: req.GId, Key: req.Key, Value: req.Value, Token: putTrace.GenerateToken(),
		LocalTailServerIPPort: req.LocalTailServerIPPort}

	addToCachedPuts(s, nextServReq)

	if s.NextServ == "" {
		// print("ENTRANCE\n")

		gidTailOrdered := req.GId
		/* re-order if req.GId < current tail's local GId */
		s.gIdMutex.Lock()
		if req.GId < s.LocalGId {
			gidTailOrdered = s.LocalGId + 1
			s.LocalGId++
		}
		s.gIdMutex.Unlock()
		// print("SECOND\n")

		putFwd.GId = gidTailOrdered
		putTrace.RecordAction(PutResult(putFwd))

		/* Call method in tail server with response*/
		clientConn, err := rpc.Dial("tcp", req.LocalTailServerIPPort)
		if err != nil {
			print(err.Error() + "\n")
			return errors.New("(PUT): Can't connect back to client to send put result" + err.Error())
		}

		defer clientConn.Close()
		fmt.Printf("Sending respones back to %s\n", req.LocalTailServerIPPort)
		clientRes := false
		putRes := PutResponse{req.ClientId, req.OpId, gidTailOrdered, req.Key, req.Value, putTrace.GenerateToken()}
		// print("VORHEES\n")
		// print(req.OpId)
		err = clientConn.Call("KVS.ReceivePutResult", putRes, &clientRes)
		if err != nil {
			return errors.New("(PUT): Can't call RPC method ReceivePutResult on client" + err.Error())
		}
	} else {
		/* call internal Put Asynchronously*/
		putTrace.RecordAction(putFwd)
		nextServ, err := rpc.Dial("tcp", s.NextServ)
		if err != nil {
			fmt.Printf("(PUT): from Head Serv - Can't connect to next server: " + err.Error() + " .Returning as normal, and waiting for Failure Notification")
			*res = true
			return nil
		}
		nextServRes := new(bool)

		putCall := nextServ.Go("Server.InternalPut", nextServReq, nextServRes, nil)
		go closeOnDone(putCall, nextServ)
	}
	return nil
}

func addToCachedPuts(s *Server, item InternalPutFwd) {
	s.putsMu.Lock()
	if len(s.cachedPuts) > int(s.cachedPutsSize) {
		s.cachedPuts = s.cachedPuts[1:]
	}
	s.cachedPuts = append(s.cachedPuts, item)
	// fmt.Printf("(CACHED PUTS) Updated cache: %v\n", s.cachedPuts)
	s.putsMu.Unlock()
}

func joinOnStartup(s *Server) {
	/* setup connection to coord, receives addr of tail*/
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

/* listen to see if some nodes wants to join after me*/
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

// GetFcheckerAddr Coord calls this to find out the address at which fcheck is listening for heartbeats
func (s *Server) SendServState(req bool, reply *InternalPutFwd) error {
	s.putsMu.Lock()
	if len(s.cachedPuts) > 0 {
		last_element := len(s.cachedPuts) - 1
		fmt.Printf("(Server Failure Protocol) Sending my last received PUT=%v to previous server\n", s.cachedPuts[last_element])
		*reply = s.cachedPuts[last_element]
	}
	s.putsMu.Unlock()
	return nil
}

func ResendMissingPuts(otherServLastPut InternalPutFwd, s *Server, nextServ *rpc.Client) {

	/* get last item from otherServCache, and find it from my cache*/
	fmt.Printf("(Server Failure Protocol) Recieved %v as last put received from next server, calculating puts to resend\n", otherServLastPut)
	s.putsMu.Lock()
	for idx, element := range s.cachedPuts {
		thisPut := PutFwd{element.ClientId, element.OpId, element.GId, element.Key, element.Value}
		otherPut := PutFwd{otherServLastPut.ClientId, otherServLastPut.OpId, otherServLastPut.GId, otherServLastPut.Key, otherServLastPut.Value}
		if reflect.DeepEqual(thisPut, otherPut) {
			/* start sending elements down the queue from following index (i+1) to end (latest element)*/
			for i := idx + 1; i < len(s.cachedPuts); i++ {
				/* call internal Put Asynchronously*/
				nextServRes := new(bool)
				nextServReq := s.cachedPuts[i]
				fmt.Printf("***\nServer Failure Protocol\n***) re-sending missing PUT %v\n", nextServReq)
				nextServ.Call("Server.InternalPut", nextServReq, nextServRes)
			}
			break
		}
	}
	nextServ.Close()
	s.putsMu.Unlock()

}

// NotifyServerFailure TEMPLATE FOR SERVER FAILURE PROTOCOL BETWEEN COORD-SERVER
// Added this to check if rpc calls are behaving as intended
func (s *Server) NotifyServerFailure(notification NotifyServerFailure, reply *NotifyServerFailureAck) error {
	fmt.Println("Received server failure notification for server with id: ", notification.FailedServerId)

	// trace action
	cTrace := s.tracer.ReceiveToken(notification.Token)
	for _, servId := range notification.FailedServerId {
		cTrace.RecordAction(ServerFailRecvd{FailedServerId: servId})
	}

	if s.ServerId == notification.PrevServer.ServerId {
		fmt.Println("I'm the new head server...")

		s.PrevServ = ""

		// trace action
		for _, servId := range notification.FailedServerId {
			cTrace.RecordAction(ServerFailHandled{servId})
		}
		*reply = NotifyServerFailureAck{ServerId: s.ServerId, Token: cTrace.GenerateToken()}
		return nil
	}
	if s.ServerId == notification.NextServer.ServerId {
		fmt.Println("I'm the new tail server...")
		s.NextServ = ""

		s.gIdMutex.Lock()
		s.LocalGId += s.TailFailureGId_K
		s.gIdMutex.Unlock()

		// trace action
		for _, servId := range notification.FailedServerId {
			cTrace.RecordAction(ServerFailHandled{servId})
		}
		*reply = NotifyServerFailureAck{ServerId: s.ServerId, Token: cTrace.GenerateToken()}

		return nil
	}
	if notification.NextServer.ServerId != 0 {
		s.NextServ = notification.NextServer.ServerListenAddr
		fmt.Println("My next server should now be server ", notification.NextServer.ServerId)
		/* Send my state to next server...*/

		conn, err := rpc.Dial("tcp", notification.NextServer.ServerListenAddr)
		if err != nil {
			return errors.New("(Server failure protocol) Can't connect to next server.." + err.Error())
		}

		res := InternalPutFwd{}
		err = conn.Call("Server.SendServState", false, &res)
		if err != nil {
			return errors.New("(Server failure protocol) Error from retrieving state from next server.." + err.Error())
		}
		// Q: should server block until restore completes to take incoming PUT/GET requests?
		go ResendMissingPuts(res, s, conn)

		// trace action
		cTrace.RecordAction(NewFailoverSuccessor{NewNextServerId: notification.NextServer.ServerId})
		for _, servId := range notification.FailedServerId {
			cTrace.RecordAction(ServerFailHandled{servId})
		}

		*reply = NotifyServerFailureAck{ServerId: s.ServerId, Token: cTrace.GenerateToken()}
		return nil
	}
	if notification.PrevServer.ServerId != 0 {
		s.PrevServ = notification.PrevServer.ServerListenAddr
		fmt.Println("My prev server should now be server ", notification.PrevServer.ServerId)

		// trace action
		cTrace.RecordAction(NewFailoverPredecessor{NewPrevServerId: notification.PrevServer.ServerId})
		for _, servId := range notification.FailedServerId {
			cTrace.RecordAction(ServerFailHandled{servId})
		}

		*reply = NotifyServerFailureAck{ServerId: s.ServerId, Token: cTrace.GenerateToken()}
		return nil
	}
	return errors.New("invalid notification msg")
}

func (s *Server) Start(serverId uint8, coordAddr string, serverAddr string, serverServerAddr string, serverListenAddr string, clientListenAddr string, strace *tracing.Tracer) error {
	/* initalize global server state*/
	s.ServerId = serverId
	s.Config = Addr{CoordAddr: coordAddr, ServerAddr: serverAddr, ServerListenAddr: serverListenAddr, ClientListenAddr: clientListenAddr}
	s.trace = strace.CreateTrace()
	s.tracer = strace
	s.Map = make(map[string]string)
	s.LocalGId = 0
	s.HeadGId_K = 10000
	s.TailFailureGId_K = 1000
	s.cachedPuts = make([]InternalPutFwd, 0)
	s.cachedPutsSize = 256
	joinComplete = make(chan bool)
	// joinComplete = false

	go startFcheck(serverAddr, int(serverId), s)

	s.trace.RecordAction(ServerStart{serverId})
	go listenForCoord(s, serverAddr)
	joinOnStartup(s)
	<-joinComplete
	// for {
	// 	if joinComplete {
	// 		break
	// 	}
	// }

	// /* After join process complete, let's listen if other servers wants to join*/
	go listenForServs(s, serverListenAddr)

	listenForClient(s)

	return nil
}
