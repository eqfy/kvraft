package kvslib

import (
	"errors"
	"net"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"

	"cs.ubc.ca/cpsc416/kvraft/util"

	"github.com/DistributedClocks/tracing"
)

// Actions to be recorded by kvslib (as part of ktrace, put trace, get trace):

type KvslibStart struct {
	ClientId string
}

type KvslibStop struct {
	ClientId string
}

type Put struct {
	ClientId string
	OpId     uint32
	Key      string
	Value    string
}

type PutResultRecvd struct {
	OpId uint32
	GId  uint64
	Key  string
}

type Get struct {
	ClientId string
	OpId     uint32
	Key      string
}

type GetResultRecvd struct {
	OpId  uint32
	GId   uint64
	Key   string
	Value string
}

type HeadReq struct {
	ClientId string
}

type HeadResRecvd struct {
	ClientId string
	ServerId uint8
}

type TailReq struct {
	ClientId string
}

type TailResRecvd struct {
	ClientId string
	ServerId uint8
}

// NotifyChannel is used for notifying the client about a mining result.
type NotifyChannel chan ResultStruct

type ResultStruct struct {
	OpId   uint32
	GId    uint64
	Result string
}

type KVS struct {
	notifyCh NotifyChannel
	// add more KVS instance state here.
	clientId string

	// opId for the client
	opId uint32

	// tracer
	kTracer *tracing.Tracer

	// traces
	ktrace   *tracing.Trace
	putTrace *tracing.Trace
	getTrace *tracing.Trace

	// trackings
	latestGId uint64
	resRecvd  map[uint32]interface{}

	// IP & ports
	headServerIPPort    string
	putListenerIPPort   string
	coordListenerIPPort string

	// TCP Addrs
	hsLocalTCPAddr *net.TCPAddr

	// RPCs:
	coordClientRPC *rpc.Client
	hsClientRPC    *rpc.Client

	// Listeners:
	putTCPListener *net.TCPListener
	// tsRPCListener *rpc.Server
	serverFailListener *net.TCPListener

	// struct with channels:
	cnl *CoordListener // coord notification listener

	// sync
	allResRecvd chan bool
	allErrRecvd chan bool

	// mutex
	reqLock sync.Mutex

	reqQueue             chan Req
	headServerLock       sync.Mutex
	tailServerLock       sync.Mutex
	headReconfiguredDone chan bool
	tailReconfiguredDone chan bool
	chCapacity           int
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

type ClientInfo struct {
	ClientId           string
	CoordAPIListenAddr string
}

type GetReq struct {
	ClientId string
	OpId     uint32
	// LatestGId uint64
	Key   string
	Token tracing.TracingToken
}

type GetRes struct {
	OpId  uint32
	GId   uint64
	Key   string
	Value string
	Token tracing.TracingToken
}

type PutReq struct {
	ClientId        string
	OpId            uint32
	NewGetGId       uint64
	Key             string
	Value           string
	PutListenerAddr string
	Token           tracing.TracingToken
}

type PutRes struct {
	OpId  uint32
	GId   uint64
	Key   string
	Token tracing.TracingToken
}

type Req struct {
	pq     PutReq
	gq     GetReq
	kind   string // kind is either PUT or GET or STOP
	tracer *tracing.Tracer
}

type ServerListener struct {
	PutResChan chan PutRes
}

type CoordListener struct {
	ServerFailChan chan ServerFail
}

type ReqQueue struct {
	QueueLock sync.Mutex
	// queue type?
}

type ServerFail struct {
	ServerPosition  ServerPos
	NewServerIpPort string
}

type ServerPos uint8

const (
	StopServer           = 100 // a server id is guanranteed to be unused
	Head       ServerPos = iota
)

type AddCNLReq struct {
	ClientId            string
	CoordListenerIpPort string
}

func NewKVS() *KVS {
	return &KVS{
		notifyCh: nil,
	}
}

// Start Starts the instance of KVS to use for connecting to the system with the given coord's IP:port.
// The returned notify-channel channel must have capacity ChCapacity and must be used by kvslib to deliver
// all get/put output notifications. ChCapacity determines the concurrency
// factor at the client: the client will never have more than ChCapacity number of operations outstanding (pending concurrently) at any one time.
// If there is an issue with connecting to the coord, this should return an appropriate err value, otherwise err should be set to nil.
func (d *KVS) Start(localTracer *tracing.Tracer, clientId string, coordIPPort string, localCoordIPPort string, localHeadServerIPPort string, chCapacity int) (NotifyChannel, error) {
	var err error
	d.reqQueue = make(chan Req, chCapacity)
	d.chCapacity = chCapacity
	d.clientId = clientId
	d.ktrace = localTracer.CreateTrace()
	d.ktrace.GenerateToken()
	d.resRecvd = make(map[uint32]interface{})
	d.allResRecvd = make(chan bool)
	d.allErrRecvd = make(chan bool)
	d.kTracer = localTracer

	/*Connect to Coord as client*/
	coordLAddr, coordLAddrErr := net.ResolveTCPAddr("tcp", localCoordIPPort)
	if err = checkCriticalErr(coordLAddrErr, "an error resolving addr for localCoordIPPort: "); err != nil {
		return nil, err
	}
	coordAddr, coordAddrErr := net.ResolveTCPAddr("tcp", coordIPPort)
	if err = checkCriticalErr(coordAddrErr, "an error resolving addr for coordIPPort: "); err != nil {
		return nil, err
	}
	cTCPConn, cTCPConnErr := net.DialTCP("tcp", coordLAddr, coordAddr)
	if err = checkCriticalErr(cTCPConnErr, "an error dialing tcp from kvslib to coord: "); err != nil {
		return nil, err
	}
	cTCPConn.SetLinger(0)
	coordClient := rpc.NewClient(cTCPConn)
	d.coordClientRPC = coordClient
	d.ktrace.RecordAction(KvslibStart{clientId})

	/*Start coord listener for server failure*/
	coordListenerIPPort := coordLAddr.IP.String() + ":0"
	coordListenerAddr, coordListenerAddrErr := net.ResolveTCPAddr("tcp", coordListenerIPPort)
	if err = checkCriticalErr(coordListenerAddrErr, "error finding a random port for coord listener: "); err != nil {
		return nil, err
	}
	coordTCPListener, coordTCPListenerErr := net.ListenTCP("tcp", coordListenerAddr)
	if err = checkCriticalErr(coordTCPListenerErr, "an error setting up server failure listener in kvslib: "); err != nil {
		return nil, err
	}
	d.coordListenerIPPort = coordTCPListener.Addr().String()

	d.serverFailListener = coordTCPListener
	cnl := &CoordListener{make(chan ServerFail)}
	d.cnl = cnl
	d.cnl.ServerFailChan = make(chan ServerFail, 16)
	coordRPCListener := rpc.NewServer()
	coordRegErr := coordRPCListener.Register(cnl)
	if err = checkCriticalErr(coordRegErr, "an error registering cnl for coordRPCListener in kvslib: "); err != nil {
		return nil, err
	}
	go coordRPCListener.Accept(coordTCPListener)

	/*Get Head Server Addr from Coord*/
	d.ktrace.RecordAction(HeadReq{clientId})
	var headServerReq CCoordGetHeadServerArg = CCoordGetHeadServerArg{ClientInfo{clientId, d.coordListenerIPPort}, d.ktrace.GenerateToken()}
	var headServerRes CCoordGetHeadServerReply
	hsReqErr := coordClient.Call("CCoord.GetHeadServer", headServerReq, &headServerRes)
	if err = checkCriticalErr(hsReqErr, "an error requesting head server from coord: "); err != nil {
		return nil, err
	}
	d.ktrace = localTracer.ReceiveToken(headServerRes.Token)
	d.ktrace.RecordAction(HeadResRecvd{clientId, headServerRes.ServerId})
	d.headServerIPPort = headServerRes.ServerIpPort

	/*Connect to Head*/
	hsLAddr, hsLAddrErr := net.ResolveTCPAddr("tcp", localHeadServerIPPort)
	if err = checkCriticalErr(hsLAddrErr, "an error resolving addr for localHeadServerIPPort: "); err != nil {
		return nil, err
	}
	d.hsLocalTCPAddr = hsLAddr
	hsAddr, hsAddrErr := net.ResolveTCPAddr("tcp", d.headServerIPPort)
	if err = checkCriticalErr(hsAddrErr, "an error resolving addr for headServerIPPort: "); err != nil {
		return nil, err
	}
	hsTCPConn, hsTCPConnErr := net.DialTCP("tcp", hsLAddr, hsAddr)
	if err = checkCriticalErr(hsTCPConnErr, "an error dialing tcp from kvslib to head server: "); err != nil {
		return nil, err
	}
	hsTCPConn.SetLinger(0)
	hsClient := rpc.NewClient(hsTCPConn)
	d.hsClientRPC = hsClient

	d.notifyCh = make(NotifyChannel, chCapacity)
	d.opId = 0
	d.latestGId = 0
	go coordRPCListener.Accept(coordTCPListener)
	go d.sender()
	go d.handleFailure()
	return d.notifyCh, nil
}

// Get non-blocking request from the client to make a get call for a given key.
// In case there is an underlying issue (for example, servers/coord cannot be reached),
// this should return an appropriate err value, otherwise err should be set to nil. Note that this call is non-blocking.
// The returned value must be delivered asynchronously to the client via the notify-channel channel returned in the Start call.
// The value opId is used to identify this request and associate the returned value with this request.
func (d *KVS) Get(tracer *tracing.Tracer, clientId string, key string) (uint32, error) {
	atomic.AddUint32(&d.opId, 1)
	req := Req{
		gq: GetReq{
			ClientId: clientId,
			OpId:     d.opId,
			Key:      key,
			Token:    tracing.TracingToken{}, // not used
		},
		kind:   "GET",
		tracer: tracer,
	}
	d.reqQueue <- req

	return d.opId, nil
}

// Put non-blocking request from the client to update the value associated with a key.
// In case there is an underlying issue (for example, the servers/coord cannot be reached),
// this should return an appropriate err value, otherwise err should be set to nil. Note that this call is non-blocking.
// The value opId is used to identify this request and associate the returned value with this request.
// The returned value must be delivered asynchronously via the notify-channel channel returned in the Start call.
func (d *KVS) Put(tracer *tracing.Tracer, clientId string, key string, value string) (uint32, error) {
	atomic.AddUint32(&d.opId, 1)
	req := Req{PutReq{clientId, d.opId, d.latestGId, key, value, d.putListenerIPPort, tracing.TracingToken{}}, GetReq{}, "PUT", tracer}
	d.reqQueue <- req

	return d.opId, nil

}

func (d *KVS) handleFailure() {
	d.headReconfiguredDone = make(chan bool, 256) // TODO maybe a bigger buffer is needed
	d.tailReconfiguredDone = make(chan bool, 256)
	for {
		serverFailure := <-d.cnl.ServerFailChan
		if serverFailure.ServerPosition == Head {
			d.headServerLock.Lock()
			/* Close old client connection to HS */
			d.headServerIPPort = serverFailure.NewServerIpPort
			hsClientRPCCloseErr := d.hsClientRPC.Close()
			checkWarning(hsClientRPCCloseErr, "error closing the RPC client for head server(HS) upon receiving failed HS msg in handlGet: ")

			/* Get new Head server from coord */
			d.ktrace.RecordAction(HeadReq{d.clientId})
			var headServerReq CCoordGetHeadServerArg = CCoordGetHeadServerArg{ClientInfo{d.clientId, d.coordListenerIPPort}, d.ktrace.GenerateToken()}
			var headServerRes CCoordGetHeadServerReply
			hsReqErr := d.coordClientRPC.Call("CCoord.GetHeadServer", headServerReq, &headServerRes)
			if hsReqErr != nil {
				checkWarning(hsReqErr, "error requesting head server from coord: ")
				continue
			}
			d.ktrace = d.kTracer.ReceiveToken(headServerRes.Token)
			d.ktrace.RecordAction(HeadResRecvd{d.clientId, headServerRes.ServerId})
			d.headServerIPPort = headServerRes.ServerIpPort

			/* Start new client connection to HS */
			newHSAddr, newHSAddrErr := net.ResolveTCPAddr("tcp", d.headServerIPPort)
			checkWarning(newHSAddrErr, "error resolving newHSAddr upon receiving failed HS msg in handlGet:")
			newHSClientTCP, newHSClientTCPErr := net.DialTCP("tcp", d.hsLocalTCPAddr, newHSAddr)
			if newHSClientTCPErr != nil {
				checkWarning(newHSClientTCPErr, "error starting TCP connection from kvslib to NEW HS upon receiving failed HS msg in handlGet: ")
				continue
			}
			newHSClientTCP.SetLinger(0)
			d.hsClientRPC = rpc.NewClient(newHSClientTCP)

			util.PrintfYellow("Before headReconfiguredDone")
			d.headReconfiguredDone <- true
			util.PrintfYellow("After headReconfiguredDone")
			d.headServerLock.Unlock()
		} else if serverFailure.ServerPosition == StopServer {
			d.allErrRecvd <- true
			return
		} /* else if serverFailure.ServerPosition == Tail { */
		// d.tailServerLock.Lock()
		// /* Close old client connection to TS */
		// d.tailServerIPPort = serverFailure.NewServerIpPort
		// tsClientRPCCloseErr := d.tsClientRPC.Close()
		// checkWarning(tsClientRPCCloseErr, "error closing the RPC client for tail server(TS) upon receiving failed TS msg in handlGet: ")

		// /* Get new Tail server from coord */
		// d.ktrace.RecordAction(TailReq{d.clientId})
		// var tailServerReq CCoordGetTailServerArg = CCoordGetTailServerArg{ClientInfo{d.clientId, d.coordListenerIPPort}, d.ktrace.GenerateToken()}
		// var tailServerRes CCoordGetTailServerReply
		// tsReqErr := d.coordClientRPC.Call("CCoord.GetTailServer", tailServerReq, &tailServerRes)
		// if tsReqErr != nil {
		// 	checkWarning(tsReqErr, "an error requesting tail server from coord: ")
		// 	continue
		// }
		// d.ktrace = d.kTracer.ReceiveToken(tailServerRes.Token)
		// d.ktrace.RecordAction(TailResRecvd{d.clientId, tailServerRes.ServerId})
		// d.tailServerIPPort = tailServerRes.ServerIpPort

		// /* Start new client connection to TS */
		// newTSAddr, newTSAddrErr := net.ResolveTCPAddr("tcp", d.tailServerIPPort)
		// checkWarning(newTSAddrErr, "error resolving newTSAddr upon receiving failed TS msg in handlGet:")
		// newTSClientTCP, newTSClientTCPErr := net.DialTCP("tcp", d.tsLocalTCPAddr, newTSAddr)
		// if newTSClientTCPErr != nil {
		// 	checkWarning(newTSClientTCPErr, "error starting TCP connection from kvslib to NEW TS upon receiving failed TS msg in handlGet: ")
		// 	continue
		// }
		// newTSClientTCP.SetLinger(0)
		// d.tsClientRPC = rpc.NewClient(newTSClientTCP)

		// util.PrintfYellow("Before tailReconfiguredDone")
		// d.tailReconfiguredDone <- true
		// util.PrintfYellow("After tailReconfiguredDone")
		// d.tailServerLock.Unlock()
		/* 	} */
	}
}

func (d *KVS) sender() {
	// queue of operations
	for {
		req := <-d.reqQueue
		if req.kind == "PUT" {
			// Send to head server
			d.putTrace = req.tracer.CreateTrace()
			d.putTrace.RecordAction(Put{req.pq.ClientId, req.pq.OpId, req.pq.Key, req.pq.Value})
			var putReq PutReq = PutReq{req.pq.ClientId, req.pq.OpId, d.latestGId, req.pq.Key, req.pq.Value, d.putListenerIPPort, d.putTrace.GenerateToken()}

			// var err error
			keepSending := true
			var putRes PutRes
			for keepSending {
				d.headServerLock.Lock()
				/*TO TEST: this is to deal with PUT blocking when previously using d.hsClientRPC.Call and having putRes := <-d.tpl.PutResChan outside of for-loop*/
				d.hsClientRPC.Go("Request.Put", putReq, nil, nil)
				d.headServerLock.Unlock()
				select {
				// case putRes = <-d.tpl.PutResChan:
				// 	keepSending = false
				case <-d.headReconfiguredDone:
					// Send again
				case <-time.After(2 * time.Second): // set a timeout of 2 second
					// Send again
				}
			}

			// waits until the tail server gets back with put success
			// putRes := <-d.tpl.PutResChan  /* This is commented out because this will block d.hsClientRPC.Call from finishing*/
			if _, ok := d.resRecvd[req.pq.OpId]; !ok && putRes.Key != "Duplicate" { // don't do anything if receiving a duplicate response
				d.putTrace = req.tracer.ReceiveToken(putRes.Token)
				d.putTrace.RecordAction(PutResultRecvd{putRes.OpId, putRes.GId, putRes.Key})
				d.resRecvd[req.pq.OpId] = putRes
				d.notifyCh <- ResultStruct{putRes.OpId, putRes.GId, putReq.Value} // FIXME maybe have putRes return the value
			}
		} else if req.kind == "GET" {
			// Send to tail server
			// d.getTrace = req.tracer.CreateTrace()
			// d.getTrace.RecordAction(Get{req.gq.ClientId, req.gq.OpId, req.gq.Key})
			// var getReq GetReq = GetReq{req.gq.ClientId, req.gq.OpId, req.gq.Key, d.getTrace.GenerateToken()}
			// var getRes GetRes

			// var err error
			// keepSending := true
			// util.PrintfYellow("In Get, before keepSending loop")
			// for keepSending {
			// 	util.PrintfYellow("In Get, inside keepSending loop")
			// 	d.tailServerLock.Lock()
			// 	util.PrintfYellow("In Get, after Lock, before call")
			// 	// err = d.tsClientRPC.Call("Request.Get", getReq, &getRes)
			// 	util.PrintfYellow("In Get, after Lock, after call")
			// 	d.tailServerLock.Unlock()
			// 	util.PrintfYellow("In Get, after unlock")
			// 	if err != nil { // will now wait for head server to have finished reconfiguring
			// 		<-d.tailReconfiguredDone
			// 	} else {
			// 		keepSending = false
			// 	}
			// }

			// if _, ok := d.resRecvd[req.gq.OpId]; !ok { // don't do anything if receiving a duplicate response
			// 	d.getTrace = req.tracer.ReceiveToken(getRes.Token)
			// 	d.getTrace.RecordAction(GetResultRecvd{getRes.OpId, getRes.GId, getRes.Key, getRes.Value})
			// 	d.resRecvd[req.gq.OpId] = getRes
			// 	if getRes.GId > d.latestGId {
			// 		d.latestGId = getRes.GId
			// 	}
			// 	d.notifyCh <- ResultStruct{getRes.OpId, getRes.GId, getRes.Value}
			// }
		} else if req.kind == "STOP" {
			d.allResRecvd <- true
			return
		}
	}
}

// Stop Stops the KVS instance from communicating with the KVS and from delivering any results via the notify-channel.
// This call always succeeds.
func (d *KVS) Stop() {
	d.reqQueue <- Req{PutReq{}, GetReq{}, "STOP", nil}
	d.cnl.ServerFailChan <- ServerFail{ServerPosition: StopServer}
	<-d.allResRecvd // Wait until all pending request have received response from the server?
	// d.allResRecvd.Wait() // Wait until all pending request have received response from the server?
	// TODO: Confirm - Close the notify channel and all RPC connections before stop???
	<-d.allErrRecvd
	// close(d.notifyCh)
	// close(d.cnl.ServerFailChan)
	// close(d.tpl.PutResChan)
	// close(d.reqQueue)
	d.coordClientRPC.Close()
	d.hsClientRPC.Close()
	// d.tsClientRPC.Close()
	d.putTCPListener.Close()
	d.serverFailListener.Close()
	d.ktrace.GenerateToken()

	d.ktrace.RecordAction(KvslibStop{d.clientId})

	// TODO: Confirm - is this enough for closing?
}

func (tpl *ServerListener) PutSuccess(putRes PutRes, isDone *bool) error {
	// util.PrintfCyan("%s\n", "in PutSuccess")
	tpl.PutResChan <- putRes
	// util.PrintfCyan("%s\n", "in PutSuccess")
	*isDone = true
	return nil
}

func (cnl *CoordListener) ChangeHeadServer(newServerIPPort string, isDone *bool) error {
	cnl.ServerFailChan <- ServerFail{Head, newServerIPPort}
	*isDone = true
	return nil
}

// func (cnl *CoordListener) ChangeTailServer(newServerIPPort string, isDone *bool) error {
// 	cnl.ServerFailChan <- ServerFail{Tail, newServerIPPort}
// 	*isDone = true
// 	return nil
// }

func checkCriticalErr(origErr error, errStartStr string) error {
	var newErr error = nil
	if origErr != nil {
		newErr = errors.New(errStartStr + origErr.Error())
		util.PrintfRed("%s%s\n", "SHOULD NOT HAPPEN: ", newErr.Error())
	}
	return newErr
}

func checkWarning(origErr error, errStartStr string) {
	if origErr != nil {
		util.PrintfYellow("%s%s\n", "WARNING: ", errStartStr+origErr.Error())
	}
}
