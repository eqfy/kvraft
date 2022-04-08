/*

This package specifies the API to the failure checking library to be
used in assignment 2 of UBC CS 416 2021W2.

You are *not* allowed to change the API below. For example, you can
modify this file by adding an implementation to Stop, but you cannot
change its API.

*/

package fchecker

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

////////////////////////////////////////////////////// DATA
// Define the message types fchecker has to use to communicate to other
// fchecker instances. We use Go's type declarations for this:
// https://golang.org/ref/spec#Type_declarations

// Heartbeat message.
type HBeatMessage struct {
	EpochNonce uint64 // Identifies this fchecker instance/epoch.
	SeqNum     uint64 // Unique for each heartbeat in an epoch.
}

// An ack message; response to a heartbeat.
type AckMessage struct {
	HBEatEpochNonce uint64 // Copy of what was received in the heartbeat.
	HBEatSeqNum     uint64 // Copy of what was received in the heartbeat.
}

// Notification of a failure, signal back to the client using this
// library.
type FailureDetected struct {
	UDPIpPort string    // The RemoteIP:RemotePort of the failed node.
	Timestamp time.Time // The time when the failure was detected.
	ServerId  uint8
}

////////////////////////////////////////////////////// API

type StartStruct struct {
	AckLocalIPAckLocalPort       string
	EpochNonce                   uint64
	HBeatLocalIPHBeatLocalPort   string
	HBeatRemoteIPHBeatRemotePort string
	LostMsgThresh                uint8
}

type StartMonitoringConfig struct {
	RemoteServerAddr map[uint8]string
	LocalAddr        map[uint8]string
	LostMsgThresh    uint8
}

type routineTerminateInfo struct {
	nonce     uint64
	terminate bool
}

// var terminateFcheck bool = false
var routinesList = make([]routineTerminateInfo, 0)
var mu sync.Mutex

var ackRoutinesList = make([]routineTerminateInfo, 0)
var ackMu sync.Mutex

var SimulateNetworkPartition = false

// Starts the fcheck library.

func Start(arg StartStruct) (notifyCh <-chan FailureDetected, err error) {
	failureChan := make(chan FailureDetected)
	if arg.HBeatLocalIPHBeatLocalPort == "" {
		// ONLY arg.AckLocalIPAckLocalPort is set
		// Start fcheck without monitoring any node, but responding to heartbeats.

		laddr, monitorErr := net.ResolveUDPAddr("udp", arg.AckLocalIPAckLocalPort)
		if monitorErr != nil {
			return nil, errors.New("Failed to set monitored mode; could not resolve address:" + err.Error())
		}

		monitorConn, listenUdpErr := net.ListenUDP("udp", laddr)
		if listenUdpErr != nil {
			return nil, errors.New("Failed to listen udp for monitored node" + listenUdpErr.Error())
		}

		ackMu.Lock()
		ackRoutinesList = append(ackRoutinesList, routineTerminateInfo{nonce: arg.EpochNonce, terminate: false})
		ackMu.Unlock()
		fmt.Println(ackRoutinesList)

		go func(routineSpecificNonce uint64) {
			ignoredHeartbeats := 0
			defer monitorConn.Close()
			for {

				for i := range ackRoutinesList {
					if ackRoutinesList[i].nonce == routineSpecificNonce && ackRoutinesList[i].terminate == true {
						// monitorConn.Close()
						fmt.Println("exiting go routine with nonce: " + strconv.FormatUint(arg.EpochNonce, 10))
						ackMu.Lock()
						if len(ackRoutinesList) > 0 {
							ackRoutinesList = ackRoutinesList[1:]
						}
						ackMu.Unlock()
						return
					}
				}

				buf := make([]byte, 1024)
				rlen, remote, readErr := monitorConn.ReadFromUDP(buf[:])
				if readErr != nil {
					fmt.Println("WARNING: could not read from udp: ", readErr.Error())
				}
				receivedHeartBeat, decodeErr := decodeHeartbeat(buf, rlen)
				if decodeErr != nil {
					fmt.Println("WARNING: Failed to decode HBeat packet received from server: ", decodeErr.Error())
				} else {
					if SimulateNetworkPartition {
						ignoredHeartbeats++
						if ignoredHeartbeats == 100 {
							SimulateNetworkPartition = false
						}
					} else {
						if receivedHeartBeat.SeqNum%45 == 0 {
							// fmt.Println("Received heartbeat: ", receivedHeartBeat)
							// fmt.Println("sending ack to", remote.String())
						}
						select {
						case <-time.After(time.Millisecond * 300):
							ack := AckMessage{receivedHeartBeat.EpochNonce, receivedHeartBeat.SeqNum}
							if _, err := monitorConn.WriteTo(encodeAck(&ack), remote); err != nil {
								fmt.Println("WARNING: could not send ack msg to: ", remote.String())
							}
						}
					}
				}
			}
		}(arg.EpochNonce)

	} else {
		// Else: ALL fields in arg are set
		// Start the fcheck library by monitoring a single node and
		// also responding to heartbeats.
		fmt.Println("Nonce: ", arg.EpochNonce, "; Server: ", arg.HBeatRemoteIPHBeatRemotePort)

		localAddr, err := net.ResolveUDPAddr("udp", arg.HBeatLocalIPHBeatLocalPort)
		if err != nil {
			return nil, errors.New("could not resolve HBeatLocalIPHBeatLocalPort:" + err.Error())
		}

		remoteAddr, err := net.ResolveUDPAddr("udp", arg.HBeatRemoteIPHBeatRemotePort)
		if err != nil {
			return nil, errors.New("could not resolve UDP address: " + err.Error())
		}

		conn, err := net.DialUDP("udp", localAddr, remoteAddr)
		if err != nil {
			return nil, errors.New("could not dial UDP: " + err.Error())
		}

		fmt.Println("starting go routine with epoch " + strconv.FormatUint(arg.EpochNonce, 10))
		mu.Lock()
		routinesList = append(routinesList, routineTerminateInfo{nonce: arg.EpochNonce, terminate: false})
		mu.Unlock()
		fmt.Println(routinesList)
		go func(routineSpecificNonce uint64) {

			continueSendReceive := true
			var unackedHeartBeats = map[int]time.Time{}

			fname := "./LOGS-FCHECK-" + strconv.FormatUint(arg.EpochNonce, 10) + ".log"
			f, fileErr := os.Create(fname)
			check(fileErr)
			defer f.Close()

			rtt := time.Second * 3
			newRtt := rtt
			var retransmissionDuration time.Duration = 0
			lostMsgs := 0
			seqNum := 0

			for {

				for i := range routinesList {
					if routinesList[i].nonce == routineSpecificNonce && routinesList[i].terminate == true {
						conn.Close()
						fmt.Println("exiting go routine with nonce: " + strconv.FormatUint(arg.EpochNonce, 10))
						for k := range unackedHeartBeats {
							delete(unackedHeartBeats, k)
						}
						mu.Lock()
						if len(routinesList) > 0 {
							routinesList = routinesList[1:]
						}
						mu.Unlock()
						return
					}
				}

				if continueSendReceive {
					heartBeat := HBeatMessage{
						EpochNonce: arg.EpochNonce,
						SeqNum:     uint64(seqNum),
					}

					var heartBeatSentTime time.Time
					if _, err := f.WriteString("retransmission time for seq num " + strconv.FormatInt(int64(seqNum), 10) + ": " + retransmissionDuration.String() + "\n"); err != nil {
						check(err)
					}
					f.Sync()
					// fmt.Println("retransmission time for seq num " + strconv.FormatInt(int64(seqNum), 10) + ": " + retransmissionDuration.String())
					// SENDING HEARTBEAT
					select {
					case <-time.After(retransmissionDuration):
						rtt = newRtt
						if _, err := f.WriteString("rtt value: " + rtt.String() + "\n"); err != nil {
							check(err)
						}
						f.Sync()
						//fmt.Println("rtt value: " + rtt.String())
						heartBeatSentTime = time.Now()
						unackedHeartBeats[seqNum] = heartBeatSentTime
						if _, err := conn.Write(encode(&heartBeat)); err != nil {
							fmt.Println("udp write failed: " + err.Error())
						}
						if _, err := f.WriteString("heartbeat sent at time: " + heartBeatSentTime.String() + "\n"); err != nil {
							check(err)
						}
						f.Sync()
						// fmt.Println("heartbeat sent at time: " + heartBeatSentTime.String())
					}

					// RECEIVE ACK CODE STARTS
					recvBuf := make([]byte, 1024)
					conn.SetReadDeadline(time.Now().Add(rtt))
					fromUDP, _, err := conn.ReadFromUDP(recvBuf)
					ackReceivedTime := time.Now()
					if err != nil {
						if _, err := f.WriteString("RTT limit exceeded for seqNum: " + strconv.FormatInt(int64(seqNum), 10) + "\n"); err != nil {
							check(err)
						}
						f.Sync()
						//fmt.Println("RTT limit exceeded for seqNum: " + strconv.FormatInt(int64(seqNum), 10))
						retransmissionDuration = 0
						lostMsgs++
						if uint8(lostMsgs) == arg.LostMsgThresh {
							if _, err := f.WriteString("sending failure info to failureChannel for routine with nonce " + strconv.FormatUint(routineSpecificNonce, 10) + "\n"); err != nil {
								check(err)
							}
							f.Sync()
							failureChan <- FailureDetected{UDPIpPort: arg.HBeatRemoteIPHBeatRemotePort, Timestamp: time.Now()}
							continueSendReceive = false
						}
					} else {
						decoded, err := decode(recvBuf, fromUDP)
						if err != nil {
							fmt.Println("could not decode server packet: " + err.Error())
						} else {
							_, ok := unackedHeartBeats[int(decoded.HBEatSeqNum)]
							if ok && decoded.HBEatEpochNonce == arg.EpochNonce {
								estimatedRtt := ackReceivedTime.Sub(unackedHeartBeats[int(decoded.HBEatSeqNum)])
								retransmissionDuration = time.Duration(math.Abs(float64(rtt - estimatedRtt)))
								if _, err := f.WriteString("Received ack; seq num: " + strconv.FormatUint(decoded.HBEatSeqNum, 10) + "; estimated rtt for this ack: " + estimatedRtt.String() + "\n"); err != nil {
									check(err)
								}
								f.Sync()
								// fmt.Println("Received ack; seq num: " + strconv.FormatUint(decoded.HBEatSeqNum, 10) + "; estimated rtt for this ack: " + estimatedRtt.String())
								delete(unackedHeartBeats, int(decoded.HBEatSeqNum))
								newRtt = (estimatedRtt + rtt) / 2
								if _, err := f.WriteString("New RTT: " + newRtt.String() + "\n\n\n"); err != nil {
									check(err)
								}
								f.Sync()
								// .Println("New RTT: " + newRtt.String())
								// fmt.Printf("\n\n")
								if lostMsgs > 0 {
									lostMsgs = 0
								}
							} else {
								fmt.Println("received invalid ack; seq num or nonce does not match: ", decoded)
								fmt.Println("args received from client: ", arg)
							}
						}
					}
					seqNum++
				}
			}
		}(arg.EpochNonce)
	}
	return failureChan, nil
}

func StartMonitoringServers(arg StartMonitoringConfig) (notifyCh <-chan FailureDetected, notifyCh2 <-chan uint8, err error) {
	failureChan := make(chan FailureDetected)
	successChan := make(chan uint8)

	for servId, servAddr := range arg.RemoteServerAddr {
		localAddr, err := net.ResolveUDPAddr("udp", arg.LocalAddr[servId])
		if err != nil {
			return nil, nil, errors.New("could not resolve local CoordAddress for fcheck:" + err.Error())
		}

		remoteAddr, err := net.ResolveUDPAddr("udp", servAddr)
		if err != nil {
			return nil, nil, errors.New("could not resolve UDP address: " + err.Error())
		}

		conn, err := net.DialUDP("udp", localAddr, remoteAddr)
		if err != nil {
			return nil, nil, errors.New("could not dial UDP: " + err.Error())
		}

		go func(conn *net.UDPConn, nonce uint8) {
			defer conn.Close()

			fmt.Println("starting go routine with epoch " + strconv.FormatInt(int64(nonce), 10))

			var unackedHeartBeats = map[int]time.Time{}
			serverUnresponsive := false

			fname := "./LOGS-FCHECK-" + strconv.FormatInt(int64(nonce), 10) + ".log"
			f, fileErr := os.Create(fname)
			check(fileErr)
			defer f.Close()

			rtt := time.Second * 3
			newRtt := rtt
			var retransmissionDuration time.Duration = 0
			lostMsgs := 0
			seqNum := 0

			for {
				heartBeat := HBeatMessage{
					EpochNonce: uint64(nonce),
					SeqNum:     uint64(seqNum),
				}

				var heartBeatSentTime time.Time
				if _, err := f.WriteString("retransmission time for seq num " + strconv.FormatInt(int64(seqNum), 10) + ": " + retransmissionDuration.String() + "\n"); err != nil {
					check(err)
				}
				f.Sync()

				// SENDING HEARTBEAT
				select {
				case <-time.After(retransmissionDuration):
					rtt = newRtt
					if _, err := f.WriteString("rtt value: " + rtt.String() + "\n"); err != nil {
						check(err)
					}
					f.Sync()

					heartBeatSentTime = time.Now()
					unackedHeartBeats[seqNum] = heartBeatSentTime
					if _, err := conn.Write(encode(&heartBeat)); err != nil {
						fmt.Println("udp write failed: " + err.Error())
					} else {
						if _, err := f.WriteString("heartbeat sent at time: " + heartBeatSentTime.String() + "\n"); err != nil {
							check(err)
						}
						f.Sync()
					}
				}

				// RECEIVE ACK CODE STARTS
				recvBuf := make([]byte, 1024)
				conn.SetReadDeadline(time.Now().Add(rtt))
				fromUDP, _, err := conn.ReadFromUDP(recvBuf)
				ackReceivedTime := time.Now()
				if err != nil {
					if _, err := f.WriteString("RTT limit exceeded for seqNum: " + strconv.FormatInt(int64(seqNum), 10) + "\n"); err != nil {
						check(err)
					}
					f.Sync()
					//fmt.Println("RTT limit exceeded for seqNum: " + strconv.FormatInt(int64(seqNum), 10))

					retransmissionDuration = 0
					lostMsgs++
					if uint8(lostMsgs) >= arg.LostMsgThresh {
						if !serverUnresponsive {
							if _, err := f.WriteString("sending failure info to failureChannel for routine with nonce " + strconv.FormatInt(int64(nonce), 10) + "\n"); err != nil {
								check(err)
							}
							f.Sync()

							failureDetected := FailureDetected{UDPIpPort: conn.RemoteAddr().String(), Timestamp: time.Now(), ServerId: nonce}
							fmt.Printf("Sending failure notification to notifyChannel inside the go routine: %v\n", failureDetected)
							serverUnresponsive = true
							failureChan <- failureDetected
						}
					}
				} else {
					decoded, err := decode(recvBuf, fromUDP)
					if err != nil {
						fmt.Println("could not decode server packet: " + err.Error())
					} else {

						if lostMsgs > 0 {
							lostMsgs = 0
						}

						if serverUnresponsive {
							serverUnresponsive = false
							unackedHeartBeats = make(map[int]time.Time)
							fmt.Printf("\nFCHECK: server %v initially unreachable is now  responding\n", decoded.HBEatEpochNonce)
							successChan <- nonce
						} else {
							_, ok := unackedHeartBeats[int(decoded.HBEatSeqNum)]
							if ok && decoded.HBEatEpochNonce == uint64(nonce) {
								estimatedRtt := ackReceivedTime.Sub(unackedHeartBeats[int(decoded.HBEatSeqNum)])
								retransmissionDuration = time.Duration(math.Abs(float64(rtt - estimatedRtt)))
								if _, err := f.WriteString("Received ack; seq num: " + strconv.FormatUint(decoded.HBEatSeqNum, 10) + "; estimated rtt for this ack: " + estimatedRtt.String() + "\n"); err != nil {
									check(err)
								}
								f.Sync()
								delete(unackedHeartBeats, int(decoded.HBEatSeqNum))
								newRtt = (estimatedRtt + rtt) / 2
								if _, err := f.WriteString("New RTT: " + newRtt.String() + "\n\n\n"); err != nil {
									check(err)
								}
								f.Sync()
							}
						}
					}
				}
				seqNum++
			}
		}(conn, servId)
	}
	return failureChan, successChan, nil
}

// Tells the library to stop monitoring/responding acks.
func Stop() {
	if len(routinesList) > 0 {
		mu.Lock()
		routinesList[0].terminate = true
		mu.Unlock()
	}
	if len(ackRoutinesList) > 0 {
		ackMu.Lock()
		ackRoutinesList[0].terminate = true
		ackMu.Unlock()
	}

	fmt.Println("stop called")
	return
}

func encode(hBeat *HBeatMessage) []byte {
	var buf bytes.Buffer
	gob.NewEncoder(&buf).Encode(hBeat)
	return buf.Bytes()
}

func decode(buf []byte, len int) (AckMessage, error) {
	var decoded AckMessage
	err := gob.NewDecoder(bytes.NewBuffer(buf[0:len])).Decode(&decoded)
	if err != nil {
		return AckMessage{}, err
	}
	return decoded, nil
}

func decodeHeartbeat(buf []byte, len int) (HBeatMessage, error) {
	var decoded HBeatMessage
	err := gob.NewDecoder(bytes.NewBuffer(buf[0:len])).Decode(&decoded)
	if err != nil {
		return HBeatMessage{}, err
	}
	return decoded, nil
}

func encodeAck(move *AckMessage) []byte {
	var buf bytes.Buffer
	gob.NewEncoder(&buf).Encode(move)
	return buf.Bytes()
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}
