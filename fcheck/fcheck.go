/*
This package specifies the API to the failure checking library for
UBC CS 416 2021W2.
*/

package fcheck

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"cs.ubc.ca/cpsc416/a3/util"
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
}

////////////////////////////////////////////////////// API

type StartStruct struct {
	AckLocalIPAckLocalPort       string
	EpochNonce                   uint64
	HBeatLocalIPHBeatLocalPort   string
	HBeatRemoteIPHBeatRemotePort string
	LostMsgThresh                uint8
}

////////////////////////////////////////////////////// Custom
var stopMonitorCh = make(chan bool, 1)
var stopAckCh = make(chan bool, 1)
var allStopWg = sync.WaitGroup{}

type hBeatAckReadMsg struct {
	raddr *net.UDPAddr
	msg   *HBeatMessage
}

type hBeatAckWriteMsg struct {
	raddr *net.UDPAddr
	msg   *AckMessage
}

// Starts the fcheck library.

func Start(arg StartStruct) (notifyCh <-chan FailureDetected, err error) {
	if arg.HBeatLocalIPHBeatLocalPort == "" {
		// ONLY arg.AckLocalIPAckLocalPort is set
		//
		// Start fcheck without monitoring any node, but responding to heartbeats.

		_, err := setupHBeatAck(arg.AckLocalIPAckLocalPort)
		if err != nil {
			return nil, err
		}
		allStopWg.Add(3)
		return nil, nil
	}
	// Else: ALL fields in arg are set
	// Start the fcheck library by monitoring a single node and
	// also responding to heartbeats.

	notifyChFull := make(chan FailureDetected)
	err = setupHBeatMonitor(arg, notifyChFull)
	if err != nil {
		return nil, err
	}

	_, err = setupHBeatAck(arg.AckLocalIPAckLocalPort)
	if err != nil {
		return nil, err
	}
	allStopWg.Add(6)
	return notifyChFull, nil
}

// Starts fcheck but only responds to ack hbeats. Also returns the local address of ack listner connection
func StartOnlyAck(serverIP string) (string, error) {
	laddr, err := setupHBeatAck(serverIP + ":0")
	if err != nil {
		return "", err
	}
	allStopWg.Add(3)
	return laddr, nil
}

// Start fcheck to monitor multiple nodes supllied by a list of StartStruct
// the function returns a buffered channel of len(args) size
// any node failures will be pushed to this channel
func StartMultiMonitor(args []StartStruct) (<-chan FailureDetected, error) {
	notifyCh := make(chan FailureDetected, len(args))
	for _, arg := range args {
		err := setupHBeatMonitor(arg, notifyCh)
		if err != nil {
			return nil, err
		}
		allStopWg.Add(3)
	}
	return notifyCh, nil
}

// Tells the library to stop monitoring/responding acks.
func Stop() {
	// TODO refactor this to use a single chan close
	stopAckCh <- true
	util.PrintfBlue("Ack stopped\n")
	stopMonitorCh <- true
	util.PrintfBlue("Monitor stopped\n")
	allStopWg.Wait()
	util.PrintfBlue("FCheck all stopped\n")
}

func setupHBeatMonitor(arg StartStruct, notifyCh chan FailureDetected) (err error) {
	var laddr *net.UDPAddr
	if arg.HBeatLocalIPHBeatLocalPort != "" {
		laddr, err = net.ResolveUDPAddr("udp", arg.HBeatLocalIPHBeatLocalPort)
		if err != nil {
			return fmt.Errorf("error resolving monitor local ip: %v", err)
		}
	} // Otherwise, laddr == nil

	raddr, err := net.ResolveUDPAddr("udp", arg.HBeatRemoteIPHBeatRemotePort)
	if err != nil {
		return fmt.Errorf("error resolving monitor remote ip: %v", err)
	}

	// setup UDP connection
	conn, err := net.DialUDP("udp", laddr, raddr) // if laddr is nil, an available ip_port is randomly chosen
	if err != nil {
		return fmt.Errorf("error connecting monitor server: %v", err)
	}

	go hBeatMonitorEventLoop(conn, arg, notifyCh)

	return nil
}

// Read
func hBeatMonitorRead(conn *net.UDPConn, readMsg chan<- AckMessage) {
	buffer := make([]byte, 1024)
	for {
		n, err := conn.Read(buffer)
		if err != nil {
			util.PrintfBlue("error reading ack from udp: %v\n", err)
			if isConnClosedError(err) {
				util.PrintfBlue("reading ack goroutine exited\n")
				allStopWg.Done()
				return
			}
			continue
		}

		ackMsg, err := decodeAckMsg(buffer, n)
		if err != nil {
			util.PrintfBlue("error decoding ack msg: %v\n", err)
			continue
		}

		readMsg <- ackMsg
	}
}

func hBeatMonitorWrite(conn *net.UDPConn, writeMsg <-chan HBeatMessage, endWrite <-chan struct{}) {
	for {
		select {
		case sendHBeatMsg := <-writeMsg:
			_, err := conn.Write(encodeHBeatMsg(&sendHBeatMsg))
			if err != nil {
				util.PrintfBlue("error writing monitor msg: %v\n", err)
				if isConnClosedError(err) {
					return
				}
				continue
			}
		case <-endWrite:
			util.PrintfBlue("writing monitor goroutine exited\n")
			allStopWg.Done()
			return
		}
	}
}

func hBeatMonitorEventLoop(conn *net.UDPConn, arg StartStruct, notifyCh chan FailureDetected) {
	nounce := arg.EpochNonce
	currWriteMsg := HBeatMessage{
		EpochNonce: arg.EpochNonce,
		SeqNum:     0,
	}
	readMsgCh := make(chan AckMessage, 1)
	writeMsgCh := make(chan HBeatMessage, 1)
	gotAck := false
	lostCount := 0
	rtt := 3 * time.Second
	rttTimer := time.NewTimer(rtt)
	endWriteCh := make(chan struct{})
	seqNumTimeMap := make(map[uint64]time.Time)

	go hBeatMonitorRead(conn, readMsgCh)
	go hBeatMonitorWrite(conn, writeMsgCh, endWriteCh)

	util.PrintfBlue("send first hbeat with conn %v -> %v\n", conn.LocalAddr(), conn.RemoteAddr())
	writeMsgCh <- currWriteMsg // Send the initial message

	for {
		select {
		case m := <-readMsgCh:
			util.PrintfBlue("read an ack %v\n ", m)
			seqNum := m.HBEatSeqNum
			startTime, found := seqNumTimeMap[seqNum] // the seqNum is expected
			if found && m.HBEatEpochNonce == nounce {
				delete(seqNumTimeMap, seqNum)

				rttNew := time.Since(startTime)
				rtt = (rtt + rttNew) / 2
				// add 10 milisecond to help test on local system
				if rtt < 10*time.Millisecond {
					rtt = 10 * time.Millisecond
				}

				gotAck = true
				util.PrintfBlue("read legal ack: %v new rtt is: %v\n", m, rtt)
			}

		case <-rttTimer.C:
			util.PrintfBlue("rtt done, sending %v\n", currWriteMsg.SeqNum)
			if gotAck {
				util.PrintfBlue("got ack\n")
				lostCount = 0
			} else {
				util.PrintfBlue("ack delayed\n")
				lostCount++
			}
			gotAck = false

			if lostCount >= int(arg.LostMsgThresh) {
				// Sends a failure detected and sets doNotRead to false
				util.PrintfBlue("Exceeded lost msg threshold\n")
				conn.Close()
				notifyCh <- FailureDetected{
					UDPIpPort: arg.HBeatRemoteIPHBeatRemotePort,
					Timestamp: time.Now(),
				}
			} else {
				// Send the hbeat msg
				rttTimer = time.NewTimer(rtt)
				currWriteMsg.SeqNum++
				seqNumTimeMap[currWriteMsg.SeqNum] = time.Now()
				writeMsgCh <- currWriteMsg
			}

		case <-stopMonitorCh:
			conn.Close()
			endWriteCh <- struct{}{}
			util.PrintfBlue("hbeat monitor conn exited\n")
			allStopWg.Done()
			return
		}
	}
}

func setupHBeatAck(local_ip_port string) (string, error) {
	var err error
	var laddr *net.UDPAddr

	if local_ip_port == "" {
		return "", fmt.Errorf("fcheck needs a hbeatAck addr")
	}

	// resolve UDP address
	laddr, err = net.ResolveUDPAddr("udp", local_ip_port)
	if err != nil {
		return "", fmt.Errorf("error resolving: %v", err)
	}

	// setup UDP connection
	conn, err := net.ListenUDP("udp", laddr)
	if err != nil {
		return "", fmt.Errorf("error setting up UDP conn: %v", err)
	}

	go hBeatAckEventLoop(conn)
	return conn.LocalAddr().String(), nil
}

func hBeatAckRead(conn *net.UDPConn, readMsg chan<- hBeatAckReadMsg) {
	buffer := make([]byte, 1024)

	for {
		util.PrintfPurple("waiting to read monitor msg %v\n", conn.LocalAddr())
		n, raddr, err := conn.ReadFromUDP(buffer)
		if err != nil {
			util.PrintfPurple("error reading monitor msg from udp: %v\n", err)
			if isConnClosedError(err) {
				util.PrintfPurple("reading monitor msg goroutine exited\n")
				allStopWg.Done()
				return
			}
			continue
		}

		hbeatMsg, err := decodeHBeatMsg(buffer, n)
		if err != nil {
			util.PrintfPurple("error decoding monitor msg: %v\n", err)
			continue
		}

		util.PrintfPurple("read a monitor msg. raddr: %v\n", raddr)

		readMsg <- hBeatAckReadMsg{
			raddr,
			&hbeatMsg,
		}
	}
}

func hBeatAckWrite(conn *net.UDPConn, writeMsg <-chan hBeatAckWriteMsg, endWrite <-chan struct{}) {
	for {
		select {
		case sendAckMsg := <-writeMsg:
			_, err := conn.WriteToUDP(encodeAckMsg(sendAckMsg.msg), sendAckMsg.raddr)
			if err != nil {
				util.PrintfPurple("error writing ack msg: %v\n", err)
				if isConnClosedError(err) {
					return
				}
				continue
			}
		case <-endWrite:
			util.PrintfPurple("writing ack goroutine exited\n")
			allStopWg.Done()
			return
		}

	}
}

func hBeatAckEventLoop(conn *net.UDPConn) {
	readMsgCh := make(chan hBeatAckReadMsg, 1)
	writeMsgCh := make(chan hBeatAckWriteMsg, 1)
	endWriteCh := make(chan struct{})

	go hBeatAckRead(conn, readMsgCh)
	go hBeatAckWrite(conn, writeMsgCh, endWriteCh)

	for {
		select {
		case m := <-readMsgCh:
			util.PrintfPurple("responding to hbeat from %v\n", m.raddr)
			writeMsgCh <- hBeatAckWriteMsg{
				raddr: m.raddr,
				msg: &AckMessage{
					m.msg.EpochNonce,
					m.msg.SeqNum,
				},
			}

		case <-stopAckCh:
			conn.Close()
			endWriteCh <- struct{}{}
			util.PrintfPurple("hbeat ack conn exited\n")
			allStopWg.Done()
			return
		}
	}
}

func encodeAckMsg(msg *AckMessage) []byte {
	var buf bytes.Buffer
	gob.NewEncoder(&buf).Encode(msg)
	return buf.Bytes()
}

func decodeAckMsg(buf []byte, len int) (AckMessage, error) {
	var decoded AckMessage
	err := gob.NewDecoder(bytes.NewBuffer(buf[0:len])).Decode(&decoded)
	if err != nil {
		return AckMessage{}, err
	}
	return decoded, nil
}

func encodeHBeatMsg(msg *HBeatMessage) []byte {
	var buf bytes.Buffer
	gob.NewEncoder(&buf).Encode(msg)
	return buf.Bytes()
}

func decodeHBeatMsg(buf []byte, len int) (HBeatMessage, error) {
	var decoded HBeatMessage
	err := gob.NewDecoder(bytes.NewBuffer(buf[0:len])).Decode(&decoded)
	if err != nil {
		return HBeatMessage{}, err
	}
	return decoded, nil
}

func isConnClosedError(err error) bool {
	return errors.Is(err, net.ErrClosed)
}
