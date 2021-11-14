package main

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const max_tries = 3

const synod_timeout = time.Second

const max_pkt_size = 65507

var bind_addr = net.UDPAddr{IP: net.ParseIP("0.0.0.0")}

const channel_capacity = 10000

func proposer_addr(node Node) *net.UDPAddr {
	return &net.UDPAddr{
		Port: node.UdpStartPort,
		IP:   net.ParseIP(node.IpAddress)}
}

func acceptor_addr(node Node) *net.UDPAddr {
	return &net.UDPAddr{
		Port: node.UdpStartPort + 1,
		IP:   net.ParseIP(node.IpAddress)}
}

func learner_addr(node Node) *net.UDPAddr {
	return &net.UDPAddr{
		Port: node.UdpStartPort + 2,
		IP:   net.ParseIP(node.IpAddress)}
}

func make_listener(addr *net.UDPAddr) *net.UDPConn {
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		fmt.Printf("make_encoder: udp listen error: %v\n", err)
	}
	return conn
}

func send_msg_to_addr(msg *Message, addr *net.UDPAddr) {
	var b bytes.Buffer
	enc := gob.NewEncoder(&b)
	err := enc.Encode(msg)
	if err != nil {
		log.Fatalf("send_msg_to_addr: Marshal error: %v\n", err)
	}
	conn, err := net.Dial("udp", addr.String())
	if err != nil {
		log.Fatalf("udp Dial error: %v\n", err)
	}
	_, err = conn.Write(b.Bytes())
	if err != nil {
		log.Fatalf("udp Write error: %v\n", err)
	}
	conn.Close()
}

func recv_from_conn(conn *net.UDPConn) *Message {
	data := make([]byte, max_pkt_size)
	_, _, err := conn.ReadFromUDP(data)
	if err != nil {
		log.Fatalf("recv_from_conn: Read error %v\n", err)
	}
	var m Message
	r := bytes.NewReader(data)
	dec := gob.NewDecoder(r)
	err = dec.Decode(&m)
	if err != nil {
		log.Fatalf("recv_from_conn: Unmarshal error: %v\n", err)
	}
	fmt.Fprintf(os.Stderr, "received %s from %s\n",
		m.messageStr(), m.SenderID)
	return &m
}

func make_message_chan() chan Message {
	return make(chan Message, channel_capacity)
}

var item_names = [4]string{
	"surgical masks",
	"hand sanitizer bottles",
	"toilet paper rolls",
	"reeses peanut butter cups"}

var original_amounts = [4]int{500, 100, 200, 200}

// Node is a struct used for unmarshalling networking
// configurations from knownhosts.json for a single node
type Node struct {
	TcpStartPort int    `json:"tcp_start_port"`
	TcpEndPort   int    `json:"tcp_end_port"`
	UdpStartPort int    `json:"udp_start_port"`
	UdpEndPort   int    `json:"udp_end_port"`
	IpAddress    string `json:"ip_address"`
}

// Map is a struct used for unmarshalling networking
// configurations from knownhosts.json for all nodes
type Map struct {
	Hosts map[string]Node `json:"hosts"`
}

// OpCodeType is the op-code for the log event
// logOrder <-> insert(x); logCancel <-> delete(x)
type OpCodeType int

const (
	Order OpCodeType = iota
	Cancel
)

type LogEvent struct {
	OpCode      OpCodeType `json:"op_code"`
	Name        string     `json:"name"`
	Amounts     [4]int     `json:"amounts"`
	Proposer_id string     `json:"proposer_id"`
}

type Message struct {
	LogIndex int    `json:"log_index"`
	SenderID string `json:"sender_id"`
	// Prepare | Promise | Accept | Accepted | Acknowledge
	Contents interface{} `json:"contents"`
}

type PrepareMessage struct {
	PrepareNumber int `json:"prepare_number"`
}

type PromiseMessage struct {
	IsNull        bool     `json:"is_null"`
	PrepareNumber int      `json:"prepare_number"`
	AcceptNum     int      `json:"accept_num"`
	AcceptVal     LogEvent `json:"accept_val"`
}

type AcceptMessage struct {
	AcceptNum int      `json:"accept_num"`
	AcceptVal LogEvent `json:"accept_val"`
}

type AcceptedMessage struct {
	AcceptNum int      `json:"accept_num"`
	AcceptVal LogEvent `json:"accept_val"`
}

type AcknowledgeMessage struct {
	AcceptNum int `json:"accept_num"`
}

type Proposer struct {
	maxPropNum int
}

type Acceptor struct {
	maxPrepare int
	isNull     bool
	acceptNum  int
	acceptVal  LogEvent
}

type Learner struct {
	log              map[int]map[string]LogEvent
	committed        map[int]LogEvent
	logSize          int
	highestLogIndex  int
	inventoryAmounts [4]int
	numPeers         int
}

type Paxos struct {
	proposer_records map[int]Proposer
	acceptor_records map[int]Acceptor
	learner_records  Learner
	proposer_mlbx    map[int]chan Message
	mlbx_mtx         sync.Mutex
	gmtx             sync.Mutex
}

type Server struct {
	site_id       string
	peers         map[string]Node
	px            Paxos
	proposer_chan chan Message
	acceptor_chan chan Message
	learner_chan  chan Message
	stdin_c       chan string
	site_ord      int
}

func dflProposer(site_ord int) *Proposer {
	return &Proposer{maxPropNum: site_ord}
}

func dflAcceptor() *Acceptor {
	return &Acceptor{
		maxPrepare: 0,
		isNull:     true,
		acceptNum:  -1,
		acceptVal:  LogEvent{OpCode: Cancel, Name: "", Amounts: [4]int{-1. - 1. - 1. - 1}, Proposer_id: ""}}
}

func (l *Learner) getMajority(LogIndex int) *LogEvent {
	log_slot, ok := l.log[LogIndex]
	if !ok {
		return nil
	}

	if mjr, exists := l.committed[LogIndex]; exists {
		return &mjr
	}

	var majority_ev *LogEvent = nil
	majority_ev_str := ""
	majority_freq := 0

	for _, ev := range log_slot {
		ev_str := ev.logEventStr()
		if ev_str == majority_ev_str {
			majority_freq++
		} else {
			majority_freq--
			if majority_freq <= 0 {
				majority_freq = 1
				majority_ev = &ev
				majority_ev_str = ev_str
			}
		}
	}

	ct := 0
	for _, ev := range log_slot {
		ev_str := ev.logEventStr()
		if ev_str == majority_ev_str {
			ct++
		}
	}
	if ct*2 > l.numPeers {
		l.committed[LogIndex] = *majority_ev
		return majority_ev
	} else {
		return nil
	}
}

func newLearner(numPeers int) *Learner {
	return &Learner{
		log:             make(map[int]map[string]LogEvent),
		committed:       make(map[int]LogEvent),
		logSize:         0,
		highestLogIndex: -1,
		inventoryAmounts: [4]int{original_amounts[0],
			original_amounts[1],
			original_amounts[2],
			original_amounts[3]},
		numPeers: numPeers}
}

func newPaxos(numPeers int) *Paxos {
	return &Paxos{
		proposer_records: make(map[int]Proposer),
		acceptor_records: make(map[int]Acceptor),
		proposer_mlbx:    make(map[int]chan Message),
		learner_records:  *newLearner(numPeers),
		gmtx:             sync.Mutex{}}
}

func newServer(own_site_id string, peers Map) *Server {
	// newServer should also handle crash recovery
	// in the future

	site_ord := 0
	site_id_arr := make([]string, 0)
	for peer_site_id := range peers.Hosts {
		site_id_arr = append(site_id_arr, peer_site_id)
	}

	sort.Slice(site_id_arr, func(i, j int) bool {
		return site_id_arr[i] < site_id_arr[j]
	})

	for idx, peer_site_id := range site_id_arr {
		if peer_site_id == own_site_id {
			site_ord = idx + 1
			break
		}
	}

	s := Server{
		site_id:       own_site_id,
		peers:         peers.Hosts,
		px:            *newPaxos(len(peers.Hosts)),
		proposer_chan: make_message_chan(),
		acceptor_chan: make_message_chan(),
		learner_chan:  make_message_chan(),
		stdin_c:       make(chan string),
		site_ord:      site_ord}

	return &s
}

func send_all_helper(msg *Message, recipients *map[string]Node, addr_fn func(Node) *net.UDPAddr) {
	for site_id, node := range *recipients {
		fmt.Fprintf(os.Stderr, "sending %s to %s addr: %s\n", msg.messageStr(), site_id,
			addr_fn(node).String())
		send_msg_to_addr(msg, addr_fn(node))
	}
}

func (srv *Server) send_all(msg *Message) {
	switch (*msg).Contents.(type) {
	case PrepareMessage:
		send_all_helper(msg, &srv.peers, acceptor_addr)
	case AcceptMessage:
		send_all_helper(msg, &srv.peers, acceptor_addr)
	case AcceptedMessage:
		send_all_helper(msg, &srv.peers, learner_addr)
	default:
		log.Fatalf("send_all: invalid message format")
	}
}

func (srv *Server) send_to_id(msg *Message, site_id string) {
	var addr *net.UDPAddr
	switch msg.Contents.(type) {
	case PromiseMessage:
		addr = proposer_addr(srv.peers[site_id])
	case AcknowledgeMessage:
		addr = proposer_addr(srv.peers[site_id])
	default:
		log.Fatalf("send_to_id: Invalid Message Format\n")
	}

	fmt.Fprintf(os.Stderr, "sending %s to %s addr: %s\n", msg.messageStr(), site_id,
		addr.String())
	send_msg_to_addr(msg, addr)
}

func (prop *Proposer) create_prepare_message() PrepareMessage {
	return PrepareMessage{
		PrepareNumber: prop.maxPropNum}
}

func (acc *Acceptor) create_promise_message(PrepareNumber int) PromiseMessage {
	return PromiseMessage{
		IsNull:        acc.isNull,
		PrepareNumber: PrepareNumber,
		AcceptNum:     acc.acceptNum,
		AcceptVal:     acc.acceptVal}
}

func (acc *Acceptor) create_accepted_message() AcceptedMessage {
	if acc.isNull {
		log.Fatalf("create_accepted_message: acc is null")
	}
	return AcceptedMessage{
		AcceptNum: acc.acceptNum,
		AcceptVal: acc.acceptVal}
}

// Acceptor
func (srv *Server) handle_prepare(LogIndex int, SenderID string,
	prepareMsg PrepareMessage) {
	srv.px.gmtx.Lock()
	if _, ok := srv.px.acceptor_records[LogIndex]; !ok {
		srv.px.acceptor_records[LogIndex] = *dflAcceptor()
	}

	acceptor := srv.px.acceptor_records[LogIndex]

	if prepareMsg.PrepareNumber > acceptor.maxPrepare {
		acceptor.maxPrepare = prepareMsg.PrepareNumber
		srv.px.acceptor_records[LogIndex] = acceptor
		promiseMessageWrap := &Message{
			LogIndex: LogIndex,
			SenderID: srv.site_id,
			Contents: acceptor.create_promise_message(prepareMsg.PrepareNumber),
		}
		srv.send_to_id(promiseMessageWrap, SenderID)
	}

	srv.px.gmtx.Unlock()
}

// Acceptor
func (srv *Server) handle_accept(LogIndex int, SenderID string,
	acceptMsg AcceptMessage) {
	srv.px.gmtx.Lock()
	acceptor := srv.px.acceptor_records[LogIndex]
	if acceptMsg.AcceptNum >= acceptor.maxPrepare {
		acceptor.isNull = false
		acceptor.maxPrepare = acceptMsg.AcceptNum
		acceptor.acceptNum = acceptMsg.AcceptNum
		acceptor.acceptVal = acceptMsg.AcceptVal
		srv.px.acceptor_records[LogIndex] = acceptor
		acceptedMessageWrap := &Message{
			LogIndex: LogIndex,
			SenderID: srv.site_id,
			Contents: acceptor.create_accepted_message()}
		acknowledgeMessageWrap := &Message{
			LogIndex: LogIndex,
			SenderID: srv.site_id,
			Contents: AcknowledgeMessage{AcceptNum: acceptMsg.AcceptNum}}
		srv.send_to_id(acknowledgeMessageWrap, SenderID)
		srv.send_all(acceptedMessageWrap)
	}
	srv.px.gmtx.Unlock()
}

func (srv *Server) acceptor_loop() {
	for {
		msg := <-srv.acceptor_chan
		LogIndex := msg.LogIndex
		SenderID := msg.SenderID
		switch v := msg.Contents.(type) {
		case PrepareMessage:
			srv.handle_prepare(LogIndex, SenderID, v)
		case AcceptMessage:
			srv.handle_accept(LogIndex, SenderID, v)
		default:
			log.Fatalf("acceptor_loop: invalid message format\n")
		}
	}
}

func on_learned_str(ev *LogEvent) string {
	switch ev.OpCode {
	case Order:
		return fmt.Sprintf("Order submitted for %s.", ev.Name)
	case Cancel:
		return fmt.Sprintf("Order for %s cancelled.", ev.Name)
	default:
		log.Fatal("on_learned_str: invalid OpCode\n")
		return ""
	}
}

// To be used with gmtx acquired
func (srv *Server) get_corresponding_order(ev *LogEvent) *LogEvent {
	if ev.OpCode != Cancel {
		log.Fatalf("get_corresponding_order: invalid OpCode\n")
		return nil
	}

	counter := 0

	for lindex := srv.px.learner_records.highestLogIndex; lindex >= 0; lindex-- {
		mjr := srv.px.learner_records.getMajority(lindex)
		if mjr != nil && mjr.Name == ev.Name {
			if ev.OpCode == Cancel {
				counter -= 1
			} else {
				counter += 1
			}
			if counter > 0 {
				return mjr
			}
		}
	}
	return nil
}

// To be used with gmtx acquired
func (srv *Server) can_apply_log_event(ev *LogEvent) bool {
	NumHoles := srv.px.learner_records.highestLogIndex + 1 - srv.px.learner_records.logSize
	if NumHoles > 0 {
		return false
	}
	if ev.OpCode == Cancel {
		return srv.get_corresponding_order(ev) != nil
	} else {
		for i := 0; i < 4; i++ {
			if ev.Amounts[i] > srv.px.learner_records.inventoryAmounts[i] {
				return false
			}
		}
		return true
	}
}

// To be used with gmtx acquired
func (srv *Server) apply_log_event(ev *LogEvent) {
	if ev.OpCode == Cancel {
		associatedOrder := srv.get_corresponding_order(ev)
		for i := 0; i < 4; i++ {
			srv.px.learner_records.inventoryAmounts[i] += associatedOrder.Amounts[i]
		}
	} else {
		for i := 0; i < 4; i++ {
			srv.px.learner_records.inventoryAmounts[i] -= ev.Amounts[i]
		}
	}
}

// Learner
func (srv *Server) handle_accepted(LogIndex int, SenderID string,
	acceptedMsg AcceptedMessage) {
	srv.px.gmtx.Lock()
	if _, slot_exists := srv.px.learner_records.log[LogIndex]; !slot_exists {
		srv.px.learner_records.log[LogIndex] = make(map[string]LogEvent)
	}
	if _, accept_exists := srv.px.learner_records.log[LogIndex][SenderID]; !accept_exists {
		majorityBefore := srv.px.learner_records.getMajority(LogIndex)
		srv.px.learner_records.log[LogIndex][SenderID] = acceptedMsg.AcceptVal
		majorityAfter := srv.px.learner_records.getMajority(LogIndex)
		if majorityBefore == nil {

			if majorityAfter != nil {
				srv.apply_log_event(&acceptedMsg.AcceptVal)
				srv.px.learner_records.logSize++
				if LogIndex > srv.px.learner_records.highestLogIndex {
					srv.px.learner_records.highestLogIndex = LogIndex
				}
				if majorityAfter.Proposer_id == srv.site_id {
					fmt.Fprintln(os.Stdout, on_learned_str(majorityAfter))
				}
			}
		} else {
			if majorityAfter == nil || majorityBefore.logEventStr() != majorityAfter.logEventStr() {
				log.Fatal("handle_accepted: learned value changed!!!")
			}
		}
	}
	srv.px.gmtx.Unlock()
}

func (srv *Server) learner_loop() {
	for {
		msg := <-srv.learner_chan
		LogIndex := msg.LogIndex
		SenderID := msg.SenderID
		switch v := msg.Contents.(type) {
		case AcceptedMessage:
			srv.handle_accepted(LogIndex, SenderID, v)
		default:
			log.Fatalf("learner_loop: invalid message format\n")
		}
	}
}

func (srv *Server) synod_attempt(propVal *LogEvent, LogIndex int) {
	srv.px.mlbx_mtx.Lock()
	if _, mlbxExists := srv.px.proposer_mlbx[LogIndex]; !mlbxExists {
		srv.px.proposer_mlbx[LogIndex] = make_message_chan()
	} else {
		log.Fatalf("synod_attempt: two simultaneous synod attempts for the same log slot\n")
		return
	}

	mlbx := srv.px.proposer_mlbx[LogIndex]
	srv.px.mlbx_mtx.Unlock()
	srv.px.gmtx.Lock()
	if _, propExists := srv.px.proposer_records[LogIndex]; !propExists {
		srv.px.proposer_records[LogIndex] = *dflProposer(srv.site_ord)
	}
	proposer_record := srv.px.proposer_records[LogIndex]
	srv.px.gmtx.Unlock()

	for tries := 0; tries < max_tries; tries += 1 {

		srv.px.gmtx.Lock()
		proposer_record.maxPropNum += len(srv.peers)
		srv.px.proposer_records[LogIndex] = proposer_record

		prepareWrap := &Message{
			LogIndex: LogIndex,
			SenderID: srv.site_id,
			Contents: proposer_record.create_prepare_message()}
		srv.px.gmtx.Unlock()

		srv.send_all(prepareWrap)

		numPromises := make(map[string]bool)
		var acceptorAccVal *LogEvent = nil
		maxAcceptorAccNum := -1
		timer := time.After(synod_timeout)
		for {
			select {
			case msg := <-mlbx:
				switch v := msg.Contents.(type) {
				case PromiseMessage:
					if v.PrepareNumber != proposer_record.maxPropNum {
						continue
					}
					numPromises[msg.SenderID] = true
					if !v.IsNull && v.AcceptNum > maxAcceptorAccNum {
						maxAcceptorAccNum = v.AcceptNum
						acceptorAccVal = &v.AcceptVal
					}
					if len(numPromises)*2 > len(srv.peers) {
						goto afterPhase1
					}
				default:
					// skip non-promise messages
				}
			case <-timer:
				goto afterPhase1
			}
		}
	afterPhase1:
		if len(numPromises)*2 <= len(srv.peers) {
			continue
		}
		var acceptMsg *AcceptMessage
		if acceptorAccVal == nil {
			if propVal == nil {
				log.Fatal("synod_attempt: majority of acceptors with null acc val and null proposed val NOT ALLOWED")
			}
			acceptMsg = &AcceptMessage{
				AcceptNum: proposer_record.maxPropNum,
				AcceptVal: *propVal}
		} else {
			acceptMsg = &AcceptMessage{
				AcceptNum: proposer_record.maxPropNum,
				AcceptVal: *acceptorAccVal}
		}
		acceptMsgWrap := &Message{
			LogIndex: LogIndex,
			SenderID: srv.site_id,
			Contents: *acceptMsg}
		srv.send_all(acceptMsgWrap)
		timer = time.After(synod_timeout)
		numAcknowledges := make(map[string]bool)
		for {
			select {
			case msg := <-mlbx:
				switch v := msg.Contents.(type) {
				case AcknowledgeMessage:
					if v.AcceptNum == proposer_record.maxPropNum {
						numAcknowledges[msg.SenderID] = true
						if len(numAcknowledges)*2 > len(srv.peers) {
							goto afterPhase2
						}
					}
				default:
					// Skip non-acknowledge messages
				}
			case <-timer:
				goto afterPhase2
			}
		}
	afterPhase2:
		if len(numAcknowledges)*2 > len(srv.peers) {
			break
		}
	}

	srv.px.mlbx_mtx.Lock()
	delete(srv.px.proposer_mlbx, LogIndex)
	srv.px.mlbx_mtx.Unlock()
	<-time.After(synod_timeout)
}

func on_unsuccessful_commit_attempt_str(ev *LogEvent) string {
	switch ev.OpCode {
	case Order:
		return fmt.Sprintf("Cannot place order for %s.", ev.Name)
	case Cancel:
		return fmt.Sprintf("Cannot cancel order for %s.", ev.Name)
	default:
		log.Fatal("on_unsuccessful_commit_attempt_str: Invalid OpCode\n")
		return ""
	}
}

func (srv *Server) submit_proposal(propVal *LogEvent) {
	fmt.Fprintf(os.Stderr, "Submitting proposal %s\n", propVal.logEventStr())
	srv.px.gmtx.Lock()
	LogIndex := srv.px.learner_records.highestLogIndex + 1
	numHoles := LogIndex - srv.px.learner_records.logSize
	srv.px.gmtx.Unlock()
	fmt.Fprintf(os.Stderr, "Currently: %d holes\n", numHoles)
	if numHoles > 0 {
		var wg sync.WaitGroup
		wg.Add(numHoles)
		for i := LogIndex - 1; numHoles > 0; i-- {
			if srv.px.learner_records.getMajority(i) == nil {
				numHoles -= 1
				go func(lindex int) {
					srv.synod_attempt(nil, lindex)
					wg.Done()
				}(i)
			}
		}
		wg.Wait()
		fmt.Fprintln(os.Stderr, "Done attempting to fill holes")
	}

	srv.px.gmtx.Lock()
	canApply := srv.can_apply_log_event(propVal)
	srv.px.gmtx.Unlock()
	if canApply {
		// If canApply is no longer true, this would only happen
		// if this log slot is no longer free. Starting a synod attempt
		// in this case won't break the algorithm
		srv.synod_attempt(propVal, LogIndex)
	}

	srv.px.gmtx.Lock()
	mjr := srv.px.learner_records.getMajority(LogIndex)
	if mjr == nil || mjr.logEventStr() != propVal.logEventStr() {
		fmt.Fprintln(os.Stdout, on_unsuccessful_commit_attempt_str(propVal))
	}
	srv.px.gmtx.Unlock()
}

// Proposer
func (srv *Server) handle_order(name string, amt [4]int) {
	propVal := LogEvent{
		OpCode:      Order,
		Name:        name,
		Amounts:     amt,
		Proposer_id: srv.site_id}

	srv.submit_proposal(&propVal)
}

// Proposer
func (srv *Server) handle_cancel(name string) {
	propVal := LogEvent{
		OpCode:      Cancel,
		Name:        name,
		Amounts:     [4]int{-1, -1, -1, -1},
		Proposer_id: srv.site_id}
	srv.submit_proposal(&propVal)
}

// Proposer
func (srv *Server) handle_list_orders() {
	srv.px.gmtx.Lock()
	defer srv.px.gmtx.Unlock()
	counter := make(map[string]int)
	orders := make([]*LogEvent, 0)
	for lindex := srv.px.learner_records.highestLogIndex; lindex >= 0; lindex-- {
		if ev := srv.px.learner_records.getMajority(lindex); ev != nil {
			if _, exists := counter[ev.Name]; !exists {
				counter[ev.Name] = 0
			}
			if ev.OpCode == Cancel {
				counter[ev.Name] -= 1
			} else {
				counter[ev.Name] += 1
			}
			if counter[ev.Name] > 0 {
				orders = append(orders, ev)
			}
		}
	}
	sort.Slice(orders, func(i, j int) bool {
		return orders[i].Name < orders[j].Name
	})
	for _, order := range orders {
		fmt.Fprintf(os.Stdout, "%s %s\n", order.Name, amountsStr(order.Amounts))
	}
}

// Proposer
func (srv *Server) handle_list_inventory() {
	for idx, val := range item_names {
		fmt.Fprintf(os.Stdout, "%s %d\n",
			val, srv.px.learner_records.inventoryAmounts[idx])
	}
}

// Proposer
func (srv *Server) handle_list_log() {
	srv.px.gmtx.Lock()
	defer srv.px.gmtx.Unlock()
	for lindex := 0; lindex <= srv.px.learner_records.highestLogIndex; lindex++ {
		if mjr := srv.px.learner_records.getMajority(lindex); mjr != nil {
			if mjr.OpCode == Order {
				fmt.Fprintf(os.Stdout, "order %s %s\n", mjr.Name, amountsStr(mjr.Amounts))
			} else {
				fmt.Fprintf(os.Stdout, "cancel %s\n", mjr.Name)
			}
		} else {
			fmt.Fprint(os.Stdout, "\n")
		}
	}
}

func (srv *Server) on_user_input(user_input string) {
	args := strings.Fields(user_input)
	if len(args) == 0 {
		fmt.Println("invalid command")
	} else if args[0] == "order" {
		valid := true
		if len(args) != 3 {
			valid = false
		} else {
			name := args[1]
			amounts := parse_int_list(&args[2])
			if len(amounts) != 4 {
				valid = false
			} else {
				var amt [4]int
				copy(amt[:], amounts[0:4])
				srv.handle_order(name, amt)
			}
		}
		if !valid {
			fmt.Println("usage: order <customer_name> <#_of_masks>,<#_of_bottles>,<#_of_rolls>,<#_of_pbcups>")
		}
	} else if args[0] == "cancel" {
		if len(args) != 2 {
			fmt.Println("usage: cancel <customer_name>")
		} else {
			name := args[1]
			srv.handle_cancel(name)
		}

	} else if args[0] == "orders" {
		if len(args) != 1 {
			fmt.Println("usage: orders")
		} else {
			srv.handle_list_orders()
		}
	} else if args[0] == "inventory" {
		if len(args) != 1 {
			fmt.Println("usage: inventory")
		} else {
			srv.handle_list_inventory()
		}
	} else if args[0] == "log" {
		if len(args) != 1 {
			fmt.Println("usage: log")
		} else {
			srv.handle_list_log()
		}
	} else if args[0] == "quit" {
		os.Exit(0)
		// return // EXIT POINT
	} else {
		fmt.Println("invalid command")
	}
}

func (srv *Server) proposer_loop() {
	for {
		user_input := <-srv.stdin_c
		srv.on_user_input(user_input)
	}
}

func (srv *Server) proposer_netwk_loop(conn *net.UDPConn) {
	for {
		msg := recv_from_conn(conn)
		srv.px.mlbx_mtx.Lock()
		if mlbx, mlbxExists := srv.px.proposer_mlbx[msg.LogIndex]; mlbxExists {
			mlbx <- *msg
		}
		srv.px.mlbx_mtx.Unlock()
	}
}

func (srv *Server) run() {
	stdin_reader := bufio.NewReader(os.Stdin)
	p_addr := proposer_addr(srv.peers[srv.site_id])
	a_addr := acceptor_addr(srv.peers[srv.site_id])
	l_addr := learner_addr(srv.peers[srv.site_id])

	proposer_listener := make_listener(p_addr)
	acceptor_listener := make_listener(a_addr)
	learner_listener := make_listener(l_addr)

	go srv.proposer_netwk_loop(proposer_listener)
	go netwk_read_loop(srv.acceptor_chan, acceptor_listener)
	go netwk_read_loop(srv.learner_chan, learner_listener)
	go stdin_read_loop(srv.stdin_c, stdin_reader)
	go srv.learner_loop()
	go srv.acceptor_loop()
	fmt.Printf("Proposer listening at: %s\n", p_addr.String())
	fmt.Printf("Acceptor listening at: %s\n", a_addr.String())
	fmt.Printf("Learner listening at: %s\n", l_addr.String())
	srv.proposer_loop()
}

func (ev *LogEvent) logEventStr() string {
	if ev == nil {
		return ""
	}
	switch ev.OpCode {
	case Order:
		return fmt.Sprintf("order %s %s @ %s",
			ev.Name, amountsStr(ev.Amounts), ev.Proposer_id)
	case Cancel:
		return fmt.Sprintf("cancel %s @ %s", ev.Name, ev.Proposer_id)
	default:
		log.Fatal("logEventStr: Invalid Opcode")
		return ""
	}
}
func (msg *Message) messageStr() string {
	switch v := msg.Contents.(type) {
	case PrepareMessage:
		return fmt.Sprintf("prepare[%d](%d)",
			msg.LogIndex, v.PrepareNumber)
	case PromiseMessage:
		if v.IsNull {
			return fmt.Sprintf("promise[%d](null, null, prepare:%d)",
				msg.LogIndex, v.PrepareNumber)
		}
		return fmt.Sprintf("promise[%d](%d, `%s`)",
			msg.LogIndex, v.AcceptNum,
			v.AcceptVal.logEventStr())
	case AcceptMessage:
		return fmt.Sprintf("accept[%d](%d, `%s`)",
			msg.LogIndex, v.AcceptNum,
			v.AcceptVal.logEventStr())
	case AcceptedMessage:
		return fmt.Sprintf("accepted[%d](%d, `%s`)",
			msg.LogIndex, v.AcceptNum,
			v.AcceptVal.logEventStr())
	case AcknowledgeMessage:
		return fmt.Sprintf("ack[%d](%d)", msg.LogIndex, v.AcceptNum)
	default:
		log.Fatal("messageStr: Invalid Message Format")
		return ""
	}
}

// amountsStr converts an array of 4 integers representing
// the inventory to a comma delimited string.
func amountsStr(amounts [4]int) string {
	var amountsStrVec [4]string
	for i := 0; i < 4; i++ {
		amountsStrVec[i] = fmt.Sprint(amounts[i])
	}
	return strings.Join(amountsStrVec[:], ",")
}

// stdin_read_loop infinitely loops while polling the stdin
// file descriptor for user input, and passing that to the
// user channel
func stdin_read_loop(stdin_c chan string, reader *bufio.Reader) {
	for {
		b, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				fmt.Fprintf(os.Stderr, "Read error: %v\n", err)
			}
			break
		}
		stdin_c <- b
	}
}

// netwk_read_loop infinitely loops while polling a UDP socket
// for messages from other nodes. Upon receiving, it will push
// unmarshal and then push messages to a network message
// designated channel
func netwk_read_loop(netwk_c chan Message, conn *net.UDPConn) {
	for {
		m := recv_from_conn(conn)
		netwk_c <- *m
	}
}

// parse a comma separated list of integers
// into an array of ints
func parse_int_list(line *string) []int {
	str_list := strings.Split(*line, ",")
	arr := make([]int, len(str_list))

	for i := 0; i < len(arr); i++ {
		tmp, err := strconv.ParseInt(str_list[i], 10, 31)
		if err != nil {
			return make([]int, 0)
		}
		arr[i] = int(tmp)
	}
	return arr
}

func main() {
	args := os.Args

	gob.Register(Message{})
	gob.Register(AcknowledgeMessage{})
	gob.Register(PrepareMessage{})
	gob.Register(AcceptMessage{})
	gob.Register(AcceptedMessage{})
	gob.Register(PromiseMessage{})
	gob.Register(LogEvent{})

	if len(args) != 2 {
		log.Fatal("USAGE: ./main <site_id>")
	}

	site_id := args[1]
	knownhosts_f, err := os.Open("knownhosts.json")

	if err != nil {
		log.Fatalf("Error opening knownhosts.json: %v\r\n", err)
	}

	byteArr, _ := ioutil.ReadAll(knownhosts_f)
	var peers Map
	err = json.Unmarshal(byteArr, &peers)
	if err != nil {
		log.Fatalf("Error unmarshalling in main: %v\n", err)
	}

	s := newServer(site_id, peers)
	s.run()
}
