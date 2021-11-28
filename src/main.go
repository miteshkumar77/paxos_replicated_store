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
	"math/rand"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const stable_path = "stable_storage.txt"

// Maximum number of proposal
// tries before declaring temporary failure
const max_tries = 3

const synod_timeout = time.Millisecond * 200

// const synod_timeout = time.Second * 5

const max_pkt_size = 65507

// For channel non-blocking
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
	// go func(msg Message, addr *net.UDPAddr) {
	// 	n := rand.Intn(3)
	// 	time.Sleep(time.Duration(n) * time.Second)
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
	// }(*msg, addr)
}

// Receive helper
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

// Initial inventory
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

// Data stored within a single log slot
type LogEvent struct {
	OpCode      OpCodeType `json:"op_code"`
	Name        string     `json:"name"`
	Amounts     [4]int     `json:"amounts"`
	Proposer_id string     `json:"proposer_id"`
}

// Message represents messages sent by the paxos algorithm
// Contents interface{} can be specialized to separate forms
// but all messages contain the LogIndex that they pertain to,
// and the id of the sender.
type Message struct {
	LogIndex int    `json:"log_index"`
	SenderID string `json:"sender_id"`
	// Prepare | Promise | Accept | Accepted | Acknowledge | Nack
	Contents interface{} `json:"contents"`
}

type NackMessage struct {
	PrepareNumber  int `json:"prepare_number"`
	ProposalNumber int `json:"proposal_number"`
}

type PrepareMessage struct {
	ProposalNumber int `json:"proposal_number"`
}

type PromiseMessage struct {
	IsNull         bool     `json:"is_null"`
	ProposalNumber int      `json:"proposal_number"`
	AcceptNum      int      `json:"accept_num"`
	AcceptVal      LogEvent `json:"accept_val"`
}

type AcceptMessage struct {
	ProposalNumber int      `json:"accept_num"`
	ProposalVal    LogEvent `json:"accept_val"`
}

type AcceptedMessage struct {
	AcceptNum int      `json:"accept_num"`
	AcceptVal LogEvent `json:"accept_val"`
}

type AcknowledgeMessage struct {
	ProposalNumber int `json:"accept_num"`
}

// Proposer state for a single LogIndex,
// is saved to stable storage.
type Proposer struct {
	MaxPropNum int
}

// Acceptor state for a single LogIndex,
// is saved to stable storage.
type Acceptor struct {
	MaxPrepare int
	IsNull     bool
	AcceptNum  int
	AcceptVal  LogEvent
}

// Learner state for all log indices,
// is saved to stable storage.
type Learner struct {
	// Each log slot is a map of the proposal number to
	// a map of the values accepted by acceptors for that proposal
	// number
	Log map[int]map[int]map[string]LogEvent

	// We cache the majority in Committed so that we don't have
	// to recompute it, since it does not change.
	Committed map[int]LogEvent

	// Number of committed entries in the log.
	LogSize int

	// Highest committed log index, is used to detect holes
	HighestLogIndex int

	// Number of peers, is used to detect majority
	NumPeers int
}

// Paxos state for all log indices
type Paxos struct {
	// LogIndex -> Proposer State
	proposer_records map[int]Proposer

	// LogIndex -> Learner State
	acceptor_records map[int]Acceptor
	learner_records  Learner
	inventoryAmounts [4]int

	// LogIndex -> Mailbox of messages pertaining to a particular log slot
	proposer_mlbx map[int]chan Message

	mlbx_mtx sync.Mutex
	gmtx     sync.Mutex
}

// State that we intend to save to stable storage
// and recover using.
type StableState struct {
	Proposer_records map[int]Proposer
	Acceptor_records map[int]Acceptor
	Learner_records  Learner
}

// The Server state encapsulates all paxos state, peer
// addressing information, synchronization mechanisms,
// user interface, and paxos algorithm subroutines.
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

// Create a new default proposer state object
func dflProposer(site_ord int) *Proposer {
	return &Proposer{MaxPropNum: site_ord}
}

// Create a new default acceptor state object
func dflAcceptor() *Acceptor {
	return &Acceptor{
		MaxPrepare: 0,
		IsNull:     true,
		AcceptNum:  -1,
		AcceptVal:  LogEvent{OpCode: Cancel, Name: "", Amounts: [4]int{-1. - 1. - 1. - 1}, Proposer_id: ""}}
}

// Serialize all stable state of the server to a
// file using the 'gob' go binary format.
func (srv *Server) dump_stable_state() {
	store := StableState{
		Proposer_records: srv.px.proposer_records,
		Acceptor_records: srv.px.acceptor_records,
		Learner_records:  srv.px.learner_records}

	storage_file, err := os.OpenFile(stable_path,
		os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		log.Fatalf("dump_stable_state: file %s open error: %v\n",
			stable_path, err)
	}
	enc := gob.NewEncoder(storage_file)
	err = enc.Encode(&store)
	if err != nil {
		fmt.Printf("dump_stable_state: file %s encode error: %v\n",
			stable_path, err)
	}
	err = storage_file.Close()
	if err != nil {
		fmt.Printf("dump_stable_state: file %s close error: %v\n",
			stable_path, err)
	}
}

// Read contents of stable storage file into
// StableState, and return a pointer to it if successful,
// otherwise return nil.
func read_stable_state() *StableState {
	var store StableState
	storage_file, err := os.Open(stable_path)
	if err != nil {
		log.Printf("read_stable_state: file %s open error: %v\n",
			stable_path, err)
		return nil
	}
	dec := gob.NewDecoder(storage_file)
	err = dec.Decode(&store)
	if err != nil {
		if err != io.EOF {
			fmt.Printf("read_stable_state: file %s decode error: %v\n",
				stable_path, err)
		}
		return nil
	}
	err = storage_file.Close()
	if err != nil {
		fmt.Printf("read_stable_state: file %s close error: %v\n",
			stable_path, err)
	}
	return &store
}

// Find the majority accepted LogEvent object for a particular log index
// and return a pointer to it, or return nil if no majority exists
// this is equivalent to returning the 'chosen' value at a particular
// log index
func (l *Learner) getMajority(LogIndex int) *LogEvent {
	log_slot, ok := l.Log[LogIndex]
	if !ok {
		return nil
	}

	if mjr, exists := l.Committed[LogIndex]; exists {
		return &mjr
	}

	for _, prop_accs := range log_slot {
		var majority_ev *LogEvent = nil
		majority_ev_str := ""
		majority_freq := 0
		for _, ev := range prop_accs {
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
		for _, ev := range prop_accs {
			ev_str := ev.logEventStr()
			if ev_str == majority_ev_str {
				ct++
			}
		}
		if ct*2 > l.NumPeers {
			l.Committed[LogIndex] = *majority_ev
			return majority_ev
		}
	}
	return nil
}

// Create a copy of the default inventory amounts
func dfl_inventory() [4]int {
	return [4]int{original_amounts[0],
		original_amounts[1],
		original_amounts[2],
		original_amounts[3]}
}

// Calculate the inventory amounts based on the filled entries
// in the log of a stable state object. Used in recovery.
func calc_inventory(stable_state *StableState) [4]int {
	ret := dfl_inventory()
	for lindex := 0; lindex <= stable_state.Learner_records.HighestLogIndex; lindex++ {
		mjr := stable_state.Learner_records.getMajority(lindex)
		if mjr != nil {
			m := -1
			if mjr.OpCode == Cancel {
				m = 1
			}
			for i := 0; i < 4; i++ {
				ret[i] += m * mjr.Amounts[i]
			}
		}
	}
	return ret
}

// Create a brand new learner object (if loading from stable storage failed).
func newLearner(numPeers int) *Learner {
	return &Learner{
		Log:             make(map[int]map[int]map[string]LogEvent),
		Committed:       make(map[int]LogEvent),
		LogSize:         0,
		HighestLogIndex: -1,
		NumPeers:        numPeers}
}

// Create a new paxos object, instantiate using stable state if possible.
func newPaxos(numPeers int) *Paxos {
	stable_state := read_stable_state()
	if stable_state == nil {
		return &Paxos{
			proposer_records: make(map[int]Proposer),
			acceptor_records: make(map[int]Acceptor),
			proposer_mlbx:    make(map[int]chan Message),
			inventoryAmounts: dfl_inventory(),
			learner_records:  *newLearner(numPeers),
			gmtx:             sync.Mutex{}}
	}
	return &Paxos{
		proposer_records: stable_state.Proposer_records,
		acceptor_records: stable_state.Acceptor_records,
		proposer_mlbx:    make(map[int]chan Message),
		inventoryAmounts: calc_inventory(stable_state),
		learner_records:  stable_state.Learner_records,
		gmtx:             sync.Mutex{}}
}

// Create a new server
func newServer(own_site_id string, peers Map) *Server {
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

// Helper function to send a message to all peers
// by utilizing a passed in addr_fn function used to translate
// a site to its address (depending on if we want to send to acceptors or learners of that site)
func send_all_helper(msg *Message, recipients *map[string]Node, addr_fn func(Node) *net.UDPAddr) {
	for site_id, node := range *recipients {
		fmt.Fprintf(os.Stderr, "sending %s to %s addr: %s\n", msg.messageStr(), site_id,
			addr_fn(node).String())
		send_msg_to_addr(msg, addr_fn(node))
	}
}

// method to send a message to all peers.
// sends prepare messages to all acceptors
//       accept message to all acceptors
//       accepted message to all learners
// fails for any other type of message
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

// method to send a message to one proposer at site site_id
// sends promise message, acknowledge message, nack message
// fails for any other type of message
func (srv *Server) send_to_id(msg *Message, site_id string) {
	var addr *net.UDPAddr
	switch msg.Contents.(type) {
	case PromiseMessage:
		addr = proposer_addr(srv.peers[site_id])
	case AcknowledgeMessage:
		addr = proposer_addr(srv.peers[site_id])
	case NackMessage:
		addr = proposer_addr(srv.peers[site_id])
	default:
		log.Fatalf("send_to_id: Invalid Message Format\n")
	}

	fmt.Fprintf(os.Stderr, "sending %s to %s addr: %s\n", msg.messageStr(), site_id,
		addr.String())
	send_msg_to_addr(msg, addr)
}

// Create a prepare message based on a proposer's current MaxPropNum.
func (prop *Proposer) create_prepare_message() PrepareMessage {
	return PrepareMessage{
		ProposalNumber: prop.MaxPropNum}
}

// Create a promise message based on the acceptor's current state and a
// prepare number of the current round.
func (acc *Acceptor) create_promise_message(PrepareNumber int) PromiseMessage {
	return PromiseMessage{
		IsNull:         acc.IsNull,
		ProposalNumber: PrepareNumber,
		AcceptNum:      acc.AcceptNum,
		AcceptVal:      acc.AcceptVal}
}

// Create an accepted message based on the acceptor's current state
// throws an error if the acceptor hasn't yet accepted a value.
func (acc *Acceptor) create_accepted_message() AcceptedMessage {
	if acc.IsNull {
		log.Fatalf("create_accepted_message: acc is null")
	}
	return AcceptedMessage{
		AcceptNum: acc.AcceptNum,
		AcceptVal: acc.AcceptVal}
}

// Acceptor: handle a prepare message for a particular log index
func (srv *Server) handle_prepare(LogIndex int, SenderID string,
	prepareMsg PrepareMessage) {
	srv.px.gmtx.Lock()

	// If there isn't an acceptor record for this particular
	// prepare message yet, then create one.
	if _, ok := srv.px.acceptor_records[LogIndex]; !ok {
		srv.px.acceptor_records[LogIndex] = *dflAcceptor()
	}

	acceptor := srv.px.acceptor_records[LogIndex]

	if prepareMsg.ProposalNumber > acceptor.MaxPrepare {
		acceptor.MaxPrepare = prepareMsg.ProposalNumber
		srv.px.acceptor_records[LogIndex] = acceptor
		promiseMessageWrap := &Message{
			LogIndex: LogIndex,
			SenderID: srv.site_id,
			Contents: acceptor.create_promise_message(prepareMsg.ProposalNumber),
		}

		// Only need to dump stable state if a change to
		// acceptor's state was made. Do this before sending.
		//
		// The algorithm can handle message loss, so if we crash
		// before the message was sent, then it can be treated as if
		// the prepare message was lost.
		srv.dump_stable_state()
		srv.send_to_id(promiseMessageWrap, SenderID)
	} else {
		nackMessageWrap := &Message{
			LogIndex: LogIndex,
			SenderID: srv.site_id,
			Contents: NackMessage{
				ProposalNumber: prepareMsg.ProposalNumber,
				PrepareNumber:  acceptor.MaxPrepare}}
		srv.send_to_id(nackMessageWrap, SenderID)
	}

	srv.px.gmtx.Unlock()
}

// Acceptor: handle an accept message for a particular log index.
func (srv *Server) handle_accept(LogIndex int, SenderID string,
	acceptMsg AcceptMessage) {
	srv.px.gmtx.Lock()

	// If there isn't an acceptor record for this particular
	// prepare message yet, then create one.
	//
	// This is necessary even for handle_accept because an acceptor
	// didn't have to send a prepare in order to get an accept message
	// I.e. the proposer only needed promises from a majority.
	if _, ok := srv.px.acceptor_records[LogIndex]; !ok {
		srv.px.acceptor_records[LogIndex] = *dflAcceptor()
	}

	acceptor := srv.px.acceptor_records[LogIndex]

	// TWO POSSIBILITIES, not sure which one is better.

	// We can accept this proposed value if
	// ProposalNumber >= MaxPrepare and
	// if the ProposalNumber was 0 then
	//    the previous majority the previous majority must be known
	//    and was proposed by the same site
	// prevMjr := srv.px.learner_records.getMajority(LogIndex - 1)
	// canAccept := acceptMsg.ProposalNumber >= acceptor.MaxPrepare &&
	// 	((acceptMsg.ProposalNumber != 0) ||
	// 		(prevMjr != nil || prevMjr.Proposer_id == SenderID))

	// We can accept this proposed value if
	// ProposalNumber >= MaxPrepare
	canAccept := acceptMsg.ProposalNumber >= acceptor.MaxPrepare

	if canAccept {
		acceptor.IsNull = false
		acceptor.MaxPrepare = acceptMsg.ProposalNumber
		acceptor.AcceptNum = acceptMsg.ProposalNumber
		acceptor.AcceptVal = acceptMsg.ProposalVal
		srv.px.acceptor_records[LogIndex] = acceptor
		acceptedMessageWrap := &Message{
			LogIndex: LogIndex,
			SenderID: srv.site_id,
			Contents: acceptor.create_accepted_message()}
		acknowledgeMessageWrap := &Message{
			LogIndex: LogIndex,
			SenderID: srv.site_id,
			Contents: AcknowledgeMessage{ProposalNumber: acceptMsg.ProposalNumber}}

		// dump to stable storage only if change to state was made
		// we should save before sending a message in order to ensure
		// correctness.
		srv.dump_stable_state()
		srv.send_to_id(acknowledgeMessageWrap, SenderID)
		srv.send_all(acceptedMessageWrap)
	} else {
		nackMessageWrap := &Message{
			LogIndex: LogIndex,
			SenderID: srv.site_id,
			Contents: NackMessage{
				ProposalNumber: acceptMsg.ProposalNumber,
				PrepareNumber:  acceptor.MaxPrepare}}
		srv.send_to_id(nackMessageWrap, SenderID)
	}
	srv.px.gmtx.Unlock()
}

// acceptor loop runs on its own thread,
// receives messages from other peers and
// trigger message handlers.
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

// Return string to print out when a learner
// a log index that came from the proposer on the same
// site id as itself.
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
// get the corresponding order event of a particular cancel log event that is to be added
func (srv *Server) get_corresponding_order(ev *LogEvent) *LogEvent {
	if ev.OpCode != Cancel {
		log.Fatalf("get_corresponding_order: invalid OpCode\n")
		return nil
	}

	for lindex := srv.px.learner_records.HighestLogIndex; lindex >= 0; lindex-- {
		mjr := srv.px.learner_records.getMajority(lindex)
		if mjr != nil && mjr.Name == ev.Name {
			switch mjr.OpCode {
			case Cancel:
				return nil
			case Order:
				return mjr
			}
		}
	}
	return nil
}

// To be used with gmtx acquired
func (srv *Server) can_apply_log_event(ev *LogEvent) bool {
	NumHoles := srv.px.learner_records.HighestLogIndex + 1 - srv.px.learner_records.LogSize
	if NumHoles > 0 {
		return false
	}
	if ev.OpCode == Cancel {
		return srv.get_corresponding_order(ev) != nil
	} else {
		for i := 0; i < 4; i++ {
			if ev.Amounts[i] > srv.px.inventoryAmounts[i] {
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
			srv.px.inventoryAmounts[i] += associatedOrder.Amounts[i]
		}
	} else {
		for i := 0; i < 4; i++ {
			srv.px.inventoryAmounts[i] -= ev.Amounts[i]
		}
	}
}

// Learner: handle an accepted message from an acceptor
func (srv *Server) handle_accepted(LogIndex int, SenderID string,
	acceptedMsg AcceptedMessage) {
	srv.px.gmtx.Lock()

	// Create a default acceptor log entry for this slot, if one does not exist.
	if _, slot_exists := srv.px.learner_records.Log[LogIndex]; !slot_exists {
		srv.px.learner_records.Log[LogIndex] = make(map[int]map[string]LogEvent)
	}

	if _, prop_exists := srv.px.learner_records.Log[LogIndex][acceptedMsg.AcceptNum]; !prop_exists {
		srv.px.learner_records.Log[LogIndex][acceptedMsg.AcceptNum] = make(map[string]LogEvent)
	}

	// Get the majority before applying accepted message
	majorityBefore := srv.px.learner_records.getMajority(LogIndex)
	srv.px.learner_records.Log[LogIndex][acceptedMsg.AcceptNum][SenderID] = acceptedMsg.AcceptVal

	// Majority after applying accepted message
	majorityAfter := srv.px.learner_records.getMajority(LogIndex)
	if majorityBefore == nil {
		if majorityAfter != nil {
			srv.apply_log_event(&acceptedMsg.AcceptVal)
			srv.px.learner_records.LogSize++
			if LogIndex > srv.px.learner_records.HighestLogIndex {
				srv.px.learner_records.HighestLogIndex = LogIndex
			}
			if majorityAfter.Proposer_id == srv.site_id {
				fmt.Fprintln(os.Stdout, on_learned_str(majorityAfter))
			}
		}
		srv.dump_stable_state()
	} else {
		if majorityAfter == nil || majorityBefore.logEventStr() != majorityAfter.logEventStr() {
			log.Fatal("handle_accepted: learned value changed!!!")
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

// Check if the current site was the proposer of the previous log slot
// majority.
func (srv *Server) can_skip_phase_1(LogIndex int) bool {
	mjr := srv.px.learner_records.getMajority(LogIndex - 1)
	return (mjr != nil && mjr.Proposer_id == srv.site_id)
}

// proposer phase1/phase2 algorithm for synod
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
	nackMaxPrepare := 0

	for tries := 0; tries < max_tries; tries += 1 {

		srv.px.gmtx.Lock()
		canSkipP1 := srv.can_skip_phase_1(LogIndex) && tries == 0 &&
			srv.px.proposer_records[LogIndex].MaxPropNum == srv.site_ord
		prepareMsg := proposer_record.create_prepare_message()
		currentRoundProposalNum := prepareMsg.ProposalNumber
		if canSkipP1 {
			currentRoundProposalNum = 0
		}
		prepareWrap := &Message{
			LogIndex: LogIndex,
			SenderID: srv.site_id,
			Contents: prepareMsg}
		srv.px.gmtx.Unlock()

		numPromises := make(map[string]bool)
		numNacks := make(map[string]bool)
		numAcknowledges := make(map[string]bool)
		var acceptMsgWrap *Message
		var acceptMsg *AcceptMessage
		var acceptorAccVal *LogEvent = nil
		var timer <-chan time.Time
		maxAcceptorAccNum := -1
		if canSkipP1 {
			goto afterPhase1
		}
		srv.send_all(prepareWrap)
		timer = time.After(synod_timeout)
		for {
			select {
			case msg := <-mlbx:
				switch v := msg.Contents.(type) {
				case PromiseMessage:
					if v.ProposalNumber == currentRoundProposalNum {
						numPromises[msg.SenderID] = true
						if !v.IsNull && v.AcceptNum > maxAcceptorAccNum {
							maxAcceptorAccNum = v.AcceptNum
							acceptorAccVal = &v.AcceptVal
						}
						if len(numPromises)*2 > len(srv.peers) {
							goto afterPhase1
						}
					}
				case NackMessage:
					if v.ProposalNumber == currentRoundProposalNum {
						if v.PrepareNumber > nackMaxPrepare {
							nackMaxPrepare = v.PrepareNumber
						}
						// proposer_record.maxPropNum: the proposal number for the current round
						if proposer_record.MaxPropNum < nackMaxPrepare {
							proposer_record.MaxPropNum = proposer_record.MaxPropNum +
								((nackMaxPrepare-proposer_record.MaxPropNum+len(srv.peers)-1)/len(srv.peers))*len(srv.peers)
							// maxPropNum = ceil((nackMaxPrepare - maxPropNum)/N) * N + maxPropNum
						}

						srv.px.gmtx.Lock()
						srv.px.proposer_records[LogIndex] = proposer_record
						srv.dump_stable_state()
						srv.px.gmtx.Unlock()

						numNacks[msg.SenderID] = true
						if len(numNacks)*2 > len(srv.peers) {
							goto afterPhase1
						}
					}

				default:
					// skip non-promise messages
				}
			case <-timer:
				goto afterPhase1
			}
		}
	afterPhase1:
		if !canSkipP1 && len(numPromises)*2 <= len(srv.peers) {
			goto afterPhase2
		}
		if acceptorAccVal == nil {
			if propVal == nil {
				// This only happens during recovery
				goto afterPhase2
			}
			acceptMsg = &AcceptMessage{
				ProposalNumber: currentRoundProposalNum,
				ProposalVal:    *propVal}
		} else {
			acceptMsg = &AcceptMessage{
				ProposalNumber: currentRoundProposalNum,
				ProposalVal:    *acceptorAccVal}
		}
		acceptMsgWrap = &Message{
			LogIndex: LogIndex,
			SenderID: srv.site_id,
			Contents: *acceptMsg}

		for k := range numNacks {
			delete(numNacks, k)
		}

		srv.send_all(acceptMsgWrap)
		timer = time.After(synod_timeout)

		for {
			select {
			case msg := <-mlbx:
				switch v := msg.Contents.(type) {
				case AcknowledgeMessage:
					if v.ProposalNumber == currentRoundProposalNum {
						numAcknowledges[msg.SenderID] = true
						if len(numAcknowledges)*2 > len(srv.peers) {
							goto afterPhase2
						}
					}
				case NackMessage:
					if v.ProposalNumber == currentRoundProposalNum {
						numNacks[msg.SenderID] = true
						if v.PrepareNumber > nackMaxPrepare {
							nackMaxPrepare = v.PrepareNumber
						}
						// proposer_record.maxPropNum: the proposal number for the current round
						if proposer_record.MaxPropNum < nackMaxPrepare {
							proposer_record.MaxPropNum = proposer_record.MaxPropNum +
								((nackMaxPrepare-proposer_record.MaxPropNum+len(srv.peers)-1)/len(srv.peers))*len(srv.peers)
							// maxPropNum = ceil((nackMaxPrepare - maxPropNum)/N) * N + maxPropNum
						}

						srv.px.gmtx.Lock()
						srv.px.proposer_records[LogIndex] = proposer_record
						srv.dump_stable_state()
						srv.px.gmtx.Unlock()
						if len(numNacks)*2 > len(srv.peers) {
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
		if currentRoundProposalNum+len(srv.peers) > proposer_record.MaxPropNum {
			proposer_record.MaxPropNum = currentRoundProposalNum + len(srv.peers)
			srv.px.gmtx.Lock()
			srv.px.proposer_records[LogIndex] = proposer_record
			srv.dump_stable_state()
			srv.px.gmtx.Unlock()
		}

		if len(numAcknowledges)*2 > len(srv.peers) {
			break
		}
	}

	srv.px.mlbx_mtx.Lock()
	delete(srv.px.proposer_mlbx, LogIndex)
	srv.px.mlbx_mtx.Unlock()
	<-time.After(2 * synod_timeout)
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

func (srv *Server) fill_holes(LogIndex, numHoles int) {
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
	}
}

func (srv *Server) synod_recover() {
	srv.px.gmtx.Lock()
	LogIndex := srv.px.learner_records.HighestLogIndex + 1
	numHoles := LogIndex - srv.px.learner_records.LogSize
	srv.px.gmtx.Unlock()

	srv.fill_holes(LogIndex, numHoles)

	srv.synod_attempt(nil, LogIndex)

	srv.px.gmtx.Lock()
	mjr := srv.px.learner_records.getMajority(LogIndex)
	cont := mjr != nil
	srv.px.gmtx.Unlock()
	if cont {
		srv.synod_recover()
	}
}

func (srv *Server) submit_proposal(propVal *LogEvent) {
	srv.px.gmtx.Lock()
	LogIndex := srv.px.learner_records.HighestLogIndex + 1
	numHoles := LogIndex - srv.px.learner_records.LogSize
	srv.px.gmtx.Unlock()

	srv.fill_holes(LogIndex, numHoles)

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
	for lindex := srv.px.learner_records.HighestLogIndex; lindex >= 0; lindex-- {
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
			val, srv.px.inventoryAmounts[idx])
	}
}

// Proposer
func (srv *Server) handle_list_log() {
	srv.px.gmtx.Lock()
	defer srv.px.gmtx.Unlock()
	for lindex := 0; lindex <= srv.px.learner_records.HighestLogIndex; lindex++ {
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

	// Recovery:
	srv.synod_recover()

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
			msg.LogIndex, v.ProposalNumber)
	case PromiseMessage:
		if v.IsNull {
			return fmt.Sprintf("promise[%d](null, null, prepare:%d)",
				msg.LogIndex, v.ProposalNumber)
		}
		return fmt.Sprintf("promise[%d](%d, `%s`)",
			msg.LogIndex, v.AcceptNum,
			v.AcceptVal.logEventStr())
	case AcceptMessage:
		return fmt.Sprintf("accept[%d](%d, `%s`)",
			msg.LogIndex, v.ProposalNumber,
			v.ProposalVal.logEventStr())
	case AcceptedMessage:
		return fmt.Sprintf("accepted[%d](%d, `%s`)",
			msg.LogIndex, v.AcceptNum,
			v.AcceptVal.logEventStr())
	case AcknowledgeMessage:
		return fmt.Sprintf("ack[%d](%d)", msg.LogIndex, v.ProposalNumber)
	case NackMessage:
		return fmt.Sprintf("nack[%d](prepare_number: %d, proposal_number: %d)",
			msg.LogIndex, v.PrepareNumber, v.ProposalNumber)
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
	rand.Seed(time.Now().UnixNano())
	args := os.Args

	gob.Register(Message{})
	gob.Register(AcknowledgeMessage{})
	gob.Register(PrepareMessage{})
	gob.Register(AcceptMessage{})
	gob.Register(AcceptedMessage{})
	gob.Register(PromiseMessage{})
	gob.Register(NackMessage{})

	gob.Register(LogEvent{})

	gob.Register(Proposer{})
	gob.Register(Acceptor{})
	gob.Register(Learner{})
	gob.Register(StableState{})

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
