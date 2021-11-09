package main

import (
	"bufio"
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
)

// largest udp packet data size
const max_pkt_size = 80000

var item_names = [4]string{
	"surgical masks",
	"hand sanitizer bottles",
	"toilet paper rolls",
	"reese’s peanut butter cups"}

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

func (opcode *OpCodeType) opCodeStr() string {
	switch *opcode {
	case Order:
		return "order"
	case Cancel:
		return "cancel"
	default:
		log.Fatal("opCodeStr: Invalid Opcode")
		return ""
	}
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
	{
		pM := msg.Contents.(PrepareMessage)
		fmt.Println(pM.PrepareNumber)
		if prepareMsg, ok := msg.Contents.(PrepareMessage); ok {
			return fmt.Sprintf("prepare[%d](%d)",
				msg.LogIndex, prepareMsg.PrepareNumber)
		}
	}

	{
		if promiseMsg, ok := msg.Contents.(PromiseMessage); ok {
			return fmt.Sprintf("promise[%d](%d, `%s`)",
				msg.LogIndex, promiseMsg.AcceptNum,
				promiseMsg.AcceptVal.logEventStr())
		}
	}

	{
		if acceptMsg, ok := msg.Contents.(AcceptMessage); ok {
			return fmt.Sprintf("accept[%d](%d, `%s`)",
				msg.LogIndex, acceptMsg.AcceptNum,
				acceptMsg.AcceptVal.logEventStr())
		}
	}

	{
		if acceptedMsg, ok := msg.Contents.(AcceptedMessage); ok {
			return fmt.Sprintf("accepted[%d](%d, `%s`)",
				msg.LogIndex, acceptedMsg.AcceptNum,
				acceptedMsg.AcceptVal.logEventStr())
		}
	}

	log.Fatal("messageStr: Invalid Message Format")
	return ""
}

type Message struct {
	LogIndex int    `json:"log_index"`
	SenderID string `json:"sender_id"`
	// Prepare | Promise | Accept | Accepted
	Contents interface{} `json:"contents"`
}

type PrepareMessage struct {
	PrepareNumber int `json:"prepare_number"`
}

type PromiseMessage struct {
	AcceptNum int      `json:"accept_num"`
	AcceptVal LogEvent `json:"accept_val"`
}

type AcceptMessage struct {
	AcceptNum int      `json:"accept_num"`
	AcceptVal LogEvent `json:"accept_val"`
}

type AcceptedMessage struct {
	AcceptNum int      `json:"accept_num"`
	AcceptVal LogEvent `json:"accept_val"`
}

// Server is instantiated and run by each node participating
// in the wuu-bernstein algorithm. It encapsulates the
// LogRecord, standard input listener, udp network listener,
// and the UDP addressing information. Contains handlers
// for all of the different UI components.
type Server struct {
	site_id        string
	peers          map[string]Node
	peer_addrs     map[string]*net.UDPAddr
	peer_encs      map[string]*gob.Encoder
	stdin_c        chan string
	netwk_c        chan Message
	server_mu      sync.Mutex
	proposer_chans map[int]chan Message
	log            map[int]LogEvent
	maxPropNums    map[int]int
	maxLogIndex    int
	site_ord       int
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

// newServer creates a new Server object for a particular site_id.
// If it finds a "stable_storage.json" file, it will load the
// LogRecord contents from there, otherwise it will create
// a blank LogRecord object.
func newServer(site_id string, peers Map) *Server {
	site_ord := 0
	site_id_arr := make([]string, 0)
	for peer_site_id := range peers.Hosts {
		site_id_arr = append(site_id_arr, peer_site_id)
	}

	sort.Slice(site_id_arr, func(i, j int) bool {
		return site_id_arr[i] < site_id_arr[j]
	})

	for idx, peer_site_id := range site_id_arr {
		if peer_site_id == site_id {
			site_ord = idx + 1
			break
		}
	}

	s := Server{site_id: site_id,
		peers:          peers.Hosts,
		peer_addrs:     make(map[string]*net.UDPAddr),
		peer_encs:      make(map[string]*gob.Encoder),
		stdin_c:        make(chan string),
		netwk_c:        make(chan Message),
		proposer_chans: make(map[int]chan Message),
		maxPropNums:    make(map[int]int),
		log:            make(map[int]LogEvent),
		maxLogIndex:    -1,
		site_ord:       site_ord}

	for id, node := range s.peers {
		site_addr := net.UDPAddr{
			Port: node.UdpStartPort,
			IP:   net.ParseIP(node.IpAddress)}

		conn, err := net.Dial("udp", site_addr.String())
		if err != nil {
			log.Fatalf("newServer: udp dial error: %v\n", err)
		}
		s.peer_encs[id] = gob.NewEncoder(conn)
		s.peer_addrs[id] = &site_addr
	}
	return &s
}

// stdin_read_loop infinitely loops while polling the stdin
// file descriptor for user input, and passing that to the
// user channel
func stdin_read_loop(stdin_c chan string, reader *bufio.Reader) {
	b := make([]byte, max_pkt_size)
	for {
		n, err := reader.Read(b)
		if err != nil {
			if err != io.EOF {
				fmt.Fprintf(os.Stderr, "Read error: %v\n", err)
			}
			break
		}
		stdin_c <- string(b[:n])
	}
}

// netwk_read_loop infinitely loops while polling a UDP socket
// for messages from other nodes. Upon receiving, it will push
// unmarshal and then push messages to a network message
// designated channel
func netwk_read_loop(netwk_c chan Message, reader *net.UDPConn) {
	dec := gob.NewDecoder(reader)
	for {
		var m Message
		err := dec.Decode(&m)
		if err != nil {
			if err != io.EOF {
				log.Fatalf("netwk_read_loop: Decode error: %v\n", err)
			}
			break
		}
		netwk_c <- m
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

func (srv *Server) handle_order(name string, amt [4]int) {
	propVal := LogEvent{
		OpCode:      Order,
		Name:        name,
		Amounts:     amt,
		Proposer_id: srv.site_id}
	srv.maxLogIndex += 1
	srv.start_synod(srv.maxLogIndex, srv.site_ord, propVal)
}

func (srv *Server) handle_cancel(name string) {

}

func (srv *Server) handle_list_orders() {

}

func (srv *Server) handle_list_inventory() {

}

func (srv *Server) handle_list_log() {

}

func (srv *Server) handle_prepare(prepareMsgPkg Message) {

}

func (srv *Server) handle_accept(acceptMsgPkg Message) {

}

func (srv *Server) handle_accepted(acceptedMsgPkg Message) {

}

func (srv *Server) handle_receive(msg Message) {
	{
		// Handled by acceptor
		if _, ok := msg.Contents.(PrepareMessage); ok {
			srv.handle_prepare(msg)
			return
		}
	}

	{
		// Handled by proposer
		if _, ok := msg.Contents.(PromiseMessage); ok {
			logIdx := msg.LogIndex
			srv.server_mu.Lock()
			if prop_chan, ok := srv.proposer_chans[logIdx]; ok {
				prop_chan <- msg
			}
			srv.server_mu.Unlock()
			return
		}
	}

	{
		// Handled by acceptor
		if _, ok := msg.Contents.(AcceptMessage); ok {
			srv.handle_accept(msg)
			return
		}
	}

	{
		// Handled by learner
		if _, ok := msg.Contents.(AcceptedMessage); ok {
			srv.handle_accepted(msg)
			return
		}
	}

	log.Fatal("handle_receive: Invalid Message Format")
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

// Starts a paxos run for a particular log slot
func (srv *Server) start_synod(logIndex int, maxPropNum int, proposedVal LogEvent) {
	srv.server_mu.Lock()
	if _, exists := srv.proposer_chans[logIndex]; exists {
		srv.server_mu.Unlock()
		log.Fatalf("start_synod(%d, %d, %s): synod already exists",
			logIndex, maxPropNum, proposedVal.OpCode.opCodeStr())
	}

	if _, exists := srv.log[logIndex]; exists {
		srv.server_mu.Unlock()
		log.Fatalf("start_synod(%d, %d, %s): log slot value already learned",
			logIndex, maxPropNum, proposedVal.OpCode.opCodeStr())
	}
	srv.server_mu.Unlock()
	go srv.proposer_loop(logIndex, maxPropNum, proposedVal)
}

// proposer for a process and a log slot
func (srv *Server) proposer_loop(logIndex int, maxPropNum int, proposedVal LogEvent) {
	srv.server_mu.Lock()
	if _, exists := srv.proposer_chans[logIndex]; exists {
		srv.server_mu.Unlock()
		log.Fatalf("proposer_loop[%d]: Two proposer loops for the same log slot", logIndex)
	}
	srv.proposer_chans[logIndex] = make(chan Message)
	srv.maxPropNums[logIndex] = maxPropNum + srv.site_ord
	srv.server_mu.Unlock()
	prepareMsg := PrepareMessage{
		PrepareNumber: srv.maxPropNums[logIndex]}
	prepareMsgWrap := Message{
		LogIndex: logIndex,
		SenderID: srv.site_id,
		Contents: prepareMsg}
	srv.sendall(prepareMsgWrap)

	for {
		srv.server_mu.Lock()
		mesg := <-srv.proposer_chans[logIndex]
		srv.server_mu.Unlock()
		fmt.Printf("Proposer Loop Mesg From: %s\n", mesg.SenderID)
	}

	srv.server_mu.Lock()
	delete(srv.proposer_chans, logIndex)
	srv.server_mu.Unlock()
}

func (srv *Server) sendall(m Message) {
	mesgStr := m.messageStr()
	for site_id, site_enc := range srv.peer_encs {
		fmt.Fprintf(os.Stderr, "sending %s to %s\n", mesgStr, site_id)
		err := site_enc.Encode(&m)
		if err != nil {
			log.Fatalf("udp Encode error: %v\n", err)
		}
	}
}

func (srv *Server) run() {
	stdin_reader := bufio.NewReader(os.Stdin)
	ser, err := net.ListenUDP("udp", srv.peer_addrs[srv.site_id])
	if err != nil {
		fmt.Fprintf(os.Stderr, "ListenUDP error: %v\n", err)
	}

	go stdin_read_loop(srv.stdin_c, stdin_reader)
	go netwk_read_loop(srv.netwk_c, ser)

	for {
		select {
		case user_input := <-srv.stdin_c:
			srv.on_user_input(user_input)
		case mesg := <-srv.netwk_c:
			fmt.Fprintf(os.Stderr, "received %s from %s\n", mesg.messageStr(), mesg.SenderID)
			srv.handle_receive(mesg)
		}
	}
}

func main() {

	args := os.Args

	gob.Register(Message{})
	gob.Register(PrepareMessage{})
	gob.Register(AcceptMessage{})
	gob.Register(AcceptedMessage{})
	gob.Register(PromiseMessage{})

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
