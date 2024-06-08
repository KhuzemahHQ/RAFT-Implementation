// Implementation of a KeyValueServer. Students should write their code in this file.

package pa1

import "DS_PA1/rpcs"

import (
	"bufio"
	// "bytes"
	// "fmt"
	// "io"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
)

type keyValueServer struct {
	// TODO: implement this!

	// Tracking the clients and connections
	// list_clients 	[]net.Conn
	list_rw    []*bufio.ReadWriter
	rw_add     chan *bufio.ReadWriter
	rw_request chan string

	// listerner object
	lsn net.Listener

	// Channels for synchronization of put and get to prevent race conditions
	done_channel    chan struct{}
	request_channel chan request

	// Channels for tracking count of clients
	num_clients    int
	count_channel  chan int
	count_request  chan int
	count_response chan int

	// Channels for RPC server synchronization of put and get requests
	rpc_get_chan    chan string
	rpc_get_reply   chan []byte
	rpc_put_request chan rpc_request
	rpc_put_reply   chan int
}

// Struct for put and get requests to Model1
type request struct {
	op  string
	msg string
}

// Struct for put and get requests to Model2
type rpc_request struct {
	key   string
	value []byte
}

// New creates and returns (but does not start) a new KeyValueServer.
func New() KeyValueServer {

	var new_server keyValueServer = keyValueServer{

		done_channel:    make(chan struct{}),
		request_channel: make(chan request),
		rw_add:          make(chan *bufio.ReadWriter),
		rw_request:      make(chan string),
		count_channel:   make(chan int),
		count_request:   make(chan int),
		count_response:  make(chan int),
	}
	return &new_server
}

func (kvs *keyValueServer) StartModel1(port int) error {

	// Converting int port to string port according to input format of net.Listen
	var port_str = ":" + strconv.Itoa(port)
	// Listening on port
	lsn, err := net.Listen("tcp", port_str)
	// Saving the server and error from the Listener object
	kvs.lsn = lsn

	// If there is no error
	if err == nil {
		// Open a go routine that always listens for new connections
		go kvs.listenAccept()
		// Open a go routine that always handles requests
		go kvs.handleReq()
		// Open a go routine to keep track of clients
		go kvs.countClient()
		// Open a go routine to keep track of all reader writers
		go kvs.handleReaderWriter()
	} else {
		// Printing the Error
		// fmt.Printf("Error in Listening at Start: %s \n", err)
	}

	// Returning so that the function that called StartModel can continue
	return nil
}

func (kvs *keyValueServer) Close() {
	// TODO: implement this!

	// Instead of using this, I made a seperate function bu the name of removeConn which is being deferred in handleConnection
}

func (kvs *keyValueServer) Count() int {
	// TODO: implement this!

	// Sending a request to the count_request channel which is being monitored by countClient() go routine
	kvs.count_request <- 1
	// Waiting for the reply from countClient() and returning it
	return <-kvs.count_response

}

func (kvs *keyValueServer) StartModel2(port int) error {

	// Code from the tutorial on RPCs in go
	rpcServer := rpc.NewServer()
	rpcServer.Register(rpcs.Wrap(kvs))
	http.DefaultServeMux = http.NewServeMux()
	rpcServer.HandleHTTP(rpc.DefaultRPCPath, rpc.DefaultDebugPath)

	// Listening on the provided port
	var port_str = ":" + strconv.Itoa(port)
	lsn, err := net.Listen("tcp", port_str)

	kvs.lsn = lsn
	// Making channels for synchronization of put and get requests so that there are no concurrent writes or reads
	kvs.rpc_get_chan = make(chan string)
	kvs.rpc_put_request = make(chan rpc_request)
	kvs.rpc_get_reply = make(chan []byte)
	kvs.rpc_put_reply = make(chan int)

	if err == nil {
		// Seperate go routine that listens
		go http.Serve(lsn, nil)
		// Seperate go routine that handles RPC requests
		go kvs.handleRPCReq()

	} else {
		// Printing the Error
		// fmt.Printf("Error in Listening at Start: %s \n", err)
	}

	// Returning so that the function that called StartModel can continue
	return nil
}

func (kvs *keyValueServer) RecvGet(args *rpcs.GetArgs, reply *rpcs.GetReply) error {

	// Sending the key to respective channel which is being monitored by handleRPCReq()
	kvs.rpc_get_chan <- args.Key
	// Waiting for the reply from handleRPCReq()
	reply.Value = <-kvs.rpc_get_reply

	return nil
}

func (kvs *keyValueServer) RecvPut(args *rpcs.PutArgs, reply *rpcs.PutReply) error {

	// Sending the key and value to respective channel which is being monitored by handleRPCReq()
	kvs.rpc_put_request <- rpc_request{key: args.Key, value: args.Value}
	// Waiting for the reply from handleRPCReq()
	<-kvs.rpc_put_reply

	return nil
}

// TODO: add additional methods/functions below!

// Function for constantly listening for new connections
func (kvs *keyValueServer) listenAccept() {
	// Infintie loop for listening
	for {
		// Trying to accept connections
		conn, err := kvs.lsn.Accept()

		// If no error in accepting connection
		if err == nil {
			// opening a seperate go routine for handling this new connection
			go kvs.handleConnection(conn)
		} else {
			// Printing the error and returning so that the thread doesn't loop forever
			// fmt.Printf("Error in Accepting Connection: %s \n", err)
		}
	}
}

// Once a connection is accepted, we need a seperate function to constantly detect messages
// handleConnection from the echo tutorial alterred to fit this assignment
func (kvs *keyValueServer) handleConnection(conn net.Conn) {

	// Updating the number of clients by sending 1 to the count_channel. This channel is monitored by the countClient() go routine.
	kvs.count_channel <- 1

	// obtain a buffered reader / writer on the connection
	rw := ConnectionToRW(conn)
	// Adding the new ReadWrite buffer to the server state via the rw_add channel which is monitored by the handleReaderWriter() go routine
	kvs.rw_add <- rw

	// Clean up once the connection closes
	defer kvs.removeConn(conn)

	for {
		// get client message
		msg, err := rw.ReadString('\n')
		if err != nil {
			// fmt.Printf("There was an error reading from a client connection: %s\n", err)
			return
		}

		msg_type := msg[:3]
		// Placing the message in a request struct to the request_channel which is monitored by handleReq()
		if msg_type == "put" {
			kvs.request_channel <- request{op: "put", msg: msg}
		} else if msg_type == "get" {
			kvs.request_channel <- request{op: "get", msg: msg}
		}

	}
}

// Function to remove connection with certain client
func (kvs *keyValueServer) removeConn(conn net.Conn) {

	// Decrement the number of clients by sending -1 to count_channel
	kvs.count_channel <- -1

	// // Remove the connection from the list
	// for i, c := range kvs.list_clients {
	// 	if c == conn {
	// 		kvs.list_clients = append(kvs.list_clients[:i], kvs.list_clients[i+1:]...)
	// 		break
	// 	}
	// }

}

// ConnectionToRW takes a connection and returns a buffered reader / writer on it. Taken from the echo tutorial.
func ConnectionToRW(conn net.Conn) *bufio.ReadWriter {
	return bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
}

// countClient function to constantly monitor the count of clients
func (kvs *keyValueServer) countClient() {

	for {
		select {
		// case <- kvs.done_channel:
		// return
		// To update the num_clients whenever there is a new connection or a connection is removed
		case val := <-kvs.count_channel:
			kvs.num_clients += val
		// When any process requests the count
		case <-kvs.count_request:
			kvs.count_response <- kvs.num_clients
		}
	}
}

// handleReaderWriter function to append new reader-writers to server state as well as for broadcasting
func (kvs *keyValueServer) handleReaderWriter() {

	for {
		select {
		// case <- kvs.done_channel:
		// return
		// Adding new reader-writer to server state
		case val := <-kvs.rw_add:
			kvs.list_rw = append(kvs.list_rw, val)

		// Broadcasting msg to all clients
		case msg := <-kvs.rw_request:
			for _, client_rw := range kvs.list_rw {
				// send message to the client
				_, err := client_rw.WriteString(msg)
				if err != nil {
					// fmt.Printf("There was an error writing to a client connection: %s\n", err)
					return
				}
				// push to underlying io writer object
				err = client_rw.Flush()
				if err != nil {
					// fmt.Printf("There was an error flushing to a client connection: %s\n", err)
					return
				}
			}
		}
	}
}

// Function to parse the request message from the client into ints constituents
func (kvs *keyValueServer) parse(msg string) (m1, m2, m3 string) {
	m1 = msg[:3]
	m2 = ""
	m3 = ""
	if m1 == "put" {
		for i := 4; i < len(msg); i++ {
			if msg[i] == ',' {
				m2 = msg[4:i]
				m3 = msg[i+1 : len(msg)-1]
				break
			}
		}
	} else {
		// If it was a get message, everything after first comma must be the key
		m2 = msg[4 : len(msg)-1]
	}
	return
}

// Helper function for when the server gets a get request
func (kvs *keyValueServer) getHelp(msg string) {
	// Parsing the message
	msg_type, msg_key, msg_val := kvs.parse(msg)
	// Checking for wrong redirection of messages
	if msg_type != "get" {
		// fmt.Printf("Got different message type in get handler \n")
	}
	if msg_val != "" {
		// fmt.Printf("Got message value in get handler \n")
	}

	// Calling the get function
	get_returned := get(msg_key)
	// Converting to string and broadcasting to all clients
	get_returned_str := string(get_returned)
	// Making the broadcast message
	broadcast_msg := msg_key + "," + get_returned_str + "\n"
	// Sending the broadcast message to the rw_request channel which is monitored by handleReaderWriter() go routine
	kvs.rw_request <- broadcast_msg

}

func (kvs *keyValueServer) putHelp(msg string) {

	// Parsing through the message
	msg_type, msg_key, msg_val := kvs.parse(msg)
	// Checking for wrong redirection of messages
	if msg_type != "put" {
		// fmt.Printf("Got different message type in put handler \n")
		return
	}
	if msg_val == "" {
		// fmt.Printf("Got no message value in put handler \n")
		return
	}
	// Converting value to bytes and calling the put function
	val_bytes := []byte(msg_val)
	put(msg_key, val_bytes)

}

// handleReq function to constantly monitor the channels
func (kvs *keyValueServer) handleReq() {

	for {
		select {
		// A request object could be a put request or get request
		case req := <-kvs.request_channel:
			if req.op == "get" {
				kvs.getHelp(req.msg)
			} else if req.op == "put" {
				kvs.putHelp(req.msg)
			}
			// case <- kvs.done_channel:
			// 	return
		}
	}
}

// handleRPCReq function to constantly monitor the channels  of the RPC server
func (kvs *keyValueServer) handleRPCReq() {
	for {
		select {
		// If a get request is recceived
		case key := <-kvs.rpc_get_chan:
			get_returned := get(key)
			kvs.rpc_get_reply <- get_returned
		// If a put request is recceived
		case req := <-kvs.rpc_put_request:
			put(req.key, req.value)
			kvs.rpc_put_reply <- 1
			// case <- kvs.done_channel:
			// 	return
		}
	}
}
