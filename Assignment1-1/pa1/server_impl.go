// Implementation of a KeyValueServer. Students should write their code in this file.

package pa1

import "DS_PA1/rpcs"

import (
	"bufio"
	// "bytes"
	// "fmt"
	// "io"
	"net"  
	"strconv"
)


type keyValueServer struct {
	// TODO: implement this!
	// Boolean to tell if the server has started. Not being used for now.
	started 		bool

	// Tracking the clients and connections
	num_clients 	int
	list_clients 	[]net.Conn
	list_rw 		[]*bufio.ReadWriter
	rw				*bufio.ReadWriter
	
	// listerner object and channels for concurrent processing
	lsn 			net.Listener
	put_channel		chan string
	get_channel		chan string

}


// New creates and returns (but does not start) a new KeyValueServer.
func New() KeyValueServer {

	var new_server keyValueServer = keyValueServer{}
	return &new_server
}

func (kvs *keyValueServer) StartModel1(port int) error {

	kvs.put_channel = make(chan string)
	kvs.get_channel = make(chan string)
	kvs.started = true

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
	}else{
		// Printing the Error
		// fmt.Printf("Error in Listening at Start: %s \n", err)
	}

	// Returning so that the function that called StartModel can continue
	return nil
}

func (kvs *keyValueServer) Close() {
	// TODO: implement this!

	// remove client connection
}

func (kvs *keyValueServer) Count() int {
	// TODO: implement this!

	return kvs.num_clients

}

func (kvs *keyValueServer) StartModel2(port int) error {
	// TODO: implement this!
	//
	// Do not forget to call rpcs.Wrap(...) on your kvs struct before
	// passing it to <sv>.Register(...)
	//
	// Wrap ensures that only the desired methods (RecvGet and RecvPut)
	// are available for RPC access. Other KeyValueServer functions
	// such as Close(), StartModel1(), etc. are forbidden for RPCs.
	//
	// Example: <sv>.Register(rpcs.Wrap(kvs))
	return nil
}

func (kvs *keyValueServer) RecvGet(args *rpcs.GetArgs, reply *rpcs.GetReply) error {
	// TODO: implement this!
	return nil
}

func (kvs *keyValueServer) RecvPut(args *rpcs.PutArgs, reply *rpcs.PutReply) error {
	// TODO: implement this!
	return nil
}

// TODO: add additional methods/functions below!


// Function for constantly listening for new connections
func (kvs *keyValueServer) listenAccept(){

	// Infintie loop for listening
	for{
		// Trying to accept connections
		conn, err := kvs.lsn.Accept()
		
		// If no error in accepting connection
		if err == nil {
			// Saving the connection to client list and updating the number of clients
			kvs.list_clients = append(kvs.list_clients, conn)
			kvs.num_clients = kvs.num_clients + 1
			// opening a seperate go routine for handling this new connection
			go kvs.handleConnection(conn)
		}else{
			// Printing the error and returning so that the thread doesn't loop forever
			// fmt.Printf("Error in Accepting Connection: %s \n", err)
			
		}
	}
}


// Once a connection is accepted, we need a seperate function to constantly detect messages
// handleConnection from the echo tutorial alterred to fit this assignment
func (kvs *keyValueServer) handleConnection(conn net.Conn) {


	// clean up once the connection closes
	// defer conn.Close()

	// obtain a buffered reader / writer on the connection
	rw := ConnectionToRW(conn)

	// Adding the new ReadWrite buffer to the server state
	kvs.list_rw = append(kvs.list_rw, rw)

	for {
		// get client message
		msg, err := rw.ReadString('\n')
		if err != nil {
			// fmt.Printf("There was an error reading from a client connection: %s\n", err)
			return
		}

		msg_type := msg[:3]

		// Placing the message in the appropriate channel to be handled by RequestHandler
		if msg_type == "put"{
			kvs.put_channel <- msg
		}else if msg_type == "get"{
			kvs.get_channel <- msg
		}

	}
}

// ConnectionToRW takes a connection and returns a buffered reader / writer on it. Taken from the echo tutorial.
func ConnectionToRW(conn net.Conn) *bufio.ReadWriter {
	return bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
}

// Function to parse the request message from the client into ints constituents
func (kvs *keyValueServer) parse(msg string) (m1,m2,m3 string){

	m1 = msg[:3]
	m2 = ""
	m3 = ""

	if m1 == "put"{
		for i := 4; i<len(msg); i++{
			if msg[i] == ','{
				m2 = msg[4:i]
				m3 = msg[i+1:len(msg)-1]
				break
			}
		}
	}else{
		// If it was a get message, everything after first comma must be the key
		m2 = msg[4:len(msg)-1]
	}
	
	return 
}

// RequestHandler function to constantly monitor the channels 
func (kvs *keyValueServer) handleReq() {

	for {

		select {
		case get := <- kvs.get_channel:
			// Calling helper function for get requests
			kvs.getHelp(get)

		case put := <- kvs.put_channel:
			// Calling helper function for put requests
			kvs.putHelp(put)
		}
	}

}

// Helper function for when the server gets a get request
func (kvs *keyValueServer) getHelp(msg string) {

	// Parsing the message
	msg_type,msg_key,msg_val := kvs.parse(msg)

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
	// Calling the broadcast function to send message to all clients
	kvs.broadcast(msg_key + "," + get_returned_str + "\n")
	
}

// Function to broadcast message to all clients
func (kvs *keyValueServer) broadcast(msg string) {

	for _,client_rw := range kvs.list_rw{

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

func (kvs *keyValueServer) putHelp(msg string) {

	// Parsing through the message
	msg_type,msg_key,msg_val := kvs.parse(msg)

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
	put(msg_key,val_bytes)

}


/////////////////////////////// Race condition solution???



// type keyValueServer struct {
//     // Other fields...

//     // Channels for synchronizing access to shared variables
//     accessCh  chan struct{}
//     doneCh    chan struct{}
// }

// // ...

// func New() KeyValueServer {
//     var new_server keyValueServer = keyValueServer{
//         accessCh: make(chan struct{}, 1), // Buffered channel with capacity 1
//         doneCh:   make(chan struct{}),
//     }
//     return &new_server
// }

// func (kvs *keyValueServer) StartModel1(port int) error {
//     // ...
// }

// // ...

// func (kvs *keyValueServer) handleConnection(conn net.Conn) {
//     // ...

//     // Acquire access to shared variables using a channel
//     kvs.accessCh <- struct{}{}

//     // Modify shared variables
//     kvs.list_rw = append(kvs.list_rw, rw)
//     kvs.num_clients++
    
//     // Release access to shared variables
//     <-kvs.accessCh

//     // ...
// }

// func (kvs *keyValueServer) handleReq() {
//     for {
//         select {
//         case get := <-kvs.get_channel:
//             // Calling helper function for get requests
//             kvs.accessCh <- struct{}{}
//             kvs.getHelp(get)
//             <-kvs.accessCh
//         case put := <-kvs.put_channel:
//             // Calling helper function for put requests
//             kvs.accessCh <- struct{}{}
//             kvs.putHelp(put)
//             <-kvs.accessCh
//         case <-kvs.doneCh:
//             return
//         }
//     }
// }

// func (kvs *keyValueServer) Close() {
//     // Signal the handleReq Goroutine to exit
//     kvs.doneCh <- struct{}{}
//     // Close the listener and clean up resources
//     kvs.lsn.Close()
// }

// // ...



//////////////////////// Solution for Count Race????

// type keyValueServer struct {
//     // Other fields...

//     // Channels for synchronizing access to shared variables
//     accessCh   chan struct{}
//     doneCh     chan struct{}
//     countCh    chan int
// }

// // ...

// func New() KeyValueServer {
//     var new_server keyValueServer = keyValueServer{
//         accessCh: make(chan struct{}, 1), // Buffered channel with capacity 1
//         doneCh:   make(chan struct{}),
//         countCh:  make(chan int),
//     }
//     return &new_server
// }

// func (kvs *keyValueServer) StartModel1(port int) error {
//     // ...

//     go kvs.countClients() // Start a Goroutine to count clients

//     // ...
// }

// func (kvs *keyValueServer) countClients() {
//     for {
//         select {
//         case <-kvs.doneCh:
//             return
//         default:
//             kvs.countCh <- kvs.num_clients
//             time.Sleep(time.Second) // Optionally add a sleep to reduce contention
//         }
//     }
// }

// func (kvs *keyValueServer) Close() {
//     // Signal the handleReq Goroutine to exit
//     kvs.doneCh <- struct{}{}
//     // Close the listener and clean up resources
//     kvs.lsn.Close()
// }

// func (kvs *keyValueServer) Count() int {
//     // Request the count from the countClients Goroutine
//     return <-kvs.countCh
// }

// func (kvs *keyValueServer) handleConnection(conn net.Conn) {
//     // ...

//     // Acquire access to shared variables using a channel
//     kvs.accessCh <- struct{}{}

//     // Modify shared variables
//     kvs.list_rw = append(kvs.list_rw, rw)
//     kvs.num_clients++

//     // Release access to shared variables
//     <-kvs.accessCh

//     // ...
// }



