// Khuzemah Hassan Qazi, 24100092

package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"labrpc"
	"sync"
	"time"
	// "fmt"
	"math/rand"
)

// import "bytes"
// import "encoding/gob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for Assignment2; only used in Assignment3
	Snapshot    []byte // ignore for Assignment2; only used in Assignment3
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	typeServer      string
	currentTerm     int
	votedFor        int
	timerStart      time.Time
	voteCount       int
	currentLeader   int
	electionTimeout int
	hbTimeout       int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.typeServer == "Leader" {
		isleader = true
	} else {
		isleader = false
	}
	term = rf.currentTerm
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

// example RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	Term        int
	CandidateID int
}

// example RequestVote RPC reply structure.
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm

	// If candidate is out of date
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		return
	}
	// If we have voted already in this term
	if rf.currentTerm == args.Term && rf.votedFor != -1 {
		reply.VoteGranted = false
		return
	}
	// if we are out of date, update the term and current term's votedFor. And we should become follower
	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.typeServer = "Follower"
	}
	reply.Term = args.Term
	rf.typeServer = "Follower"
	reply.VoteGranted = true
	rf.votedFor = args.CandidateID

	// Restarting timer
	// fmt.Printf("%d restarting timer due to getting vote \n", rf.me)
	rf.timerStart = time.Now()

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//

func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntries RPC arguments structure.
type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

// AppendEntries RPC reply structure.
type AppendEntriesReply struct {
	Term int
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	// If the heartbeat is out of date, we don't need to count this RPC for restarting timer
	if args.Term < rf.currentTerm {
		return
	}

	// Restarting timer
	// fmt.Printf("%d restarting timer due to getting heartbeat from %d \n", rf.me, args.LeaderId)

	rf.timerStart = time.Now()
	rf.typeServer = "Follower"
	rf.currentTerm = args.Term

}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	return index, term, isLeader
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Starting as Follower and having voted for no-one
	rf.typeServer = "Follower"
	rf.votedFor = -1
	rf.voteCount = 0
	rf.hbTimeout = 100

	// Starting a go routine that will moniter changes in server type
	go rf.moniterServerType()

	return rf
}

// Function to run different go routine depending on server state.
func (rf *Raft) moniterServerType() {
	for true {
		rf.mu.Lock()
		typeServer := rf.typeServer
		hbTimeout := rf.hbTimeout
		// me := rf.me
		rf.mu.Unlock()
		// fmt.Printf("%d is %s \n", me, typeServer)
		if typeServer == "Follower" {
			rf.followerRoutine()
		} else if typeServer == "Leader" {
			rf.leaderRoutine()
		} else if typeServer == "Candidate" {
			rf.candidateRoutine()
		}
		// Sleeping for the heartbeat timeout (100 milliseconds) for next heartbeat
		// This also helps prevents potential deadlocks in case some RPC's updates take time
		time.Sleep(time.Duration(hbTimeout) * time.Millisecond)
	}
}

// Function to get a new election timeout in the 600ms to 1000ms range
func getElectionTimeout() time.Duration {
	// Seeding for better randomness
	rand.Seed(time.Now().UnixNano())
	const min = 600
	const max = 1000

	electionTimeout := min + rand.Intn(max-min)
	return time.Duration(electionTimeout) * time.Millisecond
}

func (rf *Raft) followerRoutine() {
	// Getting a new random election timout every time to decrease chances of split votes
	duration := getElectionTimeout()
	time.Sleep(duration)

	// If the follower got a valid heartbeat or requestVote RPC, its timer should have restarted
	rf.mu.Lock()
	timerStart := rf.timerStart
	// me := rf.me
	rf.mu.Unlock()
	// fmt.Printf("%d got election timeout of %d \n", me, duration)

	// The first few times, the election timout will not be exceeded and this function returns
	// If the timer did not restart a few rounds in a row and this function is called again by the moniterServerType()
	// the election timeout will eventually be exceeded
	elapsedTime := time.Now().Sub(timerStart).Milliseconds()
	if elapsedTime >= duration.Milliseconds() {
		// fmt.Printf("%d becoming candidate \n", me)
		rf.mu.Lock()
		rf.typeServer = "Candidate"
		rf.currentTerm++
		rf.votedFor = -1
		rf.mu.Unlock()
	}
}

func (rf *Raft) leaderRoutine() {

	// Storing unchanging variables in local variables
	rf.mu.Lock()
	me := rf.me
	peers := rf.peers
	rf.mu.Unlock()

	// Broadcasting heartbeat to all peers asynchronously so that one failure doesn't block other communication
	for peer := range peers {
		if peer != me {
			// Defining the arguments that will be sent to the peer
			var args AppendEntriesArgs
			rf.mu.Lock()
			args.Term = rf.currentTerm
			args.LeaderId = rf.me
			rf.mu.Unlock()
			go rf.heartbeatSender(peer, args)

		}
	}
}

// Helper function to send heartbeat and check reply
func (rf *Raft) heartbeatSender(peer int, args AppendEntriesArgs) {

	var reply AppendEntriesReply
	ok := rf.sendAppendEntries(peer, args, &reply)
	if !ok {
		// If sending RPC failed, should return
		return
	}
	// Checking if leader was out of date.
	rf.mu.Lock()
	if reply.Term > rf.currentTerm {
		rf.typeServer = "Follower"
		// fmt.Printf("%d leader was out of date. Becoming follower again. \n", rf.me)
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

}

// Function to send heartbeats synchronously if current server is the leader. This caused blocking.
// func (rf *Raft) leaderRoutine() {
// 	// fmt.Printf("Started heartbeat sender \n")

// 	rf.mu.Lock()
// 	hbTimeout 	:= rf.hbTimeout
// 	me 			:= rf.me
// 	peer_list	:= rf.peers
// 	rf.mu.Unlock()

// 	for {
// 		rf.mu.Lock()
// 		currentType := rf.typeServ
// 		my_term		:= rf.currentTerm
// 		rf.mu.Unlock()

// 		if currentType == "Leader" {
// 			// Send heartbeat to all peers
// 			fmt.Printf("I am %d (%s) and sending heartbeat to all peers \n", me, currentType)

// 			var args AppendEntriesArgs
// 			var reply AppendEntriesReply
// 			args.Term = my_term
// 			args.LeaderID = me
// 			for peerIdx := range peer_list {
// 				if peerIdx == me {
// 					continue
// 				}
// 				ok := rf.sendAppendEntries(peerIdx, args, &reply)
// 				if ok {
// 					fmt.Printf("%d Received AppendEntries Reply from peer %d with term %d\n", me, peerIdx, reply.Term)
// 					if reply.Term > my_term{
// 						rf.mu.Lock()
// 						rf.currentTerm = reply.Term
// 						rf.typeServ = "Follower"
// 						rf.voteCount = 0
// 						rf.mu.Unlock()
// 						break
// 					}
// 				}
// 			}
// 			// Sleep for one heartbeat
// 			time.Sleep(time.Duration(hbTimeout) * time.Millisecond)
// 		}else{
// 			// fmt.Printf("I am %d (now follower) and breaking this loop \n", me)
// 			break
// 		}

// 	}
// }

func (rf *Raft) candidateRoutine() {
	// Starting timer for this election process
	electionTimeout := getElectionTimeout()
	startTime := time.Now()
	// Storing state variables locally
	rf.mu.Lock()
	peers := rf.peers
	me := rf.me
	term := rf.currentTerm
	rf.mu.Unlock()

	// Keeping count of votes and number of requestVotes sent
	// Starting from one by already counting ourself
	voteCount := 1
	sentCount := 1

	// Broadcasting requestVote to all peers (except itself) asynchronously to prevent blocking
	for peer := range peers {
		if peer != me {
			// Defining function here so that it can access count and sentCount easily instead of making them state variables for more potential deadlocks
			go func(peer int) {
				var args RequestVoteArgs
				var reply RequestVoteReply

				args.Term = term
				args.CandidateID = me
				ok := rf.sendRequestVote(peer, args, &reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if !ok {
					// if couldn't send RPC, should not block and should just return
					sentCount++
					return
				}
				sentCount++
				// If the server accepted the requestVote
				if reply.VoteGranted {
					voteCount++
				} else {
					// If the server rejected the requestVote
					if args.Term < reply.Term {
						// If the other server had a larger term, it means this candidate is out of date and should become a follower again
						rf.typeServer = "Follower"
						// fmt.Printf("%d candidate was out of date. Becoming follower again. \n", me)
					}
				}
			}(peer)
		}
	}

	total := len(peers)
	threshold := (total / 2) + 1

	// Waiting Loop to make sure requestVote has been sent out to everyone, or as many as needed. (unless time runs out)
	// Need to be able to end early in case some server is disconnected and we can't send to them
	for {
		// The blocking loop can exit early and this function can end if any of the following conditions are met:
		rf.mu.Lock()

		// If the server type changed while sending requestVotes. This can only happen if the reply.Term was greater and the candidate was out of date.
		if rf.typeServer != "Candidate" {
			// fmt.Printf("%d candidate was out of date and became follower again. \n", me)
			rf.mu.Unlock()
			return
		}

		// If we have already acquired majority
		if voteCount >= threshold {
			// If this server remained a candidate throughout the sending process and got a majority of votes
			// fmt.Printf("%d candidate becoming leader. \n", me)
			rf.typeServer = "Leader"

			rf.mu.Unlock()
			return
		}
		// If we have sent requestVote to all peers. Since we didn't go in the first if condition, we could not have gotten the majority.
		if sentCount == total {
			// The votes got split or this server lost the election
			// fmt.Printf("%d candidate lost election. Becoming follower again. \n", me)
			rf.typeServer = "Follower"
			rf.mu.Unlock()
			return
		}

		// If our electionTimer has run out
		if time.Now().Sub(startTime).Milliseconds() >= electionTimeout.Milliseconds() {
			rf.typeServer = "Follower"
			// fmt.Printf("%d election timed out. Becoming follower again. \n", me)
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		// Else, this thread should continue waiting for more requestVotes to be sent
		time.Sleep(100 * time.Millisecond)
	}

}
