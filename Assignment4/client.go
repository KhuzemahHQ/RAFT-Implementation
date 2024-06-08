package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"

// import "fmt"
import "time"
// import "sync"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	// mu      sync.Mutex
	clerkId int64
	lastSeq int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clerkId = nrand()

	// fmt.Printf("Making clerk %d\n", ck.clerkId)
	ck.lastSeq = nrand()

	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.

	// fmt.Printf("Get called by clerk %d with key=%s \n", ck.clerkId, key)
	// ck.mu.Lock()
	// defer ck.mu.Unlock()

	var args GetArgs
	args.Key = key
	args.Seq = nrand()

	result := ""
	noLeaderFound := true
	for noLeaderFound {
		// Waiting for 500 milliseconds, so that hopefully a leader is found next time
		time.Sleep(time.Duration(500) * time.Millisecond)
		for i := 0; i < len(ck.servers); i++ {
			var reply GetReply
			ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)

			if !ok {
				// fmt.Printf("not ok \n")
			} else {
				// fmt.Printf("%d's reply had: %t, %d\n", i, reply.WrongLeader, reply.Mistake)
				if reply.WrongLeader == false {
					// fmt.Printf("%d's reply had: %t, %s\n", i, reply.WrongLeader, reply.Value)
					noLeaderFound = false
					result = reply.Value
					break
				}
			}
		}
	}
	// fmt.Printf("End of Get loop \n")

	return result
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	// fmt.Printf("PutAppend called by clerk %d\n", ck.clerkId)
	// fmt.Printf("details: key,value,op = %s, %s, %s\n", key, value, op)

	// ck.mu.Lock()
	// defer ck.mu.Unlock()
	var args PutAppendArgs

	args.Key = key
	args.Value = value
	args.Op = op

	args.Seq = nrand()

	noLeaderFound := true
	for noLeaderFound {
		// Waiting for 500 milliseconds, so that hopefully a leader is found next time
		time.Sleep(time.Duration(500) * time.Millisecond)
		for i := 0; i < len(ck.servers); i++ {
			var reply PutAppendReply
			ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)

			if !ok {
				// fmt.Printf("not ok \n")
			} else {
				// fmt.Printf("%d's reply had: %t, %d\n", i, reply.WrongLeader, reply.Mistake)
				if reply.WrongLeader == false {
					// fmt.Printf("%d's reply had: %t\n", i, reply.WrongLeader)
					noLeaderFound = false
					break
				}
			}
		}
	}

	// fmt.Printf("End of putAppend loop \n")

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
