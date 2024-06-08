package raftkv

import (
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	// "fmt"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Key   string
	Value string
	Typ   string // "Put" or "Append" or "Get" ????
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	store	map[string]string
	getResult	chan string
}


func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	// fmt.Printf("Get RPC called for %d with key=%s \n", kv.me, args.Key)

	reply.Value = "Original get res"

	// var comm Op

	// comm.Key = args.Key
	// comm.Value = ""
	// comm.Typ = "Get"
	
	// index, term, isLeader := kv.rf.Start(comm)

	term, isLeader := kv.rf.GetState()

	if term < 0{
		// fmt.Printf("Negative term \n")
	}
	// if index < 0{
	// 	// fmt.Printf("Negative index \n")
	// }

	// fmt.Printf(" %d, %t,  %d \n", term, isLeader, kv.me)
	if isLeader{
		// fmt.Printf("term, leader, me = %d, %t  %d \n", term, isLeader, kv.me)
		reply.WrongLeader = false
		// reply.Value = <- kv.getResult

		reply.Value = kv.store[args.Key]

		return
	}else{
		// fmt.Printf("should be returning... \n")
		reply.WrongLeader = true
		return
	}

}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	// fmt.Printf("PutAppend RPC called for %d , details: key,value,op = %s, %s, %s\n", kv.me, args.Key, args.Value,args.Op)

	var comm Op

	comm.Key = args.Key
	comm.Value = args.Value
	comm.Typ = args.Op
	
	index, term, isLeader := kv.rf.Start(comm)

	if term < 0{
		// fmt.Printf("Negative term \n")
	}
	if index < 0{
		// fmt.Printf("Negative index \n")
	}

	// fmt.Printf("%d, %t  %d \n", term, isLeader, kv.me)
	if isLeader{
		// fmt.Printf("term, leader, me = %d, %t  %d \n", term, isLeader, kv.me)
		reply.WrongLeader = false
		// fmt.Printf("%t  \n", reply.WrongLeader)
		return
	}else{
		reply.WrongLeader = true
	}

	
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// fmt.Printf("Started kvserver %d\n", kv.me)

	kv.store = make(map[string]string)
	kv.getResult = make(chan string)

	go kv.moniterApply()

	return kv
}


func (kv *RaftKV) moniterApply() {
	// Your code here, if desired.
	// fmt.Printf("Started monitoring apply channel %d\n", kv.me)
	for {
		select{
		case applyMessage := <- kv.applyCh:
			// fmt.Printf("server %d applyMessage has index = %d\n",kv.me,  applyMessage.Index)
			convertedOp := applyMessage.Command.(Op)
			// fmt.Printf("applyMessage has key,value,typ  = %s,%s,%s \n", convertedOp.Key, convertedOp.Value, convertedOp.Typ )

			if convertedOp.Typ == "Put"{
				// fmt.Printf("applyMessage was Put command\n" )
				kv.store[convertedOp.Key] = convertedOp.Value
			}
			if convertedOp.Typ == "Append"{
				// fmt.Printf("applyMessage was Append command\n" )
				temp := kv.store[convertedOp.Key]
				temp = temp + convertedOp.Value
				kv.store[convertedOp.Key] = temp

			}
			if convertedOp.Typ == "Get"{
				// fmt.Printf("applyMessage was Get command\n" )
				// result := kv.store[convertedOp.Key]
				// fmt.Printf("Result is: %s\n", result)
				// kv.getResult <- result
			}

		}
	}

}
