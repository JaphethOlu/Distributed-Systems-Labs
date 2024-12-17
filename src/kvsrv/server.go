package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type DupTable struct {
	SeqNum int
	Value  string
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	Dict    map[string]string
	Clients map[int]DupTable
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	defer kv.mu.Unlock()

	kv.mu.Lock()
	value, exists := kv.Dict[args.Key]
	if exists {
		reply.Value = value
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	defer kv.mu.Unlock()

	kv.mu.Lock()
	kv.Dict[args.Key] = args.Value
	reply.Value = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	defer kv.mu.Unlock()

	kv.mu.Lock()
	dt, clientExists := kv.Clients[args.ClientID]
	if clientExists {
		if dt.SeqNum == args.SequenceNumber {
			reply.Value = dt.Value
			return
		}
	}

	value, exists := kv.Dict[args.Key]
	if exists {
		reply.Value = value
		kv.Dict[args.Key] = value + args.Value
		kv.Clients[args.ClientID] = DupTable{SeqNum: args.SequenceNumber, Value: value}
	} else {
		reply.Value = ""
		kv.Dict[args.Key] = ""
		kv.Clients[args.ClientID] = DupTable{SeqNum: args.SequenceNumber, Value: ""}
	}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.Dict = map[string]string{}
	kv.Clients = map[int]DupTable{}

	return kv
}
