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

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	store map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	key := args.Key
	value, ok := kv.store[key]
	if ok {
		reply.Value = value
		DPrintf("Get {%v} => {%v} \n", args.Key, reply.Value)
	} else {
		reply.Value = ""
		DPrintf("Get {%v} => {%v} \n", args.Key, "nil")
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("Put {%v} => {%v}\n", args.Key, args.Value)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	key := args.Key
	value := args.Value
	// replace the old value
	old, ok := kv.store[key]
	kv.store[key] = value
	if ok {
		reply.Value = old
	} else {
		reply.Value = ""
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("Append {%v} => {%v}\n", args.Key, args.Value)

	kv.mu.Lock()
	defer kv.mu.Unlock()
	key := args.Key
	value := args.Value
	old, ok := kv.store[key]
	if ok {
		kv.store[key] = old + value
		reply.Value = old
	} else {
		kv.store[key] = value
		reply.Value = ""
	}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.store = make(map[string]string)

	return kv
}
