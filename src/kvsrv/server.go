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
	store  map[string]string
	result map[int64]string
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

	cachedResult, isProcessed := kv.getProcessed(args.ID)
	if isProcessed {
		reply.Value = cachedResult
		return
	}

	// work
	key := args.Key
	value := args.Value
	old, ok := kv.store[key]
	kv.store[key] = value
	if ok {
		reply.Value = old
	} else {
		reply.Value = ""
	}
	kv.saveProcessed(args.ID, reply.Value)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("Append {%v} => {%v}\n", args.Key, args.Value)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	cachedResult, isProcessed := kv.getProcessed(args.ID)
	if isProcessed {
		reply.Value = cachedResult
		return
	}

	// work
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
	kv.saveProcessed(args.ID, reply.Value)
}

func (kv *KVServer) getProcessed(id int64) (string, bool) {
	value, ok := kv.result[id]
	return value, ok
}

func (kv *KVServer) saveProcessed(id int64, value string) {
	kv.result[id] = value
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.store = make(map[string]string)
	kv.result = make(map[int64]string)

	return kv
}
