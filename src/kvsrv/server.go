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
	kv.safeOp(args, reply, (*KVServer).unsafePut)
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("Append {%v} => {%v}\n", args.Key, args.Value)
	kv.safeOp(args, reply, (*KVServer).unsafeAppend)
}

// ResultReceived is called by the client to confirm that the result has been received for ID, so that
// the server can safely discard the result.
func (kv *KVServer) ResultReceived(args *ResultReceivedArgs, reply *ResultReceivedReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.result, args.ID)
}

// safeOp wraps the unsafeOp function with a lock to ensure that the operation is atomic.
func (kv *KVServer) safeOp(args *PutAppendArgs, reply *PutAppendReply, unsafeOp func(*KVServer, string, string) string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if cached, isProcessed := kv.result[args.ID]; isProcessed {
		reply.Value = cached
		return
	}

	reply.Value = unsafeOp(kv, args.Key, args.Value)

	kv.result[args.ID] = reply.Value
}

func (kv *KVServer) unsafePut(key string, value string) string {
	old, existed := kv.store[key]
	kv.store[key] = value
	if existed {
		return old
	} else {
		return ""
	}
}

func (kv *KVServer) unsafeAppend(key string, value string) string {
	old, existed := kv.store[key]
	if existed {
		kv.store[key] = old + value
		return old
	} else {
		kv.store[key] = value
		return ""
	}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.store = make(map[string]string)
	kv.result = make(map[int64]string)

	return kv
}
