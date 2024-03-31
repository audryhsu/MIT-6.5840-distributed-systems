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

	kvMap map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply.Value = kv.kvMap[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.kvMap[args.Key] = args.Value
	reply.Value = kv.kvMap[args.Key]
	DPrintf("Put: key=%s, value=%s", args.Key, args.Value)
}

// Append arg to key's value and returns the old value
func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	oldValue := kv.kvMap[args.Key]
	newValue := oldValue + args.Value
	kv.kvMap[args.Key] = newValue

	reply.Value = oldValue
	DPrintf("Append: key=%s, value=%s", args.Key, args.Value)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// create a map to store key-value pairs
	kv.kvMap = make(map[string]string)

	return kv
}
