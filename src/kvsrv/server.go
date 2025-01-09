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
	seen  map[int64]bool
	// keep track of the length of the latest append for each key
	latestAppend map[string]int
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply.Value = kv.kvMap[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, exists := kv.seen[args.RequestId]
	if exists {
		reply.Value = kv.kvMap[args.Key]
		return
	}

	kv.kvMap[args.Key] = args.Value
	reply.Value = kv.kvMap[args.Key]

	// add to seen
	kv.seen[args.RequestId] = true
	DPrintf("Put: key=%s, value=%s", args.Key, args.Value)
}

// Append arg to key's value and returns the old value
func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, exists := kv.seen[args.RequestId]
	if exists {
		// need to return the OLD value
		//take current key value and slice off lenght of latest apend
		// x: "12"
		// lenofLatest = 1
		// 2 - 1 = 1
		// x: "1234"
		// lenofLatest =2
		// 4 - 2 = 2
		currVal := kv.kvMap[args.Key]
		lenOfLatestAppend := kv.latestAppend[args.Key]
		delta := len(currVal) - lenOfLatestAppend
		reply.Value = currVal[:delta]
		return
	}

	oldValue := kv.kvMap[args.Key]
	newValue := oldValue + args.Value
	kv.kvMap[args.Key] = newValue
	kv.latestAppend[args.Key] = len(newValue)

	reply.Value = oldValue
	kv.seen[args.RequestId] = true
	//DPrintf("Append: key=%s, value=%s", args.Key, args.Value)
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	// create a map to store key-value pairs
	kv.kvMap = make(map[string]string)
	// create a map to store seen client request Ids to detect duplicates
	kv.seen = make(map[int64]bool)
	kv.latestAppend = make(map[string]int)

	return kv
}
