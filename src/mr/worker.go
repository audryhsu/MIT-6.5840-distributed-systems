package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// KeyValue
// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker asks for a job to work on or waits for the next one
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		doMap(mapf)
		time.Sleep(3 * time.Second)
	}
	//doReduce(reducef)

}

// doMap
// read input file and pass contents to mapf
// loop over []kv and encode and write each kv to intermediate outputfile based on ihash
// tell coordinator done and location of intermediate output
func doMap(mapf func(string, string) []KeyValue) {
	reply := &MapJobReply{}
	if ok := call("Coordinator.AssignMapJob", &MapJobArgs{}, &reply); !ok {
		log.Println("No map job available")
		return
	}
	filename := reply.Job.File
	log.Printf("reply.InputFile %s\n", filename)

	// get contents of file and pass to mapf
	f, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(f)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	f.Close()
	// returns a slice of kvs... [ {k: 'the', v: '1'}, {k: 'the', v:'1'}, ..]
	kvSlice := mapf(filename, string(content))
	fmt.Printf("read %d words from %s\n", len(kvSlice), filename)

	// iterate over kv slices and organize into reduce partitions
	intermediate := make([][]KeyValue, reply.NReduce)
	for _, kv := range kvSlice {
		reduceTaskNumber := ihash(kv.Key) % reply.NReduce
		intermediate[reduceTaskNumber] = append(intermediate[reduceTaskNumber], kv)
	}

	// store intermediate kvs in files that can be read back during reduce tasks
	ioFilename := fmt.Sprintf("mr-%d-%d", reply.Job.TaskNumber, reply.NReduce)
	// for each reduce partition, create a temp file
	for _, p := range intermediate {
		ioFile, err := os.CreateTemp("", ioFilename)
		defer ioFile.Close()
		enc := json.NewEncoder(ioFile)
		// for each kv in a partition, write to file
		for _, kv := range p {
			if err := enc.Encode(&kv); err != nil {
				log.Fatalf("couldn't encode KV to partition %v\n", err)
			}
		}
		err = os.Rename(ioFile.Name(), ioFilename)
		if err != nil {
			log.Fatalf("error renaming file %s\n", ioFilename)
		}
	}

	// does this update the reference?
	reply.Job.Status = StatusDone
	log.Printf("Worker updated %s job with status %s\n", filename, reply.Job.Status)

	arg := reply
	if ok := call("Coordinator.FinishMapJob", &arg, &MapJobReply{}); !ok {
		log.Panic("RPC failed: couldn't finish map job")
	}

}

// CallExample example function to show how to make an RPC call to the coordinator.
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
func call(rpcname string, args interface{}, reply interface{}) bool {
	//c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
