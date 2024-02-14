package mr

import (
	"fmt"
	"io"
	"os"
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

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker is called by main/mrworker.go
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// todo make RPC call to coordinator to get a task
	GetMapJob(mapf)

}

func GetMapJob(mapf func(string, string) []KeyValue) {
	reply := &MapJobReply{}
	ok := call("Coordinator.AssignMapJob", &MapJobArgs{}, &reply)
	if !ok {
		log.Panic("RPC failed: couldn't get map job")
	}
	fmt.Printf("reply.File %s\n", reply.File)
	filename := reply.File

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

	// send Coordinator memory address of intermediate ds
	a := MapJobArgs{
		IntermediateOutput: &kvSlice,
		File:               reply.File,
	}
	r := MapJobReply{}
	ok = call("Coordinator.FinishMapJob", &a, &r)
	if !ok {
		log.Panic("RPC failed: couldn't finish map job")
	}

	fmt.Printf("Coordinator updated %s job with status %s\n", filename, r.Status)

}
func Reduce(reducef func(string, []string) string) {
	// todo ask for a reduce job
	// sort the intermediateOutput slice of kvs by key
	// iterate over kvs and pass kvs to reduce function for each unique k
	// write reduce output to a file
	// tell Coord job is done
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
