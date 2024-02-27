package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
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
		// get a task from the coordinator
		reply := &RequestJobReply{}
		arg := &RequestJobArgs{}
		if ok := call("Coordinator.AssignJob", &arg, &reply); !ok {
			log.Println("No job available")
			return
		}

		switch reply.Job.JobType {
		case "map":
			doMap(mapf, reply)
		case "reduce":
			doReduce(reducef, reply)
		}
		time.Sleep(3 * time.Second)
	}

}

// doMap
// read input file and pass contents to mapf
// loop over []kv and encode and write each kv to intermediate outputfile based on ihash
// tell coordinator done and location of intermediate output
func doMap(mapf func(string, string) []KeyValue, reply *RequestJobReply) {
	filename := reply.Job.InputFile
	log.Printf("%s task: %s\n", reply.Job.JobType, filename)

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

	// iterate over kv slices and organize into reduce partitions
	intermediate := make([][]KeyValue, reply.NReduce)
	for _, kv := range kvSlice {
		reduceTaskNumber := ihash(kv.Key) % reply.NReduce
		intermediate[reduceTaskNumber] = append(intermediate[reduceTaskNumber], kv)
	}

	// store intermediate kvs in files that can be read back during reduce tasks
	for i, p := range intermediate {
		ioFilename := fmt.Sprintf("mr-%d-%d", reply.Job.TaskNumber, i)
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

	reply.Job.Status = StatusDone
	log.Printf("%s: %s\n", reply.Job.Status, filename)

	arg := &RequestJobArgs{
		Job: reply.Job,
	}
	if ok := call("Coordinator.NotifyJobComplete", &arg, &RequestJobReply{}); !ok {
		log.Panic("RPC failed: couldn't finish map job")
	}

}

// doReduce
// for each intermediate output file, read and decode kvs into []kv
// sort []kv
// create final output file
// run reduce for each unique key and write to outputfile
func doReduce(reducef func(string, []string) string, reply *RequestJobReply) {
	log.Printf("reduce task: %d\n", reply.Job.TaskNumber)
	// for each inputfile, read in intermediate kvs from filesystem into a slice
	intermediate := []KeyValue{}
	for _, filename := range reply.Job.InputFiles {
		file, err := os.Open(filename)
		defer file.Close()
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		//log.Printf("reading kvs from %s\n", filename)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}
	// create the final output file
	ofile, _ := os.Create(reply.Job.OutputFile)
	defer ofile.Close()

	// sort the KVs
	sort.Sort(ByKey(intermediate))

	i := 0
	for i < len(intermediate) {
		j := i + 1
		// find range of index where keys are the same
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		// gather all values for the same key
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
			// saves to a queue to be assigned to reduce workers
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	reply.Job.Status = StatusDone

	if ok := call("Coordinator.NotifyJobComplete", reply, &RequestJobReply{}); !ok {
		log.Panic("RPC failed: couldn't finish reduce job")
	}
	log.Printf("%s: %d\n", reply.Job.Status, reply.Job.TaskNumber)
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
