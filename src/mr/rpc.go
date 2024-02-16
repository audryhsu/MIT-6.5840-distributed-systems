package mr

// RPC definitions.

import "os"
import "strconv"

type MapJobReply struct {
	Job     *Job
	NReduce int
}
type MapJobArgs struct {
	File string
	// stored on local worker, so pass it the memory address
	IntermediateOutput *[]KeyValue
	IntermediateFile string
}
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
