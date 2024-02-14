package mr

import (
	"fmt"
	"log"
	"sort"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	TotalMapJobs        int
	MapJobs             []*Job
	IntermediateOutputs []*[]KeyValue

	TotalReduceJobs int
	ReduceJobs      []*Job
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//------- RPC handlers

// AssignMapJob assigns each worker an input file to process
// receive intermediate maps from workers and store in Job struct
func (c *Coordinator) AssignMapJob(args *MapJobArgs, reply *MapJobReply) error {
	job := c.getNextJob("map")
	if job == nil {
		log.Println("No more map jobs available!")
		return nil
	}
	reply.File = job.File

	// update tracker that a job was assigned
	job.Status = StatusInProgress
	return nil
}

func (c *Coordinator) getNextJob(jobType string) *Job {
	if jobType == "map" {
		for _, job := range c.MapJobs {
			if job.Status == StatusNotStarted {
				return job
			}
		}
	} else if jobType == "reduce" {
		for _, job := range c.ReduceJobs {
			if job.Status == StatusNotStarted {
				return job
			}
		}
	}
	return nil
}

func (c *Coordinator) FinishMapJob(args *MapJobArgs, reply *MapJobReply) error {
	filename := args.File
	for _, job := range c.MapJobs {
		if job.File == filename {
			job.Status = StatusDone
			c.IntermediateOutputs = append(c.IntermediateOutputs, args.IntermediateOutput)

			// confirm with worker we've marked this job done
			reply.Status = StatusDone
		}
	}
	return nil
}

func (c *Coordinator) AssignReduceJobs() error {
	// once all map jobs are done
	// sort each slice of KVs
	// partition each slice of KVs into nReduce tasks
	for _, output := range c.IntermediateOutputs {
		// does this work?
		sort.Sort(ByKey(*output))
	}

	var partitions [][]KeyValue
	// todo bucket the intermediate outputs into p buckets

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
	log.Println("coordinator is listening...")
}

// Done called by main/mrcoordinator.go periodically to find out if the entire job has finished.
func (c *Coordinator) Done() bool {
	for _, job := range c.MapJobs {
		if job.Status == StatusNotStarted || job.Status == StatusInProgress {
			return false
		}
	}

	return true
}

// MakeCoordinator create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// track map jobs and their status
	c.TotalMapJobs = len(files)
	for _, file := range files {
		c.MapJobs = append(c.MapJobs, &Job{File: file, Status: StatusNotStarted})
	}
	// todo track reduce jobs
	c.TotalReduceJobs = nReduce

	fmt.Printf("There are %d map jobs, and %d reduce jobs\n", c.TotalMapJobs, c.TotalReduceJobs)

	c.server()
	return &c
}
