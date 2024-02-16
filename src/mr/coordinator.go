package mr

import (
	"fmt"
	"log"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	TotalMapJobs       int
	MapJobs            []*Job
	IntermediateOutput []KeyValue

	MapAllDone bool

	TotalReduceJobs int
	ReduceJobs      []*Job
	OutputFile      *os.File
}

//------- RPC handlers

// AssignMapJob assigns each worker an input file to process
// receive intermediate maps from workers and store in Job struct
func (c *Coordinator) AssignMapJob(args *MapJobArgs, reply *MapJobReply) error {
	job := c.getNextJob("map")
	if job == nil {
		log.Println("Coordinator: No more map jobs available!")
		return fmt.Errorf("No more map jobs available!")
	}
	reply.Job = job
	reply.NReduce = c.TotalReduceJobs
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

func (c *Coordinator) FinishMapJob(args *MapJobReply, reply *MapJobReply) error {
	filename := args.Job.File
	for _, job := range c.MapJobs {
		if job.File == filename {
			job.Status = StatusDone
			log.Printf("updating map job %s status to:  %v\n", job.File, job.Status)
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
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// track map jobs and their status
	c.TotalMapJobs = len(files)
	for i, file := range files {
		c.MapJobs = append(c.MapJobs, &Job{File: file, Status: StatusNotStarted, TaskNumber: i})
	}
	c.MapAllDone = false

	c.ReduceJobs = make([]*Job, nReduce)
	c.TotalReduceJobs = nReduce

	fmt.Printf("There are %d map jobs, and %d reduce jobs\n", c.TotalMapJobs, c.TotalReduceJobs)

	c.server()
	return &c
}
