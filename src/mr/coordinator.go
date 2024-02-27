package mr

import (
	"fmt"
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	TotalMapJobs int
	MapJobs      []*Job

	MapAllDone    bool
	ReduceAllDone bool

	TotalReduceJobs int
	ReduceJobs      []*Job
}

func (c *Coordinator) AssignJob(args *RequestJobArgs, reply *RequestJobReply) error {
	// keep assigning map jobs until all done
	var selectedJob *Job
	if !c.MapAllDone {
		for _, job := range c.MapJobs {
			if job.Status == StatusNotStarted {
				selectedJob = job
				reply.Job = selectedJob
				reply.NReduce = c.TotalReduceJobs
				selectedJob.Status = StatusInProgress

				log.Printf("Assigning map job %s to worker\n", selectedJob.InputFile)
				return nil
			}
		}
		// sleep, then if any are still in progress, move back to "not started queue
		time.Sleep(10 * time.Second)
		for _, job := range c.MapJobs {
			if job.Status == StatusInProgress {
				log.Printf("Move %s job back to queue\n", job.InputFile)
				job.Status = StatusNotStarted
			}
		}
	}

	// check if maps are all done, remaining might all in in progress
	count := 0
	for _, job := range c.MapJobs {
		if job.Status == StatusDone {
			count++
		}
	}
	if count == c.TotalMapJobs {
		c.MapAllDone = true
	} else {
		return fmt.Errorf("No more map jobs to assign, but not done yet")
	}

	if c.MapAllDone {
		log.Printf("All map jobs are done, now assigning reduce jobs\n")
		for _, job := range c.ReduceJobs {
			if job.Status == StatusNotStarted {
				selectedJob = job
				reply.Job = selectedJob
				log.Printf("Assigning reduce job %d to worker\n", selectedJob.TaskNumber)
				return nil
			}
		}
	}
	c.ReduceAllDone = true

	return nil
}
func (c *Coordinator) NotifyJobComplete(args *RequestJobArgs, reply *RequestJobReply) error {
	job := args.Job
	switch job.JobType {
	case "map":
		c.MapJobs[job.TaskNumber].Status = StatusDone
	case "reduce":
		c.ReduceJobs[job.TaskNumber].Status = StatusDone
	}
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

	for _, job := range c.ReduceJobs {
		if job.Status == StatusNotStarted || job.Status == StatusInProgress {
			return false
		}
	}
	return true
}

// MakeCoordinator create a Coordinator.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		TotalMapJobs:    len(files),
		MapJobs:         make([]*Job, len(files)),
		TotalReduceJobs: nReduce,
		ReduceJobs:      make([]*Job, nReduce),
		MapAllDone:      false,
		ReduceAllDone:   false,
	}

	// initialize map jobs
	for i, file := range files {
		c.MapJobs[i] = &Job{InputFile: file, Status: StatusNotStarted, TaskNumber: i, JobType: "map"}
	}

	// initialize final output files and reduce jobs
	for i := 0; i < nReduce; i++ {
		oname := fmt.Sprintf("mr-out-%d", i)
		_, _ = os.Create(oname)
		c.ReduceJobs[i] = &Job{
			InputFiles: generateIntermediateFiles(len(files), i),
			OutputFile: oname,
			Status:     StatusNotStarted,
			TaskNumber: i,
			JobType:    "reduce"}
	}

	fmt.Printf("There are %d map jobs, and %d reduce jobs\n", c.TotalMapJobs, c.TotalReduceJobs)

	c.server()
	return &c
}

// each reduce job will have n number of reduce task input files
// where m is number of input files to map
// mr-m-n
func generateIntermediateFiles(m int, reduceTask int) []string {
	files := []string{}
	for i := 0; i < m; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, reduceTask)
		files = append(files, filename)
	}
	return files
}
