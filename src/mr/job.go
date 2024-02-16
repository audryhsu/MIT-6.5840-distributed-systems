package mr

type Status string
type Job struct {
	File       string // either the input file to map, or the intermediate output file for reduce
	Status     Status // not started, done, in progress ?
	TaskNumber int
}
type Status string

const (
	StatusInProgress Status = "in progress"
	StatusDone       Status = "done"
	StatusNotStarted        = "not started"
)
