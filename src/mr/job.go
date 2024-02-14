package mr

type Job struct {
	File   string
	Status Status // not started, done, in progress ?
	//IntermediateOutput *[]KeyValue
}
type Status string

const (
	StatusInProgress Status = "in progress"
	StatusDone       Status = "done"
	StatusNotStarted        = "not started"
)
