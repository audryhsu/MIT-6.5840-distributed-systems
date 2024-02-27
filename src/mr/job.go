package mr

type Status string
type Job struct {
	JobType    string
	InputFile  string // input file for map
	Status     Status // not started, done, in progress
	TaskNumber int

	InputFiles []string // input files for reduce
	OutputFile string   // final output files
}

const (
	StatusInProgress Status = "in progress"
	StatusDone       Status = "done"
	StatusNotStarted        = "not started"
)
