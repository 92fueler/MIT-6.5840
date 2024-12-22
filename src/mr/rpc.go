package mr

import (
	"os"
	"strconv"
)

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
	ExitTask
)

type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

type GetTaskArgs struct{}

type GetTaskReply struct {
	TaskType TaskType
	TaskID   int
	FileName string
	NReduce  int
	NMap     int
}

type TaskCompletionArgs struct {
	TaskType TaskType
	TaskID   int
}

type TaskCompletionReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
