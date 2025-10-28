package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type TaskType int

const (
	MapTask    TaskType = 0
	ReduceTask TaskType = 1
	WaitTask   TaskType = 2
	ExitTask   TaskType = 3
)

type TaskArgs struct {
}

type TaskReply struct {
	TaskType     TaskType
	TaskId       int
	NReduce      int
	NMap         int
	MapInputFile string
	ReduceFiles  []string
}

type TaskCompleteArgs struct {
	TaskType TaskType
	TaskId   int
}

type TaskCompleteReply struct {
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
