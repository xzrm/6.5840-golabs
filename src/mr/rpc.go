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

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

// worker → coordinator: request a task
type RequestTaskArgs struct{}

type RequestTaskReply struct {
	TaskType string // "map", "reduce", "wait", "exit"
	TaskID   int
	Filename string // only for map tasks
	NReduce  int
	NMap     int // reducers need to know how many map tasks there were
}

// worker → coordinator: report task done
type ReportTaskDoneArgs struct {
	TaskType string // "map" or "reduce"
	TaskID   int
}

type ReportTaskDoneReply struct {
	Ack bool
}
