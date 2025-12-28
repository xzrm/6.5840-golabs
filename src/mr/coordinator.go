package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

type Task struct {
	ID        int    // task number (map or reduce index)
	Filename  string // for map tasks only (input file)
	Status    TaskStatus
	StartTime time.Time // when it was assigned to a worker
}

type Coordinator struct {
	mu sync.Mutex

	// Input files for map tasks
	files []string

	// Map and Reduce tasks
	mapTasks    []Task
	reduceTasks []Task

	nReduce int

	// What phase are we in? (maps first, then reduces)
	mapDone    bool
	reduceDone bool
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
}

// todo: in case of multiple workers there might be two processes that work on the same mapID; probably good to add a mechanism to kill the previous process before the same ID is given away
func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.mapDone {
		for i := range c.mapTasks {
			t := &c.mapTasks[i]
			if t.Status == Idle || (t.Status == InProgress && time.Since(t.StartTime) > 10*time.Second) {
				t.StartTime = time.Now()
				t.Status = InProgress

				reply.TaskType = "map"
				reply.TaskID = t.ID
				reply.Filename = t.Filename
				reply.NReduce = c.nReduce
				return nil
			}
		}
		reply.TaskType = "wait"
		return nil
	}

	if !c.reduceDone {
		for i := range c.reduceTasks {
			t := &c.reduceTasks[i]
			if t.Status == Idle || (t.Status == InProgress && time.Since(t.StartTime) > 10*time.Second) {
				t.StartTime = time.Now()
				t.Status = InProgress

				reply.TaskType = "reduce"
				reply.TaskID = t.ID
				reply.NMap = len(c.mapTasks)
				return nil
			}
		}
		reply.TaskType = "wait"
		return nil
	}

	reply.TaskType = "exit"
	return nil
}

// worker reports finishing a task
func (c *Coordinator) ReportTaskDone(args *ReportTaskDoneArgs, reply *ReportTaskDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.TaskType {
	case "map":
		c.mapTasks[args.TaskID].Status = Completed
		c.mapDone = c.allTasksDone(c.mapTasks)
	case "reduce":
		c.reduceTasks[args.TaskID].Status = Completed
		c.reduceDone = c.allTasksDone(c.reduceTasks)
	}

	reply.Ack = true
	return nil
}

func (c *Coordinator) allTasksDone(tasks []Task) bool {
	for _, t := range tasks {
		if t.Status != Completed {
			return false
		}
	}
	return true
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.mapDone && c.reduceDone
}

// func (c *Coordinator) Done() bool {
// 	ret := false

// 	// Your code here.

// 	return ret
// }

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{files: files, nReduce: nReduce}

	for i, f := range files {
		c.mapTasks = append(c.mapTasks, Task{ID: i, Filename: f, Status: Idle})
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTasks = append(c.reduceTasks, Task{ID: i, Status: Idle})
	}

	c.server()
	return &c
}
