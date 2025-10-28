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

type TaskState int

const (
	Idle       TaskState = 0
	InProgress TaskState = 1
	Completed  TaskState = 2
)

type TaskInfo struct {
	State     TaskState
	StartTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	mu              sync.Mutex
	mapTasks        []TaskInfo
	reduceTasks     []TaskInfo
	mapFiles        []string
	nReduce         int
	nMap            int
	mapCompleted    int
	reduceCompleted int
	done            bool
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// RPC handler for workers to request tasks
func (c *Coordinator) RequestTask(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check for any timed-out tasks and mark them as Idle
	c.checkTimeouts()

	// Try to assign a map task first
	for i := 0; i < c.nMap; i++ {
		if c.mapTasks[i].State == Idle {
			c.mapTasks[i].State = InProgress
			c.mapTasks[i].StartTime = time.Now()
			reply.TaskType = MapTask
			reply.TaskId = i
			reply.NReduce = c.nReduce
			reply.NMap = c.nMap
			reply.MapInputFile = c.mapFiles[i]
			return nil
		}
	}

	// If all map tasks are done, try to assign a reduce task
	if c.mapCompleted == c.nMap {
		for i := 0; i < c.nReduce; i++ {
			if c.reduceTasks[i].State == Idle {
				c.reduceTasks[i].State = InProgress
				c.reduceTasks[i].StartTime = time.Now()
				reply.TaskType = ReduceTask
				reply.TaskId = i
				reply.NReduce = c.nReduce
				reply.NMap = c.nMap
				return nil
			}
		}
	}

	// If all tasks are done, tell worker to exit
	if c.reduceCompleted == c.nReduce && c.mapCompleted == c.nMap {
		reply.TaskType = ExitTask
		return nil
	}

	// Otherwise, tell worker to wait
	reply.TaskType = WaitTask
	return nil
}

// RPC handler for workers to report task completion
func (c *Coordinator) TaskComplete(args *TaskCompleteArgs, reply *TaskCompleteReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskType == MapTask {
		if args.TaskId >= 0 && args.TaskId < c.nMap {
			if c.mapTasks[args.TaskId].State == InProgress {
				c.mapTasks[args.TaskId].State = Completed
				c.mapCompleted++
			}
		}
	} else if args.TaskType == ReduceTask {
		if args.TaskId >= 0 && args.TaskId < c.nReduce {
			if c.reduceTasks[args.TaskId].State == InProgress {
				c.reduceTasks[args.TaskId].State = Completed
				c.reduceCompleted++
			}
		}
	}

	return nil
}

// Check for timed-out tasks and reset them to Idle
func (c *Coordinator) checkTimeouts() {
	now := time.Now()
	timeout := 10 * time.Second

	for i := 0; i < c.nMap; i++ {
		if c.mapTasks[i].State == InProgress {
			if now.Sub(c.mapTasks[i].StartTime) > timeout {
				c.mapTasks[i].State = Idle
			}
		}
	}

	for i := 0; i < c.nReduce; i++ {
		if c.reduceTasks[i].State == InProgress {
			if now.Sub(c.reduceTasks[i].StartTime) > timeout {
				c.reduceTasks[i].State = Idle
			}
		}
	}
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.mapCompleted == c.nMap && c.reduceCompleted == c.nReduce
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nReduce = nReduce
	c.nMap = len(files)
	c.mapFiles = files
	c.mapTasks = make([]TaskInfo, c.nMap)
	c.reduceTasks = make([]TaskInfo, c.nReduce)
	c.mapCompleted = 0
	c.reduceCompleted = 0
	c.done = false

	// Initialize all tasks as Idle
	for i := 0; i < c.nMap; i++ {
		c.mapTasks[i].State = Idle
	}
	for i := 0; i < c.nReduce; i++ {
		c.reduceTasks[i].State = Idle
	}

	c.server()
	return &c
}
