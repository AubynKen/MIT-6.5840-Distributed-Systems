package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	sync.Mutex

	inputFiles []string

	nMap      int
	mapTasks  []Task
	mapStatus []TaskStatus
	// mapStart[i] is the time when the task i was assigned to a worker
	// this value has no meaning if the task is in progress
	mapStart []time.Time

	nReduce      int
	reduceTasks  []Task
	reduceStatus []TaskStatus
	reduceStart  []time.Time
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// findFirstIdleTask returns the index of the first idle task and true if found,
// otherwise -1 and false
func findFirstIdleTask(
	statusSlice []TaskStatus) (index int, found bool) {

	for index, status := range statusSlice {
		if status == IDLE {
			return index, true
		}
	}
	return -1, false
}

// findOldestInProgressTask returns the index of the oldest in progress task and true if found,
// otherwise -1 and false
func findOldestInProgressTask(tasks []Task, statusSlice []TaskStatus) (index int, found bool) {
	oldest, index := time.Now(), -1

	for idx, status := range c.mapStatus {
		if status != IN_PROGRESS {
			continue
		}

		index = idx
		if c.mapStart[index].Before(oldest) {
			oldest = c.mapStart[index]
		}
	}

	if index == -1 {
		return -1, false
	}
	return index, true
}

// RequestTask is an RPC handler that returns a task to a worker
func (c *Coordinator) RequestTask(
	_ *RequestTaskArgs, reply *RequestTaskReply,
) error {
	c.Lock()
	defer c.Unlock()

	// if there are idle map tasks, return one
	idx, found := findFirstIdleTask(c.mapStatus)
	if found {
		reply.Task = c.mapTasks[idx]
		c.mapStatus[idx] = IN_PROGRESS
		c.mapStart[idx] = time.Now()
		return nil
	}

	// if there are in progress map tasks, return the oldest one
	idx, found = findOldestInProgressTask(c.mapTasks, c.mapStatus)
	if found {
		reply.Task = c.mapTasks[idx]
		c.mapStart[idx] = time.Now()
		return nil
	}

	// if there are idle reduce tasks, return one
	idx, found = findFirstIdleTask(c.reduceStatus)
	if found {
		reply.Task = c.reduceTasks[idx]
		c.reduceStatus[idx] = IN_PROGRESS
		c.reduceStart[idx] = time.Now()
		return nil
	}

	// if there are in progress reduce tasks, return the oldest one
	idx, found = findOldestInProgressTask(c.reduceTasks, c.reduceStatus)
	if found {
		reply.Task = c.reduceTasks[idx]
		c.reduceStart[idx] = time.Now()
		return nil
	}

	// if there are no idle or in progress tasks, return terminate task
	reply.Task = &TerminateTask{}
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	nMap := len(files)

	c := Coordinator{
		inputFiles: files,
		nMap:       nMap,
		mapTasks:   make([]Task, nMap),
		mapStatus:  make([]TaskStatus, nMap),
		mapStart:   make([]time.Time, nMap),

		nReduce:      nReduce,
		reduceTasks:  make([]Task, nReduce),
		reduceStatus: make([]TaskStatus, nReduce),
		reduceStart:  make([]time.Time, nReduce),
	}

	c.server()
	return &c
}
