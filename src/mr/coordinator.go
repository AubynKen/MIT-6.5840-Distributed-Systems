package mr

import (
	"encoding/gob"
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
	// mapStart[i] is the time when the task 'i' was assigned to a worker
	// this value has no meaning if the task is in progress
	mapStart []time.Time

	nReduce      int
	reduceTasks  []Task
	reduceStatus []TaskStatus
	reduceStart  []time.Time
}

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// findFirstIdleTask returns the index of the first idle task and true if found,
// otherwise -1 and false
func findFirstIdleTask(
	statusSlice []TaskStatus) (index int, found bool) {

	for index, status := range statusSlice {
		if status == Idle {
			return index, true
		}
	}
	return -1, false
}

// findOldestInProgressTask returns the index of the oldest in progress task and true if found,
// otherwise -1 and false
func findOldestInProgressTask(statusSlice []TaskStatus, startSlice []time.Time) (index int, found bool) {
	oldest, index := time.Now(), -1

	for idx, status := range statusSlice {
		if status != InProgress {
			continue
		}

		index = idx
		if startSlice[index].Before(oldest) {
			oldest = startSlice[index]
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

	gob.Register(Task{})

	// if there are idle map tasks, return one
	idx, found := findFirstIdleTask(c.mapStatus)
	if found {
		reply.Task = c.mapTasks[idx]
		c.mapStatus[idx] = InProgress
		c.mapStart[idx] = time.Now()
		return nil
	}

	// if there are in progress map tasks, return the oldest one
	idx, found = findOldestInProgressTask(c.mapStatus, c.mapStart)
	if found {
		reply.Task = c.mapTasks[idx]
		c.mapStart[idx] = time.Now()
		return nil
	}

	// if there are idle reduce tasks, return one
	idx, found = findFirstIdleTask(c.reduceStatus)
	if found {
		reply.Task = c.reduceTasks[idx]
		c.reduceStatus[idx] = InProgress
		c.reduceStart[idx] = time.Now()
		return nil
	}

	// if there are in progress reduce tasks, return the oldest one
	idx, found = findOldestInProgressTask(c.reduceStatus, c.reduceStart)
	if found {
		reply.Task = c.reduceTasks[idx]
		c.reduceStart[idx] = time.Now()
		return nil
	}

	// if there are no idle or in progress tasks, return terminate task
	reply.Task = MakeTerminateTask()
	return nil
}

// ReportTask is an RPC handler that reports the completion of a task
func (c *Coordinator) ReportTask(
	args *ReportTaskArgs, _ *ReportTaskReply,
) error {
	c.Lock()
	defer c.Unlock()

	switch args.Task.Type {
	case MapType:
		c.mapStatus[args.Task.Index] = Completed
	case ReduceType:
		c.reduceStatus[args.Task.Index] = Completed
	default:
		log.Fatalf("Unknown task type: %v", args.Task)
	}

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	err := rpc.Register(c)
	if err != nil {
		log.Fatal("rpc error:", err)
	}
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	socketName := coordinatorSock()
	err = os.Remove(socketName)
	if err != nil {
		//log.Fatal("remove socket error:", err)
		log.Println("remove socket error:", err)
	}
	l, e := net.Listen("unix", socketName)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go func() {
		err := http.Serve(l, nil)
		if err != nil {
			log.Fatal("serve error:", err)
		}
	}()
}

// Done is called by main/mrcoordinator.go periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.Lock()
	defer c.Unlock()

	// check if all map tasks are completed
	for _, status := range c.mapStatus {
		if status != Completed {
			return false
		}
	}

	// check if all reduce tasks are completed
	for _, status := range c.reduceStatus {
		if status != Completed {
			return false
		}
	}

	return true
}

// MakeCoordinator create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	nMap := len(files)

	c := Coordinator{
		inputFiles: files,
		nMap:       nMap,
		mapTasks:   make([]Task, 0, nMap),
		mapStatus:  make([]TaskStatus, nMap),
		mapStart:   make([]time.Time, nMap),

		nReduce:      nReduce,
		reduceTasks:  make([]Task, 0, nReduce),
		reduceStatus: make([]TaskStatus, nReduce),
		reduceStart:  make([]time.Time, nReduce),
	}

	// create map tasks
	for i, file := range files {
		c.mapTasks = append(c.mapTasks, MakeMapTask(file, i, nMap, nReduce))
		c.mapStatus[i] = Idle
	}

	// create reduce tasks
	for i := 0; i < nReduce; i++ {
		c.reduceTasks = append(c.reduceTasks, MakeReduceTask(i, nMap, nReduce))
		c.reduceStatus[i] = Idle
	}

	c.server()
	return &c
}
