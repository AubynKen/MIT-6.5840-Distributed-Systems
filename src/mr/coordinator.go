package mr

import (
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	sync.Mutex

	inputFiles []string

	nMap           int
	mapTasks       []Task
	mapStatus      []TaskStatus
	mapStart       []time.Time // time when the task was assigned to a worker
	nMapInProgress int
	nMapCompleted  int

	nReduce          int
	reduceTasks      []Task
	reduceStatus     []TaskStatus
	reduceStart      []time.Time
	nReduceDone      int
	nReduceCompleted int
}

// init registers Task with gob so that it can be used for RPC
func init() {
	gob.Register(Task{})
}

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// findFirstIdleTask returns the index of the first idle task and true if found,
// otherwise -1 and false
func (c *Coordinator) findFirstIdleTask(taskType TaskType) (index int, found bool, err error) {
	var tasksStatuses []TaskStatus

	switch taskType {
	case TaskTypeMap:
		tasksStatuses = c.mapStatus
	case TaskTypeReduce:
		tasksStatuses = c.reduceStatus
	default:
		return -1, false, fmt.Errorf("unknown job type: %v", taskType)
	}

	for index, status := range tasksStatuses {
		if status == taskStatusIdle {
			return index, true, nil
		}
	}
	return -1, false, nil
}

// findOldestInProgressTask returns the index of the oldest in progress task and true if found,
// otherwise -1 and false
func (c *Coordinator) findOldestInProgressTask(taskType TaskType) (index int, found bool, err error) {
	var statusSlice []TaskStatus
	var startSlice []time.Time

	switch taskType {
	case TaskTypeMap:
		statusSlice = c.mapStatus
		startSlice = c.mapStart
	case TaskTypeReduce:
		statusSlice = c.reduceStatus
		startSlice = c.reduceStart
	default:
		return -1, false, fmt.Errorf("unknown job type: %v", taskType)
	}

	oldest, index, found := time.Now(), -1, false
	for idx, status := range statusSlice {
		if status != taskStatusInProgress {
			continue
		}

		index, found = idx, true
		if startSlice[index].Before(oldest) {
			oldest = startSlice[index]
		}
	}

	return index, found, nil
}

type findTaskFunc func(taskType TaskType) (taskIndex int, found bool, err error)

// RequestTask is an RPC handler that returns a task to a worker
func (c *Coordinator) RequestTask(
	_ *RequestTaskArgs, reply *RequestTaskReply,
) error {
	c.Lock()
	defer c.Unlock()

	for _, taskType := range []TaskType{TaskTypeMap, TaskTypeReduce} {
		idx, found, err := c.findFirstIdleTask(taskType)
		if err != nil {
			return fmt.Errorf("error finding idle task: %v", err)
		}
		if found {
			switch taskType {
			case TaskTypeMap:
				reply.Task = c.mapTasks[idx]
				c.mapStatus[idx] = taskStatusInProgress
				c.mapStart[idx] = time.Now()
			case TaskTypeReduce:
				reply.Task = c.reduceTasks[idx]
				c.reduceStatus[idx] = taskStatusInProgress
				c.reduceStart[idx] = time.Now()
			}
			return nil
		}

		idx, found, err = c.findOldestInProgressTask(taskType)
		if err != nil {
			return fmt.Errorf("error finding oldest in progress task: %v", err)
		}
		if found {
			switch taskType {
			case TaskTypeMap:
				reply.Task = c.mapTasks[idx]
			case TaskTypeReduce:
				reply.Task = c.reduceTasks[idx]
			}
			return nil
		}
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
	case TaskTypeMap:
		c.mapStatus[args.Task.Index] = taskStatusCompleted
	case TaskTypeReduce:
		c.reduceStatus[args.Task.Index] = taskStatusCompleted
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
		if status != taskStatusCompleted {
			return false
		}
	}

	// check if all reduce tasks are completed
	for _, status := range c.reduceStatus {
		if status != taskStatusCompleted {
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
		c.mapStatus[i] = taskStatusIdle
	}

	// create reduce tasks
	for i := 0; i < nReduce; i++ {
		c.reduceTasks = append(c.reduceTasks, MakeReduceTask(i, nMap, nReduce))
		c.reduceStatus[i] = taskStatusIdle
	}

	c.server()
	return &c
}
