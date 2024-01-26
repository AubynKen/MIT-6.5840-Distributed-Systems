package mr

import "fmt"

type TaskType uint8

const (
	TaskTypeReduce TaskType = iota
	TaskTypeMap
	TaskTypeTerminate // used to indicate that the worker should terminate
)

type TaskStatus uint8

const (
	taskStatusIdle TaskStatus = iota
	taskStatusInProgress
	taskStatusCompleted
)

type Task struct {
	Type          TaskType
	InputFiles    []string
	Index         int // index of the task in the list of tasks of the same type
	NMap, NReduce int
}

func MakeMapTask(inputFile string, index, nMap, nReduce int) Task {
	return Task{
		Type:       TaskTypeMap,
		InputFiles: []string{inputFile},
		Index:      index,
		NMap:       nMap,
		NReduce:    nReduce,
	}
}

func MakeReduceTask(index, nMap, nReduce int) Task {
	task := Task{
		Type:       TaskTypeReduce,
		InputFiles: make([]string, 0, nMap),
		Index:      index,
		NMap:       nMap,
		NReduce:    nReduce,
	}

	for i := 0; i < nMap; i++ {
		task.InputFiles = append(task.InputFiles,
			fmt.Sprintf("mr-%d-%d", i, index))
	}

	return task
}

func MakeTerminateTask() Task {
	return Task{Type: TaskTypeTerminate}
}
