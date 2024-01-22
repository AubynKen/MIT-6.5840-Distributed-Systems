package mr

type TaskType uint8

const (
	REDUCE_TYPE TaskType = iota
	MAP_TYPE
	TERMINATE_TYPE // used to indicate that the worker should terminate
)

type TaskStatus uint8

const (
	IDLE TaskStatus = iota
	IN_PROGRESS
	COMPLETED
)

type Task interface {
	Type() TaskType
	Index() int // index of the task in the list of tasks of the same type
}

// MapTask implements the Task interface
type MapTask struct {
	InputFile string
	index     int
}

func (t *MapTask) Type() TaskType {
	return MAP_TYPE
}

func (t *MapTask) Index() int {
	return t.index
}

// ReduceTask implements the Task interface
type ReduceTask struct {
	InputFiles []string
	index      int
}

func (t *ReduceTask) Type() TaskType {
	return REDUCE_TYPE
}

func (t *ReduceTask) Index() int {
	return t.index
}

// TerminateTask implements the Task interface
type TerminateTask struct{}

func (t *TerminateTask) Type() TaskType {
	return TERMINATE_TYPE
}

func (t *TerminateTask) Index() int {
	return -1
}
