package mr

import (
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type worker struct {
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

// requestTask requests a task from the coordinator and returns it if successful
func (w *worker) requestTask() (task *Task, ok bool) {
	args := RequestTaskArgs{}
	reply := RequestTaskReply{}
	ok = call("Coordinator.RequestTask", &args, &reply)
	if !ok {
		return nil, false
	}
	return &reply.Task, true
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func (w *worker) processMap(task *Task) (err error) {
	// Read input file
	filename := task.InputFiles[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	// insert intermediate results into buckets
	buckets := make([][]KeyValue, task.NReduce)
	for _, kv := range w.mapf(filename, string(content)) {
		hashCode := ihash(kv.Key) % task.NReduce
		buckets[hashCode] = append(buckets[hashCode], kv)
	}

	// sort buckets
	for _, bucket := range buckets {
		sort.Sort(ByKey(bucket))
	}

	// write intermediate results to files
	i := task.Index
	for j, bucket := range buckets {
		oname := fmt.Sprintf("mr-%d-%d", i, j)
		ofile, err := os.Create(oname)
		if err != nil {
			log.Fatalf("cannot create %v", oname)
			return err
		}

		for _, kv := range bucket {
			fmt.Fprintf(ofile, "%v %v\n", kv.Key, kv.Value)
		}
		ofile.Close()
	}

	// notify coordinator that task is complete
	args := ReportTaskArgs{Task: *task}
	reply := ReportTaskReply{}
	ok := call("Coordinator.ReportTask", &args, &reply)
	if !ok {
		return fmt.Errorf("Failed to notify coordinator of task completion")
	}

	return nil
}

func (w *worker) processReduce(task *Task) interface{} {
	// read intermediate files
	reduceIdx := task.Index
	var intermediate []KeyValue
	for mapIndex := 0; mapIndex < task.NMap; mapIndex++ {
		filename := fmt.Sprintf("mr-%d-%d", mapIndex, reduceIdx)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()

		for _, line := range strings.Split(string(content), "\n") {
			if line == "" {
				continue
			}
			kv := strings.Split(line, " ")
			intermediate = append(intermediate, KeyValue{kv[0], kv[1]})
		}
	}

	// sort intermediate
	sort.Sort(ByKey(intermediate))

	// call reduce function
	oname := fmt.Sprintf("mr-out-%d", task.Index)
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("cannot create %v", oname)
		return err
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		output := w.reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()

	// notify coordinator that task is complete
	args := ReportTaskArgs{Task: *task}
	reply := ReportTaskReply{}
	ok := call("Coordinator.ReportTask", &args, &reply)
	if !ok {
		return fmt.Errorf("Failed to notify coordinator of task completion")
	}

	return nil
}

// // main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	w := worker{mapf, reducef}
	//gob.Register(MapTask{})
	gob.Register(Task{})

	for {
		task, ok := w.requestTask()
		if !ok {
			fmt.Println("Failed to request task, retrying in 1 second...")
			time.Sleep(time.Second)
			continue
		}

		switch task.Type {
		case TaskTypeTerminate:
			fmt.Println("Received terminate task, terminating...")
			return

		case TaskTypeMap:
			fmt.Println("Received map task")
			if err := w.processMap(task); err != nil {
				log.Fatalf("Failed to process map task: %v", err)
			}

		case TaskTypeReduce:
			fmt.Println("Received reduce task")
			if err := w.processReduce(task); err != nil {
				log.Fatalf("Failed to process reduce task: %v", err)
			}
		}
	}

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	return
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
