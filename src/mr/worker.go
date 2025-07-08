package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

type ByKey []KeyValue

func (b ByKey) Len() int           { return len(b) }
func (b ByKey) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }
func (b ByKey) Less(i, j int) bool { return b[i].Key < b[j].Key }

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

func requestTask() RequestTaskReply {
	args, reply := RequestTaskArgs{}, RequestTaskReply{}
	ok := call("Master.AssignTask", &args, &reply)
	if !ok {
		fmt.Println("call Master.AssignTask failed")
	}
	return reply
}

func reportTaskFinished(taskId int) {
	args, reply := ReportTaskArgs{}, ReportTaskReply{}
	args.TaskId = taskId
	ok := call("Master.UpdateTaskFinished", &args, &reply)
	if !ok {
		fmt.Println("call Master.UpdateTaskFinished failed")
	}
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	CallExample()
	loop := true
	for loop {
		reply := requestTask()
		task := &reply.Task
		switch reply.RequestTaskFlag {
		case TaskAssigned:
			switch task.TaskType {
			case MapTask:
				fmt.Printf("performing map task %d %v\n", task.TaskId, task.InputFiles)
				performMapTask(mapf, task)
				reportTaskFinished(task.TaskId)
			case ReduceTask:
				fmt.Printf("performing reduce task %d %v\n", task.TaskId, task.InputFiles)
				performReduceTask(reducef, task)
				reportTaskFinished(task.TaskId)
			}
		case WaitAndTryAgain:
			time.Sleep(time.Second)
		case FinishedAndExit:
			loop = false
		}
	}
}

func performReduceTask(reducef func(string, []string) string, task *Task) {
	intermediate := shuffle(task.InputFiles)

	dir, _ := os.Getwd()
	tmpfile, err := os.CreateTemp(dir, "mr-tmp-"+strconv.Itoa(task.ReduceKth))
	fmt.Printf("%s\t%s\n", dir, tmpfile.Name())
	if err != nil {
		log.Fatal("can not create mr-tmpfile")
	}

	i := 0
	for i < len(intermediate) {
		values := []string{}
		j := i
		for ; j < len(intermediate) && intermediate[i].Key == intermediate[j].Key; j++ {
			values = append(values, intermediate[j].Value)
		}
		res := reducef(intermediate[i].Key, values)
		fmt.Fprintf(tmpfile, "%v %v\n", intermediate[i].Key, res)
		i = j
	}
	tmpfile.Close()

	filename := dir + "/mr-out-" + strconv.Itoa(task.ReduceKth)
	os.Rename(tmpfile.Name(), filename)
}

func shuffle(files []string) []KeyValue {
	kva := []KeyValue{}
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatal("can not open %s", filename)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(kva))

	return kva
}

func performMapTask(mapf func(string, string) []KeyValue, task *Task) {
	intermediate := []KeyValue{}
	for _, filename := range task.InputFiles {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatal("can not open %s", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatal("can not read %s", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}

	for i := 0; i < task.ReduceNum; i++ {
		midFileName := "mr-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
		midFile, _ := os.Create(midFileName)
		enc := json.NewEncoder(midFile)
		for _, kv := range intermediate {
			if ihash(kv.Key)%task.ReduceNum == i {
				enc.Encode(&kv)
			}
		}
		midFile.Close()
	}
}

// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
