package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type TaskType int

type TaskState int

type JobPhase int

const (
	MapTask TaskType = iota
	ReduceTask
)

const (
	Waiting TaskState = iota
	Working
	Finished
)

const (
	MapPhase JobPhase = iota
	ReducePhase
	AllDone
)

var mu sync.Mutex

type Task struct {
	TaskId     int
	TaskType   TaskType
	TaskState  TaskState
	InputFiles []string
	ReduceNum  int
	ReduceKth  int
	StartTime  time.Time
}

type Master struct {
	TaskIdForGen int
	CurrentPhase JobPhase
	MapTasks     chan *Task
	ReduceTasks  chan *Task
	TaskMap      map[int]*Task
	MapperNum    int
	ReducerNum   int
}

func (m *Master) generateTaskId() int {
	res := m.TaskIdForGen
	m.TaskIdForGen++
	return res
}

func (m *Master) makeMapTasks(files []string) {
	for _, file := range files {
		task := Task{
			TaskId:     m.generateTaskId(),
			TaskType:   MapTask,
			TaskState:  Waiting,
			InputFiles: []string{file},
			ReduceNum:  m.ReducerNum,
		}
		fmt.Printf("map task %d %v generated\n", task.TaskId, task.InputFiles)
		m.MapTasks <- &task
	}
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) AssignTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	mu.Lock()
	defer mu.Unlock()
	switch m.CurrentPhase {
	case MapPhase:
		if len(m.MapTasks) > 0 {
			task := <-m.MapTasks
			reply.RequestTaskFlag = TaskAssigned
			reply.Task = *task
			task.TaskState = Working
			task.StartTime = time.Now()
			m.TaskMap[task.TaskId] = task
			fmt.Printf("map task %d %v assigned\n", task.TaskId, task.InputFiles)
		} else {
			reply.RequestTaskFlag = WaitAndTryAgain
			if m.checkTaskDone(MapTask, m.MapperNum) {
				m.toNextPhase()
			}
		}
	case ReducePhase:
		if len(m.ReduceTasks) > 0 {
			task := <-m.ReduceTasks
			reply.RequestTaskFlag = TaskAssigned
			reply.Task = *task
			task.TaskState = Working
			task.StartTime = time.Now()
			m.TaskMap[task.TaskId] = task
			fmt.Printf("reduce task %d %v assigned\n", task.TaskId, task.InputFiles)
		} else {
			reply.RequestTaskFlag = WaitAndTryAgain
			if m.checkTaskDone(ReduceTask, m.ReducerNum) {
				m.toNextPhase()
			}
		}
	case AllDone:
		reply.RequestTaskFlag = FinishedAndExit
		fmt.Println("all tasks done")
	default:
		panic("undefined phase")
	}

	return nil
}

func (m *Master) UpdateTaskFinished(args *ReportTaskArgs, reply *ReportTaskReply) error {
	mu.Lock()
	defer mu.Unlock()
	m.TaskMap[args.TaskId].TaskState = Finished
	fmt.Printf("task %d %v finished\n", args.TaskId, m.TaskMap[args.TaskId].InputFiles)
	return nil
}

func (m *Master) toNextPhase() {
	switch m.CurrentPhase {
	case MapPhase:
		m.CurrentPhase = ReducePhase
		m.makeReduceTasks()
	case ReducePhase:
		m.CurrentPhase = AllDone
	}
}

func (m *Master) makeReduceTasks() {
	dir, _ := os.Getwd()
	files, err := os.ReadDir(dir)
	if err != nil {
		fmt.Println(err)
	}

	for i := 0; i < m.ReducerNum; i++ {
		input := []string{}
		for _, file := range files {
			if strings.HasPrefix(file.Name(), "mr-") && strings.HasSuffix(file.Name(), strconv.Itoa(i)) {
				input = append(input, file.Name())
			}
		}

		task := Task{
			TaskId:     m.generateTaskId(),
			TaskType:   ReduceTask,
			TaskState:  Waiting,
			InputFiles: input,
			ReduceNum:  m.ReducerNum,
			ReduceKth:  i,
		}
		fmt.Printf("reduce task %d %v generated\n", task.TaskId, task.InputFiles)
		m.ReduceTasks <- &task
	}
}

func (m *Master) checkTaskDone(taskType TaskType, doneNum int) bool {
	done := 0
	undone := 0
	for _, task := range m.TaskMap {
		if task.TaskType == taskType {
			if task.TaskState == Finished {
				done++
			} else {
				undone++
			}
		}
	}

	if done == doneNum && undone == 0 {
		return true
	}

	return false
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
//	reply.Y = args.X + 1
//	return nil
//}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (m *Master) crashHandler() {
	for {
		time.Sleep(time.Second)
		mu.Lock()
		defer mu.Unlock()
		if m.CurrentPhase == AllDone {
			break
		}

		for _, task := range m.TaskMap {
			if task.TaskState == Working && time.Since(task.StartTime) > 10*time.Second {
				task.TaskState = Waiting
				var taskType string
				switch task.TaskType {
				case MapTask:
					m.MapTasks <- task
					taskType = "map"
				case ReduceTask:
					m.ReduceTasks <- task
					taskType = "reduce"
				}
				fmt.Printf("%s task %d %v crashed and reput into waiting queue", taskType, task.TaskId, task.InputFiles)
				delete(m.TaskMap, task.TaskId)
			}
		}
	}
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	ret := false
	mu.Lock()
	defer mu.Unlock()
	if m.CurrentPhase == AllDone {
		ret = true
	}
	return ret
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		TaskIdForGen: 0,
		CurrentPhase: MapPhase,
		MapTasks:     make(chan *Task, len(files)),
		ReduceTasks:  make(chan *Task, nReduce),
		TaskMap:      make(map[int]*Task, len(files)+nReduce),
		MapperNum:    len(files),
		ReducerNum:   nReduce,
	}

	m.makeMapTasks(files)

	m.server()

	go m.crashHandler()
	return &m
}
