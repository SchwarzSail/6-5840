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
	// Your definitions here.
	TaskQueue    TaskQueue
	TaskMeta     map[int]*CoordinatorTask
	Files        []string
	Phase        Status
	Intermediate [][]string
	NReduced     int
	Mu           *sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) AssignTask(args *ExampleArgs, reply *TaskReply) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	switch c.Phase {
	case Map:
		if c.TaskQueue.Size() > 0 {
			reply.TaskWrapper.Task = c.TaskQueue.Dequeue().(*MapTask)
			reply.NReduce = c.NReduced
			reply.TaskWrapper.Category = Map
			c.TaskMeta[reply.TaskWrapper.Task.GetNumber()].TaskReference.SetStatus(Running)
			c.TaskMeta[reply.TaskWrapper.Task.GetNumber()].StartTime = time.Now()
		} else if c.TaskQueue.Size() == 0 {
			reply.TaskWrapper.Task = &MapTask{Status: Waiting}
			reply.TaskWrapper.Category = Map
		}
	case Reduce:
		if c.TaskQueue.Size() > 0 {
			reply.TaskWrapper.Task = c.TaskQueue.Dequeue().(*ReduceTask)
			reply.TaskWrapper.Category = Reduce
			c.TaskMeta[reply.TaskWrapper.Task.GetNumber()].TaskReference.SetStatus(Running)
			c.TaskMeta[reply.TaskWrapper.Task.GetNumber()].StartTime = time.Now()
		} else if c.TaskQueue.Size() == 0 {
			reply.TaskWrapper.Task = &ReduceTask{Status: Waiting}
			reply.TaskWrapper.Category = Reduce
		}
	default:
		reply.TaskWrapper.Task = &ReduceTask{Status: Exit}
		reply.TaskWrapper.Category = Reduce
	}
	return nil
}

func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *ExampleReply) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	if args.TaskCategory != c.Phase {
		return nil
	}
	c.TaskMeta[args.TaskID].TaskReference.SetStatus(Completed)
	go c.processTaskResult(args)
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
	ret := c.Phase == Exit

	// Your code here.

	return ret
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		TaskQueue:    TaskQueue{Queue: make([]Task, 0, len(files))},
		TaskMeta:     make(map[int]*CoordinatorTask),
		Phase:        Map,
		Intermediate: make([][]string, nReduce),
		NReduced:     nReduce,
		Mu:           new(sync.Mutex),
		Files:        files,
	}

	// Your code here.
	c.createMapTask()
	c.server()
	go c.checkCrushing()
	return &c
}

func (c *Coordinator) createMapTask() {
	for idx, file := range c.Files {
		task := &MapTask{
			Number: idx,
			Status: Ready,
			Input:  file,
		}
		c.TaskQueue.Enqueue(task)
		c.TaskMeta[idx] = &CoordinatorTask{
			TaskReference: task,
		}
	}
}

func (c *Coordinator) createReduceTask() {
	//清空一下TaskMeta
	c.TaskQueue = TaskQueue{Queue: make([]Task, 0, c.NReduced)}
	c.TaskMeta = make(map[int]*CoordinatorTask)
	for idx, file := range c.Intermediate {
		task := &ReduceTask{
			Number:       idx,
			Status:       Ready,
			Intermediate: file,
		}
		c.TaskQueue.Enqueue(task)
		c.TaskMeta[idx] = &CoordinatorTask{
			TaskReference: task,
		}
	}
}

func (c *Coordinator) processTaskResult(args *CompleteTaskArgs) {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	switch args.TaskCategory {
	case Map:
		//收集从Map阶段产生的结果，结果保存在本地的文件中
		for idx, filePath := range args.Intermediate {
			c.Intermediate[idx] = append(c.Intermediate[idx], filePath)
		}
		log.Printf("map task done: %d", args.TaskID)
		if c.allTaskDone() {
			//进入reduce阶段
			c.createReduceTask()
			c.Phase = Reduce
		}
	case Reduce:
		log.Printf("reduce task done: %d", args.TaskID)
		if c.allTaskDone() {
			c.Phase = Exit
		}
	default:
		panic("invalid task category")
	}
}

func (c *Coordinator) allTaskDone() bool {
	for _, task := range c.TaskMeta {
		if task.TaskReference.GetStatus() != Completed {
			return false
		}
	}
	return true
}

func (c *Coordinator) checkCrushing() {
	for {
		time.Sleep(3 * time.Second)
		if c.Phase == Exit {
			return
		}
		c.Mu.Lock()
		for _, task := range c.TaskMeta {
			if task.TaskReference.GetStatus() == Running && time.Since(task.StartTime) > 10*time.Second {
				temp := task.TaskReference
				log.Printf("catch the timeout Task: %d", temp.GetNumber())
				temp.SetStatus(Ready)
				c.TaskQueue.Enqueue(temp)
			}
		}
		c.Mu.Unlock()
	}
}
