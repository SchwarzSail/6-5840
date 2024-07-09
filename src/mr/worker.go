package mr

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
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

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		args := ExampleArgs{}
		reply := TaskReply{}
		call("Coordinator.AssignTask", &args, &reply)
		if reply.TaskWrapper.Category == Map {
			res := reply.TaskWrapper.Task.(*MapTask)
			if res.Status == Waiting {
				time.Sleep(3 * time.Second)
			} else {
				mapper(res, reply.NReduce, mapf)
			}
		} else if reply.TaskWrapper.Category == Reduce {
			res := reply.TaskWrapper.Task.(*ReduceTask)
			if res.Status == Waiting {
				time.Sleep(3 * time.Second)
			} else if res.Status == Exit {
				return
			} else {
				reducer(res, reducef)
			}
		}
	}

	// Your worker implementation here.
	//for {
	//	args := ExampleArgs{}
	//	reply := TaskReply{}
	//	call("Coordinator.AssignTask", &args, &reply)
	//	switch reply.TaskWrapper.Category {
	//	case Map:
	//		res := reply.TaskWrapper.Task.(*MapTask)
	//		if res.Status == Waiting {
	//			time.Sleep(3 * time.Second)
	//		} else {
	//			mapper(res, reply.NReduce, mapf)
	//		}
	//	case Reduce:
	//		res := reply.TaskWrapper.Task.(*ReduceTask)
	//		if res.Status == Waiting {
	//			time.Sleep(3 * time.Second)
	//		} else if res.Status == Exit {
	//			return
	//		} else {
	//			reducer(res, reducef)
	//		}
	//	}
	//}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func mapper(task *MapTask, nReduce int, mapf func(string, string) []KeyValue) {
	//从文件名中读取文件
	content, err := os.ReadFile(task.Input)
	if err != nil {
		log.Fatal("mapper 阶段打开文件失败", err)
	}
	//将文件内容交给mapf函数，获取结果
	kvs := mapf(task.Input, string(content))

	//将结果分成R份，保存到本地磁盘
	buffer := make([][]KeyValue, nReduce)
	for _, kv := range kvs {
		slot := ihash(kv.Key) % nReduce
		buffer[slot] = append(buffer[slot], kv)
	}
	mapOutput := make([]string, 0)
	for i := 0; i < nReduce; i++ {
		mapOutput = append(mapOutput, saveToLocalFile(task.GetNumber(), i, &buffer[i]))
	}
	//通知coordinator
	args := CompleteTaskArgs{
		TaskCategory: Map,
		TaskID:       task.GetNumber(),
		Intermediate: mapOutput,
	}
	reply := ExampleReply{}
	call("Coordinator.CompleteTask", &args, &reply)
}

func reducer(task *ReduceTask, reducef func(string, []string) string) {
	//先从本地磁盘获取map阶段产生的文件
	intermediate := *readFromLocalFile(task.Intermediate)
	//根据kv排序
	sort.Sort(ByKey(intermediate))
	dir, _ := os.Getwd()
	tempFile, err := os.CreateTemp(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal(err)
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
		reduceOutput := reducef(intermediate[i].Key, values)
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, reduceOutput)
		i = j
	}
	tempFile.Close()
	outputName := fmt.Sprintf("mr-out-%d", task.GetNumber())
	os.Rename(tempFile.Name(), outputName)
	//通知coordinator
	args := CompleteTaskArgs{
		TaskCategory: Reduce,
		TaskID:       task.GetNumber(),
	}
	reply := ExampleReply{}
	call("Coordinator.CompleteTask", &args, &reply)
}

// 将结果保存到本地磁盘，返回文件路径
func saveToLocalFile(x, y int, kvs *[]KeyValue) string {
	dir, _ := os.Getwd()
	tempFile, err := os.CreateTemp(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal(err)
	}
	encoder := json.NewEncoder(tempFile)
	for _, kv := range *kvs {
		if err := encoder.Encode(&kv); err != nil {
			log.Fatal(err)
		}
	}
	tempFile.Close()
	outputFileName := fmt.Sprintf("mr-%d-%d", x, y)
	os.Rename(tempFile.Name(), outputFileName)
	return filepath.Join(dir, outputFileName)
}

// 将map阶段产生的结果从磁盘读取出来
func readFromLocalFile(files []string) *[]KeyValue {
	kvs := []KeyValue{}
	for _, filePath := range files {
		file, err := os.Open(filePath)
		if err != nil {
			log.Fatal("reduce阶段打开文件失败", err)
		}
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
		file.Close()
	}
	return &kvs
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
