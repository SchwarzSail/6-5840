package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"encoding/gob"
	"os"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type TaskReply struct {
	TaskWrapper TaskWrapper
	NReduce     int
}

type CompleteTaskArgs struct {
	TaskCategory Status
	TaskID       int
	Intermediate []string
}

func init() {
	gob.Register(&TaskWrapper{})
	gob.Register(&MapTask{})
	gob.Register(&ReduceTask{})
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
