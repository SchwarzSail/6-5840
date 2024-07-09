package mr

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"time"
)

type Status int

const (
	Map Status = iota + 1
	Reduce
	Ready
	Running
	Completed
	Exit
	Waiting
)

type Task interface {
	GetNumber() int
	GetStatus() Status
}

type TaskWrapper struct {
	Task     Task
	Category Status //用于区别是Map还是Reduce
}

func (tw *TaskWrapper) GobEncode() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)

	// Encode the Category first
	err := enc.Encode(tw.Category)
	if err != nil {
		return nil, err
	}

	// Encode the Task based on Category
	switch tw.Category {
	case Map:
		err = enc.Encode(tw.Task.(*MapTask))
	case Reduce:
		err = enc.Encode(tw.Task.(*ReduceTask))
	default:
		return nil, fmt.Errorf("unknown task category")
	}
	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	return buf.Bytes(), nil
}

func (tw *TaskWrapper) GobDecode(data []byte) error {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)

	// Decode the Category first
	err := dec.Decode(&tw.Category)
	if err != nil {
		return err
	}

	// Based on the Category, create the appropriate task type
	switch tw.Category {
	case Map:
		var task MapTask
		err = dec.Decode(&task)
		if err != nil {
			log.Fatal(err)
			return err
		}
		tw.Task = &task
	case Reduce:
		var task ReduceTask
		err = dec.Decode(&task)
		if err != nil {
			log.Fatal(err)
			return err
		}
		tw.Task = &task
	default:
		return fmt.Errorf("unknown task category")
	}

	return nil
}

type MapTask struct {
	Number int
	Status Status
	Input  string //文件名
}

func (m *MapTask) GetNumber() int {
	return m.Number
}

func (m *MapTask) GetStatus() Status {
	return m.Status
}

type ReduceTask struct {
	Number       int
	Status       Status
	Intermediate []string
}

func (r *ReduceTask) GetNumber() int {
	return r.Number
}

func (r *ReduceTask) GetStatus() Status {
	return r.Status
}

type CoordinatorTask struct {
	TaskReference Task
	StartTime     time.Time
	TaskStatus    Status
}
