package mr

type TaskQueue struct {
	Queue []Task
}

func (q *TaskQueue) Enqueue(t Task) {
	q.Queue = append(q.Queue, t)
}

func (q *TaskQueue) Dequeue() Task {
	if len(q.Queue) == 0 {
		panic("Dequeue called on empty queue")
	}
	task := q.Queue[0]
	q.Queue = q.Queue[1:]
	return task
}

func (q *TaskQueue) Size() int {
	return len(q.Queue)
}
