package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Task struct {
	Type      TaskType
	ID        int
	FileName  string
	Status    TaskStatus
	StartTime time.Time
}

type Coordinator struct {
	mu          sync.Mutex
	mapTasks    []Task
	reduceTasks []Task
	nReduce     int
	nMap        int
	mapsDone    bool
	reducesDone bool
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.mapsDone {
		for i, task := range c.mapTasks {
			if task.Status == Idle || (task.Status == InProgress && time.Since(task.StartTime) > 10*time.Second) {
				c.mapTasks[i].Status = InProgress
				c.mapTasks[i].StartTime = time.Now()
				*reply = GetTaskReply{TaskType: MapTask, TaskID: task.ID, FileName: task.FileName, NReduce: c.nReduce, NMap: c.nMap}
				return nil
			}
		}
		if c.allMapTasksCompleted() {
			c.mapsDone = true
		} else {
			*reply = GetTaskReply{TaskType: WaitTask}
			return nil
		}
	}

	if !c.reducesDone {
		for i, task := range c.reduceTasks {
			if task.Status == Idle || (task.Status == InProgress && time.Since(task.StartTime) > 10*time.Second) {
				c.reduceTasks[i].Status = InProgress
				c.reduceTasks[i].StartTime = time.Now()
				*reply = GetTaskReply{TaskType: ReduceTask, TaskID: task.ID, NReduce: c.nReduce, NMap: c.nMap}
				return nil
			}
		}
		if c.allReduceTasksCompleted() {
			c.reducesDone = true
		} else {
			*reply = GetTaskReply{TaskType: WaitTask}
			return nil
		}
	}

	*reply = GetTaskReply{TaskType: ExitTask}
	return nil
}

func (c *Coordinator) TaskCompleted(args *TaskCompletionArgs, reply *TaskCompletionReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.TaskType == MapTask {
		c.mapTasks[args.TaskID].Status = Completed
	} else if args.TaskType == ReduceTask {
		c.reduceTasks[args.TaskID].Status = Completed
	}

	return nil
}

func (c *Coordinator) allMapTasksCompleted() bool {
	for _, task := range c.mapTasks {
		if task.Status != Completed {
			return false
		}
	}
	return true
}

func (c *Coordinator) allReduceTasksCompleted() bool {
	for _, task := range c.reduceTasks {
		if task.Status != Completed {
			return false
		}
	}
	return true
}

func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reducesDone
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce:     nReduce,
		nMap:        len(files),
		mapTasks:    make([]Task, len(files)),
		reduceTasks: make([]Task, nReduce),
	}

	for i, file := range files {
		c.mapTasks[i] = Task{Type: MapTask, ID: i, FileName: file, Status: Idle}
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = Task{Type: ReduceTask, ID: i, Status: Idle}
	}

	c.server()
	return &c
}
