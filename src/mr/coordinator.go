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

type TaskManager struct {
	pendingTasks  chan Task
	finishedTasks map[int]bool
	mtx           sync.Mutex

	timeout time.Duration
	nTasks  int
}

func NewTaskManager(n int, timeout time.Duration) TaskManager {
	return TaskManager{
		pendingTasks:  make(chan Task, n),
		finishedTasks: make(map[int]bool),
		timeout:       timeout,
		nTasks:        n,
	}
}

func (t *TaskManager) done() bool {
	t.mtx.Lock()
	defer t.mtx.Unlock()
	return len(t.finishedTasks) == t.nTasks
}

func (t *TaskManager) next() Task {
	select {
	case task := <-t.pendingTasks:
		go func() {
			// check if the task is done after timeout
			time.Sleep(t.timeout)
			t.mtx.Lock()
			defer t.mtx.Unlock()
			if _, ok := t.finishedTasks[task.GetId()]; !ok {
				log.Printf("task %v is not finished after timeout, put it back to pendingTasks", task.GetId())
				t.pendingTasks <- task
			}
		}()
		return task
	default:
		return NoopTask{}
	}
}

func (t *TaskManager) addTask(task Task) {
	t.pendingTasks <- task
}

func (t *TaskManager) markTaskDone(task Task) {
	t.mtx.Lock()
	defer t.mtx.Unlock()
	t.finishedTasks[task.GetId()] = true
}

type Coordinator struct {
	mapTaskManager    TaskManager
	reduceTaskManager TaskManager

	timeout time.Duration
}

func (c *Coordinator) init(files []string, nReduce int) {
	for i := 0; i < len(files); i++ {
		c.mapTaskManager.addTask(
			MapTask{
				Id:       i,
				Filename: files[i],
				NReduce:  nReduce,
			},
		)
	}

	nMap := len(files)
	for i := 0; i < nReduce; i++ {
		c.reduceTaskManager.addTask(
			ReduceTask{
				Id:   i,
				NMap: nMap,
			},
		)
	}
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	if !c.mapTaskManager.done() {
		reply.Task = c.mapTaskManager.next()
		return nil
	} else if !c.reduceTaskManager.done() {
		reply.Task = c.reduceTaskManager.next()
		return nil
	} else {
		reply.Task = StopTask{}
		return nil
	}
}

func (c *Coordinator) ReportTaskDone(args *ReportTaskDoneArgs, reply *ReportTaskDoneReply) error {
	// TODO:
	// corner case: task is assigned to worker A but it finished after coordinator think A has crashed and assigned it to worker B
	switch args.Task.(type) {
	case MapTask:
		c.mapTaskManager.markTaskDone(args.Task)
	case ReduceTask:
		c.reduceTaskManager.markTaskDone(args.Task)
	default:
		log.Fatalf("invalid task type: %v for ReportTaskDone", args.Task)
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	err := rpc.Register(c)
	if err != nil {
		log.Fatal("Failed to register RPC", err)
		return
	}
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
	return c.reduceTaskManager.done()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	timeout := 10 * time.Second
	c := Coordinator{
		mapTaskManager:    NewTaskManager(len(files), timeout),
		reduceTaskManager: NewTaskManager(nReduce, timeout),
	}

	c.init(files, nReduce)
	c.server()
	return &c
}
