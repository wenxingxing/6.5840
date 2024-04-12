package mr

import (
    "errors"
    "fmt"
    "log"
    "sync"
    "time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

//type TaskManager interface {
//	Next() Task
//	Done() bool
//	ReportDone(Task) error
//}

type Coordinator struct {
    pendingMapTasks     chan MapTask
    finishedMapTasks    map[int]bool
    mtxFinishedMapTasks sync.Mutex

    pendingReduceTasks     chan ReduceTask
    finishedReduceTasks    map[int]bool
    mtxFinishedReduceTasks sync.Mutex

    files   []string
    mapId   map[string]int
    nMap    int
    nReduce int

    timeout time.Duration
}

func (c *Coordinator) init() {
    for i, file := range c.files {
        c.mapId[file] = i
    }
    for i := 0; i < c.nMap; i++ {
        c.pendingMapTasks <- MapTask{
            Id:       i,
            Filename: c.files[i],
            NReduce:  c.nReduce,
        }
    }
    for i := 0; i < c.nReduce; i++ {
        c.pendingReduceTasks <- ReduceTask{
            Id:   i,
            NMap: c.nMap,
        }
    }
}

func (c *Coordinator) allMapTasksDone() bool {
    c.mtxFinishedMapTasks.Lock()
    defer c.mtxFinishedMapTasks.Unlock()
    return len(c.finishedMapTasks) == c.nMap
}

func (c *Coordinator) nextMapTask() Task {
    // if pendingMapTasks is empty, return NoopTask
    // else return the next map task
    select {
    case task := <-c.pendingMapTasks:
        go func() {
            // check if the task is done after timeout
            time.Sleep(c.timeout)
            c.mtxFinishedMapTasks.Lock()
            defer c.mtxFinishedMapTasks.Unlock()
            if _, ok := c.finishedMapTasks[task.Id]; !ok {
                log.Printf("task %v is not finished after timeout, put it back to pendingMapTasks", task.Id)
                c.pendingMapTasks <- task
            }
        }()
        return task
    default:
        return NoopTask{}
    }
}

func (c *Coordinator) allReduceTasksDone() bool {
    c.mtxFinishedReduceTasks.Lock()
    defer c.mtxFinishedReduceTasks.Unlock()
    return len(c.finishedReduceTasks) == c.nReduce
}

func (c *Coordinator) nextReduceTask() Task {
    // if pendingReduceTasks is empty, return NoopTask
    // else return the next reduce task
    select {
    case task := <-c.pendingReduceTasks:
        go func() {
            // check if the task is done after timeout
            time.Sleep(c.timeout)
            c.mtxFinishedReduceTasks.Lock()
            defer c.mtxFinishedReduceTasks.Unlock()
            if _, ok := c.finishedReduceTasks[task.Id]; !ok {
                log.Printf("task %v is not finished after timeout, put it back to pendingReduceTasks", task.Id)
                c.pendingReduceTasks <- task
            }
        }()
        return task
    default:
        return NoopTask{}
    }
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
    if !c.allMapTasksDone() {
        reply.Task = c.nextMapTask()
        return nil
    } else if !c.allReduceTasksDone() {
        reply.Task = c.nextReduceTask()
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
        c.mtxFinishedMapTasks.Lock()
        defer c.mtxFinishedMapTasks.Unlock()
        task := args.Task.(MapTask)
        if _, ok := c.finishedMapTasks[task.Id]; ok {
            errMsg := fmt.Sprintf("map task %v has been finished before", task.Id)
            log.Printf(errMsg)
            return errors.New(errMsg)
        }
        c.finishedMapTasks[task.Id] = true
    case ReduceTask:
        c.mtxFinishedReduceTasks.Lock()
        defer c.mtxFinishedReduceTasks.Unlock()
        task := args.Task.(ReduceTask)
        if _, ok := c.finishedReduceTasks[task.Id]; ok {
            errMsg := fmt.Sprintf("reduce task %v has been finished before", task.Id)
            log.Printf(errMsg)
            return errors.New(errMsg)
        }
        c.finishedReduceTasks[task.Id] = true
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
    return c.allReduceTasksDone()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
    c := Coordinator{
        pendingMapTasks:     make(chan MapTask, len(files)),
        finishedMapTasks:    make(map[int]bool),
        pendingReduceTasks:  make(chan ReduceTask, nReduce),
        finishedReduceTasks: make(map[int]bool),

        files:   files,
        mapId:   make(map[string]int),
        nMap:    len(files),
        nReduce: nReduce,

        timeout: 10 * time.Second,
    }

    c.init()
    c.server()
    return &c
}
