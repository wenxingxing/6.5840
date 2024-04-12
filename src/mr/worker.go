package mr

import (
	"errors"
	"log"
	"time"
)

type MRWorker struct {
	mapF    MapFunc
	reduceF ReduceFunc
}

func New(mapF MapFunc, reduceF ReduceFunc) MRWorker {
	return MRWorker{
		mapF:    mapF,
		reduceF: reduceF,
	}
}

func (w *MRWorker) getTask() (Task, error) {
	args := GetTaskArgs{}
	reply := GetTaskReply{}

	if !rpcCall("Coordinator.GetTask", &args, &reply) {
		return nil, errors.New("rpc call getTask failed")
	}

	return reply.Task, nil
}

func (w *MRWorker) reportTaskDone(task Task) error {
	args := ReportTaskDoneArgs{
		Task: task,
	}
	reply := ReportTaskDoneReply{}

	if !rpcCall("Coordinator.ReportTaskDone", &args, &reply) {
		return errors.New("rpc call reportMapTaskDone failed")
	}
	return nil
}

// run will require tasks from coordinator until it ends
func (w *MRWorker) run() {
	for {
		t, err := w.getTask()
		if err != nil {
			log.Printf("get task error: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}
		if t == nil {
			log.Fatalf("get nil task")
		}

		stop, err := t.Process(w)
		if err != nil {
			log.Printf("Process task error: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}

		if stop {
			break
		}
	}
}

func Worker(mapF MapFunc, reduceF ReduceFunc) {
	worker := New(mapF, reduceF)
	worker.run()
}
