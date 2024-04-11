package mr

import (
	"math/rand"
	"strconv"
	"time"
)

type MRWorker struct {
	mapF    MapFunc
	reduceF ReduceFunc
	uuid    string
}

func simpleUUID() string {
	// generate a unique from random number and timestamp
	randInt := rand.Int()
	t := time.Now().UnixNano()
	return strconv.Itoa(randInt) + strconv.FormatInt(t, 10)
}

func New(mapF MapFunc, reduceF ReduceFunc) MRWorker {
	return MRWorker{
		mapF:    mapF,
		reduceF: reduceF,
		uuid:    simpleUUID(),
	}
}

func (w *MRWorker) getTask() (Task, error) {
	// todo
}

func (w *MRWorker) reportMapTaskDone(task MapTask) error {
	// todo
}

func (w *MRWorker) reportReduceTaskDone(task ReduceTask) error {
	// todo
}

// run will require tasks from coordinator until it ends
func (w *MRWorker) run() {
	for {
		t, err := w.getTask()
		if err != nil {
			// todo
		}

		stop, err := t.process(w)
		if err != nil {
			// todo
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
