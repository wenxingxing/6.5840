package mr

type MRWorker struct {
	// todo
}

func New(mapF MapFunc, reduceF ReduceFunc) MRWorker {
	// todo
	return MRWorker{}
}

func (w *MRWorker) getTask() (Task, error) {
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
