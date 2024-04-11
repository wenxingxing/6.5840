package mr

type MRWorker struct {
	// todo
}

func New(mapF MapFunc, reduceF ReduceFunc) MRWorker {
	// todo
	return MRWorker{}
}

// run will require tasks from coordinator until it ends
func (w *MRWorker) run() {
	// todo
}

func Worker(mapF MapFunc, reduceF ReduceFunc) {
	worker := New(mapF, reduceF)
	worker.run()
}
