package mr

import (
	"log/slog"
	"time"
)

type KeyValue struct {
	Key   string
	Value string
}

type MapFunc func(string, string) []KeyValue

type ReduceFunc func(string, []string) string

type Task interface {
	// process do the task and return True if need to stop
	process(w *MRWorker) (bool, error)
}

type NoopTask struct{}

func (t NoopTask) process(_ *MRWorker) (bool, error) {
	slog.Info("Noop task, sleep 1 sec")
	time.Sleep(1 * time.Second)
	return false, nil
}

type StopTask struct{}

func (t StopTask) process(_ *MRWorker) (bool, error) {
	return true, nil
}

type MapTask struct{}

func (t MapTask) process(_ *MRWorker) (bool, error) {
	return false, nil
}

type ReduceTask struct{}

func (t ReduceTask) process(w *MRWorker) (bool, error) {
	return false, nil
}
