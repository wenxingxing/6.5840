package mr

import (
	"io/ioutil"
	"log"
	"log/slog"
	"os"
	"sort"
	"time"
)

type KeyValue struct {
	Key   string
	Value string
}

type MapFunc func(string, string) []KeyValue

type ReduceFunc func(string, []string) string

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

type MapTask struct {
	filename string
	nReduce  int
}

func (t MapTask) process(w *MRWorker) (bool, error) {
	filename := t.filename
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := w.mapF(filename, string(content))

	sort.Sort(ByKey(kva))

	// output into nReduce buckets
	/*
		oname := "mr-out-0"
		ofile, _ := os.Create(oname)

		//
		// call Reduce on each distinct key in intermediate[],
		// and print the result to mr-out-0.
		//
		i := 0
		for i < len(intermediate) {
			j := i + 1
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, intermediate[k].Value)
			}
			output := reducef(intermediate[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

			i = j
		}

		ofile.Close()
	*/

	return false, nil
}

type ReduceTask struct{}

func (t ReduceTask) process(w *MRWorker) (bool, error) {
	return false, nil
}
