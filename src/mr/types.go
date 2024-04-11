package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"time"
)

type KeyValue struct {
	Key   string
	Value string
}

type ShouldStop bool

const (
	Stop     ShouldStop = true
	Continue ShouldStop = false
)

type MapFunc func(string, string) []KeyValue

type ReduceFunc func(string, []string) string

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type Task interface {
	// process do the task and return True if need to stop
	process(w *MRWorker) (ShouldStop, error)
}

type NoopTask struct{}

func (t NoopTask) process(_ *MRWorker) (ShouldStop, error) {
	slog.Info("NoopTask, sleep 1s")
	time.Sleep(1 * time.Second)
	return Continue, nil
}

type StopTask struct{}

func (t StopTask) process(_ *MRWorker) (ShouldStop, error) {
	return true, nil
}

type MapTask struct {
	filename string
	id       int
	nReduce  int
}

func (t MapTask) process(w *MRWorker) (ShouldStop, error) {
	slog.Info("MapTask", t.id, t.filename)
	// read task file
	filename := t.filename
	file, err := os.Open(filename)
	defer file.Close()
	if err != nil {
		slog.Error("cannot open", filename, err)
		return Continue, err
	}
	content, err := io.ReadAll(file)
	if err != nil {
		slog.Error("cannot read", filename, err)
		return Continue, err
	}
	kva := w.mapF(filename, string(content))

	// open all nReduce files
	ofiles := make([]*os.File, t.nReduce)
	encs := make([]*json.Encoder, t.nReduce)
	for i := 0; i < t.nReduce; i++ {
		oname := intermediateFilename(t.id, i)
		// append to file, create if not exist
		ofile, err := os.OpenFile(oname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			slog.Error("cannot open", oname, err)
			return Continue, err
		}
		ofiles[i] = ofile
		encs[i] = json.NewEncoder(ofile)
	}
	defer func() {
		for _, ofile := range ofiles {
			ofile.Close()
		}
	}()

	// output into nReduce buckets
	for _, kv := range kva {
		bucket := ihash(kv.Key) % t.nReduce
		err := encs[bucket].Encode(kv)
		if err != nil {
			slog.Error("Failed to encode kv", kv, "into bucket", bucket)
			return Continue, err
		}
	}

	err = w.reportMapTaskDone(t)
	if err != nil {
		slog.Error("Failed to report map task done", t)
		return Continue, err
	}

	return Continue, nil
}

type ReduceTask struct {
	id   int
	nMap int
}

func (t ReduceTask) process(w *MRWorker) (ShouldStop, error) {
	slog.Info("ReduceTask", t.id)
	intermediate := []KeyValue{}

	// read mr-0-id -> mr-(nMap-1)-id
	for i := 0; i < t.nMap; i++ {
		oname := intermediateFilename(i, t.id)
		file, err := os.Open(oname)
		if err != nil {
			slog.Error("cannot open", oname, err)
			return Continue, err
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err == io.EOF {
				break
			} else if err != nil {
				slog.Error("Failed to decode kv from", oname)
				return Continue, err
			}
			intermediate = append(intermediate, kv)
		}
	}

	// sort by key
	sort.Sort(ByKey(intermediate))

	// open reduce output file
	oname := "mr-out-" + strconv.Itoa(t.id)
	ofile, err := os.Create(oname)
	if err != nil {
		slog.Error("cannot create", oname, err)
		return Continue, err
	}
	defer ofile.Close()

	// call reduce
	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := w.reduceF(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	err = w.reportReduceTaskDone(t)
	if err != nil {
		slog.Error("Failed to report reduce task done", t)
		return Continue, err
	}

	return Continue, nil
}
