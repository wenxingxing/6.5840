package mr

import (
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"time"
)

func init() {
	gob.Register(MapTask{})
	gob.Register(ReduceTask{})
	gob.Register(NoopTask{})
	gob.Register(StopTask{})
}

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
	// Process do the task and return True if need to stop
	Process(w *MRWorker) (ShouldStop, error)
	GetId() int
}

type NoopTask struct{}

func (t NoopTask) GetId() int {
	return -1
}

func (t NoopTask) Process(_ *MRWorker) (ShouldStop, error) {
	slog.Debug("NoopTask, sleep 1s")
	time.Sleep(1 * time.Second)
	return Continue, nil
}

type StopTask struct{}

func (t StopTask) GetId() int {
	return -1
}

func (t StopTask) Process(_ *MRWorker) (ShouldStop, error) {
	return true, nil
}

type MapTask struct {
	Filename string
	Id       int
	NReduce  int
}

func (t MapTask) GetId() int {
	return t.Id
}

func (t MapTask) Process(w *MRWorker) (ShouldStop, error) {
	// read task file
	filename := t.Filename
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

	// open all NReduce files
	ofiles := make([]*os.File, t.NReduce)
	encs := make([]*json.Encoder, t.NReduce)
	for i := 0; i < t.NReduce; i++ {
		oname := intermediateFilename(t.Id, i)
		// append to file, create if not exist
		ofile, err := os.Create(oname)
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

	// output into NReduce buckets
	for _, kv := range kva {
		bucket := ihash(kv.Key) % t.NReduce
		err := encs[bucket].Encode(kv)
		if err != nil {
			slog.Error("Failed to encode kv", kv, "into bucket", bucket)
			return Continue, err
		}
	}

	err = w.reportTaskDone(t)
	if err != nil {
		slog.Error("Failed to report map task done", t)
		return Continue, err
	}

	return Continue, nil
}

type ReduceTask struct {
	Id   int
	NMap int
}

func (t ReduceTask) GetId() int {
	return t.Id
}

func (t ReduceTask) Process(w *MRWorker) (ShouldStop, error) {
	intermediate := []KeyValue{}

	// read all intermediate files
	for i := 0; i < t.NMap; i++ {
		oname := intermediateFilename(i, t.Id)
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
	oname := "mr-out-" + strconv.Itoa(t.Id)
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

	err = w.reportTaskDone(t)
	if err != nil {
		slog.Error("Failed to report reduce task done", t)
		return Continue, err
	}

	return Continue, nil
}
