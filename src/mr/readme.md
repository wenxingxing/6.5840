
https://pdos.csail.mit.edu/6.824/labs/lab-mr.html

# overview

mrSequential
```go
// 1. map
kva := mapf(filename, string(content))
intermediate = append(intermediate, kva...)

// 2. sort
sort.Sort(ByKey(intermediate))

// 3. reduce
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
```

mrParallel
```md

coordinator is responsible for assigning MapTask and RecudeTask to workers.

for MapTask, coordinator should keep track of the worker's progress, re-assign task if failed.

for ReduceTask_i, it should read all intermediate files: mr-x-i
    
1. MapTask:
    input: filename, task_id
    output: mr-task_id-0, mr-task_id-1, ..., mr-task_id-(NReduce-1)

2. ReduceTask
    input: reduce_id
    output: mr-out-reduce_id
```

# worker
Each **worker** process will, in a loop, ask the coordinator for a task, read the task's input from one or more files, execute the task, write the task's output to one or more files, and again ask the coordinator for a new task. 

In one or more other windows, run some workers:

```shell
$ go run mrworker.go wc.so
```

# coordinator
The coordinator should notice if a worker hasn't completed its task in a reasonable amount of time (for this lab, use ten seconds), and give the same task to a different worker.

in main directory
```shell
$ rm mr-out*
$ go run mrcoordinator.go pg-*.txt
```

The `pg-*.txt` arguments to mrcoordinator.go are the input files; each file corresponds to one "split", and is the input to **one** Map task.

# output

```shell
$ cat mr-out-* | sort | more
A 509
ABOUT 2
ACT 8
...
```

# check

```shell
$ cd ~/6.5840/src/main
$ bash test-mr.sh
*** Starting wc test.
```