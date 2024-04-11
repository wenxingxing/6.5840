
https://pdos.csail.mit.edu/6.824/labs/lab-mr.html

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

The `pg-*.txt` arguments to mrcoordinator.go are the input files; each file corresponds to one "split", and is the input to one Map task.

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