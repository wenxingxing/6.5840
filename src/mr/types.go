package mr

type KeyValue struct {
	Key   string
	Value string
}

type MapFunc func(string, string) []KeyValue

type ReduceFunc func(string, []string) string
