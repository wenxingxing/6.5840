package mr

import (
	"hash/fnv"
	"strconv"
)

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func intermediateFilename(mapId int, reduceId int) string {
	// mr-mapId-reduceId
	return "intermediate-" + strconv.Itoa(mapId) + "-" + strconv.Itoa(reduceId)
}
