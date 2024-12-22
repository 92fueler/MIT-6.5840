package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		task := getTask()

		switch task.TaskType {
		case MapTask:
			performMapTask(mapf, task)
		case ReduceTask:
			performReduceTask(reducef, task)
		case WaitTask:
			time.Sleep(time.Second)
		case ExitTask:
			return
		}
	}
}

func getTask() GetTaskReply {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	if !call("Coordinator.GetTask", &args, &reply) {
		// If call fails, assume coordinator has exited and exit worker
		os.Exit(0)
	}
	return reply
}

func performMapTask(mapf func(string, string) []KeyValue, task GetTaskReply) {
	content, err := ioutil.ReadFile(task.FileName)
	if err != nil {
		log.Fatalf("cannot read %v", task.FileName)
	}

	kva := mapf(task.FileName, string(content))

	intermediate := make([][]KeyValue, task.NReduce)
	for _, kv := range kva {
		idx := ihash(kv.Key) % task.NReduce
		intermediate[idx] = append(intermediate[idx], kv)
	}

	for i := 0; i < task.NReduce; i++ {
		oname := fmt.Sprintf("mr-%d-%d", task.TaskID, i)
		tempFile, err := ioutil.TempFile("", "mr-tmp-*")
		if err != nil {
			log.Fatal("cannot create temp file", err)
		}
		enc := json.NewEncoder(tempFile)
		for _, kv := range intermediate[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatal("cannot encode json", err)
			}
		}
		tempFile.Close()
		os.Rename(tempFile.Name(), oname)
	}

	callTaskCompleted(MapTask, task.TaskID)
}

func performReduceTask(reducef func(string, []string) string, task GetTaskReply) {
	intermediate := []KeyValue{}
	for i := 0; i < task.NMap; i++ {
		iname := fmt.Sprintf("mr-%d-%d", i, task.TaskID)
		file, err := os.Open(iname)
		if err != nil {
			log.Fatal("cannot open file", err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", task.TaskID)
	tempFile, err := ioutil.TempFile("", "mr-tmp-*")
	if err != nil {
		log.Fatal("cannot create temp file", err)
	}

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

		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	tempFile.Close()
	os.Rename(tempFile.Name(), oname)

	callTaskCompleted(ReduceTask, task.TaskID)
}

func callTaskCompleted(taskType TaskType, taskID int) {
	args := TaskCompletionArgs{TaskType: taskType, TaskID: taskID}
	reply := TaskCompletionReply{}
	call("Coordinator.TaskCompleted", &args, &reply)
}

func call(rpcname string, args interface{}, reply interface{}) bool {
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
