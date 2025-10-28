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
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		// Request a task from the coordinator
		args := TaskArgs{}
		reply := TaskReply{}
		ok := call("Coordinator.RequestTask", &args, &reply)
		if !ok {
			// Failed to contact coordinator, assume it has exited
			break
		}

		switch reply.TaskType {
		case MapTask:
			executeMapTask(&reply, mapf)
		case ReduceTask:
			executeReduceTask(&reply, reducef)
		case WaitTask:
			// Wait a bit before requesting again
			// time.Sleep(time.Second)
			continue
		case ExitTask:
			// All tasks done, exit
			return
		}
	}
}

// Execute a map task
func executeMapTask(task *TaskReply, mapf func(string, string) []KeyValue) {
	// Read input file
	file, err := os.Open(task.MapInputFile)
	if err != nil {
		log.Fatalf("cannot open %v", task.MapInputFile)
		return
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.MapInputFile)
		file.Close()
		return
	}
	file.Close()

	// Call the map function
	kva := mapf(task.MapInputFile, string(content))

	// Partition the intermediate key/value pairs into nReduce buckets
	buckets := make([][]KeyValue, task.NReduce)
	for _, kv := range kva {
		bucket := ihash(kv.Key) % task.NReduce
		buckets[bucket] = append(buckets[bucket], kv)
	}

	// Write intermediate files
	for i := 0; i < task.NReduce; i++ {
		oname := fmt.Sprintf("mr-%d-%d", task.TaskId, i)
		ofile, err := ioutil.TempFile("", "mr-tmp-*")
		if err != nil {
			log.Fatalf("cannot create temp file")
			return
		}

		enc := json.NewEncoder(ofile)
		for _, kv := range buckets[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot encode json")
			}
		}
		ofile.Close()

		// Atomically rename temp file to final name
		os.Rename(ofile.Name(), oname)
	}

	// Report task completion
	completeArgs := TaskCompleteArgs{
		TaskType: MapTask,
		TaskId:   task.TaskId,
	}
	completeReply := TaskCompleteReply{}
	call("Coordinator.TaskComplete", &completeArgs, &completeReply)
}

// Execute a reduce task
func executeReduceTask(task *TaskReply, reducef func(string, []string) string) {
	// Read all intermediate files for this reduce task
	intermediate := []KeyValue{}
	for i := 0; i < task.NMap; i++ {
		iname := fmt.Sprintf("mr-%d-%d", i, task.TaskId)
		file, err := os.Open(iname)
		if err != nil {
			// File might not exist if map task produced no keys for this reduce bucket
			continue
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

	// Sort by key
	sort.Sort(ByKey(intermediate))

	// Create output file
	oname := fmt.Sprintf("mr-out-%d", task.TaskId)
	ofile, err := ioutil.TempFile("", "mr-out-tmp-*")
	if err != nil {
		log.Fatalf("cannot create temp file")
		return
	}

	// Call Reduce on each distinct key
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

	// Atomically rename temp file to final name
	os.Rename(ofile.Name(), oname)

	// Report task completion
	completeArgs := TaskCompleteArgs{
		TaskType: ReduceTask,
		TaskId:   task.TaskId,
	}
	completeReply := TaskCompleteReply{}
	call("Coordinator.TaskComplete", &completeArgs, &completeReply)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
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
