package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	for {
		args := RequestTaskArgs{}
		reply := RequestTaskReply{}
		ok := call("Coordinator.RequestTask", &args, &reply)
		if !ok {
			log.Fatal("call to Coordinator.RequestTask failed")
		}

		switch reply.TaskType {
		case "map":
			doMapTask(reply.TaskID, reply.Filename, reply.NReduce, mapf)
			doneArgs := ReportTaskDoneArgs{TaskType: "map", TaskID: reply.TaskID}
			doneReply := ReportTaskDoneReply{}
			call("Coordinator.ReportTaskDone", &doneArgs, &doneReply)

		case "reduce":
			doReduceTask(reply.TaskID, reply.NMap, reducef)
			doneArgs := ReportTaskDoneArgs{TaskType: "reduce", TaskID: reply.TaskID}
			doneReply := ReportTaskDoneReply{}
			call("Coordinator.ReportTaskDone", &doneArgs, &doneReply)

		case "wait":
			time.Sleep(time.Second)

		case "exit":
			return
		}
	}
}

func doMapTask(mapID int, filename string, nReduce int, mapf func(string, string) []KeyValue) {

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()

	kva := mapf(filename, string(content))

	files := make([]*os.File, nReduce)
	encoders := make([]*json.Encoder, nReduce)
	for r := 0; r < nReduce; r++ {
		oname := fmt.Sprintf("mr-%d-%d", mapID, r)
		f, err := os.Create(oname)
		if err != nil {
			log.Fatalf("cannot create %v", oname)
		}
		files[r] = f
		encoders[r] = json.NewEncoder(f)
	}

	for _, kv := range kva {
		r := ihash(kv.Key) % nReduce
		err := encoders[r].Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode kv to file: %v", err)
		}
	}

	for _, f := range files {
		f.Close()
	}
}

func doReduceTask(reduceID int, nMap int, reducef func(string, []string) string) {

	intermediate := []KeyValue{}

	// 1. Read all intermediate files from all map tasks
	for m := 0; m < nMap; m++ {
		iname := fmt.Sprintf("mr-%d-%d", m, reduceID)
		f, err := os.Open(iname)
		if err != nil {
			log.Fatalf("cannot open %v", iname)
		}

		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				if err == io.EOF {
					break
				}
				log.Fatalf("cannot decode: %v", err)
			}
			intermediate = append(intermediate, kv)
		}
		f.Close()
	}

	// 2. Sort intermediate by key
	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	// 3. Group by key and apply reducef
	oname := fmt.Sprintf("mr-out-%d", reduceID)
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("cannot create %v", oname)
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

		// Write key + reduce result to final output
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
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
