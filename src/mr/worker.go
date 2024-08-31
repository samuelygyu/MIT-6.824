package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

const IntermediateDir = "/home/youyg/go/intermediate"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
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
		args := GetTaskArgs{}
		reply := GetTaskReply{}

		call("Coordinator.HandleGetTask", &args, &reply)
		// log.Printf("rpc call args: %v reply: %v", args, reply)

		switch reply.TaskType {
		case Map:
			executeMapTask(mapf, reply)
		case Reduce:
			executeReduceTask(reducef, reply)
		case Done:
			os.Exit(1)
		default:
			// log.Println("wrong task type!!!")
			time.Sleep(time.Millisecond * 100)
			continue	
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func executeMapTask(mapf func(string, string) []KeyValue, reply GetTaskReply) {
	kva := mapf(reply.Filename, getContentByFilename(reply.Filename))

	m := make(map[int][]KeyValue)
	for i := 0; i < reply.NReduceTasks; i++ {
		m[i] = []KeyValue{}
	}

	for _, kv := range kva {
		keyh := ihash(kv.Key) % reply.NReduceTasks
		m[keyh] = append(m[keyh], kv)
	}

	// file, _ := os.Create("log.out")
	// defer file.Close()
	// fmt.Fprint(file, m)
	// err := os.MkdirAll(IntermediateDir, 0755)
	// if err != nil {
	// 	log.Printf("can't create the dir [%v] cause: %v", IntermediateDir, err)
	// }
	// err = os.Chdir(IntermediateDir)
	// if err != nil {
	// 	log.Println(err)
	// }

	// Create a channel to wait thread to complete it's task
	c := make(chan int, 10)

	for k, v := range m {
		// parallel write the itnermediate file to local disk
		go func(k int, v []KeyValue, c chan int) {
			file, err := os.Create(fmt.Sprintf("mr-%v-%v", reply.TaskNum, k))

			if err != nil {
				log.Fatal(err)
			}
			defer file.Close()
			enc := json.NewEncoder(file)
			for _, kv := range v {
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatalf("[TASK: %v]intermediate file write to local disk fail", reply.TaskNum)
				}
			}
			c <- 1
		}(k, v, c)
	}

	// Wait channel
	a := 0
	for {
		b := <-c
		a += b
		if a == len(m) {
			break
		}
	}

	// send the done msg to the coordinator
	args := FinishedTaskArgs{}
	args.TaskType = Map
	args.TaskNum = reply.TaskNum
	freply := FinishedTaskReply{}
	call("Coordinator.HandleFinishedTask", &args, &freply)
}

func executeReduceTask(reducef func(string, []string) string, reply GetTaskReply) {
	intermediate := mergeAndParseIntermediate(reply.NMapTasks, reply.TaskNum)

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%v", reply.TaskNum)
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

	// send done msg to coordinator
	args := FinishedTaskArgs{}
	args.TaskType = Reduce
	args.TaskNum = reply.TaskNum
	freply := FinishedTaskReply{}
	call("Coordinator.HandleFinishedTask", &args, &freply)
}

func mergeAndParseIntermediate(nMapTasks int, taskNum int) []KeyValue {
	kva := []KeyValue{}
	for i := 0; i < nMapTasks; i++ {

		filename := fmt.Sprintf("mr-%v-%v", i, taskNum)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("merge cannot open %v, %v", filename, err)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}

		file.Close()
	}

	return kva
}

func getContentByFilename(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("fuck you cannot open %v %v", filename, err)
	}
	defer file.Close()
	contents, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v %v", filename, err)
	}

	return string(contents)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
/* func CallExample() {

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
} */

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
