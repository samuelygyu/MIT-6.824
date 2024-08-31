package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mu sync.Mutex

	mapFiles []string
	// The count of map task which have done
	nMapTasks int
	// The total number of the reduce task
	nReduceTasks int

	// Trace task status
	mapTasksFinished    []bool
	mapTasksIssued      []time.Time
	reduceTasksFinished []bool
	reduceTasksIssued   []time.Time

	isDone bool
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
//	func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
//		reply.Y = args.X + 1
//		return nil
//	}
func (c *Coordinator) HandleGetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Assign the map task
	for i, f := range c.mapFiles {
		// Skip the finished task and processing task
		if c.mapTasksFinished[i] || c.mapTasksIssued[i].After(time.Now()) {
			continue
		}

		reply.Filename = f
		reply.NReduceTasks = c.nReduceTasks
		reply.TaskNum = i
		reply.TaskType = Map
		c.mapTasksIssued[i] = time.Now().Add(time.Second * 10)
		return nil
	}
	if c.nMapTasks < len(c.mapFiles) {
		return nil
	}

	// Assign the reduce task
	reduceDoneFlag := true
	for i := 0; i < c.nReduceTasks; i++ {
		// Skip the finished task and processing task
		if c.reduceTasksFinished[i] {
			continue
		}
		if c.reduceTasksIssued[i].After(time.Now()) {
			reduceDoneFlag = false
		}

		reply.NMapTasks = len(c.mapFiles)
		reply.NReduceTasks = c.nReduceTasks
		reply.TaskNum = i
		reply.TaskType = Reduce
		c.reduceTasksIssued[i] = time.Now().Add(time.Second * 10)
		return nil
	}

	if reduceDoneFlag {
		c.isDone = true
		reply.TaskType = Done
	}
	return nil
}

func (c *Coordinator) HandleFinishedTask(args *FinishedTaskArgs, reply *FinishedTaskReply) error {
	// log.Printf("[TASK:	%v] finished", args.TaskNum)
	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.TaskType {
	case Map:
		if c.mapTasksFinished[args.TaskNum] {
			return nil
		}

		c.mapTasksFinished[args.TaskNum] = true
		c.nMapTasks++
		return nil
	case Reduce:
		if c.reduceTasksFinished[args.TaskNum] {
			return nil
		}

		c.reduceTasksFinished[args.TaskNum] = true
		return nil
	default:
		// log.Println("wrong type for the finished task")
		return nil
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.isDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.mu = sync.Mutex{}
	c.mapFiles = files
	c.nReduceTasks = nReduce
	c.mapTasksFinished = make([]bool, len(files))
	c.mapTasksIssued = make([]time.Time, len(files))
	c.reduceTasksFinished = make([]bool, nReduce)
	c.reduceTasksIssued = make([]time.Time, nReduce)
	c.server()
	return &c
}
