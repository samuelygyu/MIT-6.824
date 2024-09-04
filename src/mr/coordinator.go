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
	mu   sync.Mutex
	cond sync.Cond

	mapFiles []string
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
	for {
		mapDoneFlag := true
		for i, done := range c.mapTasksFinished {
			if !done {
				// log.Println(c.mapTasksIssued)
				if c.mapTasksIssued[i].IsZero() || time.Since(c.mapTasksIssued[i]).Seconds() > 10 {
					reply.Filename = c.mapFiles[i]
					reply.NReduceTasks = c.nReduceTasks
					reply.TaskNum = i
					reply.TaskType = Map
					c.mapTasksIssued[i] = time.Now()
					return nil
				} else {
					mapDoneFlag = false
				}
			}
		}

		if !mapDoneFlag {
			c.cond.Wait()
		} else {
			break
		}
	}

	// Assign the reduce task loop
	for {
		reduceDoneFlag := true
		for i, done := range c.reduceTasksFinished {
			if !done {
				// if true task	unprocess or timeout 
				if c.reduceTasksIssued[i].IsZero() || time.Since(c.reduceTasksIssued[i]).Seconds() > 10 {
					reply.NMapTasks = len(c.mapFiles)
					reply.NReduceTasks = c.nReduceTasks
					reply.TaskNum = i
					reply.TaskType = Reduce
					c.reduceTasksIssued[i] = time.Now()
					return nil
				} else {
					reduceDoneFlag = false
				}
			}
		}
		
		if !reduceDoneFlag {
			c.cond.Wait()
		} else {
			break
		}

	}

	c.isDone = true
	reply.TaskType = Done
	return nil
}

func (c *Coordinator) HandleFinishedTask(args *FinishedTaskArgs, reply *FinishedTaskReply) error {
	// log.Printf("[TASK:	%v] finished", args.TaskNum)
	c.mu.Lock()
	defer c.mu.Unlock()
	defer c.cond.Broadcast()

	switch args.TaskType {
	case Map:
		if c.mapTasksFinished[args.TaskNum] {
			return nil
		}

		c.mapTasksFinished[args.TaskNum] = true
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
	c.mu.Lock()
	defer c.mu.Unlock()
	done := c.isDone
	return done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.mu = sync.Mutex{}
	c.cond = *sync.NewCond(&c.mu)
	c.mapFiles = files
	c.nReduceTasks = nReduce
	c.mapTasksFinished = make([]bool, len(files))
	c.mapTasksIssued = make([]time.Time, len(files))
	c.reduceTasksFinished = make([]bool, nReduce)
	c.reduceTasksIssued = make([]time.Time, nReduce)

	// Regular wait up the getHandler
	go func() {
		for {
			c.mu.Lock()
			c.cond.Broadcast()
			c.mu.Unlock()
			time.Sleep(time.Second)
		}
	}()

	c.server()
	return &c
}
