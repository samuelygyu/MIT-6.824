package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type TaskType int

const (
	Map    TaskType = 1
	Reduce TaskType = 2
	Done   TaskType = 3
)

type GetTaskArgs struct{}

type GetTaskReply struct {
	TaskType 		TaskType

	// In map handle, TaskNum is the offset of the maptask list
	// In reduce handle, TaskNum is the offset of the reducetask list
	TaskNum 		int
	
	Filename 		string

	// the len of the Coordinator's MapFiles
	NMapTasks 		int
	// the total number of the reduce task
	NReduceTasks 	int
}

type FinishedTaskArgs struct {
	TaskType TaskType

	// In map handle, TaskNum is the offset of the maptask list
	// In reduce handle, TaskNum is the offset of the reducetask list
	TaskNum int
}

type FinishedTaskReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
