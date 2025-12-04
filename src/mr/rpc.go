package mr

import (
	"os"
	"strconv"
)

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type Stage int

const (
	StageInit Stage = iota
	StageMap
	StageWaitMap
	StageReduce
	StageWaitReduce
	StageDone
)

type TaskStatus int

const (
	TaskStatusIdle TaskStatus = iota + 1
	TaskStatusRunning
	TaskStatusDone
)

type MapTaskEntity struct {
	MapID       int64
	NReduce     int
	FileName    string
	FileContent string
}

type ReduceTaskEntity struct {
	MapCount int
	ReduceID int64
}

type PullTaskArgs struct {
	FinishedTaskKey *string
	IP              string
	Port            string
}

type PullTaskReply struct {
	Stage              Stage
	AssignedMapTask    *MapTaskEntity
	AssignedReduceTask *ReduceTaskEntity
	MapTaskAddrInfo    map[int64]string
}

type ReadIntermediateFileContentArgs struct {
	MapID    int64
	ReduceID int64
}

type ReadIntermediateFileContentReply struct {
	Content string
}

type RegisterArgs struct {
	IP string
}

type RegisterReply struct {
	
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
