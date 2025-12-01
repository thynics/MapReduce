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
	MapID       int64  // 固定的 Map 编号
	NReduce     int    // Reduce 任务数量
	FileName    string // 输入文件名
	FileContent string // 文件内容
}

type ReduceTaskEntity struct {
	MapCount int   // Map 任务数
	ReduceID int64 // 固定的 Reduce 编号
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

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
