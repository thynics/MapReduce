package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// TaskInfo BaseInfo of Task
type TaskInfo struct {
	Status     TaskStatus // Idle / Running / Done
	AssignedAt time.Time  // TimeStamp of assignment
	Index      int        // Uniqiue ID
	IP         string
	Port       string
}

type Coordinator struct {
	mu sync.Mutex

	CurrentStage Stage

	NReduce      int
	NMap         int
	WorkerCount  int
	ReadyWorkers map[string]bool

	// Map Task, key = filename
	MapTasks map[string]*TaskInfo
	// Reduce Task, key = 0..NReduce-1
	ReduceTasks map[string]*TaskInfo

	MapTaskAddrInfo map[int64]string
}

// taskTimeout Timeout of WorkerState response
const taskTimeout = 10 * time.Second

// startMapStage start map stage
func (c *Coordinator) startMapStage(files []string) {
	for len(c.ReadyWorkers) != c.WorkerCount {
		fmt.Printf("Waiting for all workers ready, current: %d, expected: %d\n", len(c.ReadyWorkers), c.WorkerCount)
		time.Sleep(1 * time.Second)
	}
	c.CurrentStage = StageMap
	c.NMap = len(files)
	c.MapTasks = make(map[string]*TaskInfo)

	for idx, file := range files {
		c.MapTasks[file] = &TaskInfo{
			Status: TaskStatusIdle,
			Index:  idx, // as map id
		}
	}
}

// startReduceStage start reduce stage, all map task done
func (c *Coordinator) startReduceStage() {
	c.CurrentStage = StageReduce
	c.ReduceTasks = make(map[string]*TaskInfo)

	for i := 0; i < c.NReduce; i++ {
		key := fmt.Sprintf("%d", i)
		c.ReduceTasks[key] = &TaskInfo{
			Status: TaskStatusIdle,
			Index:  i, // as reduce id
		}
	}

	c.MapTaskAddrInfo = make(map[int64]string, len(c.MapTasks))
	for _, task := range c.MapTasks {
		c.MapTaskAddrInfo[int64(task.Index)] = fmt.Sprintf("%s:%s", task.IP, task.Port)
	}
}

// allTasksDone all tasks are marked as done by workers
func allTasksDone(tasks map[string]*TaskInfo) bool {
	if len(tasks) == 0 {
		return false
	}
	for _, t := range tasks {
		if t.Status != TaskStatusDone {
			return false
		}
	}
	return true
}

// pickTask
// 1. Find idle first
// 2. Find running but timeout then
func pickTask(tasks map[string]*TaskInfo, ip, port string) (string, *TaskInfo, bool) {
	now := time.Now()

	for k, t := range tasks {
		if t.Status == TaskStatusIdle {
			t.Status = TaskStatusRunning
			t.AssignedAt = now
			t.IP = ip
			t.Port = port
			return k, t, true
		}
	}

	for k, t := range tasks {
		if t.Status == TaskStatusRunning && now.Sub(t.AssignedAt) > taskTimeout {
			// re-schedule
			t.AssignedAt = now
			t.IP = ip
			t.Port = port
			return k, t, true
		}
	}

	return "", nil, false
}

func (c *Coordinator) RegisterWorker(args *RegisterArgs, reply *RegisterArgs) error {
	c.ReadyWorkers[args.IP] = true
	return nil
}

// PullTask RPC call of WorkerState, pull task && submit task result
func (c *Coordinator) PullTask(args *PullTaskArgs, reply *PullTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply.AssignedMapTask = nil
	reply.AssignedReduceTask = nil

	if args.FinishedTaskKey != nil {
		switch c.CurrentStage {
		case StageMap, StageWaitMap:
			if t, ok := c.MapTasks[*args.FinishedTaskKey]; ok {
				t.Status = TaskStatusDone
			}
		case StageReduce, StageWaitReduce:
			if t, ok := c.ReduceTasks[*args.FinishedTaskKey]; ok {
				t.Status = TaskStatusDone
			}
		}
	}

	switch c.CurrentStage {

	case StageMap, StageWaitMap:
		if allTasksDone(c.MapTasks) {
			c.startReduceStage()
			reply.Stage = c.CurrentStage // StageReduce
			return nil
		}

		if key, info, ok := pickTask(c.MapTasks, args.IP, args.Port); ok {
			c.CurrentStage = StageMap
			reply.Stage = StageMap
			reply.AssignedMapTask = &MapTaskEntity{
				MapID:    int64(info.Index),
				NReduce:  c.NReduce,
				FileName: key,
			}
			// map task need to read content from coordinator locally
			f, _ := os.ReadFile(reply.AssignedMapTask.FileName)
			reply.AssignedMapTask.FileContent = string(f)
		} else {
			c.CurrentStage = StageWaitMap
			reply.Stage = StageWaitMap
		}

	case StageReduce, StageWaitReduce:
		if allTasksDone(c.ReduceTasks) {
			c.CurrentStage = StageDone
			reply.Stage = StageDone
			return nil
		}

		if _, info, ok := pickTask(c.ReduceTasks, args.IP, args.Port); ok {
			c.CurrentStage = StageReduce
			reply.Stage = StageReduce
			reply.AssignedReduceTask = &ReduceTaskEntity{
				MapCount: c.NMap,
				ReduceID: int64(info.Index),
			}
			reply.MapTaskAddrInfo = c.MapTaskAddrInfo
		} else {
			c.CurrentStage = StageWaitReduce
			reply.Stage = StageWaitReduce
		}

	case StageDone:
		reply.Stage = StageDone

	default:
		reply.Stage = StageInit
	}

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from WorkerState.go
func (c *Coordinator) local_server() {
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

func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", "0.0.0.0:1234")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	log.Println("Coordinator RPC listening on 0.0.0.0:1234")
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.CurrentStage == StageDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int, workerCount int) *Coordinator {
	c := Coordinator{
		NReduce:     nReduce,
		WorkerCount: workerCount,
	}
	c.server()
	c.startMapStage(files)
	return &c
}
