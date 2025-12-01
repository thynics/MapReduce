package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// WorkerState local states
type WorkerState struct {
	finishedTaskID *string

	stepRetryCount int

	terminateFlag bool

	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string

	mapAddrInfo map[int64]string

	ip   string
	port string
	addr string

	coordinatorAddr string
}

const stepRetryLimit = 3

func newWorker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) *WorkerState {
	w := new(WorkerState)
	w.mapf = mapf
	w.reducef = reducef
	return w
}

func (w *WorkerState) stepFailAdd() {
	w.stepRetryCount++
}

func (w *WorkerState) stepFailReset() {
	w.stepRetryCount = 0
}

func (w *WorkerState) step() error {
	args := PullTaskArgs{FinishedTaskKey: w.finishedTaskID, IP: w.ip, Port: w.port}
	reply := PullTaskReply{}
	ok := call(w.coordinatorAddr, "Coordinator.PullTask", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
		return fmt.Errorf("Coordinator.PullTask failed")
	}
	fmt.Println("call success")
	bytes, _ := json.Marshal(reply)
	fmt.Println(string(bytes))
	w.mapAddrInfo = reply.MapTaskAddrInfo

	w.finishedTaskID = nil

	switch reply.Stage {
	case StageMap:
		if reply.AssignedMapTask != nil {
			w.doMapTask(reply.AssignedMapTask)
		} else {
			w.doNothing()
		}
	case StageReduce:
		if reply.AssignedReduceTask != nil {
			w.doReduceTask(reply.AssignedReduceTask)
		} else {
			w.doNothing()
		}
	case StageDone:
		w.terminate()
	default:
		w.doNothing()
	}
	return nil
}

func writeKVsToFile(filename string, kvs []KeyValue) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	enc := json.NewEncoder(file)
	for _, kv := range kvs {
		if err := enc.Encode(&kv); err != nil {
			return err
		}
	}
	return nil
}

func readKVsFromFile(filename string) ([]KeyValue, error) {
	b, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	return decodeKVsFromString(string(b))
}

func decodeKVsFromString(content string) ([]KeyValue, error) {
	dec := json.NewDecoder(strings.NewReader(content))
	var kvs []KeyValue

	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		kvs = append(kvs, kv)
	}
	return kvs, nil
}

// 控制本worker读中间文件
func (w *WorkerState) readKVsFromFile(mapID int64, reduceID int64) ([]KeyValue, error) {
	addr, ok := w.mapAddrInfo[mapID]
	if !ok {
		fmt.Println("mapID not found!")
		return nil, fmt.Errorf("mapID not found!")
	}
	if addr == w.addr {
		// read local
		return readKVsFromFile(fmt.Sprintf("mr-%d-%d", mapID, reduceID))
	}
	// read local
	args := new(ReadIntermediateFileContentArgs)
	args.MapID = mapID
	args.ReduceID = reduceID
	reply := new(ReadIntermediateFileContentReply)
	_ = call(addr, "WorkerState.ReadIntermediateFileContent", args, reply)
	if reply != nil && len(reply.Content) != 0 {
		return decodeKVsFromString(reply.Content)
	}
	return nil, fmt.Errorf("files not found")
}

// PRC Method，支持其它worker读本worker
func (w *WorkerState) ReadIntermediateFileContent(args *ReadIntermediateFileContentArgs, reply *ReadIntermediateFileContentReply) error {
	filename := fmt.Sprintf("mr-%d-%d", args.MapID, args.ReduceID)
	b, err := os.ReadFile(filename)
	if err != nil {
		return err
	}
	reply.Content = string(b)
	return nil
}

func writeFinalKVsSorted(filename string, kvs []KeyValue) error {
	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i].Key < kvs[j].Key
	})

	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	for _, kv := range kvs {
		if _, err := fmt.Fprintf(f, "%s %s\n", kv.Key, kv.Value); err != nil {
			return err
		}
	}
	return nil
}

func (w *WorkerState) doMapTask(task *MapTaskEntity) {
	content := task.FileContent

	kvs := w.mapf(task.FileName, content)

	kvBuckets := make(map[int][]KeyValue)
	for _, kv := range kvs {
		rid := ihash(kv.Key) % task.NReduce
		kvBuckets[rid] = append(kvBuckets[rid], kv)
	}

	for rid, partKVs := range kvBuckets {
		fileName := fmt.Sprintf("mr-%d-%d", task.MapID, rid)
		if err := writeKVsToFile(fileName, partKVs); err != nil {
			log.Printf("writeKVsToFile failed for %s: %v", fileName, err)
		}
	}

	w.finishedTaskID = &task.FileName
}

// doReduceTask
// 1. read all mr-MapID-ReduceID
// 2. reduce values by key
// 3. call reducef per key
// 4. write final result to mr-out-ReduceID
func (w *WorkerState) doReduceTask(task *ReduceTaskEntity) {
	kvsByKey := make(map[string][]string)

	for i := 0; i < task.MapCount; i++ {
		kvs, err := w.readKVsFromFile(int64(i), task.ReduceID)
		if err != nil {
			continue
		}
		for _, kv := range kvs {
			kvsByKey[kv.Key] = append(kvsByKey[kv.Key], kv.Value)
		}
	}

	results := make([]KeyValue, 0, len(kvsByKey))
	for key, values := range kvsByKey {
		out := w.reducef(key, values)
		results = append(results, KeyValue{Key: key, Value: out})
	}

	outFile := fmt.Sprintf("mr-out-%d", task.ReduceID)
	if err := writeFinalKVsSorted(outFile, results); err != nil {
		log.Fatalf("writeFinalKVsSorted failed for %s: %v", outFile, err)
	}

	key := strconv.Itoa(int(task.ReduceID))
	w.finishedTaskID = &key
}

func (w *WorkerState) terminate() {
	w.terminateFlag = true
}

func (w *WorkerState) shouldTerminate() bool {
	if w.terminateFlag {
		fmt.Println("All task done, terminate")
		return true
	}
	if w.stepRetryCount >= stepRetryLimit {
		fmt.Println("Coordinator down, step retry limit reached")
		return true
	}
	return false
}

func (w *WorkerState) doNothing() {
	time.Sleep(time.Second)
}

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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string, ip, port, coordinatorAddr string) {
	w := newWorker(mapf, reducef)
	w.ip = ip
	w.port = port
	w.coordinatorAddr = coordinatorAddr
	w.addr = fmt.Sprintf("%s:%s", ip, port)
	w.server()
	fmt.Println("start step")
	for !w.shouldTerminate() {
		err := w.step()
		if err != nil {
			fmt.Println("Step failed, retrying...")
			time.Sleep(1 * time.Second)
			w.stepFailAdd()
		} else {
			fmt.Println("Step success")
			w.stepFailReset()
		}
	}
}

func (w *WorkerState) server() {
	rpc.Register(w)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", "0.0.0.0:1234")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	log.Println("Worker RPC listening on 0.0.0.0:1234")
	go http.Serve(l, nil)
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
	ok := call("", "Coordinator.Example", &args, &reply)
	if ok {
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(addr, rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", addr)
	if err != nil {
		log.Printf("dial error (%s): %v\n", addr, err)
		return false
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err != nil {
		log.Printf("rpc call %s error: %v\n", rpcname, err)
		return false
	}
	return true
}

// How to read intermediate file
// 1. worker - ip:port (mesh), Coordinator::RegisterWorker(ip+port)

// map_id - (ip+port)
// 2.
