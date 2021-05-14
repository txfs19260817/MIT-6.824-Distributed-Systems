package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"net/rpc"
	"os"
	"reflect"
	"sort"
	"time"

	log "6.824/logger"
	"github.com/sirupsen/logrus"
)

const bucketSize = 12000

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker is the worker process.
// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	CallDispatchTask(mapf, reducef)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// CallDispatchTask asks Coordinator for a task
// TODO: might (not) need to add a return value to indicate to exit
func CallDispatchTask(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	for {
		reply := TaskReply{}
		if !call("Coordinator.DispatchTask", &TaskArgs{}, &reply) {
			log.Logger.Warn("a worker could not connect to the coordinator")
			break
		}
		if reply.Complete == 1 { // the job has been completed, so worker exits
			log.Logger.Info("a worker exits since the job is completed")
			break
		}
		if reflect.DeepEqual(reply.Task, Task{}) { // no task received, but the job is in-progress
			log.Logger.Info("a worker has suspended for 1s due to no task received")
			time.Sleep(1 * time.Second)
			continue
		}
		if reply.LifeCycle != WIP {
			log.Logger.Fatalf("the assigned task's LifeCycle is %d, but `WIP` is expected", reply.LifeCycle)
		}
		log.Logger.WithFields(logrus.Fields{
			"ID":         reply.ID,
			"FileName":   reply.FileName,
			"TaskType":   reply.TaskType,
			"AssignedAt": reply.AssignedAt,
		}).Info("a worker was assigned a task")

		resultArgs := ResultArgs{Success: true}
		switch reply.TaskType {
		case MAP:
			if err := DoMapTask(&reply.Task, mapf); err != nil {
				log.Logger.WithField("ID", reply.Task.ID).Error("a worker is failing on a Map task: ", err)
				resultArgs.Success, resultArgs.Task = false, reply.Task
				continue
			}
			log.Logger.WithFields(logrus.Fields{
				"ID":         reply.ID,
				"FileName":   reply.FileName,
				"AssignedAt": reply.AssignedAt,
			}).Info("a worker has finished a Map task")
			resultArgs.Task = reply.Task
			CallReceiveResult(&resultArgs)

		case REDUCE:
			if err := DoReduceTask(&reply.Task, reducef); err != nil {
				log.Logger.WithField("ID", reply.Task.ID).Error("a worker is failing on a Reduce task: ", err)
				resultArgs.Success, resultArgs.Task = false, reply.Task
				continue
			}
			log.Logger.WithFields(logrus.Fields{
				"ID":         reply.ID,
				"FileName":   reply.FileName,
				"AssignedAt": reply.AssignedAt,
			}).Info("a worker has finished a Reduce task")
			resultArgs.Task = reply.Task
			CallReceiveResult(&resultArgs)

		default:
			log.Logger.Panicf("a worker received a task which is neither Map nor Reduce: %d", reply.TaskType)
		}
	}
}

func CallReceiveResult(args *ResultArgs) {
	if !call("Coordinator.ReceiveResult", args, &ResultReply{}) {
		log.Logger.Fatalf("a worker is failing on reporting result to the coordinator")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Logger.Warn("dialing:", err)
		return false
	}
	defer c.Close()

	if err = c.Call(rpcname, args, reply); err != nil {
		log.Logger.Warn(err)
		return false
	}

	return true
}

func DoMapTask(t *Task, mapf func(string, string) []KeyValue) error {
	// setup output files
	filename, outFiles := t.FileName, make([]*os.File, t.NReduce)
	for i := range outFiles {
		ofn := fmt.Sprintf("mr-%d-%d", t.IMap, i)
		f, err := os.Create(ofn)
		if err != nil {
			return err
		}
		outFiles[i] = f
		defer outFiles[i].Close()
	}

	// do map task
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	intermediate := mapf(filename, string(content))

	// write each kv to corresponded intermediate file according to hashed key
	buckets := make([][]KeyValue, t.NReduce)
	for i := range buckets {
		buckets[i] = make([]KeyValue, 0, bucketSize)
	}
	for _, kv := range intermediate {
		reduceId := ihash(kv.Key) % t.NReduce
		buckets[reduceId] = append(buckets[reduceId], kv)
	}
	for i := 0; i < t.NReduce; i++ {
		enc := json.NewEncoder(outFiles[i])
		if err := enc.Encode(&buckets[i]); err != nil {
			return err
		}
	}

	t.LifeCycle = FINISHED
	return nil
}

func DoReduceTask(t *Task, reducef func(string, []string) string) error {
	// read intermediate files
	var intermediate []KeyValue
	for m := 0; m < t.NMap; m++ {
		// read files
		interFilename := fmt.Sprintf("mr-%d-%d", m, t.IReduce)
		file, err := os.Open(interFilename)
		if err != nil {
			return err
		}
		defer file.Close()
		// decode them
		var tempKV []KeyValue
		dec := json.NewDecoder(file)
		if err := dec.Decode(&tempKV); err != nil {
			return err
		}
		intermediate = append(intermediate, tempKV...)
	}
	sort.Slice(intermediate, func(i, j int) bool {
		return intermediate[i].Key < intermediate[j].Key
	})

	// setup the output file
	ofile, err := os.Create(fmt.Sprintf("mr-out-%d", t.IReduce))
	if err != nil {
		return err
	}
	defer ofile.Close()

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := make([]string, 0, j-i)
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	t.LifeCycle = FINISHED
	return nil
}
