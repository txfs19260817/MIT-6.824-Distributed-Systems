package mr

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"

	log "6.824/logger"
)

type Coordinator struct {
	// Your definitions here.
	inputFilePaths      []string
	finishedMapTasks    uint32
	finishedReduceTasks uint32
	nMap                int
	nReduce             int
	mapTaskManager      *TaskManager
	reduceTaskManager   *TaskManager
	complete            uint32
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//DispatchTask dispatches a task from the ReadyQueue, and put it into the WIPTable
func (c *Coordinator) DispatchTask(args *TaskArgs, reply *TaskReply) error {
	var t *Task
	select {
	case t = <-c.mapTaskManager.ReadyQueue:
		log.Logger.Info("popped a Map task from mapTaskManager")
		defer func() {
			log.Logger.WithFields(logrus.Fields{
				"ID":         reply.ID,
				"FileName":   reply.FileName,
				"TaskType":   reply.TaskType,
				"AssignedAt": reply.AssignedAt,
			}).Info("a Map task has been dispatched to a worker")
			c.mapTaskManager.WIPTable.Store(t.ID, t)
			log.Logger.Debug(c.mapTaskManager.WIPTable.Load(t.ID))
		}()
	case t = <-c.reduceTaskManager.ReadyQueue:
		log.Logger.Info("popped a Reduce task from reduceTaskManager")
		defer func() {
			log.Logger.WithFields(logrus.Fields{
				"ID":         reply.ID,
				"FileName":   reply.FileName,
				"TaskType":   reply.TaskType,
				"AssignedAt": reply.AssignedAt,
			}).Info("a Reduce task has been dispatched to a worker")
			c.reduceTaskManager.WIPTable.Store(t.ID, t)
			log.Logger.Debug(c.reduceTaskManager.WIPTable.Load(t.ID))
		}()
	default: // no task available
		reply = &TaskReply{Task: Task{}, Complete: atomic.LoadUint32(&c.complete)}
		return nil
	}
	t.LifeCycle = WIP
	t.UpdateNow()
	reply.Task = *t
	return nil
}

// ReceiveResult harvests the finished task from workers
func (c *Coordinator) ReceiveResult(args *ResultArgs, reply *ResultReply) error {
	t := args.Task
	// check if done successfully
	if !args.Success || t.IsTimeout() || t.LifeCycle != FINISHED {
		log.Logger.WithFields(logrus.Fields{
			"ID":        t.ID,
			"Success":   args.Success,
			"Timeout":   t.IsTimeout(),
			"LifeCycle": t.LifeCycle,
		}).Warn("the coordinator received a failure task, reset it")
		t.LifeCycle = READY
		switch t.TaskType {
		case MAP:
			c.mapTaskManager.WIPTable.Delete(t.ID)
			c.mapTaskManager.ReadyQueue <- &t
		case REDUCE:
			c.reduceTaskManager.WIPTable.Delete(t.ID)
			c.reduceTaskManager.ReadyQueue <- &t
		default:
			log.Logger.Panic("invalid taskType: ", t.TaskType)
		}
		return nil
	}

	// success, update counters
	switch t.TaskType {
	case MAP:
		c.mapTaskManager.WIPTable.Delete(t.ID)
		atomic.AddUint32(&c.finishedMapTasks, 1)
		log.Logger.WithField("ID", t.ID).Infof("the coordinator received a succeed Map task, progress: %d/%d", atomic.LoadUint32(&c.finishedMapTasks), c.nMap)

	case REDUCE:
		c.reduceTaskManager.WIPTable.Delete(t.ID)
		atomic.AddUint32(&c.finishedReduceTasks, 1)
		log.Logger.WithField("ID", t.ID).Infof("the coordinator received a succeed Reduce task, progress: %d/%d", atomic.LoadUint32(&c.finishedReduceTasks), c.nReduce)

	default:
		log.Logger.Panic("invalid taskType: ", t.TaskType)
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	if err := rpc.Register(c); err != nil {
		log.Logger.Panic(err)
	}
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	if err := os.Remove(sockname); err != nil {
		log.Logger.Warn(err) // TRAP 1
	}
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Logger.Fatal("listen error:", e)
	}
	go func() {
		err := http.Serve(l, nil)
		if err != nil {
			log.Logger.Panic(err)
		}
	}()
}

// Done checks if the entire job has finished.
// main/mrcoordinator.go calls Done() periodically
func (c *Coordinator) Done() bool {
	//// ret := false

	// Your code here.
	if !(len(c.mapTaskManager.ReadyQueue) == 0 && c.mapTaskManager.GetWIPTableLen() == 0) {
		return false
	}
	log.Logger.Info("all Map tasks have been done!")
	if !(atomic.LoadUint32(&c.finishedReduceTasks) == uint32(c.nReduce) && len(c.reduceTaskManager.ReadyQueue) == 0 && c.reduceTaskManager.GetWIPTableLen() == 0) {
		return false
	}
	log.Logger.Info("all Reduce tasks have been done!")
	// validate output files
	for i := 0; i < c.nReduce; i++ {
		filename := fmt.Sprintf("mr-out-%d", i)
		if _, err := os.Stat(filename); os.IsNotExist(err) {
			log.Logger.Panicf(filename + " does not exist")
		}
	}
	atomic.StoreUint32(&c.complete, 1)
	return true
}

// MakeCoordinator creates a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of Reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	//// c := Coordinator{}

	// Your code here.
	c := Coordinator{
		inputFilePaths:    files,
		nMap:              len(files),
		nReduce:           nReduce,
		mapTaskManager:    NewTaskManager(len(files)),
		reduceTaskManager: NewTaskManager(nReduce),
	}

	// generate Map tasks
	for i, filePath := range files {
		c.mapTaskManager.ReadyQueue <- NewTask(MAP, filePath, len(files), i, nReduce, -1)
	}

	// start a timeout collector goroutine
	go c.TimeoutCollector()

	// start to generate Reduce tasks when all Map tasks finished
	go c.GenerateReduceTasks()

	c.server()
	log.Logger.Info("A Coordinator started")
	return &c
}

func (c *Coordinator) GenerateReduceTasks() {
	// spin until all Map tasks have been done
	for atomic.LoadUint32(&c.finishedMapTasks) != uint32(c.nMap) {
		time.Sleep(100 * time.Millisecond)
	}

	// validations
	// 1. assert no left Map tasks
	if !(len(c.mapTaskManager.ReadyQueue) == 0 && c.mapTaskManager.GetWIPTableLen() == 0) {
		log.Logger.Panic("all Map tasks have been done, but non-finished Map tasks exist")
	}
	// 2. check if intermediate files exist
	for r := 0; r < c.nReduce; r++ {
		for m := 0; m < c.nMap; m++ {
			filename := fmt.Sprintf("mr-%d-%d", m, r)
			if _, err := os.Stat(filename); os.IsNotExist(err) {
				log.Logger.Panic(filename+" does not exist", err)
			}
		}
	}

	// generate Reduce tasks
	for r := 0; r < c.nReduce; r++ {
		c.reduceTaskManager.ReadyQueue <- NewTask(REDUCE, "", c.nMap, -1, c.nReduce, r)
	}
}

// TimeoutCollector is a goroutine to collect overdue tasks, if any, and reassign it to a certain worker
func (c *Coordinator) TimeoutCollector() {
	for {
		c.mapTaskManager.ResetOverdueTask()
		c.reduceTaskManager.ResetOverdueTask()
		time.Sleep(1 * time.Second)
	}
}
