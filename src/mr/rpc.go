package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	log "6.824/logger"
	"github.com/sirupsen/logrus"
)

//
// Task definitions
//
const (
	READY = iota // LifeCycle of a task
	WIP
	FINISHED
	MAP = iota // Type of a task
	REDUCE
	TIMEOUT = 10 * time.Second
)

// Task is either a Map task or a Reduce task
type Task struct {
	ID         string    // ID represents a task id, in format `m-IMap` or `r-IReduce`
	LifeCycle  int       // LifeCycle should be READY | WIP | FINISHED
	TaskType   int       // TaskType should be either MAP task or REDUCE task
	AssignedAt time.Time // AssignedAt records the time when this task is assigned to a worker, i.e. READY -> WIP
	FileName   string    // FileName is the input file name
	NMap       int       // NMap is the number of map tasks to use, as well as that of input files
	IMap       int       // IMap is the id of the input file for Map task
	NReduce    int       // NReduce is the number of reduce tasks to use, as well as that of intermediate files
	IReduce    int       // IReduce is the id of the input file for Reduce task
}

// NewTask constructs a task. The argument `filename` should be ignored when `taskType` = REDUCE.
func NewTask(taskType int, filename string, NMap int, IMap int, NReduce int, IReduce int) *Task {
	logger := log.Logger.WithFields(logrus.Fields{
		"FileName": filename,
		"TaskType": taskType,
	})

	// Generate a task id and validate taskType
	var ID string
	switch taskType {
	case MAP:
		ID = fmt.Sprintf("m-%d", IMap)
	case REDUCE:
		ID = fmt.Sprintf("r-%d", IReduce)
	default:
		logger.Panic("invalid taskType")
	}
	logger.WithField("ID", ID).Infof("A new task generated")
	return &Task{ID: ID, LifeCycle: READY, TaskType: taskType, FileName: filename, NMap: NMap, IMap: IMap, NReduce: NReduce, IReduce: IReduce}
}

// UpdateNow updates a Task's AssignedAt field
func (t *Task) UpdateNow() {
	t.AssignedAt = time.Now()
}

// IsTimeout returns if the current task is timeout
func (t *Task) IsTimeout() bool {
	return time.Now().Sub(t.AssignedAt) > TIMEOUT
}

// TaskManager manages a set of Tasks in FIFO manner
type TaskManager struct {
	Capacity   int        // Capacity for ReadyQueue
	ReadyQueue chan *Task // ReadyQueue is a channel storing Tasks whose LifeCycle = READY
	WIPTable   sync.Map   // Table maps Task.ID to the correspond Task whose LifeCycle = WIP
}

// NewTaskManager constructs a TaskManager
func NewTaskManager(cap int) *TaskManager {
	return &TaskManager{Capacity: cap, ReadyQueue: make(chan *Task, cap)}
}

// ResetOverdueTask detects overdue tasks among WIPTable and move them to ReadyQueue
func (m *TaskManager) ResetOverdueTask() {
	m.WIPTable.Range(func(key, value interface{}) bool {
		if t := value.(*Task); t.IsTimeout() {
			if t.LifeCycle != WIP {
				log.Logger.Fatalf("the LifeCycle of the task under check is %d, but `WIP` is expected", t.LifeCycle)
			}
			t.LifeCycle = READY
			m.ReadyQueue <- t
			m.WIPTable.Delete(key)
			log.Logger.WithFields(logrus.Fields{
				"ID":       t.ID,
				"TaskType": t.TaskType,
			}).Warn("reset an overdue task")
			return false
		}
		return true
	})
}

// GetWIPTableLen implements a Len() method of a sync.Map instance
func (m *TaskManager) GetWIPTableLen() (lenWIPTable int) {
	m.WIPTable.Range(func(key, value interface{}) bool {
		lenWIPTable++
		return true
	})
	return
}

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// TaskArgs is the request argument of the RPC Coordinator.DispatchTask
type TaskArgs struct {
}

// TaskReply is the response argument of the RPC Coordinator.DispatchTask
type TaskReply struct {
	Task
	Complete uint32
}

// ResultArgs is the request argument of the RPC Coordinator.ReceiveResult
type ResultArgs struct {
	Task
	Success bool
}

// ResultReply is the response argument of the RPC Coordinator.ReceiveResult
type ResultReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
