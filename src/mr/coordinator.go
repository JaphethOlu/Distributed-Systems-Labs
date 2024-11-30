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

type State uint8
type Type uint8

const (
	Idle State = iota
	InProgress
	Completed
)

func (s State) String() string {
	return [...]string{"Idle", "In-progress", "Completed"}[s]
}

const (
	Map Type = iota
	Reduce
	Exit
)

func (s Type) String() string {
	return [...]string{"Map", "Reduce"}[s]
}

type MapTask struct {
	filename   string
	taskNumber uint
	state      State
	startTime  time.Time
}

type ReduceTask struct {
	files     []string
	state     State
	startTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	mu            sync.RWMutex
	MapTasks      map[string]MapTask
	ReduceTasks   map[uint]ReduceTask
	MapNumber     uint
	MapsCompleted bool
	nReduce       int
}

func (c *Coordinator) AddMapTask(id string, t *MapTask) {
	defer c.mu.Unlock()

	c.mu.Lock()
	c.MapNumber++
	t.taskNumber = c.MapNumber
	c.MapTasks[id] = *t
}

func (c *Coordinator) MapTaskState(id string) State {
	defer c.mu.RUnlock()

	c.mu.RLock()
	state := c.MapTasks[id].state
	return state
}

func (c *Coordinator) ReduceTaskState(id uint) State {
	defer c.mu.RUnlock()

	c.mu.RLock()
	state := c.ReduceTasks[id].state
	return state
}

func (c *Coordinator) UpdateMapTaskState(id string, s State) {
	defer c.mu.Unlock()

	c.mu.Lock()
	task := c.MapTasks[id]
	task.state = s
	if s == InProgress {
		task.startTime = time.Now()
	}
	c.MapTasks[id] = task
}

func (c *Coordinator) UpdateReduceTaskState(id uint, s State) {
	defer c.mu.Unlock()

	c.mu.Lock()
	task := c.ReduceTasks[id]
	task.state = s
	if s == InProgress {
		task.startTime = time.Now()
	}
	c.ReduceTasks[id] = task
}

func (c *Coordinator) CheckMapsCompleted() {
	defer c.mu.Unlock()

	c.mu.Lock()
	for _, mTask := range c.MapTasks {
		if mTask.state == Idle || mTask.state == InProgress {
			return
		}
	}
	c.MapsCompleted = true
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// RPC call to find and return task
func (c *Coordinator) TaskRequest(args *TaskArgs, reply *TaskReply) error {
	// Return Map Task if there is a map task that is yet to be processes
	defer c.mu.Unlock()

	c.mu.Lock()
	restartTime := 500 * time.Millisecond

	if !c.MapsCompleted {
		for _, task := range c.MapTasks {
			if task.state == Idle {
				c.mu.Unlock()
				c.UpdateMapTaskState(task.filename, InProgress)
				c.mu.Lock()

				reply.Filename = task.filename
				reply.Type = Map
				reply.TaskNumber = task.taskNumber
				reply.NReduce = c.nReduce

				return nil
			}

			if task.state == InProgress {
				elapsed := time.Since(task.startTime)
				if elapsed > restartTime {
					c.mu.Unlock()
					c.UpdateMapTaskState(task.filename, InProgress)
					c.mu.Lock()

					reply.Filename = task.filename
					reply.Type = Map
					reply.TaskNumber = task.taskNumber
					reply.NReduce = c.nReduce

					return nil
				}
			}
		}
	} else {
		for id, task := range c.ReduceTasks {
			if task.state == Idle {
				c.mu.Unlock()
				c.UpdateReduceTaskState(id, InProgress)
				c.mu.Lock()

				reply.Files = task.files
				reply.Type = Reduce
				reply.TaskNumber = id

				return nil
			}

			if task.state == InProgress {
				elapsed := time.Since(task.startTime)
				if elapsed > time.Second {
					c.mu.Unlock()
					c.UpdateReduceTaskState(id, InProgress)
					c.mu.Lock()

					reply.Files = task.files
					reply.Type = Reduce
					reply.TaskNumber = id

					return nil
				}
			}
		}
	}
	reply.Type = Exit
	return nil
}

// RPC call for recieving the results of a map and adding them to the task list
func (c *Coordinator) MapResult(args *MapResult, reply *MapResultReply) error {
	state := c.MapTaskState(args.MapFilename)
	if state != Completed {

		c.mu.Lock()
		for _, rFiles := range args.Files {
			task, exists := c.ReduceTasks[rFiles.ReducerNumber]
			if exists {
				task.files = append(task.files, rFiles.Filename)
				c.ReduceTasks[rFiles.ReducerNumber] = task
			} else {
				rTask := ReduceTask{
					files: []string{rFiles.Filename},
					state: Idle,
				}
				c.ReduceTasks[rFiles.ReducerNumber] = rTask
			}
		}

		c.mu.Unlock()
		c.UpdateMapTaskState(args.MapFilename, Completed)
	}
	c.CheckMapsCompleted()
	return nil
}

// RPC call for recieving the results of a reduce
func (c *Coordinator) ReduceResult(args *ReduceResult, reply *ReduceResultReply) error {

	state := c.ReduceTaskState(args.ReducerNumber)
	if state != Completed {
		c.UpdateReduceTaskState(args.ReducerNumber, Completed)
	}
	return nil
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
	ret := false

	if len(c.MapTasks) == 0 || len(c.ReduceTasks) == 0 {
		return ret
	}

	// Your code here.
	for _, mTask := range c.MapTasks {
		if mTask.state != Completed {
			return ret
		}
	}

	for _, rTask := range c.ReduceTasks {
		if rTask.state != Completed {
			return ret
		}
	}

	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		MapTasks:      make(map[string]MapTask),
		ReduceTasks:   make(map[uint]ReduceTask),
		MapNumber:     0,
		MapsCompleted: false,
		nReduce:       nReduce,
	}

	// Your code here.

	// Get filenames for map task
	for _, filename := range files {
		var mTask = MapTask{
			filename: filename,
			state:    Idle,
		}
		c.AddMapTask(filename, &mTask)
	}

	c.server()
	return &c
}
