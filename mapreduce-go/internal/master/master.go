package master

import (
	"log"
	"mapreduce-go/internal/common"
	"os"
	"sync"
	"time"
)

type taskState int

const (
	taskIdle taskState = iota
	taskInProgress
	taskDone
)

const taskTimeout = 10 * time.Second

type Master struct {
	NMap    int
	NReduce int
	Files   []string

	mu          sync.Mutex
	mapTasks    []taskState
	reduceTasks []taskState
	phase       string // "map", "reduce", "done"

	mapTaskStart    []time.Time
	reduceTaskStart []time.Time
}

func NewMaster(files []string, nMap, nReduce int) *Master {
	m := &Master{
		NMap:            nMap,
		NReduce:         nReduce,
		Files:           files,
		mapTasks:        make([]taskState, nMap),
		reduceTasks:     make([]taskState, nReduce),
		phase:           "map",
		mapTaskStart:    make([]time.Time, nMap),
		reduceTaskStart: make([]time.Time, nReduce),
	}
	go m.monitorTasks()
	return m
}

func (m *Master) monitorTasks() {
	for {
		time.Sleep(time.Second)
		m.mu.Lock()
		if m.phase == "done" {
			m.mu.Unlock()
			log.Println("All tasks complete. Master shutting down.")
			os.Exit(0)
			return
		}
		var now = time.Now()
		if m.phase == "map" {
			for i, state := range m.mapTasks {
				if state == taskInProgress && now.Sub(m.mapTaskStart[i]) > taskTimeout {
					log.Printf("Reassigning MAP task #%d due to timeout", i)
					m.mapTasks[i] = taskIdle
				}
			}
		} else if m.phase == "reduce" {
			for i, state := range m.reduceTasks {
				if state == taskInProgress && now.Sub(m.reduceTaskStart[i]) > taskTimeout {
					log.Printf("Reassigning REDUCE task #%d due to timeout", i)
					m.reduceTasks[i] = taskIdle
				}
			}
		}
		m.mu.Unlock()
	}
}

// RPC handler: AssignTask
func (m *Master) AssignTask(req *common.TaskRequest, resp *common.TaskResponse) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.phase == "map" {
		for i, state := range m.mapTasks {
			if state == taskIdle {
				m.mapTasks[i] = taskInProgress
				m.mapTaskStart[i] = time.Now()
				log.Printf("Assigning MAP task #%d to worker %d", i, req.WorkerID)
				resp.TaskType = common.TaskTypeMap
				resp.TaskNumber = i
				resp.NMap = m.NMap
				resp.NReduce = m.NReduce
				resp.InputFile = m.Files[i]
				return nil
			}
		}
		// No idle map tasks, check if any in progress
		for _, state := range m.mapTasks {
			if state == taskInProgress {
				resp.TaskType = common.TaskTypeWait
				return nil
			}
		}
		// All map tasks done, move to reduce phase
		m.phase = "reduce"
	}

	if m.phase == "reduce" {
		for i, state := range m.reduceTasks {
			if state == taskIdle {
				m.reduceTasks[i] = taskInProgress
				m.reduceTaskStart[i] = time.Now()
				log.Printf("Assigning REDUCE task #%d to worker %d", i, req.WorkerID)
				resp.TaskType = common.TaskTypeReduce
				resp.TaskNumber = i
				resp.NMap = m.NMap
				resp.NReduce = m.NReduce
				return nil
			}
		}
		for _, state := range m.reduceTasks {
			if state == taskInProgress {
				resp.TaskType = common.TaskTypeWait
				return nil
			}
		}
		m.phase = "done"
	}

	resp.TaskType = common.TaskTypeExit
	return nil
}

// RPC handler: ReportTaskComplete
func (m *Master) ReportTaskComplete(args *common.TaskCompleteArgs, reply *common.TaskCompleteReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	switch args.TaskType {
	case common.TaskTypeMap:
		if args.TaskNumber >= 0 && args.TaskNumber < len(m.mapTasks) {
			m.mapTasks[args.TaskNumber] = taskDone
			log.Printf("MAP task #%d completed by worker %d", args.TaskNumber, args.WorkerID)
		}
	case common.TaskTypeReduce:
		if args.TaskNumber >= 0 && args.TaskNumber < len(m.reduceTasks) {
			m.reduceTasks[args.TaskNumber] = taskDone
			log.Printf("REDUCE task #%d completed by worker %d", args.TaskNumber, args.WorkerID)
		}
	}
	return nil
}
