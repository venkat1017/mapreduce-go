package common

// TaskType represents the type of task assigned to a worker.
type TaskType int

const (
	TaskTypeMap TaskType = iota
	TaskTypeReduce
	TaskTypeWait
	TaskTypeExit
)

// TaskRequest is sent from a worker to the master to request a task.
type TaskRequest struct {
	WorkerID int
}

// TaskResponse is sent from the master to a worker with task details.
type TaskResponse struct {
	TaskType   TaskType
	TaskNumber int    // Map or reduce task number
	NMap       int    // Total number of map tasks
	NReduce    int    // Total number of reduce tasks
	InputFile  string // For map tasks
}

// TaskCompleteArgs is sent from a worker to the master to report completion.
type TaskCompleteArgs struct {
	WorkerID   int
	TaskType   TaskType
	TaskNumber int
}

// TaskCompleteReply is a placeholder for RPC reply (can be extended).
type TaskCompleteReply struct{}
