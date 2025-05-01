package worker

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"mapreduce-go/internal/common"
	"net/rpc"
	"os"
	"sort"
)

type Worker struct {
	ID         int
	MasterAddr string
	// Add fields for worker state, etc.
}

func NewWorker(id int, masterAddr string) *Worker {
	return &Worker{
		ID:         id,
		MasterAddr: masterAddr,
	}
}

// Request a task from the master
func (w *Worker) RequestTask() (*common.TaskResponse, error) {
	client, err := rpc.Dial("tcp", w.MasterAddr)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	req := &common.TaskRequest{WorkerID: w.ID}
	resp := &common.TaskResponse{}
	err = client.Call("Master.AssignTask", req, resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// Report task completion to the master
func (w *Worker) ReportTaskComplete(taskType common.TaskType, taskNumber int) error {
	client, err := rpc.Dial("tcp", w.MasterAddr)
	if err != nil {
		return err
	}
	defer client.Close()

	args := &common.TaskCompleteArgs{
		WorkerID:   w.ID,
		TaskType:   taskType,
		TaskNumber: taskNumber,
	}
	reply := &common.TaskCompleteReply{}
	return client.Call("Master.ReportTaskComplete", args, reply)
}

// ExecuteMapTask runs the map function on the input file and writes intermediate files for each reduce partition.
func (w *Worker) ExecuteMapTask(inputFile string, mapTaskNum, nReduce int, mapF func(string) []common.KeyValue) error {
	file, err := os.Open(inputFile)
	if err != nil {
		return fmt.Errorf("failed to open input file: %v", err)
	}
	defer file.Close()

	// Prepare writers for each reduce partition
	encoders := make([]*json.Encoder, nReduce)
	files := make([]*os.File, nReduce)
	for r := 0; r < nReduce; r++ {
		fname := fmt.Sprintf("mr-%d-%d", mapTaskNum, r)
		f, err := os.Create(fname)
		if err != nil {
			return fmt.Errorf("failed to create intermediate file %s: %v", fname, err)
		}
		files[r] = f
		encoders[r] = json.NewEncoder(f)
	}
	defer func() {
		for _, f := range files {
			if f != nil {
				f.Close()
			}
		}
	}()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		for _, kv := range mapF(line) {
			partition := ihash(kv.Key) % nReduce
			if err := encoders[partition].Encode(&kv); err != nil {
				return fmt.Errorf("failed to write kv to partition %d: %v", partition, err)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("failed to read input file: %v", err)
	}
	return nil
}

// ihash hashes a string to an int (for partitioning keys)
func ihash(s string) int {
	h := 0
	for _, c := range s {
		h = int(c) + 31*h
	}
	return h & 0x7fffffff
}

// ExecuteReduceTask runs the reduce function on all intermediate files for this reduce partition and writes the output.
func (w *Worker) ExecuteReduceTask(reduceTaskNum, nMap int, reduceF func(string, []string) string) error {
	kvs := make(map[string][]string)

	// Read all intermediate files for this reduce partition
	for m := 0; m < nMap; m++ {
		fname := fmt.Sprintf("mr-%d-%d", m, reduceTaskNum)
		f, err := os.Open(fname)
		if err != nil {
			if os.IsNotExist(err) {
				continue // skip missing files
			}
			return fmt.Errorf("failed to open intermediate file %s: %v", fname, err)
		}
		dec := json.NewDecoder(f)
		for {
			var kv common.KeyValue
			if err := dec.Decode(&kv); err == io.EOF {
				break
			} else if err != nil {
				f.Close()
				return fmt.Errorf("failed to decode kv from %s: %v", fname, err)
			}
			kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
		}
		f.Close()
	}

	// Sort keys for deterministic output
	var keys []string
	for k := range kvs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Write output
	outF, err := os.Create(fmt.Sprintf("mr-out-%d", reduceTaskNum))
	if err != nil {
		return fmt.Errorf("failed to create output file: %v", err)
	}
	defer outF.Close()

	for _, k := range keys {
		output := reduceF(k, kvs[k])
		fmt.Fprintf(outF, "%s %s\n", k, output)
	}
	return nil
}
