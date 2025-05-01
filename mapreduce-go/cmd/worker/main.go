package main

import (
	"flag"
	"fmt"
	"mapreduce-go/examples"
	"mapreduce-go/internal/worker"
	"os"
	"time"
)

const (
	ColorReset  = "\033[0m"
	ColorRed    = "\033[31m"
	ColorGreen  = "\033[32m"
	ColorYellow = "\033[33m"
	ColorBlue   = "\033[34m"
	ColorCyan   = "\033[36m"
	ColorWhite  = "\033[37m"
)

func printError(msg string) {
	fmt.Println(ColorRed, "Error:", msg, ColorReset)
}

func printSuccess(msg string) {
	fmt.Println(ColorGreen, msg, ColorReset)
}

func printInfo(msg string) {
	fmt.Println(ColorCyan, msg, ColorReset)
}

func printBanner() {
	fmt.Println(ColorBlue, "\nMapReduce Go - Worker Node\n", ColorReset)
}

func printUsage() {
	fmt.Println(ColorYellow, "Usage:", ColorReset)
	fmt.Println("  go run ./cmd/worker --job <job> [--id N] [--master host:port]")
	fmt.Println(ColorCyan, "\nFlags:", ColorReset)
	fmt.Println("  --job      Job to run (wordcount, invertedindex, topn)")
	fmt.Println("  --id       Worker ID (default 1)")
	fmt.Println("  --master   Master address (default localhost:1234)")
}

func main() {
	printBanner()
	if len(os.Args) > 1 && (os.Args[1] == "-h" || os.Args[1] == "--help") {
		printUsage()
		os.Exit(0)
	}

	id := flag.Int("id", 1, "Worker ID")
	masterAddr := flag.String("master", "localhost:1234", "Master address")
	jobName := flag.String("job", "wordcount", "Job to run (e.g., wordcount)")
	flag.Parse()

	job, ok := examples.Jobs[*jobName]
	if !ok {
		fmt.Printf("Unknown job: %s\n", *jobName)
		os.Exit(1)
	}

	w := worker.NewWorker(*id, *masterAddr)
	_ = w // Ensure 'w' is used to avoid linter error
	fmt.Printf("Worker %d started, connecting to master at %s, job: %s\n", *id, *masterAddr, *jobName)

	for {
		resp, err := w.RequestTask()
		if err != nil {
			fmt.Printf("Error requesting task: %v\n", err)
			break
		}
		switch resp.TaskType {
		case 0: // TaskTypeMap
			fmt.Printf("Worker %d received MAP task #%d, file: %s\n", w.ID, resp.TaskNumber, resp.InputFile)
			err := w.ExecuteMapTask(resp.InputFile, resp.TaskNumber, resp.NReduce, job.Map)
			if err != nil {
				fmt.Printf("Worker %d: error executing MAP task #%d: %v\n", w.ID, resp.TaskNumber, err)
			} else {
				fmt.Printf("Worker %d completed MAP task #%d\n", w.ID, resp.TaskNumber)
				w.ReportTaskComplete(0, resp.TaskNumber)
			}
		case 1: // TaskTypeReduce
			fmt.Printf("Worker %d received REDUCE task #%d\n", w.ID, resp.TaskNumber)
			err := w.ExecuteReduceTask(resp.TaskNumber, resp.NMap, job.Reduce)
			if err != nil {
				fmt.Printf("Worker %d: error executing REDUCE task #%d: %v\n", w.ID, resp.TaskNumber, err)
			} else {
				fmt.Printf("Worker %d completed REDUCE task #%d\n", w.ID, resp.TaskNumber)
				w.ReportTaskComplete(1, resp.TaskNumber)
			}
		case 2: // TaskTypeWait
			fmt.Printf("Worker %d: no task available, waiting...\n", w.ID)
			time.Sleep(time.Second)
			continue
		case 3: // TaskTypeExit
			fmt.Printf("Worker %d: received exit signal, shutting down.\n", w.ID)
			return
		}
	}
}
