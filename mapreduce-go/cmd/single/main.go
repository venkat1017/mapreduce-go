package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"mapreduce-go/examples"
	"mapreduce-go/internal/common"
	"os"
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
	fmt.Println(ColorBlue, "\nMapReduce Go - Distributed Data Processing\n", ColorReset)
}

func printUsage() {
	fmt.Println(ColorYellow, "Usage:", ColorReset)
	fmt.Println("  go run ./cmd/single --job <job> [--output <file>] <input1> [input2 ...]")
	fmt.Println(ColorCyan, "\nAvailable jobs: wordcount, invertedindex, topn", ColorReset)
	fmt.Println("  --job        Job to run (wordcount, invertedindex, topn)")
	fmt.Println("  --output     Output file (optional)")
	fmt.Println("  --nmap       Number of map tasks (default 2)")
	fmt.Println("  --nreduce    Number of reduce tasks (default 2)")
	fmt.Println("  --sort       Sort output by 'key' or 'value' (default 'key')")
}

func main() {
	printBanner()
	if len(os.Args) > 1 && (os.Args[1] == "-h" || os.Args[1] == "--help") {
		printUsage()
		os.Exit(0)
	}
	// Add flags for sorting method, nMap, nReduce, job, and output
	sortBy := flag.String("sort", "key", "Sort output by 'key' or 'value'")
	nMap := flag.Int("nmap", 2, "Number of map tasks")
	nReduce := flag.Int("nreduce", 2, "Number of reduce tasks")
	jobName := flag.String("job", "wordcount", "Job to run (e.g., wordcount, invertedindex)")
	outputFile := flag.String("output", "", "Output file (default: print to console)")
	flag.Parse()

	job, ok := examples.Jobs[*jobName]
	if !ok {
		log.Fatalf("Unknown job: %s", *jobName)
	}

	// Support multiple input files
	inputFiles := flag.Args()
	if len(inputFiles) == 0 {
		log.Fatalf("Please specify at least one input file")
	}

	var lines []string
	for _, fname := range inputFiles {
		file, err := os.Open(fname)
		if err != nil {
			log.Fatalf("failed to open input file %s: %v", fname, err)
		}
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			lines = append(lines, scanner.Text())
		}
		if err := scanner.Err(); err != nil {
			log.Fatalf("failed to read input file %s: %v", fname, err)
		}
		file.Close()
	}

	// Run MapReduce
	var sortMethod common.SortBy
	switch *sortBy {
	case "value":
		sortMethod = common.SortByValue
	default:
		sortMethod = common.SortByKey
	}
	results := common.RunSingleProcessMapReduce(
		lines,
		job.Map,
		job.Reduce,
		sortMethod,
		*nMap,
		*nReduce,
	)

	// Output results
	if *outputFile != "" {
		f, err := os.Create(*outputFile)
		if err != nil {
			log.Fatalf("failed to create output file: %v", err)
		}
		defer f.Close()
		for _, kv := range results {
			fmt.Fprintf(f, "%s: %s\n", kv.Key, kv.Value)
		}
	} else {
		for _, kv := range results {
			fmt.Printf("%s: %s\n", kv.Key, kv.Value)
		}
	}

	// If job is topn, print the top-N results
	if *jobName == "topn" {
		fmt.Println("\nTop N Results:")
		fmt.Print(examples.TopNFinal(results, 3))
	}
}
