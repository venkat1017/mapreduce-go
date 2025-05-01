package main

import (
	"flag"
	"fmt"
	"log"
	"mapreduce-go/internal/master"
	"net"
	"net/rpc"
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
	fmt.Println(ColorBlue, "\nMapReduce Go - Master Node\n", ColorReset)
}

func printUsage() {
	fmt.Println(ColorYellow, "Usage:", ColorReset)
	fmt.Println("  go run ./cmd/master --files <file1,file2,...> [--nmap N] [--nreduce N] [--addr :port]")
	fmt.Println(ColorCyan, "\nFlags:", ColorReset)
	fmt.Println("  --files     Comma-separated list of input files (or specify as arguments after flags)")
	fmt.Println("  --nmap      Number of map tasks (default 2)")
	fmt.Println("  --nreduce   Number of reduce tasks (default 2)")
	fmt.Println("  --addr      Address to listen for worker RPCs (default :1234)")
}

func main() {
	printBanner()
	if len(os.Args) > 1 && (os.Args[1] == "-h" || os.Args[1] == "--help") {
		printUsage()
		os.Exit(0)
	}
	filesFlag := flag.String("files", "", "Comma-separated list of input files (or specify as arguments after flags)")
	nMap := flag.Int("nmap", 2, "Number of map tasks")
	nReduce := flag.Int("nreduce", 2, "Number of reduce tasks")
	addr := flag.String("addr", ":1234", "Address to listen for worker RPCs")
	flag.Parse()

	files := []string{}
	if *filesFlag != "" {
		files = append(files, splitCommaSeparated(*filesFlag)...)
	}
	files = append(files, flag.Args()...)

	if len(files) == 0 {
		log.Fatal("You must specify input files with --files or as arguments")
	}

	m := master.NewMaster(files, *nMap, *nReduce)
	fmt.Printf("Master started: %+v\n", m)

	// Start RPC server
	rpc.Register(m)
	l, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", *addr, err)
	}
	fmt.Printf("Listening for workers at %s\n", *addr)
	for {
		conn, err := l.Accept()
		if err != nil {
			log.Printf("Accept error: %v", err)
			continue
		}
		go rpc.ServeConn(conn)
	}
}

func splitCommaSeparated(s string) []string {
	var out []string
	for _, part := range splitAndTrim(s, ',') {
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func splitAndTrim(s string, sep rune) []string {
	var res []string
	start := 0
	for i, c := range s {
		if c == sep {
			res = append(res, trimSpace(s[start:i]))
			start = i + 1
		}
	}
	res = append(res, trimSpace(s[start:]))
	return res
}

func trimSpace(s string) string {
	for len(s) > 0 && (s[0] == ' ' || s[0] == '\t') {
		s = s[1:]
	}
	for len(s) > 0 && (s[len(s)-1] == ' ' || s[len(s)-1] == '\t') {
		s = s[:len(s)-1]
	}
	return s
}
