# MapReduce in Go

[![Go Version](https://img.shields.io/badge/go-1.20%2B-blue)](https://golang.org/) 
[![License](https://img.shields.io/badge/license-MIT-green)](./LICENSE) 
[![Build](https://img.shields.io/badge/build-passing-brightgreen)]() 
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-blue)](./CONTRIBUTING.md)

> **A modern, demo-ready, distributed MapReduce framework in Go.**  
> Inspired by the original [Google MapReduce paper](https://research.google/pubs/pub62/).

---

## ✨ Features

- **Single-process and distributed (master/worker) execution**
- **Pluggable jobs:** Word Count, Inverted Index, Top-N Words, and more
- **Fault tolerance:** Task timeouts and reassignment
- **Colorful, user-friendly CLI** (no external dependencies)
- **Multiple input files, flexible output**
- **Easy extensibility:** Add your own map/reduce jobs in minutes

---

## 📦 Project Structure

```
mapreduce-go/
├── cmd/           # CLI entry points
│   ├── single/    # Single-process runner
│   ├── master/    # Distributed master
│   └── worker/    # Distributed worker
├── examples/      # Example map/reduce jobs
├── internal/      # Core logic and RPC
├── testdata/      # Sample input files
└── go.mod         # Go module definition
```

---

## 🚀 Quick Start

### 1. **Clone and Build**
```sh
git clone https://github.com/yourusername/mapreduce-go.git
cd mapreduce-go
go mod tidy
```

### 2. **Prepare Input Data**
- For word count/topn:  
  ```
  hello world
  hello mapreduce
  this is a test
  mapreduce is fun
  ```
- For inverted index:  
  ```
  doc1: hello world
  doc2: hello mapreduce
  doc3: world of mapreduce
  ```

---

## 🏃 Usage

### **Single-Process Mode**
```sh
go run ./cmd/single --job wordcount testdata/sample.txt
go run ./cmd/single --job invertedindex testdata/sample.txt
go run ./cmd/single --job topn testdata/sample.txt
go run ./cmd/single --job wordcount --output result.txt testdata/sample.txt
go run ./cmd/single --job wordcount file1.txt file2.txt
```

### **Distributed Mode**
**Terminal 1 (Master):**
```sh
go run ./cmd/master --files testdata/sample.txt --nmap 2 --nreduce 2
```
**Terminal 2+ (Workers):**
```sh
go run ./cmd/worker --job wordcount
go run ./cmd/worker --job invertedindex
go run ./cmd/worker --job topn
```

---

## 🛠️ Jobs & Input Formats

| Job            | Description                        | Input Example                                      |
|----------------|------------------------------------|----------------------------------------------------|
| wordcount      | Count word frequencies             | `hello world`<br>`hello mapreduce`                 |
| invertedindex  | List docs for each word            | `doc1: hello world`<br>`doc2: hello mapreduce`     |
| topn           | Top N most frequent words (N=3)    | `hello world`<br>`hello mapreduce`                 |

---

## 🎨 CLI & Help

All commands support `-h` or `--help` for colorful, detailed usage info.

---

## 🧩 Extending: Add Your Own Job

1. Add your map/reduce functions to `examples/`.
2. Register them in `examples/registry.go`:
   ```go
   var Jobs = map[string]Job{
       "myjob": {Map: MyMapFunc, Reduce: MyReduceFunc},
       // ...
   }
   ```
3. Run with `--job myjob`.

---

## 🛡️ Advanced Features

- Fault-tolerant task assignment and reassignment
- Worker backoff/retry
- Graceful master shutdown
- Robust error handling and exit codes
- Colorful CLI banners and output

## ⚡ Combiner Optimization

A **combiner** is a mini-reduce function that runs after the map phase, before shuffling data to reducers. It aggregates duplicate keys locally on each map worker, reducing the amount of intermediate data written and transferred.

**How it works in this implementation:**
- After the map function runs for all lines in a map task, the worker groups key-value pairs by key.
- For jobs like word count, the combiner sums the counts for each word before writing to intermediate files.
- This means each word appears only once per map task per partition, with its local count.

**Example (Word Count):**

Suppose a map task processes these lines:
```
hello world
hello mapreduce
hello world
```

**Without combiner, intermediate output:**
```
{"Key":"hello","Value":"1"}
{"Key":"world","Value":"1"}
{"Key":"hello","Value":"1"}
{"Key":"mapreduce","Value":"1"}
{"Key":"hello","Value":"1"}
{"Key":"world","Value":"1"}
```

**With combiner, intermediate output:**
```
{"Key":"hello","Value":"3"}
{"Key":"world","Value":"2"}
{"Key":"mapreduce","Value":"1"}
```

This reduces disk and network I/O, making your MapReduce jobs faster and more scalable!

---

## 🤝 Contributing

We welcome contributions! To get started:

1. Fork this repository and clone your fork.
2. Create a new branch for your feature or bugfix.
3. Make your changes and add tests if applicable.
4. Open a pull request with a clear description of your changes.

**Guidelines:**
- Please keep code style consistent with the project.
- Add or update documentation as needed.
- For major changes, open an issue first to discuss your idea.

---

## ❓ FAQ

**Q: Can I use this for large-scale production workloads?**  
A: This project is a learning/teaching implementation, but the architecture is scalable. For true production, consider integrating with a distributed file system and adding more robust monitoring and security.

**Q: How do I add a new MapReduce job?**  
A: See the [Extending](#-extending-add-your-own-job) section above.

**Q: Does it work on Windows, Mac, and Linux?**  
A: Yes! It's pure Go and works cross-platform.

**Q: Can I run with more than one worker?**  
A: Yes! Start as many workers as you like, even after the master has started.

**Q: How do I see colored output?**  
A: Most modern terminals support ANSI colors. If you don't see color, try a different terminal or check your settings.

---

## 📝 Credits

- Inspired by the original [MapReduce paper by Google](https://research.google/pubs/pub62/)
- CLI color via ANSI codes (no external dependencies)

---

## 💬 License

MIT (or your preferred license)

---

**Happy hacking! 🚀** 
