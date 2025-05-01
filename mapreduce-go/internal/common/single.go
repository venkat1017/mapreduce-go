package common

import (
	"hash/fnv"
	"sort"
)

// MapFunc defines the type for the map function.
type MapFunc func(string) []KeyValue

// ReduceFunc defines the type for the reduce function.
type ReduceFunc func(string, []string) string

// KeyValue is a type used for intermediate key-value pairs.
type KeyValue struct {
	Key   string
	Value string
}

// SortBy defines the sorting method for the results.
type SortBy int

const (
	SortByKey SortBy = iota
	SortByValue
)

// RunSingleProcessMapReduce runs the MapReduce job in a single process.
func RunSingleProcessMapReduce(
	input []string,
	mapF MapFunc,
	reduceF ReduceFunc,
	sortBy SortBy,
	nMap int,
	nReduce int,
) []KeyValue {
	// Split input into nMap chunks
	chunks := make([][]string, nMap)
	for i, line := range input {
		chunks[i%nMap] = append(chunks[i%nMap], line)
	}

	// Map phase: process each chunk
	intermediate := []KeyValue{}
	for _, chunk := range chunks {
		for _, line := range chunk {
			kvs := mapF(line)
			intermediate = append(intermediate, kvs...)
		}
	}

	// Partition intermediate key-value pairs into nReduce buckets by key hash
	buckets := make([][]KeyValue, nReduce)
	for _, kv := range intermediate {
		bucket := ihash(kv.Key) % nReduce
		buckets[bucket] = append(buckets[bucket], kv)
	}

	// Reduce phase: for each bucket, group by key and apply reduceF
	results := []KeyValue{}
	for _, bucket := range buckets {
		groups := make(map[string][]string)
		for _, kv := range bucket {
			groups[kv.Key] = append(groups[kv.Key], kv.Value)
		}
		for key, values := range groups {
			output := reduceF(key, values)
			results = append(results, KeyValue{Key: key, Value: output})
		}
	}

	// Sort results
	switch sortBy {
	case SortByKey:
		sort.Slice(results, func(i, j int) bool {
			return results[i].Key < results[j].Key
		})
	case SortByValue:
		sort.Slice(results, func(i, j int) bool {
			return results[i].Value < results[j].Value
		})
	}

	return results
}

// ihash hashes a string to an int (for partitioning keys)
func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}
