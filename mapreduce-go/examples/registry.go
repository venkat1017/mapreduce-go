package examples

import (
	"mapreduce-go/internal/common"
	"sort"
	"strconv"
	"strings"
)

type MapFunc = func(string) []common.KeyValue

type ReduceFunc = func(string, []string) string

type Job struct {
	Map    MapFunc
	Reduce ReduceFunc
}

// InvertedIndexMap emits (word, document) pairs for each word in the line, assuming the line is formatted as "document: text..."
func InvertedIndexMap(line string) []common.KeyValue {
	parts := strings.SplitN(line, ":", 2)
	if len(parts) != 2 {
		return nil
	}
	doc := strings.TrimSpace(parts[0])
	words := strings.Fields(parts[1])
	kvs := make([]common.KeyValue, 0, len(words))
	for _, word := range words {
		kvs = append(kvs, common.KeyValue{Key: word, Value: doc})
	}
	return kvs
}

// InvertedIndexReduce emits a comma-separated list of unique documents for each word
func InvertedIndexReduce(key string, values []string) string {
	docSet := make(map[string]struct{})
	for _, doc := range values {
		docSet[doc] = struct{}{}
	}
	docs := make([]string, 0, len(docSet))
	for doc := range docSet {
		docs = append(docs, doc)
	}
	return strings.Join(docs, ",")
}

// TopNWordsMap emits (word, 1) for each word in the line
func TopNWordsMap(line string) []common.KeyValue {
	words := strings.Fields(line)
	kvs := make([]common.KeyValue, 0, len(words))
	for _, word := range words {
		kvs = append(kvs, common.KeyValue{Key: word, Value: "1"})
	}
	return kvs
}

// TopNWordsReduce emits the top N words by frequency (N=3 for this example)
func TopNWordsReduce(key string, values []string) string {
	// This function just returns the count for each word; the actual top-N selection is done in a final aggregation step.
	return strconv.Itoa(len(values))
}

// TopNFinal aggregates all word counts and returns the top N as a string
func TopNFinal(results []common.KeyValue, N int) string {
	type wordCount struct {
		Word  string
		Count int
	}
	var wcList []wordCount
	for _, kv := range results {
		count, _ := strconv.Atoi(kv.Value)
		wcList = append(wcList, wordCount{Word: kv.Key, Count: count})
	}
	sort.Slice(wcList, func(i, j int) bool {
		return wcList[i].Count > wcList[j].Count
	})
	out := ""
	for i := 0; i < N && i < len(wcList); i++ {
		out += wcList[i].Word + ": " + strconv.Itoa(wcList[i].Count) + "\n"
	}
	return out
}

var Jobs = map[string]Job{
	"wordcount": {
		Map:    WordCountMap,
		Reduce: WordCountReduce,
	},
	"invertedindex": {
		Map:    InvertedIndexMap,
		Reduce: InvertedIndexReduce,
	},
	"topn": {
		Map:    TopNWordsMap,
		Reduce: TopNWordsReduce,
	},
	// Add more jobs here (e.g., "invertedindex": {...})
}
