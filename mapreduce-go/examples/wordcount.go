package examples

import (
	"mapreduce-go/internal/common"
	"strconv"
	"strings"
)

// WordCountMap is the map function for word count.
func WordCountMap(line string) []common.KeyValue {
	words := strings.Fields(line)
	kvs := make([]common.KeyValue, 0, len(words))
	for _, word := range words {
		kvs = append(kvs, common.KeyValue{Key: word, Value: "1"})
	}
	return kvs
}

// WordCountReduce is the reduce function for word count.
func WordCountReduce(key string, values []string) string {
	return strconv.Itoa(len(values))
}
