package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"unsafe"
)

const (
	// FNV-1 64-bit constants from hash/fnv
	offset64 = 14695981039346656037
	prime64  = 1099511628211
	// Power of 2 map size
	numBuckets = 1 << 16
)

type MapItem struct {
	path string
	date string
	count int
}

type Result struct {
	items []MapItem
}

func newResult() *Result {
	return &Result{
		items: make([]MapItem, numBuckets),
	}
}

func (r *Result) add(path string, date string) {
	hash := uint64(offset64)
	for i := 0; i < len(path); i++ {
		hash ^= uint64(path[i])
		hash *= prime64
	}
	for i := 0; i < len(date); i++ {
		hash ^= uint64(date[i])
		hash *= prime64
	}

	index := int(hash & uint64(numBuckets-1))
	for {
		if r.items[index].path == "" {
			r.items[index].path = path
			r.items[index].date = date
			r.items[index].count = 1
			return
		}
		if r.items[index].path == path && r.items[index].date == date {
			r.items[index].count++
			return
		}
		index++
		if index >= numBuckets {
			index = 0
		}
	}
}

func process(allocator *sync.Pool, inputPath, outputPath string) error {
	file, err := os.Open(inputPath)
	if err != nil {
		return err
	}
	defer file.Close()

	mmapData, err := mmap(file)
	if err != nil {
		return err
	}
	defer munmap(mmapData)

	numThreads := runtime.NumCPU()
	chunkSize := len(mmapData) / numThreads

	var boundaries []int
	boundaries = append(boundaries, 0)

	for i := 1; i < numThreads; i++ {
		pos := i * chunkSize
		for pos < len(mmapData) && mmapData[pos] != '\n' {
			pos++
		}
		if pos < len(mmapData) {
			pos++
		}
		boundaries = append(boundaries, pos)
	}
	boundaries = append(boundaries, len(mmapData))

	var wg sync.WaitGroup
	results := make(chan *Result, numThreads)

	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		go func(chunk []byte) {
			defer wg.Done()
			results <- processChunk(allocator, chunk)
		}(mmapData[boundaries[i]:boundaries[i+1]])
	}

	wg.Wait()
	close(results)

	merged := allocator.Get().(*Result)
	defer allocator.Put(merged)

	for result := range results {
		mergeMaps(merged, result)
		// Clear the map before putting it back to the pool
		for i := range result.items {
			result.items[i].path = ""
		}
		allocator.Put(result)
	}

	// Count unique paths for logging
	uniquePaths := make(map[string]bool)
	for _, item := range merged.items {
		if item.path != "" {
			uniquePaths[item.path] = true
		}
	}

	fmt.Printf("Processed %d unique paths to %s\n", len(uniquePaths), outputPath)

	jsonData, err := formatJSONConcurrently(merged)
	if err != nil {
		return err
	}

	return os.WriteFile(outputPath, jsonData, 0644)
}

func processChunk(allocator *sync.Pool, chunk []byte) *Result {
	result := allocator.Get().(*Result)

	start := 0
	for start < len(chunk) {
		end := bytes.IndexByte(chunk[start:], '\n')
		if end == -1 {
			break
		}

		// Line format: https://stitcher.io<path>,YYYY-MM-DDTHH:MM:SS+00:00\n
		// "https://stitcher.io" is 19 bytes.
		// "YYYY-MM-DDTHH:MM:SS+00:00" is 25 bytes.
		// "," is 1 byte.
		// Total fixed bytes per line = 19 (url prefix) + 1 (comma) + 25 (date) = 45 bytes
		
		if end < 45 {
			start += end + 1
			continue
		}

		pathLen := end - 45
		
		pathBytes := chunk[start+19 : start+19+pathLen]
		dateBytes := chunk[start+end-25 : start+end-15]

		path := unsafe.String(unsafe.SliceData(pathBytes), pathLen)
		date := unsafe.String(unsafe.SliceData(dateBytes), 10)

		result.add(path, date)
		
		start += end + 1
	}
	return result
}

func mergeMaps(dest, src *Result) {
	for _, item := range src.items {
		if item.path != "" {
			// Fast path for merging: directly add multiple counts
			
			hash := uint64(offset64)
			for i := 0; i < len(item.path); i++ {
				hash ^= uint64(item.path[i])
				hash *= prime64
			}
			for i := 0; i < len(item.date); i++ {
				hash ^= uint64(item.date[i])
				hash *= prime64
			}

			index := int(hash & uint64(numBuckets-1))
			for {
				if dest.items[index].path == "" {
					dest.items[index].path = item.path
					dest.items[index].date = item.date
					dest.items[index].count = item.count
					break
				}
				if dest.items[index].path == item.path && dest.items[index].date == item.date {
					dest.items[index].count += item.count
					break
				}
				index++
				if index >= numBuckets {
					index = 0
				}
			}
		}
	}
}

func formatJSONConcurrently(result *Result) ([]byte, error) {
	// Rebuild a standard map structure to use the existing JSON formatting logic easily
	standardResult := make(map[string]map[string]int)
	for _, item := range result.items {
		if item.path != "" {
			if _, ok := standardResult[item.path]; !ok {
				standardResult[item.path] = make(map[string]int)
			}
			standardResult[item.path][item.date] += item.count
		}
	}

	paths := make([]string, 0, len(standardResult))
	for path := range standardResult {
		paths = append(paths, path)
	}
	sort.Strings(paths)

	var wg sync.WaitGroup
	jsonParts := make(chan string, len(paths))

	for _, path := range paths {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			var builder strings.Builder
			builder.WriteString(fmt.Sprintf("    \"%s\": {\n", strings.ReplaceAll(p, "/", "\\/")))

			dateMap := standardResult[p]
			dates := make([]string, 0, len(dateMap))
			for date := range dateMap {
				dates = append(dates, date)
			}
			sort.Strings(dates)

			for j, date := range dates {
				builder.WriteString(fmt.Sprintf("        \"%s\": %d", date, dateMap[date]))
				if j < len(dates)-1 {
					builder.WriteString(",\n")
				} else {
					builder.WriteString("\n")
				}
			}
			builder.WriteString("    }")
			jsonParts <- builder.String()
		}(path)
	}

	wg.Wait()
	close(jsonParts)

	var builder strings.Builder
	builder.WriteString("{\n")

	parts := make([]string, 0, len(paths))
	for part := range jsonParts {
		parts = append(parts, part)
	}

	for i, part := range parts {
		builder.WriteString(part)
		if i < len(parts)-1 {
			builder.WriteString(",\n")
		} else {
			builder.WriteString("\n")
		}
	}

	builder.WriteString("}\n")

	// Pretty print the JSON
	var prettyJSON bytes.Buffer
	if err := json.Indent(&prettyJSON, []byte(builder.String()), "", "    "); err != nil {
		// Fallback to the original string if indenting fails
		return []byte(builder.String()), nil
	}

	return prettyJSON.Bytes(), nil
}