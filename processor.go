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

	"github.com/edsrzf/mmap-go"
)

type Result map[string]map[string]int

func process(allocator *sync.Pool, inputPath, outputPath string) error {
	file, err := os.Open(inputPath)
	if err != nil {
		return err
	}
	defer file.Close()

	mmap, err := mmap.Map(file, mmap.RDONLY, 0)
	if err != nil {
		return err
	}
	defer mmap.Unmap()

	numThreads := runtime.NumCPU()
	chunkSize := len(mmap) / numThreads

	var boundaries []int
	boundaries = append(boundaries, 0)

	for i := 1; i < numThreads; i++ {
		pos := i * chunkSize
		for pos < len(mmap) && mmap[pos] != '\n' {
			pos++
		}
		if pos < len(mmap) {
			pos++
		}
		boundaries = append(boundaries, pos)
	}
	boundaries = append(boundaries, len(mmap))

	var wg sync.WaitGroup
	results := make(chan Result, numThreads)

	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		go func(chunk []byte) {
			defer wg.Done()
			results <- processChunk(allocator, chunk)
		}(mmap[boundaries[i]:boundaries[i+1]])
	}

	wg.Wait()
	close(results)

	merged := allocator.Get().(Result)
	defer allocator.Put(merged)

	for result := range results {
		mergeMaps(merged, result)
		// Clear the map before putting it back to the pool
		for k := range result {
			delete(result, k)
		}
		allocator.Put(result)
	}

	fmt.Printf("Processed %d unique paths to %s\n", len(merged), outputPath)

	jsonData, err := formatJSONConcurrently(merged)
	if err != nil {
		return err
	}

	return os.WriteFile(outputPath, jsonData, 0644)
}

func processChunk(allocator *sync.Pool, chunk []byte) Result {
	result := allocator.Get().(Result)
	start := 0
	for start < len(chunk) {
		end := bytes.IndexByte(chunk[start:], '\n')
		var line []byte
		if end == -1 {
			line = chunk[start:]
			start = len(chunk)
		} else {
			line = chunk[start : start+end]
			start += end + 1
		}

		if len(line) == 0 {
			continue
		}

		path, date, ok := parseLine(line)
		if !ok {
			continue
		}

		if _, ok := result[path]; !ok {
			result[path] = make(map[string]int)
		}
		result[path][date]++
	}
	return result
}

func parseLine(line []byte) (string, string, bool) {
	commaPos := bytes.LastIndexByte(line, ',')
	if commaPos == -1 {
		return "", "", false
	}

	url := line[:commaPos]
	datetime := line[commaPos+1:]

	schemeEnd := bytes.Index(url, []byte("://"))
	if schemeEnd == -1 {
		return "", "", false
	}

	afterScheme := url[schemeEnd+3:]
	pathStart := bytes.IndexByte(afterScheme, '/')
	if pathStart == -1 {
		return "", "", false
	}

	path := afterScheme[pathStart:]

	if len(datetime) < 10 {
		return "", "", false
	}

	return string(path), string(datetime[:10]), true
}

func mergeMaps(dest, src Result) {
	for path, dateMap := range src {
		if _, ok := dest[path]; !ok {
			dest[path] = make(map[string]int)
		}
		for date, count := range dateMap {
			dest[path][date] += count
		}
	}
}

func formatJSONConcurrently(result Result) ([]byte, error) {
	paths := make([]string, 0, len(result))
	for path := range result {
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

			dateMap := result[p]
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