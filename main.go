package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("Usage: %s <generate|process> [options]\n", os.Args[0])
		fmt.Println("  generate [count]  - Generate test data (default: 1000000)")
		fmt.Println("  process           - Process measurements.txt -> output.json")
		os.Exit(1)
	}

	dataDir := "data"
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		fmt.Printf("Failed to create data directory: %v\n", err)
		os.Exit(1)
	}

	inputFile := dataDir + "/measurements.txt"
	outputFile := dataDir + "/output.json"

	switch os.Args[1] {
	case "generate":
		count := 1_000_000
		if len(os.Args) > 2 {
			cleaned := strings.ReplaceAll(os.Args[2], "_", "")
			if parsedCount, err := strconv.Atoi(cleaned); err == nil {
				count = parsedCount
			}
		}
		if err := generate(inputFile, count); err != nil {
			fmt.Printf("Error generating data: %v\n", err)
			os.Exit(1)
		}
	case "process":
		allocator := &sync.Pool{
			New: func() interface{} {
				return make(Result)
			},
		}
		start := time.Now()
		if err := process(allocator, inputFile, outputFile); err != nil {
			fmt.Printf("Error processing data: %v\n", err)
			os.Exit(1)
		}
		elapsed := time.Since(start)
		fmt.Printf("Completed in %s\n", elapsed)
	default:
		fmt.Printf("Unknown command: %s\n", os.Args[1])
		os.Exit(1)
	}
}
