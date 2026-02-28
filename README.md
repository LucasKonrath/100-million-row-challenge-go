# 100 Million Row Challenge - Go Solution

This project is a Go implementation of the 100 million row challenge.

## How to Run

### Prerequisites

- Go 1.18 or higher

### Steps

1. **Generate the data:**
   To generate the default 1 million rows:
   ```bash
   go run . generate
   ```
   To generate a specific number of rows (e.g., 100 million):
   ```bash
   go run . generate 100_000_000
   ```

2. **Process the data:**
   ```bash
   go run . process
   ```
   This will read the `data/measurements.txt` file and create `data/output.json` with the results.

## Optimizations

This implementation uses several optimizations to process the data efficiently:

- **Memory-Mapped Files:** The `mmap-go` library is used to map the input file into memory. This avoids the overhead of reading the file into memory and allows the OS to handle memory management.
- **Goroutines for Parallel Processing:** The file is divided into chunks, and each chunk is processed by a separate goroutine. This allows the processing to be parallelized across multiple CPU cores.
- **`sync.Pool` for Reduced Allocations:** A `sync.Pool` is used to reuse `Result` maps, which reduces the number of memory allocations and the garbage collector's workload.
- **Concurrent JSON Formatting:** The final JSON output is formatted concurrently. Each path's JSON representation is generated in a separate goroutine, and the results are combined at the end.
- **Efficient String Concatenation:** The `strings.Builder` is used to efficiently build strings in the data generation process, avoiding the performance overhead of repeated string concatenation.
