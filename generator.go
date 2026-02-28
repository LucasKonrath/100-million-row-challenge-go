package main

import (
	"bufio"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
)

const (
	domain   = "https://stitcher.io"
	bufSize  = 8 * 1024 * 1024
)

var paths = []string{
	"/blog/php-enums",
	"/blog/11-million-rows-in-seconds",
	"/blog/laravel-beyond-crud",
	"/blog/php-81-enums",
	"/blog/a-project-at-stitcher",
	"/blog/php-what-i-dont-like",
	"/blog/new-in-php-81",
	"/blog/new-in-82",
	"/blog/new-in-php-83",
	"/blog/new-in-php-84",
	"/blog/generics-in-php",
	"/blog/readonly-classes-in-php-82",
	"/blog/fibers-with-a-grain-of-salt",
	"/blog/php-enum-style-guide",
	"/blog/constructor-promotion-in-php-8",
	"/blog/php-match-or-switch",
	"/blog/named-arguments-in-php-80",
	"/blog/php-enums-and-static-analysis",
	"/blog/short-closures-in-php",
	"/blog/attributes-in-php-8",
	"/blog/typed-properties-in-php-74",
	"/blog/a-letter-to-the-php-community",
	"/blog/union-types-in-php-80",
	"/blog/what-is-new-in-php",
	"/blog/readonly-properties-in-php-82",
	"/blog/nullsafe-operator-in-php",
	"/blog/php-deprecations-84",
	"/blog/property-hooks-in-php-84",
	"/blog/asymmetric-visibility-in-php-84",
	"/blog/crafting-quality-code",
	"/blog/object-oriented-programming",
	"/blog/design-patterns-explained",
	"/blog/functional-programming-in-php",
	"/blog/testing-best-practices",
	"/blog/clean-architecture",
	"/blog/domain-driven-design",
	"/blog/event-sourcing-patterns",
	"/blog/cqrs-explained",
	"/blog/microservices-patterns",
	"/blog/api-design-principles",
	"/blog/rest-vs-graphql",
	"/blog/database-optimization",
	"/blog/caching-strategies",
	"/blog/message-queues-explained",
	"/blog/security-best-practices",
	"/blog/ci-cd-pipelines",
	"/blog/docker-for-developers",
	"/blog/kubernetes-basics",
	"/blog/serverless-architecture",
	"/blog/web-performance-tips",
	"/blog/frontend-frameworks-comparison",
}

var years = []int{2024, 2025, 2026}

func generate(path string, count int) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriterSize(file, bufSize)
	defer writer.Flush()

	numThreads := runtime.NumCPU()
	linesPerThread := count / numThreads

	var wg sync.WaitGroup
	dataChan := make(chan string, numThreads)

	for i := 0; i < numThreads; i++ {
		wg.Add(1)
		go func(threadID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(threadID)))
			var output strings.Builder
			// Pre-allocate memory for the output string. Average line length is around 120 chars.
			output.Grow(linesPerThread * 120)
			for j := 0; j < linesPerThread; j++ {
				pathIdx := rng.Intn(len(paths))
				year := years[rng.Intn(len(years))]
				month := rng.Intn(12) + 1
				day := rng.Intn(28) + 1
				hour := rng.Intn(24)
				minute := rng.Intn(60)
				second := rng.Intn(60)

				line := fmt.Sprintf("%s%s,%04d-%02d-%02dT%02d:%02d:%02d+00:00\n",
					domain,
					paths[pathIdx],
					year,

					month,
					day,
					hour,
					minute,
					second,
				)
				output.WriteString(line)
			}
			dataChan <- output.String()
		}(i)
	}

	go func() {
		wg.Wait()
		close(dataChan)
	}()

	for data := range dataChan {
		if _, err := writer.WriteString(data); err != nil {
			return err
		}
	}

	fmt.Printf("Generated %d rows to %s\n", count, path)
	return nil
}
