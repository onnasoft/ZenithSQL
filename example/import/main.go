package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/onnasoft/ZenithSQL/core/executor"
	"github.com/onnasoft/ZenithSQL/core/storage"
	"github.com/onnasoft/ZenithSQL/io/statement"
	"github.com/onnasoft/ZenithSQL/model/catalog"
	"github.com/onnasoft/ZenithSQL/model/fields"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
)

const (
	delimiter        = '\n'
	separator        = ';'
	batchSize        = 1_000_000
	progressInterval = 1 * time.Second
	maxWorkers       = 16
)

type ImportStats struct {
	totalBytes     int64
	bytesProcessed int64
	processedRows  int64
	failedRows     int64
	startTime      time.Time
	lastBytes      int64
	lastTime       time.Time
	speed          float64
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run main.go <file_path>")
		os.Exit(1)
	}

	startTime := time.Now()
	filePath := os.Args[1]

	catalog, err := catalog.OpenCatalog(&catalog.CatalogConfig{
		Path:   "./data",
		Logger: logrus.New(),
	})
	if err != nil {
		log.Fatalf("error opening catalog: %v", err)
	}
	defer catalog.Close()

	_, err = catalog.CreateDatabase("testdb")
	if err != nil {
		log.Fatalf("error creating database: %v", err)
	}

	db, err := catalog.GetDatabase("testdb")
	if err != nil {
		log.Fatalf("error opening database: %v", err)
	}

	schema, err := db.CreateSchema("public")
	if err != nil {
		log.Fatalf("error creating schema: %v", err)
	}

	table, err := schema.CreateTable("temperatures", &storage.TableConfig{
		Fields: []fields.FieldMeta{
			{
				Name:   "city",
				Type:   fields.String,
				Length: 100,
			},
			{
				Name:   "temperature",
				Type:   fields.Float64,
				Length: 8,
			},
		},
	})
	if err != nil {
		log.Println("table already exists, using existing table")
		table, err = schema.GetTable("temperatures")
		if err != nil {
			log.Fatalf("error getting table: %v", err)
		}

		executor := executor.New(catalog)
		stmt, err := statement.NewTruncateTableStatement("testdb", "public", "temperatures")
		if err != nil {
			log.Fatalf("error creating truncate statement: %v", err)
		}
		if response := executor.Execute(context.Background(), stmt); !response.IsSuccess() {
			log.Fatalf("error truncating table: %v", response.GetMessage())
		}
	}

	table.LockImport()
	defer table.UnlockImport()

	stats := &ImportStats{startTime: startTime, lastTime: startTime}
	if err := importFileConcurrent(filePath, catalog, stats); err != nil {
		log.Fatalf("import failed: %v", err)
	}

	showFinalStats(stats)
}

func importFileConcurrent(filePath string, catalog *catalog.Catalog, stats *ImportStats) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("opening file: %w", err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("getting file info: %w", err)
	}
	stats.totalBytes = info.Size()

	data, err := unix.Mmap(int(file.Fd()), 0, int(stats.totalBytes), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("mmap failed: %w", err)
	}
	defer func() {
		if err := unix.Munmap(data); err != nil {
			log.Printf("warning: error unmapping file: %v", err)
		}
	}()

	numWorkers := runtime.NumCPU()
	if numWorkers > maxWorkers {
		numWorkers = maxWorkers
	}

	lines := make(chan []byte, numWorkers*2)
	errors := make(chan error, numWorkers)
	done := make(chan struct{})
	var hasErrors atomic.Bool

	var workersWg sync.WaitGroup
	var tickerWg sync.WaitGroup

	// Start worker goroutines
	for i := 0; i < numWorkers; i++ {
		workersWg.Add(1)
		go func() {
			defer workersWg.Done()
			var batch []map[string]interface{}
			executor := executor.New(catalog)

			for line := range lines {
				row, err := parseLine(line)
				if err != nil {
					atomic.AddInt64(&stats.failedRows, 1)
					continue
				}
				batch = append(batch, row)

				if len(batch) >= batchSize {
					if err := processBatch(executor, batch, stats); err != nil {
						errors <- err
						hasErrors.Store(true)
						return
					}

					batch = batch[:0]
				}
			}

			// Process remaining batch
			if len(batch) > 0 && !hasErrors.Load() {
				if err := processBatch(executor, batch, stats); err != nil {
					errors <- err
					hasErrors.Store(true)
				}
			}
		}()
	}

	// Start progress reporter
	tickerWg.Add(1)
	go func() {
		defer tickerWg.Done()
		ticker := time.NewTicker(progressInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				reportProgress(stats)
			case <-done:
				reportProgress(stats)
				return
			}
		}
	}()

	// Start line reader
	go func() {
		defer close(lines)
		lineStart := 0
		for lineStart < len(data) && !hasErrors.Load() {
			lineEnd := bytes.IndexByte(data[lineStart:], delimiter)
			if lineEnd == -1 {
				break
			}
			line := data[lineStart : lineStart+lineEnd]
			select {
			case lines <- line:
				lineStart += lineEnd + 1
				atomic.AddInt64(&stats.bytesProcessed, int64(lineEnd+1))
			case <-done:
				return
			}
		}
	}()

	// Wait for workers to finish and close channels
	go func() {
		workersWg.Wait()
		close(errors)
		close(done)
	}()

	// Check for errors
	for err := range errors {
		if err != nil {
			return err
		}
	}

	tickerWg.Wait()
	return nil
}

func processBatch(executor executor.Executor, batch []map[string]interface{}, stats *ImportStats) error {
	stmt, err := statement.NewImportStatement("testdb", "public", "temperatures", batch)
	if err != nil {
		return fmt.Errorf("creating import statement: %w", err)
	}

	if response := executor.Execute(context.Background(), stmt); !response.IsSuccess() {
		atomic.AddInt64(&stats.failedRows, int64(len(batch)))
		return fmt.Errorf("import failed: %s", response.GetMessage())
	}

	atomic.AddInt64(&stats.processedRows, int64(len(batch)))
	return nil
}

func parseLine(line []byte) (map[string]interface{}, error) {
	parts := bytes.Split(line, []byte{separator})
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid format")
	}

	row := make(map[string]interface{})
	row["city"] = string(parts[0])

	temp, err := strconv.ParseFloat(string(parts[1]), 64)
	if err != nil {
		return nil, fmt.Errorf("invalid temperature value: %w", err)
	}
	row["temperature"] = temp

	return row, nil
}

func reportProgress(stats *ImportStats) {
	now := time.Now()
	bytesProcessed := atomic.LoadInt64(&stats.bytesProcessed)

	elapsed := now.Sub(stats.lastTime).Seconds()
	if elapsed > 0 {
		bytesDiff := bytesProcessed - stats.lastBytes
		stats.speed = (float64(bytesDiff) / (1024 * 1024)) / elapsed
		stats.lastBytes = bytesProcessed
		stats.lastTime = now
	}

	progress := 0.0
	if stats.totalBytes > 0 {
		progress = float64(bytesProcessed) / float64(stats.totalBytes) * 100
	}

	elapsedTotal := now.Sub(stats.startTime).Seconds()
	remaining := 0.0
	if progress > 0 && stats.speed > 0 {
		remaining = (float64(stats.totalBytes-bytesProcessed) / (1024 * 1024)) / stats.speed
	}

	processedMB := float64(bytesProcessed) / (1024 * 1024)
	totalMB := float64(stats.totalBytes) / (1024 * 1024)

	fmt.Printf("\rProgress: %.2f%% | %.2f/%.2f MB | Speed: %.2f MB/s | Processed Rows: %d | Failed Rows: %d | Elapsed: %s | Remaining: %s",
		progress,
		processedMB,
		totalMB,
		stats.speed,
		atomic.LoadInt64(&stats.processedRows),
		atomic.LoadInt64(&stats.failedRows),
		formatDuration(time.Duration(elapsedTotal)*time.Second),
		formatDuration(time.Duration(remaining)*time.Second))
}

func showFinalStats(stats *ImportStats) {
	elapsed := time.Since(stats.startTime)
	processedMB := float64(stats.bytesProcessed) / (1024 * 1024)
	avgSpeed := processedMB / elapsed.Seconds()

	fmt.Printf("\n\nImport completed successfully!\n")
	fmt.Printf("Total time: %s\n", formatDuration(elapsed))
	fmt.Printf("Total data processed: %.2f MB\n", processedMB)
	fmt.Printf("Average speed: %.2f MB/s\n", avgSpeed)
	fmt.Printf("Total rows processed: %d\n", stats.processedRows)
	fmt.Printf("Failed rows: %d\n", stats.failedRows)
}

func formatDuration(d time.Duration) string {
	d = d.Round(time.Second)
	h := d / time.Hour
	d -= h * time.Hour
	m := d / time.Minute
	d -= m * time.Minute
	s := d / time.Second
	return fmt.Sprintf("%02d:%02d:%02d", h, m, s)
}
