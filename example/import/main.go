package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/onnasoft/ZenithSQL/core/executor"
	"github.com/onnasoft/ZenithSQL/model/catalog"
	"github.com/onnasoft/ZenithSQL/model/entity"
	"github.com/onnasoft/ZenithSQL/model/record"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
)

const (
	delimiter        = '\n'
	separator        = ';'
	batchSize        = 1000
	progressInterval = 1 * time.Second
	maxWorkers       = 8
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

	db, err := catalog.OpenDatabase(&catalog.DatabaseConfig{
		Name:   "testdb",
		Path:   "./data",
		Logger: logrus.New(),
	})
	if err != nil {
		log.Fatalf("error opening database: %v", err)
	}
	defer db.Close()

	schema, err := db.GetSchema("public")
	if err != nil {
		log.Fatalf("error getting schema: %v", err)
	}

	sc := entity.NewSchema()
	sc.AddField(catalog.NewFieldString("city", 100))
	sc.AddField(catalog.NewFieldFloat64("temperature"))

	table, err := schema.CreateTable("temperatures", sc)
	if err != nil {
		log.Fatalf("error creating table: %v", err)
	}

	table.LockImport()
	defer table.UnlockImport()

	stats := &ImportStats{startTime: startTime, lastTime: startTime}
	if err := importFileConcurrent(filePath, table, stats); err != nil {
		log.Fatalf("import failed: %v", err)
	}

	if err := table.BufData.Sync(); err != nil {
		log.Fatalf("syncing data buffer: %v", err)
	}

	if err := table.BufMeta.Sync(); err != nil {
		log.Fatalf("syncing meta buffer: %v", err)
	}
	if err := table.SaveStats(); err != nil {
		log.Fatalf("syncing stats buffer: %v", err)
	}

	//table.BufData.DisableWriteMode()
	//table.BufMeta.DisableWriteMode()

	showFinalStats(stats)
}
func importFileConcurrent(filePath string, table *catalog.Table, stats *ImportStats) error {
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
	defer unix.Munmap(data)

	numWorkers := runtime.NumCPU()
	if numWorkers > maxWorkers {
		numWorkers = maxWorkers
	}

	lines := make(chan []byte, numWorkers*2)
	errors := make(chan error, numWorkers)
	done := make(chan struct{})

	var workersWg sync.WaitGroup
	var tickerWg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		workersWg.Add(1)
		go func() {
			defer workersWg.Done()
			var batch []*record.Row
			for line := range lines {
				row, err := parseLine(table, line)
				if err != nil {
					atomic.AddInt64(&stats.failedRows, 1)
					continue
				}
				batch = append(batch, row)
				if len(batch) >= batchSize {
					if err := executor.Import(table, batch...); err != nil {
						atomic.AddInt64(&stats.failedRows, int64(len(batch)))
					} else {
						atomic.AddInt64(&stats.processedRows, int64(len(batch)))
					}
					batch = batch[:0]
				}
			}
			if len(batch) > 0 {
				if err := executor.Import(table, batch...); err != nil {
					atomic.AddInt64(&stats.failedRows, int64(len(batch)))
				} else {
					atomic.AddInt64(&stats.processedRows, int64(len(batch)))
				}
			}
		}()
	}

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

	go func() {
		lineStart := 0
		for lineStart < len(data) {
			lineEnd := bytes.IndexByte(data[lineStart:], delimiter)
			if lineEnd == -1 {
				break
			}
			line := data[lineStart : lineStart+lineEnd]
			lines <- line
			lineStart += lineEnd + 1
			atomic.AddInt64(&stats.bytesProcessed, int64(lineEnd+1))
		}
		close(lines)
	}()

	go func() {
		workersWg.Wait()
		close(errors)
		close(done)
	}()

	for err := range errors {
		if err != nil {
			return err
		}
	}

	tickerWg.Wait()
	return nil
}

func parseLine(table *catalog.Table, line []byte) (*record.Row, error) {
	parts := bytes.Split(line, []byte{separator})
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid format")
	}

	row := table.NewRow()

	if err := row.Data.SetValue("city", string(parts[0])); err != nil {
		return nil, err
	}

	temp, err := strconv.ParseFloat(string(parts[1]), 64)
	if err != nil {
		return nil, err
	}

	if err := row.Data.SetValue("temperature", temp); err != nil {
		return nil, err
	}

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
		stats.processedRows,
		stats.failedRows,
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
