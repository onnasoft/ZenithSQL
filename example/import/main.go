package main

import (
	"bytes"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/onnasoft/ZenithSQL/model/catalog"
	"github.com/onnasoft/ZenithSQL/model/entity"
	"golang.org/x/sys/unix"
)

const (
	delimiter        = '\n'
	separator        = ';'
	batchSize        = 5000
	progressInterval = 1 * time.Second // Intervalo de reporte de progreso
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
	speed          float64 // MB/s
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run main.go <file_path>")
		os.Exit(1)
	}

	startTime := time.Now()
	filePath := os.Args[1]

	db, table, err := initializeDatabase()
	if err != nil {
		fmt.Printf("Initialization error: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	stats := &ImportStats{startTime: startTime, lastTime: startTime}
	if err := importFileConcurrent(filePath, table, stats); err != nil {
		fmt.Printf("Import error: %v\n", err)
		os.Exit(1)
	}

	showFinalStats(stats)
}

func initializeDatabase() (*catalog.Database, *catalog.Table, error) {
	db, err := catalog.NewDatabase("testdb", "./data")
	if err != nil {
		return nil, nil, fmt.Errorf("creating database: %w", err)
	}

	schema, err := db.CreateSchema("public")
	if err != nil {
		return nil, nil, fmt.Errorf("creating schema: %w", err)
	}

	fields := []*entity.Field{
		{
			Name:   "city",
			Type:   entity.StringType,
			Length: 100,
		},
		{
			Name:   "temperature",
			Type:   entity.Float64Type,
			Length: 8,
		},
	}

	table, err := schema.CreateTable("temperatures", fields)
	if err != nil {
		return nil, nil, fmt.Errorf("creating table: %w", err)
	}

	return db, table, nil
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
	records := make(chan *entity.Entity, numWorkers*2)
	errors := make(chan error, numWorkers)
	done := make(chan bool)

	var wg sync.WaitGroup

	// Workers de procesamiento
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for line := range lines {
				record, err := parseLine(table, line)
				if err != nil {
					atomic.AddInt64(&stats.failedRows, 1)
					continue
				}
				records <- record
				atomic.AddInt64(&stats.processedRows, 1)
			}
		}()
	}

	// Worker de inserción
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := batchInsertWorker(table, records); err != nil {
			errors <- err
		}
	}()

	// Worker de progreso
	wg.Add(1)
	go func() {
		defer wg.Done()
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

	// Productor de líneas
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

			// Actualizar bytes procesados (incluyendo el delimitador)
			atomic.AddInt64(&stats.bytesProcessed, int64(lineEnd+1))
		}
		close(lines)
	}()

	go func() {
		wg.Wait()
		close(records)
		close(errors)
		done <- true
	}()

	for err := range errors {
		if err != nil {
			return err
		}
	}

	return nil
}

func batchInsertWorker(table *catalog.Table, records <-chan *entity.Entity) error {
	batch := make([]*entity.Entity, 0, batchSize)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case record, ok := <-records:
			if !ok {
				if len(batch) > 0 {
					if err := table.BulkImport(batch, len(batch)); err != nil {
						return fmt.Errorf("inserting final batch: %w", err)
					}
				}
				return nil
			}
			batch = append(batch, record)
			if len(batch) >= batchSize {
				if err := table.BulkImport(batch, len(batch)); err != nil {
					return fmt.Errorf("inserting batch: %w", err)
				}
				batch = batch[:0]
			}
		case <-ticker.C:
			if len(batch) > 0 {
				if err := table.BulkImport(batch, len(batch)); err != nil {
					return fmt.Errorf("inserting batch: %w", err)
				}
				batch = batch[:0]
			}
		}
	}
}

func parseLine(table *catalog.Table, line []byte) (*entity.Entity, error) {
	parts := bytes.Split(line, []byte{separator})
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid format, expected 2 parts got %d", len(parts))
	}

	record, err := entity.NewEntity(table.Schema)
	if err != nil {
		return nil, fmt.Errorf("creating entity: %w", err)
	}

	if err := record.SetByName("city", string(parts[0])); err != nil {
		return nil, fmt.Errorf("setting city: %w", err)
	}

	temp, err := strconv.ParseFloat(string(parts[1]), 64)
	if err != nil {
		return nil, fmt.Errorf("parsing temperature: %w", err)
	}

	if err := record.SetByName("temperature", temp); err != nil {
		return nil, fmt.Errorf("setting temperature: %w", err)
	}

	return record, nil
}

func reportProgress(stats *ImportStats) {
	now := time.Now()
	bytesProcessed := atomic.LoadInt64(&stats.bytesProcessed)

	// Calcular velocidad (MB/s)
	elapsed := now.Sub(stats.lastTime).Seconds()
	if elapsed > 0 {
		bytesDiff := bytesProcessed - stats.lastBytes
		stats.speed = (float64(bytesDiff) / (1024 * 1024)) / elapsed
		stats.lastBytes = bytesProcessed
		stats.lastTime = now
	}

	// Calcular progreso y tiempo restante
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

	fmt.Printf("\rProgress: %.2f%% | %.2f/%.2f MB | Speed: %.2f MB/s | Elapsed: %s | Remaining: %s",
		progress,
		processedMB,
		totalMB,
		stats.speed,
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
