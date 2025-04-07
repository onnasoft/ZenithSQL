package main

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"
	"unsafe"

	"github.com/onnasoft/ZenithSQL/core/buffer"
	"github.com/onnasoft/ZenithSQL/model/catalog"
	"github.com/sirupsen/logrus"
)

var (
	log       = logrus.New()
	batchSize = uint64(1000)
)

func main() {
	startTime := time.Now()

	db, err := catalog.OpenDatabase(&catalog.DatabaseConfig{
		Name:   "testdb",
		Path:   "./data",
		Logger: log,
	})
	if err != nil {
		log.Fatalf("error opening database: %v", err)
	}

	schema, err := db.GetSchema("public")
	if err != nil {
		log.Fatalf("error getting schema: %v", err)
	}

	table, err := schema.GetTable("temperatures")
	if err != nil {
		log.Fatalf("error getting table: %v", err)
	}

	log.Infof("Processing table %s", table.Name)
	totalSum := processDataOptimized(table)

	log.Infof("Finished processing. Total sum: %.2f, Time taken: %v",
		totalSum, time.Since(startTime))
}

func processDataOptimized(table *catalog.Table) float64 {
	numWorkers := runtime.NumCPU()
	totalRows := table.Stats.GetValue("rows").(uint64)

	jobs := make(chan [2]uint64, numWorkers)
	results := make(chan float64, numWorkers)

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(table, jobs, results, &wg)
	}

	go func() {
		defer close(jobs)
		for start := uint64(1); start <= totalRows; start += batchSize {
			end := start + batchSize - 1
			if end > totalRows {
				end = totalRows
			}
			jobs <- [2]uint64{start, end}
		}
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	var totalSum float64
	for sum := range results {
		totalSum += sum
	}

	return totalSum
}

func worker(table *catalog.Table, jobs <-chan [2]uint64, results chan<- float64, wg *sync.WaitGroup) {
	defer wg.Done()

	rw := buffer.NewReadWriter(table.BufData)
	rowSize := table.SchemaData.Size()

	temperatureField, _ := table.SchemaData.GetFieldByName("temperature")
	if temperatureField.Reader == nil {
		log.Error("temperatureField.Reader is nil")
		return
	}

	for job := range jobs {
		start, end := job[0], job[1]
		var sum float64
		offset := rowSize * int(start-1)
		parser := Float64Type

		for i := start; i <= end; i++ {
			rw.Seek(offset)
			isSet, _ := rw.Read(temperatureField.IsSettedFlagPos, 1)
			if isSet[0] == 0 {
				continue
			}
			data, err := rw.Read(temperatureField.StartPosition, temperatureField.Length)
			if err != nil {
				fmt.Println("error reading data:", err)
				continue
			}
			val, _ := parser(data)
			sum += val

			//sum += entity.GetFloat64ValueAtOffset(temperatureField, table.BufData, offset)
			offset += rowSize
		}

		results <- sum
	}
}

func Float64Type(data []byte) (float64, error) {
	if len(data) < 8 {
		return 0, errors.New("insufficient data for Float64 (need 8 bytes)")
	}
	return *(*float64)(unsafe.Pointer(&data[0])), nil
}
