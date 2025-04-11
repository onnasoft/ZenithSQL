package main

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"
	"unsafe"

	"github.com/onnasoft/ZenithSQL/model/catalog"
	"github.com/sirupsen/logrus"
)

var (
	log       = logrus.New()
	batchSize = int64(100_000)
)

func main() {
	startTime := time.Now()

	catalog, err := catalog.OpenCatalog(&catalog.CatalogConfig{
		Path:   "./data",
		Logger: logrus.New(),
	})
	if err != nil {
		log.Fatalf("error opening catalog: %v", err)
	}
	defer catalog.Close()

	table, err := catalog.GetTable("testdb", "public", "temperatures")
	if err != nil {
		log.Fatalf("error getting table: %v", err)
	}

	log.Infof("Processing table %s", table.Name)
	totalSum := processDataOptimized(table)

	log.Infof("Finished processing. Total sum: %.2f, Time taken: %v", totalSum, time.Since(startTime))
}

func processDataOptimized(table *catalog.Table) float64 {
	numWorkers := runtime.NumCPU()
	totalRows := table.Stats().TotalRows
	fmt.Println("Total rows:", totalRows)

	jobs := make(chan [2]int64, numWorkers)
	results := make(chan float64, numWorkers)

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(table, jobs, results, &wg)
	}

	go func() {
		defer close(jobs)
		for start := int64(1); start <= totalRows; start += batchSize {
			end := start + batchSize - 1
			if end > totalRows {
				end = totalRows
			}
			jobs <- [2]int64{start, end}
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

func worker(table *catalog.Table, jobs <-chan [2]int64, results chan<- float64, wg *sync.WaitGroup) {
	defer wg.Done()

	reader, err := table.Reader()
	if err != nil {
		log.Errorf("error creating reader: %v", err)
	}
	defer reader.Close()

	temperatureField, ok := reader.ColumnsData()["temperature"]
	if !ok {
		log.Errorf("error getting temperature field from table %s", table.Name)
		return
	}

	for job := range jobs {
		start, end := job[0], job[1]
		var sum float64
		var num float64

		for i := start; i <= end; i++ {
			reader.See(i)

			_, err = reader.FastGetValue(temperatureField, &num)
			if err != nil {
				log.Errorf("error reading field value: %v", err)
				continue
			}
			sum += num
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
