package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/onnasoft/ZenithSQL/model/catalog"
	"github.com/sirupsen/logrus"
)

const (
	numWorkers = 64
)

var log = logrus.New()

func main() {
	db, err := catalog.OpenDatabase(&catalog.DatabaseConfig{
		Name:   "testdb",
		Path:   "./data",
		Logger: logrus.New(),
	})
	if err != nil {
		log.Fatalf("error getting database %v, %v", "testdb", err)
	}

	schema, err := db.GetSchema("public")
	if err != nil {
		log.Fatalf("error getting schema %v, %v", "public", err)
	}

	table, err := schema.GetTable("temperatures")
	if err != nil {
		log.Fatalf("error getting table %v, %v", "temperatures", err)
	}

	startTime := time.Now()
	log.Infof("Processing table %s", table.Name)
	processData(table)

	log.Infof("Finished processing table %s in %v", table.Name, time.Since(startTime))

}

func processData(table *catalog.Table) {
	totalRows := table.Stats.GetValue("rows").(uint64)
	chunkSize := totalRows / numWorkers

	var wg sync.WaitGroup
	results := make(chan float64, numWorkers)
	fmt.Println("Total rows:", totalRows)

	for i := uint64(0); i < numWorkers; i++ {
		startRow := i * chunkSize
		endRow := startRow + chunkSize
		if endRow > totalRows {
			endRow = totalRows
		}
		start := startRow + 1
		end := endRow
		if start >= totalRows {
			break
		}
		if end > totalRows {
			end = totalRows
		}

		wg.Add(1)
		go func(start, end uint64) {
			log.Infof("Processing rows %d to %d", start, end)
			defer wg.Done()
			results <- sumTemperatures(table, start, end)
		}(start, end)
	}

	wg.Wait()
	close(results)

	var total float64
	for r := range results {
		total += r
	}

	fmt.Printf("Total sum of temperatures: %.2f\n", total)
}

func sumTemperatures(table *catalog.Table, start, end uint64) float64 {
	var sum float64
	row := table.NewEntityWithNoCache()
	offset := table.SchemaData.Size() * int(start)

	for i := start; i <= end; i++ {
		row.RW().Seek(offset)
		if row == nil {
			break
		}
		temp := row.GetValue("temperature")
		if temp != nil {
			if t, ok := temp.(float64); ok {
				sum += t
			}
		}

		offset += table.SchemaData.Size()
	}
	return sum
}
