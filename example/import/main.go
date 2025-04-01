package main

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/onnasoft/ZenithSQL/dataframe"
	"github.com/onnasoft/ZenithSQL/entity"
	"golang.org/x/sys/unix"
)

const (
	delimiter  = '\n'
	separator  = ';'
	numWorkers = 32
)

func InsertDataFromFile(filePath string, table *dataframe.Table) error {
	startTime := time.Now()

	// Open the file
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	// Get file size
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to get file info: %v", err)
	}
	fileSize := info.Size()

	// mmap the file
	data, err := unix.Mmap(int(file.Fd()), 0, int(fileSize), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return fmt.Errorf("failed to mmap file: %v", err)
	}
	defer unix.Munmap(data)

	// Calculate chunk bounds
	chunkBounds := getChunkBounds(data, fileSize)

	var totalRows int64
	var totalBytesProcessed int64
	var wg sync.WaitGroup
	sem := make(chan struct{}, 128) // Max concurrent workers

	for i := 0; i < len(chunkBounds)-1; i++ {
		startOffset := chunkBounds[i]
		endOffset := chunkBounds[i+1] - 1

		// Semaphore to limit concurrent workers
		sem <- struct{}{}
		wg.Add(1)

		// Process each chunk concurrently
		go func(start, end int64) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := processAndInsertChunk(data[start:end+1], table, &totalRows, &totalBytesProcessed, startTime, fileSize); err != nil {
				fmt.Printf("Error processing chunk: %v\n", err)
			}
		}(startOffset, endOffset)
	}

	wg.Wait()

	// Calculate elapsed time and remaining time
	elapsed := time.Since(startTime)
	percentage := float64(totalBytesProcessed) / float64(fileSize)
	eta := time.Duration(float64(elapsed) * (1 - percentage) / percentage)

	// Print final summary
	fmt.Printf("\nData insertion completed in: %s\n", elapsed)
	fmt.Printf("Remaining time: %s\n", eta)
	fmt.Printf("Total Rows Inserted: %d\n", totalRows)
	fmt.Printf("Total Bytes Processed: %d\n", totalBytesProcessed)
	fmt.Printf("Total Bytes Remaining: %d\n", fileSize-totalBytesProcessed)

	return nil
}

func getChunkBounds(data []byte, fileSize int64) []int64 {
	bounds := []int64{0}
	chunkSize := fileSize / numWorkers

	var offset int64
	for i := 1; i < numWorkers; i++ {
		offset += chunkSize
		for {
			if offset >= fileSize {
				offset = fileSize
				break
			}
			if data[offset] == delimiter {
				offset++
				break
			}
			offset++
		}
		bounds = append(bounds, offset)
	}
	bounds = append(bounds, fileSize)
	return bounds
}

func processAndInsertChunk(buffer []byte, table *dataframe.Table, totalRows *int64, totalBytesProcessed *int64, start time.Time, fileSize int64) error {
	lineStart := 0
	for {
		lineEnd := bytes.IndexByte(buffer[lineStart:], delimiter)
		if lineEnd == -1 {
			break
		}
		line := buffer[lineStart : lineStart+lineEnd]
		lineStart += lineEnd + 1

		// Split the line into city and temperature
		parts := bytes.SplitN(line, []byte{separator}, 2)
		if len(parts) < 2 {
			continue
		}
		city := string(parts[0])
		temp := string(parts[1])

		// Insert into table (assuming you have appropriate column positions in the table)
		record, err := entity.NewEntity(table.Fields)
		if err != nil {
			return fmt.Errorf("failed to create new entity: %v", err)
		}
		if err := record.SetByName("city", city); err != nil {
			return fmt.Errorf("failed to set city: %v", err)
		}
		if err := record.SetByName("temperature", temp); err != nil {
			return fmt.Errorf("failed to set temperature: %v", err)
		}

		// Insert the record into the table
		if err := table.Insert(record); err != nil {
			return fmt.Errorf("failed to insert record: %v", err)
		}

		// Update total rows and progress
		(*totalRows)++
		*totalBytesProcessed += int64(lineEnd + 1) // Update the bytes processed with the size of this line

		// Every 100000 rows, print progress
		if *totalRows%100000 == 0 {
			elapsed := time.Since(start)
			percentage := float64(*totalBytesProcessed) / float64(fileSize)
			eta := time.Duration(float64(elapsed) * (1 - percentage) / percentage)
			fmt.Printf("\rProgress: %.2f%% | Elapsed: %s | ETA: %s | Total Rows: %d | Bytes Processed: %d | Bytes Remaining: %d",
				percentage*100, formatDuration(elapsed), formatDuration(eta), *totalRows, *totalBytesProcessed, fileSize-*totalBytesProcessed)
		}
	}
	return nil
}

func formatDuration(d time.Duration) string {
	secs := int64(d.Seconds())
	h := secs / 3600
	m := (secs % 3600) / 60
	s := secs % 60
	return fmt.Sprintf("%02dh:%02dm:%02ds", h, m, s)
}

func main() {
	// Verifica si se pasó el archivo como argumento
	if len(os.Args) < 2 {
		fmt.Println("Error: No file path provided.")
		fmt.Println("Usage: go run main.go <file_path>")
		return
	}

	// Ejemplo de uso
	db, err := dataframe.NewDatabase("testdb", "./data")
	if err != nil {
		fmt.Println("Error creating database:", err)
		return
	}

	schema, err := db.CreateSchema("public")
	if err != nil {
		fmt.Println("Error creating schema:", err)
		return
	}

	table, err := schema.CreateTable("cities_temperatures")
	if err != nil {
		fmt.Println("Error creating table:", err)
		return
	}

	// Agregar columnas a la tabla
	if err := table.AddColumn("city", entity.StringType, 50); err != nil {
		fmt.Println("Error adding column:", err)
		return
	}
	if err := table.AddColumn("temperature", entity.StringType, 10); err != nil {
		fmt.Println("Error adding column:", err)
		return
	}

	// Obtén el archivo pasado como argumento
	filePath := os.Args[1]

	// Llamar a la función InsertDataFromFile para insertar datos desde el archivo CSV
	if err := InsertDataFromFile(filePath, table); err != nil {
		fmt.Println("Error inserting data:", err)
	}

}
