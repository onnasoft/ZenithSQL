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
	numWorkers = 32 // Change the number of workers as needed
)

func InsertDataFromFile(filePath string, table *dataframe.Table) error {
	start := time.Now()

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
			if err := processAndInsertChunk(data[start:end+1], table); err != nil {
				fmt.Printf("Error processing chunk: %v\n", err)
			}
		}(startOffset, endOffset)
	}

	wg.Wait()

	fmt.Printf("Data insertion completed in: %s\n", time.Since(start))
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

func processAndInsertChunk(buffer []byte, table *dataframe.Table) error {
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
	}
	return nil
}

func main() {
	// Example usage
	filePath := "your_file_path_here.csv"
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

	table, err := schema.CreateTable("cities")
	if err != nil {
		fmt.Println("Error creating table:", err)
		return
	}

	// Add city and temperature columns to the table
	if err := table.AddColumn("city", entity.StringType, 50); err != nil {
		fmt.Println("Error adding column:", err)
		return
	}
	if err := table.AddColumn("temperature", entity.StringType, 10); err != nil {
		fmt.Println("Error adding column:", err)
		return
	}

	// Call the InsertDataFromFile function to insert data from the CSV file
	if err := InsertDataFromFile(filePath, table); err != nil {
		fmt.Println("Error inserting data:", err)
	}
}
