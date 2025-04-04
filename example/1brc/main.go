package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
	"unsafe"

	"github.com/onnasoft/ZenithSQL/core/engine"
	"github.com/onnasoft/ZenithSQL/model/entity"
	"github.com/sirupsen/logrus"
	"golang.org/x/sys/unix"
)

const (
	numWorkers = 16 // Número máximo de workers simultáneos
	steps      = 32
)

var log = logrus.New()

func main() {
	db, err := engine.OpenDatabase("testdb", "./data")
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

	processData(table)
}

func processData(table *engine.Table) {
	startTime := time.Now()

	filePath := filepath.Join(table.Path, "data.bin")
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		log.Fatalf("error getting file info: %v", err)
	}
	fileSize := info.Size()

	data, err := unix.Mmap(int(file.Fd()), 0, int(fileSize), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		log.Fatalf("error mmap file: %v", err)
	}
	defer unix.Munmap(data)

	stepSize := int(table.Length() / int64(steps) * int64(table.EffectiveSize()))
	if stepSize == 0 {
		stepSize = int(table.EffectiveSize()) // Evita dividir por 0
	}

	var totalSum float64
	var wg sync.WaitGroup
	sem := make(chan struct{}, numWorkers)
	pool := sync.Pool{
		New: func() interface{} {
			record, _ := entity.NewEntity(table.Fields)
			return record
		},
	}

	startOffset := 0
	for endOffset := stepSize; startOffset < int(fileSize); endOffset += stepSize {
		if endOffset > int(fileSize) {
			endOffset = int(fileSize)
		}

		sem <- struct{}{}
		wg.Add(1)

		go func(start, end int) {
			defer wg.Done()
			defer func() { <-sem }()

			record := pool.Get().(*entity.Entity)
			sum := runWorker(record, data[start:end], table.EffectiveSize())
			totalSum += sum
			pool.Put(record)
		}(startOffset, endOffset)

		startOffset = endOffset
	}

	wg.Wait()
	fmt.Printf("\nTotal sum of temperatures: %.2f\n", totalSum)
	fmt.Printf("Elapsed: %s\n", time.Since(startTime))
}

func runWorker(record *entity.Entity, buffer []byte, rowSize int) float64 {
	var sum float64
	field, _ := record.Fields.GetByName("temperature")
	tempOffset := field.StartPosition
	endPos := field.EndPosition

	// Comprobación de seguridad básica
	if endPos <= tempOffset || (endPos-tempOffset) != 8 {
		return 0
	}

	for i := 0; i <= len(buffer)-rowSize; i += rowSize {
		if i+tempOffset+8 > len(buffer) {
			break
		}
		// Conversión directa sin copia
		sum += *(*float64)(unsafe.Pointer(&buffer[i+tempOffset]))
	}

	return sum
}
