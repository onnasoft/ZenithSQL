package engine

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/onnasoft/ZenithSQL/allocator"
	"github.com/onnasoft/ZenithSQL/entity"
	"github.com/onnasoft/ZenithSQL/validate"
)

const (
	defaultBatchSize   = 1
	defaultBufferSize  = 64
	defaultCacheSize   = 1000
	defaultWorkerCount = 8
	minBufferSize      = 64
	maxBatchSize       = 1000
)

type Table struct {
	mu            sync.RWMutex
	Name          string
	Path          string
	Fields        *entity.Fields
	length        int64
	effectiveSize int // Tamaño efectivo de los datos (calculado)
	file          *os.File
	writer        *bufio.Writer
	reader        *bufio.Reader
	writePool     *allocator.BufferPool
	readPool      *allocator.BufferPool
	cache         *lru.Cache[int64, []byte]
	workerWg      sync.WaitGroup
	stopWorkers   chan struct{}
	schemaVersion int
	insertMutex   sync.Mutex
	batchBuffer   []byte
	fieldOffsets  []int
	fieldIndex    map[string]int
	paddingSize   int // Tamaño de padding adicional si es necesario
}

type TableConfig struct {
	Name   string
	Path   string
	Fields []*entity.Field
}

func NewTable(config *TableConfig) (*Table, error) {
	fullPath := filepath.Join(config.Path, config.Name)
	if err := os.MkdirAll(fullPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create table directory: %w", err)
	}

	filePath := filepath.Join(fullPath, "data.bin")

	info, err := os.Stat(filePath)
	if err == nil {
		log.Println(info)
		return nil, fmt.Errorf("file %v already exists", filePath)
	}

	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open data file: %w", err)
	}

	t, err := openTable(fullPath, config.Name, file, config.Fields)
	if err != nil {
		return nil, err
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(map[string]interface{}{
		"name":   t.Name,
		"path":   t.Path,
		"fields": t.Fields.Iter(),
	})

	return t, nil
}

func openTable(path, name string, file *os.File, fields []*entity.Field) (*Table, error) {
	t := &Table{
		Name:          name,
		Path:          path,
		file:          file,
		writer:        bufio.NewWriterSize(file, defaultBufferSize),
		reader:        bufio.NewReader(file),
		stopWorkers:   make(chan struct{}),
		schemaVersion: 1,
		fieldIndex:    make(map[string]int),
		paddingSize:   0,
	}

	t.buildFields(fields)
	t.effectiveSize = t.calculateEffectiveSize()
	t.fieldOffsets = t.calculateFieldOffsets()
	t.buildFieldIndex()

	rowSize := t.effectiveSize
	if rowSize < minBufferSize {
		t.paddingSize = minBufferSize - rowSize
		rowSize = minBufferSize
	}

	t.writePool = allocator.NewBufferPool(100, rowSize)
	t.readPool = allocator.NewBufferPool(100, t.effectiveSize)
	t.cache, _ = lru.New[int64, []byte](defaultCacheSize)

	t.insertMutex = sync.Mutex{}
	t.batchBuffer = make([]byte, maxBatchSize*rowSize)

	return t, nil
}

func (t *Table) buildFields(fields []*entity.Field) {
	allFields := make([]*entity.Field, 1, len(fields)+4)
	allFields[0] = &entity.Field{
		Name:   "id",
		Type:   entity.Int64Type,
		Length: 8,
	}
	allFields = append(allFields, fields...)
	timeFields := []*entity.Field{
		{
			Name:   "created_at",
			Type:   entity.TimestampType,
			Length: 8,
		}, {
			Name:   "updated_at",
			Type:   entity.TimestampType,
			Length: 8,
		}, {
			Name:   "deleted_at",
			Type:   entity.TimestampType,
			Length: 8,
		},
	}
	allFields = append(allFields, timeFields...)

	defaultFields := entity.NewFields()
	for i := 0; i < len(allFields); i++ {
		field := reflect.ValueOf(allFields[i]).Elem().Interface().(entity.Field)
		field.Prepare(t.effectiveSize)
		t.effectiveSize += field.Length + 1 // +1 for null indicator

		if field.Type == entity.StringType {
			field.Validators = append(field.Validators, validate.StringLengthValidator{Min: 0, Max: field.Length})
		}

		defaultFields.Add(&field)
	}

	t.Fields = defaultFields
}

func (t *Table) calculateRowSize() int {
	size := t.effectiveSize

	if size < minBufferSize {
		return minBufferSize
	}
	return size
}

func (t *Table) GetRowSize() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.calculateRowSize()
}

func (t *Table) insertSingle(entity *entity.Entity) error {
	t.insertMutex.Lock()
	defer t.insertMutex.Unlock()

	now := time.Now()
	if entity.GetByName("created_at") == nil {
		entity.SetByName("created_at", now)
	}
	if entity.GetByName("updated_at") == nil {
		entity.SetByName("updated_at", now)
	}
	entity.SetByName("id", t.length+1)

	rowSize := t.calculateRowSize()
	buffer := make([]byte, rowSize)
	if err := entity.Write(buffer[:t.effectiveSize]); err != nil {
		return fmt.Errorf("failed to serialize entity: %w", err)
	}

	offset := t.length * int64(rowSize)
	if _, err := t.file.WriteAt(buffer, offset); err != nil {
		return fmt.Errorf("failed to write entity: %w", err)
	}

	t.length++
	return nil
}

func (t *Table) processBatch(batch []*entity.Entity) error {
	now := time.Now()
	rowSize := t.calculateRowSize()
	buffer := make([]byte, len(batch)*rowSize)

	for i, entity := range batch {
		if entity.GetByName("created_at") == nil {
			entity.SetByName("created_at", now)
		}
		if entity.GetByName("updated_at") == nil {
			entity.SetByName("updated_at", now)
		}
		entity.SetByName("id", t.length+int64(i)+1)

		start := i * rowSize
		end := start + t.effectiveSize
		if err := entity.Write(buffer[start:end]); err != nil {
			return fmt.Errorf("failed to serialize entity %d: %w", i, err)
		}
	}

	offset := t.length * int64(rowSize)
	if _, err := t.file.WriteAt(buffer, offset); err != nil {
		return fmt.Errorf("failed to write batch: %w", err)
	}

	t.length += int64(len(batch))
	return nil
}

func (t *Table) buildFieldIndex() {
	for i, field := range t.Fields.Iter() {
		t.fieldIndex[field.Name] = i
	}
}

func (t *Table) Get(id int64, record *entity.Entity) error {
	return t.fullRead(id, record)
}

func (t *Table) fullRead(id int64, record *entity.Entity) error {
	if cached, ok := t.cache.Get(id); ok {
		if err := record.Read(cached); err != nil {
			return fmt.Errorf("failed to deserialize cached entity: %w", err)
		}
		return nil
	}

	t.mu.RLock()
	defer t.mu.RUnlock()

	if id < 1 || id > t.length {
		return fmt.Errorf("invalid id %d", id)
	}

	buffer := make([]byte, t.effectiveSize)

	offset := (id - 1) * int64(t.effectiveSize)
	if _, err := t.file.ReadAt(buffer, offset); err != nil {
		return fmt.Errorf("failed to read row %d: %w", id, err)
	}

	if err := record.Read(buffer); err != nil {
		return fmt.Errorf("failed to parse row %d: %w", id, err)
	}

	t.cache.Add(id, buffer)

	return nil
}

func (t *Table) AddColumn(name string, typ entity.DataType, length int, validators ...validate.Validator) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if length <= 0 {
		return fmt.Errorf("invalid length for type %s", typ.String())
	}

	col := &entity.Field{
		Name:       name,
		Type:       typ,
		Length:     length,
		Validators: validators,
	}
	if typ == entity.StringType && length > 0 {
		col.Validators = append(col.Validators, validate.StringLengthValidator{Min: 0, Max: length})
	}
	col.Prepare(t.effectiveSize)
	t.Fields.Add(col)
	t.schemaVersion++
	t.effectiveSize = t.calculateEffectiveSize()
	t.fieldOffsets = t.calculateFieldOffsets()
	t.buildFieldIndex() // Reconstruir el índice
	t.cache.Purge()     // Clear cache on schema change

	t.readPool = allocator.NewBufferPool(100, t.effectiveSize)
	t.writePool = allocator.NewBufferPool(100, t.effectiveSize)

	return nil
}

func (t *Table) BulkSum(fieldName string) (float64, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	idx, ok := t.fieldIndex[fieldName]
	if !ok {
		return 0, fmt.Errorf("field %s not found", fieldName)
	}

	field, err := t.Fields.Get(idx)
	if err != nil {
		return 0, fmt.Errorf("failed to get field %s: %w", fieldName, err)
	}
	if !field.IsNumeric() {
		return 0, fmt.Errorf("field %s is not numeric", fieldName)
	}

	var sum float64
	buf := make([]byte, field.Length)

	for id := int64(1); id <= t.length; id++ {
		offset := (id-1)*int64(t.effectiveSize) + int64(t.fieldOffsets[idx])
		if _, err := t.file.ReadAt(buf, offset); err != nil {
			return 0, fmt.Errorf("failed to read field %s at row %d: %w", fieldName, id, err)
		}

		val, err := field.DecodeNumeric(buf)
		if err != nil {
			return 0, fmt.Errorf("failed to decode field %s at row %d: %w", fieldName, id, err)
		}
		switch v := val.(type) {
		case int64:
			sum += float64(v)
		case float32:
			sum += float64(v)
		case float64:
			sum += v
		default:
			return 0, fmt.Errorf("unsupported type %T for field %s", v, fieldName)
		}
	}

	return sum, nil
}

func (t *Table) BulkCount(condition func(*entity.Entity) bool) (int64, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var count int64
	tempEntity := &entity.Entity{}

	for id := int64(1); id <= t.length; id++ {
		if err := t.fullRead(id, tempEntity); err != nil {
			return 0, fmt.Errorf("failed to read row %d: %w", id, err)
		}

		if condition(tempEntity) {
			count++
		}
		tempEntity.Reset()
	}

	return count, nil
}

func (t *Table) calculateEffectiveSize() int {
	size := 0
	for _, col := range t.Fields.Iter() {
		size += col.Length + 1
	}
	return size
}

func (t *Table) calculateFieldOffsets() []int {
	offsets := make([]int, t.Fields.Len())
	offset := 0
	for i, col := range t.Fields.Iter() {
		offsets[i] = offset
		offset += col.Length + 1
	}
	return offsets
}

func (t *Table) Insert(entities ...*entity.Entity) error {
	if len(entities) == 0 {
		return nil
	}

	if len(entities) == 1 {
		return t.insertSingle(entities[0])
	}

	return t.insertBatch(entities)
}

func (t *Table) insertBatch(entities []*entity.Entity) error {
	t.insertMutex.Lock()
	defer t.insertMutex.Unlock()

	for start := 0; start < len(entities); start += maxBatchSize {
		end := start + maxBatchSize
		if end > len(entities) {
			end = len(entities)
		}

		chunk := entities[start:end]
		if err := t.processBatch(chunk); err != nil {
			return fmt.Errorf("batch insert failed at chunk %d-%d: %w", start, end, err)
		}
	}

	return nil
}

func (t *Table) GetBatch(ids []int64) ([]*entity.Entity, error) {
	results := make([]*entity.Entity, len(ids))
	var wg sync.WaitGroup
	errCh := make(chan error, len(ids))

	for i, id := range ids {
		wg.Add(1)
		go func(i int, id int64) {
			defer wg.Done()
			entity, err := entity.NewEntity(t.Fields)
			if err != nil {
				errCh <- err
				return
			}

			err = t.Get(id, entity)
			if err != nil {
				errCh <- err
				return
			}
			results[i] = entity
		}(i, id)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		return nil, err
	}

	return results, nil
}

func (t *Table) Close() {
	close(t.stopWorkers)
	t.workerWg.Wait()

	if err := t.writer.Flush(); err != nil {
		fmt.Printf("failed to flush writer: %v\n", err)
	}

	if err := t.file.Sync(); err != nil {
		fmt.Printf("failed to sync file: %v\n", err)
	}

	if err := t.file.Close(); err != nil {
		fmt.Printf("failed to close file: %v\n", err)
	}
}

func (t *Table) Length() int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.length
}

func (t *Table) EffectiveSize() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.effectiveSize
}

func (t *Table) BulkImport(entities []*entity.Entity, batchSize int) error {
	if len(entities) == 0 {
		return nil
	}

	if batchSize <= 0 {
		batchSize = maxBatchSize
	} else if batchSize > maxBatchSize {
		batchSize = maxBatchSize
	}

	rowSize := t.calculateRowSize()

	for start := 0; start < len(entities); start += batchSize {
		end := start + batchSize
		if end > len(entities) {
			end = len(entities)
		}

		batch := entities[start:end]
		if err := t.processBulkBatch(batch, rowSize); err != nil {
			return fmt.Errorf("bulk import failed at batch %d-%d: %w", start, end, err)
		}
	}

	return nil
}

func (t *Table) processBulkBatch(batch []*entity.Entity, rowSize int) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()
	buffer := make([]byte, len(batch)*rowSize)

	for i, entity := range batch {
		if entity.GetByName("created_at") == nil {
			entity.SetByName("created_at", now)
		}
		if entity.GetByName("updated_at") == nil {
			entity.SetByName("updated_at", now)
		}
		entity.SetByName("id", t.length+int64(i)+1)

		start := i * rowSize
		end := start + t.effectiveSize
		if err := entity.Write(buffer[start:end]); err != nil {
			return fmt.Errorf("failed to serialize entity %d: %w", i, err)
		}
	}

	offset := t.length * int64(rowSize)
	if _, err := t.file.WriteAt(buffer, offset); err != nil {
		return fmt.Errorf("failed to write batch: %w", err)
	}

	t.length += int64(len(batch))

	return nil
}
