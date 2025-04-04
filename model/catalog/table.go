package catalog

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/onnasoft/ZenithSQL/core/utils"
	"github.com/onnasoft/ZenithSQL/model/entity"
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
	Schema        *entity.Schema
	length        int64
	effectiveSize int
	file          *os.File
	cache         *lru.Cache[int64, *entity.Entity]
	stopWorkers   chan struct{}
	schemaVersion int
	insertMutex   sync.Mutex
	batchBuffer   []byte
	fieldOffsets  []int
	fieldIndex    map[string]int
	paddingSize   int
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

	_, err := os.Stat(filePath)
	if err == nil {
		return nil, fmt.Errorf("file %v already exists", filePath)
	}

	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open data file: %w", err)
	}

	t, err := openTable(fullPath, config.Name, file)
	if err != nil {
		return nil, err
	}

	fields := make([]*entity.Field, 1, len(config.Fields)+4)
	fields[0] = &entity.Field{
		Name:   "id",
		Type:   entity.Int64Type,
		Length: 8,
	}
	fields = append(fields, config.Fields...)
	timeSchema := []*entity.Field{
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
	fields = append(fields, timeSchema...)
	t.effectiveSize = 0
	schema := entity.NewSchema()
	for i := 0; i < len(fields); i++ {
		field := reflect.ValueOf(fields[i]).Elem().Interface().(entity.Field)
		field.Prepare(t.effectiveSize)
		t.effectiveSize += field.Length + 1 // +1 for null indicator

		if field.Type == entity.StringType {
			field.Validators = append(field.Validators, &validate.StringLength{Min: 0, Max: field.Length})
		}

		schema.Add(&field)
	}
	t.Schema = schema
	t.effectiveSize = t.calculateEffectiveSize()
	t.fieldOffsets = t.calculateFieldOffsets()
	t.buildFieldIndex()

	err = t.saveSchema()
	if err != nil {
		return nil, err
	}

	return t, nil
}

func OpenTable(config *TableConfig) (*Table, error) {
	fullPath := filepath.Join(config.Path, config.Name)
	if err := os.MkdirAll(fullPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create table directory: %w", err)
	}

	filePath := filepath.Join(fullPath, "data.bin")
	_, err := os.Stat(filePath)
	if err != nil {
		return nil, fmt.Errorf("file %v not exists", filePath)
	}

	fields, err := loadSchema(fullPath)
	if err != nil {
		return nil, err
	}

	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open data file: %w", err)
	}

	t, err := openTable(fullPath, config.Name, file)
	if err != nil {
		return nil, err
	}

	defaultSchema := entity.NewSchema()
	for i := 0; i < len(fields); i++ {
		field := reflect.ValueOf(fields[i]).Elem().Interface().(entity.Field)
		defaultSchema.Add(&field)
	}
	t.Schema = defaultSchema

	t.effectiveSize = t.calculateEffectiveSize()
	t.fieldOffsets = t.calculateFieldOffsets()
	t.buildFieldIndex()

	stats, err := os.Stat(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read stats in file: %w", err)
	}
	t.length = stats.Size() / int64(t.effectiveSize)

	return t, nil
}

func loadSchema(fullPath string) ([]*entity.Field, error) {
	filePath := path.Join(fullPath, "schema.json")
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	data := map[string]interface{}{}
	if err := json.NewDecoder(file).Decode(&data); err != nil {
		return nil, err
	}

	length := len(data["fields"].([]interface{}))
	fields := make([]*entity.Field, length)
	dataSchema := data["fields"].([]interface{})

	for i := 0; i < length; i++ {
		current := dataSchema[i]
		fields[i] = &entity.Field{}
		err := fields[i].FromMap(current.(map[string]interface{}))
		if err != nil {
			return nil, err
		}
	}

	return fields, nil
}

func (t *Table) saveSchema() error {
	filePath := path.Join(t.Path, "schema.json")

	if _, err := os.Stat(filePath); err == nil {
		os.Remove(filePath)
	}

	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return err
	}

	enc := json.NewEncoder(file)
	enc.SetIndent("", "  ")
	fields := make([]interface{}, t.Schema.Len())

	for i := 0; i < t.Schema.Len(); i++ {
		field, _ := t.Schema.Get(i)
		fields[i] = field.ToMap()
	}

	enc.Encode(map[string]interface{}{
		"name":   t.Name,
		"path":   t.Path,
		"fields": fields,
	})

	return nil
}

func openTable(path, name string, file *os.File) (*Table, error) {
	t := &Table{
		Name:          name,
		Path:          path,
		file:          file,
		stopWorkers:   make(chan struct{}),
		schemaVersion: 1,
		fieldIndex:    make(map[string]int),
		paddingSize:   0,
	}

	rowSize := t.effectiveSize
	if rowSize < minBufferSize {
		t.paddingSize = minBufferSize - rowSize
		rowSize = minBufferSize
	}

	t.cache, _ = lru.New[int64, *entity.Entity](defaultCacheSize)

	t.insertMutex = sync.Mutex{}
	t.batchBuffer = make([]byte, maxBatchSize*rowSize)

	return t, nil
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
	for i, field := range t.Schema.Iter() {
		t.fieldIndex[field.Name] = i
	}
}

func (t *Table) Get(id int64) (*entity.Entity, error) {
	return t.fullRead(id)
}

func (t *Table) fullRead(id int64) (*entity.Entity, error) {
	defer utils.RecoverFromPanic("fullRead", utils.Log)

	if cached, ok := t.cache.Get(id); ok {
		return cached, nil
	}

	t.mu.RLock()
	defer t.mu.RUnlock()

	if id < 1 || id > t.length {
		return nil, fmt.Errorf("invalid id %d", id)
	}

	buffer := make([]byte, t.effectiveSize)

	offset := (id - 1) * int64(t.effectiveSize)
	_, err := t.file.ReadAt(buffer, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read row %d: %w", id, err)
	}

	record, _ := entity.NewEntity(t.Schema)
	if err := record.Read(buffer); err != nil {
		return nil, fmt.Errorf("failed to parse row %d: %w", id, err)
	}

	t.cache.Add(id, record)

	return record, nil
}

func (t *Table) BulkSum(fieldName string) (float64, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	idx, ok := t.fieldIndex[fieldName]
	if !ok {
		return 0, fmt.Errorf("field %s not found", fieldName)
	}

	field, err := t.Schema.Get(idx)
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

	for id := int64(1); id <= t.length; id++ {
		record, err := t.fullRead(id)
		if err != nil {
			return 0, fmt.Errorf("failed to read row %d: %w", id, err)
		}

		if condition(record) {
			count++
		}

		fmt.Printf("\rTotal: %v", id)
	}

	return count, nil
}

func (t *Table) calculateEffectiveSize() int {
	size := 0
	for _, col := range t.Schema.Iter() {
		size += col.Length + 1
	}
	return size
}

func (t *Table) calculateFieldOffsets() []int {
	offsets := make([]int, t.Schema.Len())
	offset := 0
	for i, col := range t.Schema.Iter() {
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
			entity, err := t.Get(id)
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
