package catalog

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/onnasoft/ZenithSQL/core/buffer"
	"github.com/onnasoft/ZenithSQL/model/entity"
	"github.com/onnasoft/ZenithSQL/model/record"
	"github.com/sirupsen/logrus"
)

const (
	FileDataSchema = "data.schema.json"
	FileMetaSchema = "meta.schema.json"
	FileDataBin    = "data.bin"
	FileMetaBin    = "meta.bin"
	FileIndex      = "index.idx"
	FileStats      = "stats.json"
	FileLog        = "wal.log"
)

type Table struct {
	Name           string
	BasePath       string
	PathDataSchema string
	PathMetaSchema string
	PathDataBin    string
	PathMetaBin    string
	PathIndex      string
	PathStats      string
	PathLog        string
	SchemaData     *entity.Schema
	SchemaMeta     *entity.Schema
	BufData        *buffer.Buffer
	BufMeta        *buffer.Buffer
	BufIndex       *buffer.Buffer
	BufStats       *buffer.Buffer
	BufLog         *buffer.Buffer
	Logger         *logrus.Logger
	Stats          *TableStats
	RowCount       atomic.Uint64
	RowSize        atomic.Uint64
	insertMutex    sync.Mutex
}

type TableConfig struct {
	Name   string
	Path   string
	Schema *entity.Schema
	Logger *logrus.Logger
}

func NewTable(cfg *TableConfig) (*Table, error) {
	if cfg.Schema == nil {
		return nil, errors.New("schema cannot be nil")
	}

	tableDir := filepath.Join(cfg.Path, cfg.Name)
	if err := os.MkdirAll(tableDir, os.ModePerm); err != nil {
		return nil, fmt.Errorf("failed to create table directory: %w", err)
	}

	cfg.Schema.Lock()
	table, err := OpenTable(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to open table: %w", err)
	}

	if err = saveSchema(table.PathDataSchema, cfg.Schema); err != nil {
		return nil, fmt.Errorf("failed to save schema: %w", err)
	}

	return table, nil
}

func OpenTable(cfg *TableConfig) (*Table, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}

	base := filepath.Join(cfg.Path, cfg.Name)
	if err := ensureTableDirectoryExists(base); err != nil {
		return nil, err
	}

	metaSchema, err := loadOrCreateMeta(filepath.Join(base, FileMetaSchema))
	if err != nil {
		return nil, err
	}

	if err := loadOrLockSchema(cfg, filepath.Join(base, FileDataSchema)); err != nil {
		return nil, err
	}

	t := &Table{
		Name:           cfg.Name,
		BasePath:       cfg.Path,
		PathDataSchema: filepath.Join(base, FileDataSchema),
		PathMetaSchema: filepath.Join(base, FileMetaSchema),
		PathDataBin:    filepath.Join(base, FileDataBin),
		PathMetaBin:    filepath.Join(base, FileMetaBin),
		PathIndex:      filepath.Join(base, FileIndex),
		PathStats:      filepath.Join(base, FileStats),
		PathLog:        filepath.Join(base, FileLog),
		SchemaMeta:     metaSchema,
		SchemaData:     cfg.Schema,
		Logger:         cfg.Logger,
	}

	if err := t.initBuffers(); err != nil {
		return nil, fmt.Errorf("failed to initialize buffers: %w", err)
	}

	if err := t.loadOrInitStats(); err != nil {
		return nil, fmt.Errorf("failed to initialize stats: %w", err)
	}

	return t, nil
}

func (t *Table) initBuffers() error {
	var err error

	if err = os.MkdirAll(t.BasePath, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create base path: %w", err)
	}

	if t.BufMeta, err = buffer.NewBuffer(t.PathMetaBin); err != nil {
		return fmt.Errorf("failed to open meta buffer: %w", err)
	}
	if t.BufData, err = buffer.NewBuffer(t.PathDataBin); err != nil {
		return fmt.Errorf("failed to open data buffer: %w", err)
	}
	if t.BufIndex, err = buffer.NewBuffer(t.PathIndex); err != nil {
		return fmt.Errorf("failed to open index buffer: %w", err)
	}
	if t.BufStats, err = buffer.NewBuffer(t.PathStats); err != nil {
		return fmt.Errorf("failed to open stats buffer: %w", err)
	}
	if t.BufLog, err = buffer.NewBuffer(t.PathLog); err != nil {
		return fmt.Errorf("failed to open log buffer: %w", err)
	}

	if err := os.WriteFile(t.PathLog, []byte(""), 0644); err != nil {
		return fmt.Errorf("failed to initialize log file: %w", err)
	}

	return nil
}

func (t *Table) loadOrInitStats() error {
	data, err := os.ReadFile(t.PathStats)
	if err != nil {
		return err
	}

	var stats TableStats
	if err := json.Unmarshal(data, &stats); err != nil {
		t.InitStats(0)
	} else {
		t.RowCount.Store(stats.Rows)
		t.RowSize.Store(stats.RowSize)
		t.Stats = &stats
	}

	return nil
}

func (t *Table) SetRows(rows uint64) {
	t.RowCount.Store(rows)
}

func (t *Table) GetNextID() uint64 {
	return t.RowCount.Load() + 1
}

func (t *Table) GetRowSize() uint64 {
	return t.RowSize.Load()
}

func (t *Table) NewEntity(id uint64) *entity.Entity {
	ent, err := entity.NewEntity(&entity.EntityConfig{
		Schema: t.SchemaData,
		RW:     buffer.NewReadWriter(t.BufData),
	})
	if err != nil {
		t.Logger.Fatal(err)
	}
	ent.SetValue("id", id)
	ent.RW.Seek(ent.Schema.Size() * int(id))
	return ent
}

func (t *Table) NewMetaEntity(id uint64) *entity.Entity {
	meta, err := entity.NewEntity(&entity.EntityConfig{
		Schema: t.SchemaMeta,
		RW:     buffer.NewReadWriter(t.BufMeta),
	})
	if err != nil {
		t.Logger.Fatal(err)
	}
	meta.SetValue("id", id)
	meta.RW.Seek(meta.Schema.Size() * int(id))
	return meta
}

func (t *Table) NewRow() *record.Row {
	id := t.GetNextID()
	return &record.Row{
		ID:   id,
		Data: t.NewEntity(id),
		Meta: t.NewMetaEntity(id),
	}
}

func (t *Table) LoadRow(id uint64) *record.Row {
	return &record.Row{
		ID:   id,
		Data: t.NewEntity(id),
		Meta: t.NewMetaEntity(id),
	}
}

func (t *Table) LockInsert() {
	t.insertMutex.Lock()
}

func (t *Table) UnlockInsert() {
	t.insertMutex.Unlock()
}
