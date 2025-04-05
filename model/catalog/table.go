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
	"github.com/sirupsen/logrus"
)

const (
	DataSchemaFile = "data.schema.json"
	MetaSchemaFile = "meta.schema.json"
	DataBinFile    = "data.bin"
	MetaBinFile    = "meta.bin"
	IndexFileName  = "index.idx"
	StatsFileName  = "stats.json"
	LogFileName    = "wal.log"
)

type Table struct {
	Name           string
	Path           string
	DataSchemaFile string
	MetaSchemaFile string
	DataBinFile    string
	MetaBinFile    string
	IndexFile      string
	StatsFile      string
	LogFile        string
	metaSchema     *entity.Schema
	dataSchema     *entity.Schema
	metaDataBuf    *buffer.Buffer
	dataBuf        *buffer.Buffer
	indexBuf       *buffer.Buffer
	statsBuf       *buffer.Buffer
	logBuf         *buffer.Buffer
	logger         *logrus.Logger
	stats          *TableStats
	rows           atomic.Uint64
	rowSize        atomic.Uint64
	insertLock     sync.Mutex
}

type TableConfig struct {
	Name   string
	Path   string
	Schema *entity.Schema
	Logger *logrus.Logger
}

func NewTable(config *TableConfig) (*Table, error) {
	if config.Schema == nil {
		return nil, errors.New("schema cannot be nil")
	}

	if err := os.MkdirAll(filepath.Join(config.Path, config.Name), os.ModePerm); err != nil {
		return nil, fmt.Errorf("failed to create table directory: %w", err)
	}

	config.Schema.Lock()
	table, err := OpenTable(config)
	if err != nil {
		return nil, fmt.Errorf("failed to open table: %w", err)
	}

	if err = saveSchema(table.DataSchemaFile, config.Schema); err != nil {
		return nil, fmt.Errorf("failed to save schema: %w", err)
	}

	return table, nil
}

func OpenTable(config *TableConfig) (*Table, error) {
	if err := validateConfig(config); err != nil {
		return nil, err
	}

	base := filepath.Join(config.Path, config.Name)
	if err := ensureTableDirectoryExists(base); err != nil {
		return nil, err
	}

	metaSchema, err := loadOrCreateMeta(filepath.Join(base, MetaSchemaFile))
	if err != nil {
		return nil, err
	}

	if err := loadOrLockSchema(config, filepath.Join(base, DataSchemaFile)); err != nil {
		return nil, err
	}

	t := &Table{
		Name:           config.Name,
		Path:           config.Path,
		DataSchemaFile: filepath.Join(base, DataSchemaFile),
		MetaSchemaFile: filepath.Join(base, MetaSchemaFile),
		DataBinFile:    filepath.Join(base, DataBinFile),
		MetaBinFile:    filepath.Join(base, MetaBinFile),
		IndexFile:      filepath.Join(base, IndexFileName),
		StatsFile:      filepath.Join(base, StatsFileName),
		LogFile:        filepath.Join(base, LogFileName),
		metaSchema:     metaSchema,
		dataSchema:     config.Schema,
		logger:         config.Logger,
	}

	if err := t.initializeFiles(); err != nil {
		return nil, fmt.Errorf("failed to initialize table files: %w", err)
	}

	if err := t.loadOrInitStats(); err != nil {
		return nil, fmt.Errorf("failed to initialize stats: %w", err)
	}

	return t, nil
}

func (t *Table) initializeFiles() error {
	var err error
	if err = os.MkdirAll(t.Path, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create table directory: %w", err)
	}

	t.metaDataBuf, err = buffer.NewBuffer(t.MetaBinFile)
	if err != nil {
		return fmt.Errorf("failed to create meta data buffer: %w", err)
	}
	t.dataBuf, err = buffer.NewBuffer(t.DataBinFile)
	if err != nil {
		return fmt.Errorf("failed to create data buffer: %w", err)
	}
	t.indexBuf, err = buffer.NewBuffer(t.IndexFile)
	if err != nil {
		return fmt.Errorf("failed to create index buffer: %w", err)
	}
	t.statsBuf, err = buffer.NewBuffer(t.StatsFile)
	if err != nil {
		return fmt.Errorf("failed to create stats buffer: %w", err)
	}
	t.logBuf, err = buffer.NewBuffer(t.LogFile)
	if err != nil {
		return fmt.Errorf("failed to create log buffer: %w", err)
	}

	if err := os.WriteFile(t.LogFile, []byte(""), 0644); err != nil {
		return fmt.Errorf("failed to initialize log file: %w", err)
	}

	return nil
}

func (t *Table) loadOrInitStats() error {
	data, err := os.ReadFile(t.StatsFile)
	if err != nil {
		return err
	}

	var stats TableStats
	if err := json.Unmarshal(data, &stats); err != nil {
		t.InitStats(0)
	} else {
		t.rows.Store(stats.Rows)
		t.rowSize.Store(stats.RowSize)
		t.stats = &stats
	}

	return nil
}

func (t *Table) SetRows(rows uint64) {
	t.rows.Store(rows)
}

func (t *Table) GetNextID() uint64 {
	return t.rows.Load() + 1
}

func (t *Table) GetRowSize() uint64 {
	return t.rowSize.Load()
}

func (t *Table) MakeEntity() *entity.Entity {
	ent, err := entity.NewEntity(&entity.EntityConfig{
		Schema: t.dataSchema,
		RW:     buffer.NewReadWriter(t.dataBuf),
	})
	if err != nil {
		t.logger.Fatal(err)
	}
	return ent
}

func (t *Table) MakeMeta(id uint64) *entity.Entity {
	meta, err := entity.NewEntity(&entity.EntityConfig{
		Schema: t.metaSchema,
		RW:     buffer.NewReadWriter(t.metaDataBuf),
	})
	if err != nil {
		t.logger.Fatal(err)
	}
	return meta
}

func (t *Table) InsertLock() {
	t.insertLock.Lock()
}

func (t *Table) InsertUnlock() {
	t.insertLock.Unlock()
}
