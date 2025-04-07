package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	configFileName = "config.json"
	statsFileName  = "stats.bin"
)

type statsHeader struct {
	LastModified int64
	FieldCount   int32
}

type fieldStats struct {
	DiskSize      int64
	NullCount     int64
	DistinctCount int64
	MinValue      float64
	MaxValue      float64
}

type TableConfig struct {
	Fields []FieldMeta  `json:"fields"`
	Stats  StorageStats `json:"stats,omitempty"`
}

type ConfigManager struct {
	basePath string
	mu       sync.RWMutex
}

func NewConfigManager(basePath string) *ConfigManager {
	return &ConfigManager{
		basePath: basePath,
		mu:       sync.RWMutex{},
	}
}

func (cm *ConfigManager) SaveTableConfig(tableName string, config *TableConfig) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	tablePath := filepath.Join(cm.basePath, tableName)
	if err := os.MkdirAll(tablePath, 0755); err != nil {
		return err
	}

	configPath := filepath.Join(tablePath, configFileName)
	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return err
	}

	if err := os.WriteFile(configPath, data, 0644); err != nil {
		return err
	}

	// Crear stats.bin vacío inicial
	statsPath := filepath.Join(tablePath, statsFileName)
	file, err := os.Create(statsPath)
	if err != nil {
		return err
	}
	defer file.Close()

	header := statsHeader{
		LastModified: time.Now().UnixNano(),
		FieldCount:   int32(len(config.Fields)),
	}

	if err := binary.Write(file, binary.LittleEndian, &header); err != nil {
		return err
	}

	for range config.Fields {
		fs := fieldStats{}
		if err := binary.Write(file, binary.LittleEndian, &fs); err != nil {
			return err
		}
	}

	return nil
}

func (cm *ConfigManager) LoadTableConfig(tableName string) (TableConfig, error) {
	fmt.Println("Loading config for table:", configFileName, tableName, cm.basePath)
	//cm.mu.RLock()
	//defer cm.mu.RUnlock()

	var config TableConfig
	configPath := filepath.Join(cm.basePath, tableName, configFileName)

	data, err := os.ReadFile(configPath)
	if err != nil {
		return config, err
	}

	if err := json.Unmarshal(data, &config); err != nil {
		return config, err
	}

	return config, nil
}

func (cm *ConfigManager) TableExists(tableName string) bool {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	configPath := filepath.Join(cm.basePath, tableName, configFileName)
	_, err := os.Stat(configPath)
	return !os.IsNotExist(err)
}

func (cm *ConfigManager) DeleteTableConfig(tableName string) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	return os.RemoveAll(filepath.Join(cm.basePath, tableName))
}

func (cm *ConfigManager) ListTables() ([]string, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	entries, err := os.ReadDir(cm.basePath)
	if err != nil {
		return nil, err
	}

	var tables []string
	for _, entry := range entries {
		if entry.IsDir() {
			configPath := filepath.Join(cm.basePath, entry.Name(), configFileName)
			if _, err := os.Stat(configPath); err == nil {
				tables = append(tables, entry.Name())
			}
		}
	}

	return tables, nil
}

func (cm *ConfigManager) UpdateStats(tableName string, stats StorageStats) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	tablePath := filepath.Join(cm.basePath, tableName)
	statsPath := filepath.Join(tablePath, statsFileName)

	file, err := os.OpenFile(statsPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	header := statsHeader{
		LastModified: time.Now().UnixNano(),
		FieldCount:   int32(len(stats.FieldStats)),
	}

	if err := binary.Write(file, binary.LittleEndian, &header); err != nil {
		return err
	}

	for _, field := range stats.FieldStats {
		fs := fieldStats{
			DiskSize:      field.DiskSize,
			NullCount:     field.NullCount,
			DistinctCount: field.DistinctCount,
		}

		if min, ok := convertToFloat64(field.MinValue); ok {
			fs.MinValue = min
		}

		if max, ok := convertToFloat64(field.MaxValue); ok {
			fs.MaxValue = max
		}

		if err := binary.Write(file, binary.LittleEndian, &fs); err != nil {
			return err
		}
	}

	return nil
}

func (cm *ConfigManager) LoadStats(tableName string) (StorageStats, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	var stats StorageStats
	stats.FieldStats = make(map[string]FieldStats)

	tablePath := filepath.Join(cm.basePath, tableName)
	statsPath := filepath.Join(tablePath, statsFileName)

	file, err := os.Open(statsPath)
	if os.IsNotExist(err) {
		return stats, nil
	}
	if err != nil {
		return stats, err
	}
	defer file.Close()

	var header statsHeader
	if err := binary.Read(file, binary.LittleEndian, &header); err != nil {
		return stats, err
	}

	stats.LastModified = time.Unix(0, header.LastModified)

	config, err := cm.LoadTableConfig(tableName)
	if err != nil {
		return stats, err
	}

	for i := 0; i < len(config.Fields); i++ {
		var fs fieldStats
		if err := binary.Read(file, binary.LittleEndian, &fs); err != nil {
			return stats, err
		}

		if i < len(config.Fields) {
			fieldName := config.Fields[i].Name
			stats.FieldStats[fieldName] = FieldStats{
				DiskSize:      fs.DiskSize,
				NullCount:     fs.NullCount,
				DistinctCount: fs.DistinctCount,
				MinValue:      fs.MinValue,
				MaxValue:      fs.MaxValue,
			}
		}
	}

	return stats, nil
}

func convertToFloat64(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	case float32:
		return float64(v), true
	case float64:
		return v, true
	default:
		return 0, false
	}
}
