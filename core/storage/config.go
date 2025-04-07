package storage

import (
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	configFileName = "config.json"
	statsFileName  = "stats.bin"
)

type TableConfig struct {
	Fields []FieldMeta  `json:"fields"`
	Stats  StorageStats `json:"-"`
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

	if err := binary.Write(file, binary.LittleEndian, config.Stats); err != nil {
		return err
	}

	return nil
}

func (cm *ConfigManager) LoadTableConfig(tableName string) (TableConfig, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

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

	temp := storageStats{
		TotalRows:    stats.TotalRows,
		TotalSize:    stats.TotalSize,
		LastModified: stats.LastModified.Unix(),
	}
	if err := binary.Write(file, binary.LittleEndian, temp); err != nil {
		return err
	}

	return nil
}

func (cm *ConfigManager) LoadStats(tableName string) (StorageStats, error) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	var stats StorageStats
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

	temp := storageStats{}
	if err := binary.Read(file, binary.LittleEndian, &temp); err != nil {
		return stats, err
	}

	stats.TotalRows = temp.TotalRows
	stats.TotalSize = temp.TotalSize
	stats.LastModified = time.Unix(temp.LastModified, 0)

	return stats, nil
}
