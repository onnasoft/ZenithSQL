package catalog

import (
	"encoding/json"
	"os"
	"time"
)

type TableStats struct {
	Rows      uint64    `json:"rows"`
	RowSize   uint64    `json:"row_size"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (t *Table) SaveStats() error {
	stats := t.Stats

	if stats == nil {
		stats = &TableStats{CreatedAt: time.Now()}
		t.Stats = stats
	}

	stats.UpdatedAt = time.Now()
	stats.Rows = t.RowCount.Load()
	stats.RowSize = t.RowSize.Load()

	data, err := json.MarshalIndent(stats, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(t.PathStats, data, 0644)
}

func (t *Table) InitStats(rowSize uint64) error {
	stats := &TableStats{
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	stats.Rows = t.RowCount.Load()
	stats.RowSize = rowSize
	t.Stats = stats
	return t.SaveStats()
}
