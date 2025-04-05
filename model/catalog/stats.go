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
	stats := t.stats

	if stats == nil {
		stats = &TableStats{CreatedAt: time.Now()}
		t.stats = stats
	}

	stats.UpdatedAt = time.Now()
	stats.Rows = t.rows.Load()
	stats.RowSize = t.rowSize.Load()

	data, err := json.MarshalIndent(stats, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(t.StatsFile, data, 0644)
}

func (t *Table) InitStats(rowSize uint64) error {
	stats := &TableStats{
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	stats.Rows = t.rows.Load()
	stats.RowSize = rowSize
	t.stats = stats
	return t.SaveStats()
}
