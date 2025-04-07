package executor

import (
	"time"

	"github.com/onnasoft/ZenithSQL/core/storage"
	"github.com/onnasoft/ZenithSQL/model/catalog"
)

func Insert(table *catalog.Table, value ...map[string]interface{}) error {
	table.LockInsert()
	defer table.UnlockInsert()

	writer, err := insert(table, value...)
	if err != nil {
		return err
	}
	defer writer.Close()

	if err := writer.Commit(); err != nil {
		writer.Rollback()
		return err
	}

	if err := table.UpdateRowCount(table.RowCount() + int64(len(value))); err != nil {
		writer.Rollback()
		return err
	}

	return nil
}

func insert(table *catalog.Table, value ...map[string]interface{}) (storage.Writer, error) {
	now := time.Now()

	writer, err := table.Writer()
	if err != nil {
		return nil, err
	}

	id := table.GetNextID()
	for _, row := range value {
		row["created_at"] = now
		row["updated_at"] = now

		row["id"] = id

		if err := writer.Write(row); err != nil {
			return nil, err
		}

		id++
	}

	return writer, nil
}
