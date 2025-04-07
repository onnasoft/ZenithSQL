package executor

import (
	"context"
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

	if len(value) <= 100 {
		if err := writer.Commit(); err != nil {
			writer.Rollback()
			return err
		}
	} else {
		if err := writer.Flush(); err != nil {
			writer.Rollback()
			return err
		}
	}

	if err := writer.Close(); err != nil {
		return err
	}

	return nil
}

func insert(table *catalog.Table, value ...map[string]interface{}) (storage.Writer, error) {
	now := time.Now()

	writer, err := table.Writer(context.Background())
	if err != nil {
		return nil, err
	}

	for _, row := range value {
		row["created_at"] = now
		row["updated_at"] = now

		id := table.GetNextID()
		row["id"] = id

		if err := writer.Write(row); err != nil {
			return nil, err
		}
	}

	return writer, nil
}
