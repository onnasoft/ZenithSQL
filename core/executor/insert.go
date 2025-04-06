package executor

import (
	"time"

	"github.com/onnasoft/ZenithSQL/model/catalog"
	"github.com/onnasoft/ZenithSQL/model/record"
)

func Insert(table *catalog.Table, e ...*record.Row) error {
	table.LockInsert()
	defer table.UnlockInsert()

	if err := insert(table, e...); err != nil {
		return err
	}

	if err := table.BufStats.Sync(); err != nil {
		return err
	}

	if err := table.BufMeta.Sync(); err != nil {
		return err
	}

	if err := table.SaveStats(); err != nil {
		return err
	}

	return nil
}

func insert(table *catalog.Table, e ...*record.Row) error {
	now := time.Now()

	for _, row := range e {
		if err := row.Meta.SetValue("created_at", now); err != nil {
			return err
		}

		if err := row.Meta.SetValue("updated_at", now); err != nil {
			return err
		}

		id := table.GetNextID()
		if err := row.SetID(id); err != nil {
			return err
		}

		if err := row.Meta.Save(); err != nil {
			return err
		}

		if err := row.Data.Save(); err != nil {
			return err
		}
	}

	return nil
}
