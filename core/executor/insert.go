package executor

import (
	"time"

	"github.com/onnasoft/ZenithSQL/model/catalog"
	"github.com/onnasoft/ZenithSQL/model/record"
)

func Insert(table *catalog.Table, e ...*record.Row) error {
	table.InsertLock()
	defer table.InsertUnlock()

	now := time.Now()
	id := table.GetNextID()
	for _, row := range e {
		if err := row.SetID(id); err != nil {
			return err
		}

		if err := row.Meta.SetValue("created_at", now); err != nil {
			return err
		}

		if err := row.Meta.SetValue("updated_at", now); err != nil {
			return err
		}

		if err := row.Meta.Save(); err != nil {
			return err
		}

		if err := row.Data.Save(); err != nil {
			return err
		}
		id++
	}

	table.SetRows(id)
	table.SaveStats()

	return nil
}
