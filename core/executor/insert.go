package executor

import (
	"fmt"
	"time"

	"github.com/onnasoft/ZenithSQL/model/catalog"
	"github.com/onnasoft/ZenithSQL/model/entity"
)

func Insert(table *catalog.Table, e ...*entity.Entity) error {
	table.InsertLock()
	defer table.InsertUnlock()

	id := table.GetNextID()
	for _, entity := range e {

		now := time.Now()
		metaData := table.MakeMeta(id)
		metaData.SetValue("id", id)
		metaData.SetValue("created_at", now)
		metaData.SetValue("updated_at", now)

		offset := metaData.Schema.Size() * int(id)
		metaData.RW.Seek(offset)

		if err := metaData.Save(); err != nil {
			return err
		}

		offset = entity.Schema.Size() * int(id)
		entity.RW.Seek(offset)
		if err := entity.Save(); err != nil {
			return err
		}
		id++

		fmt.Println("Insert ID:", id)
		fmt.Println("Insert Entity:", entity)
	}

	table.SetRows(id)
	table.SaveStats()

	return nil
}
