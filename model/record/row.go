package record

import (
	"fmt"

	"github.com/onnasoft/ZenithSQL/model/entity"
)

type Row struct {
	ID   uint64
	Data *entity.Entity
	Meta *entity.Entity
}

func NewRow(id uint64, data, meta *entity.Entity) *Row {
	return &Row{
		ID:   id,
		Data: data,
		Meta: meta,
	}
}

func (row *Row) GetID() uint64 {
	return row.ID
}

func (row *Row) SetID(id uint64) error {
	err := row.Meta.SetValue("id", id)
	if err != nil {
		return err
	}
	row.ID = id
	row.Data.RW.Seek(row.Data.Schema.Size() * int(id))
	row.Meta.RW.Seek(row.Meta.Schema.Size() * int(id))

	return nil
}

func (row *Row) String() string {
	return fmt.Sprintf("Row ID: %d, Data: %s, Meta: %s", row.ID, row.Data.String(), row.Meta.String())
}
