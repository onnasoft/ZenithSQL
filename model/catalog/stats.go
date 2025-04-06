package catalog

import (
	"github.com/onnasoft/ZenithSQL/model/entity"
)

func initStatsSchema() *entity.Schema {
	statsSchema := entity.NewSchema()
	statsSchema.AddField(NewFieldUInt64("rows"))
	statsSchema.AddField(NewFieldInt32("row_size"))
	statsSchema.AddField(NewFieldTimestamp("created_at"))
	statsSchema.AddField(NewFieldTimestamp("updated_at"))
	statsSchema.Lock()

	return statsSchema
}
