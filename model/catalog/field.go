package catalog

import (
	"github.com/onnasoft/ZenithSQL/model/entity"
	"github.com/onnasoft/ZenithSQL/validate"
)

func NewField(name string, dataType entity.DataType, length int, validators ...validate.Validator) *entity.Field {
	return &entity.Field{
		Name:       name,
		Type:       dataType,
		Length:     length,
		Validators: validators,
	}
}

func NewFieldInt8(name string, validators ...validate.Validator) *entity.Field {
	return NewField(name, entity.Int8Type, 1, validators...)
}

func NewFieldInt16(name string, validators ...validate.Validator) *entity.Field {
	return NewField(name, entity.Int16Type, 2, validators...)
}

func NewFieldInt32(name string, validators ...validate.Validator) *entity.Field {
	return NewField(name, entity.Int32Type, 4, validators...)
}

func NewFieldUInt8(name string, validators ...validate.Validator) *entity.Field {
	return NewField(name, entity.Uint8Type, 1, validators...)
}

func NewFieldUInt16(name string, validators ...validate.Validator) *entity.Field {
	return NewField(name, entity.Uint16Type, 2, validators...)
}

func NewFieldUInt32(name string, validators ...validate.Validator) *entity.Field {
	return NewField(name, entity.Uint32Type, 4, validators...)
}

func NewFieldInt64(name string, validators ...validate.Validator) *entity.Field {
	return NewField(name, entity.Int64Type, 8, validators...)
}

func NewFieldUInt64(name string, validators ...validate.Validator) *entity.Field {
	return NewField(name, entity.Uint64Type, 8, validators...)
}

func NewFieldUint8(name string, validators ...validate.Validator) *entity.Field {
	return NewField(name, entity.Uint8Type, 1, validators...)
}

func NewFieldUint16(name string, validators ...validate.Validator) *entity.Field {
	return NewField(name, entity.Uint16Type, 2, validators...)
}

func NewFieldUint32(name string, validators ...validate.Validator) *entity.Field {
	return NewField(name, entity.Uint32Type, 4, validators...)
}

func NewFieldUint64(name string, validators ...validate.Validator) *entity.Field {
	return NewField(name, entity.Uint64Type, 8, validators...)
}

func NewFieldFloat32(name string, validators ...validate.Validator) *entity.Field {
	return NewField(name, entity.Float32Type, 4, validators...)
}

func NewFieldFloat64(name string, validators ...validate.Validator) *entity.Field {
	return NewField(name, entity.Float64Type, 8, validators...)
}

func NewFieldString(name string, length int, validators ...validate.Validator) *entity.Field {
	validators = append(validators, &validate.StringLength{Min: 0, Max: length})
	return NewField(name, entity.StringType, length, validators...)
}

func NewFieldBool(name string, validators ...validate.Validator) *entity.Field {
	return NewField(name, entity.BoolType, 1, validators...)
}

func NewFieldTimestamp(name string, validators ...validate.Validator) *entity.Field {
	return NewField(name, entity.TimestampType, 8, validators...)
}
