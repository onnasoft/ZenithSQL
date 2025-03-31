package dataframe

import (
	"encoding/binary"
	"fmt"
	"math"
	"time"
)

type Row struct {
	Columns *Columns
	values  []interface{}
}

func NewRow(schema *Columns) *Row {
	return &Row{
		Columns: schema,
		values:  make([]interface{}, schema.Len()),
	}
}

func (r Row) Get(index int) interface{} {
	if index < 0 || index >= len(r.values) {
		return nil
	}
	return r.values[index]
}

func (r Row) Set(index int, value interface{}) {
	if index < 0 || index >= len(r.values) {
		return
	}
	r.values[index] = value
}

func (r Row) String() string {
	var format = "%s: %v, "
	var result string
	result += fmt.Sprintf(format, (*r.Columns)[0].Name, r.Get(0))
	for i := 4; i < len(r.values); i++ {
		result += fmt.Sprintf(format, (*r.Columns)[i].Name, r.Get(i))
	}
	result += fmt.Sprintf(format, (*r.Columns)[1].Name, r.Get(1))
	result += fmt.Sprintf(format, (*r.Columns)[2].Name, r.Get(2))
	result += fmt.Sprintf("%s: %v", (*r.Columns)[3].Name, r.Get(3))
	return result
}

var parseTypes = map[DataType]func([]byte) interface{}{
	Int8Type: func(b []byte) interface{} {
		return int8(b[0])
	},
	Int16Type: func(b []byte) interface{} {
		return int16(binary.LittleEndian.Uint16(b))
	},
	Int32Type: func(b []byte) interface{} {
		return int32(binary.LittleEndian.Uint32(b))
	},
	Int64Type: func(b []byte) interface{} {
		return int64(binary.LittleEndian.Uint64(b))
	},
	Uint8Type: func(b []byte) interface{} {
		return uint8(b[0])
	},
	Uint16Type: func(b []byte) interface{} {
		return uint16(binary.LittleEndian.Uint16(b))
	},
	Uint32Type: func(b []byte) interface{} {
		return uint32(binary.LittleEndian.Uint32(b))
	},
	Uint64Type: func(b []byte) interface{} {
		return uint64(binary.LittleEndian.Uint64(b))
	},
	Float32Type: func(b []byte) interface{} {
		return math.Float32frombits(binary.LittleEndian.Uint32(b))
	},
	Float64Type: func(b []byte) interface{} {
		return math.Float64frombits(binary.LittleEndian.Uint64(b))
	},
	StringType: func(b []byte) interface{} {
		return string(b)
	},
	TimestampType: func(b []byte) interface{} {
		return time.Unix(0, int64(binary.LittleEndian.Uint64(b))).Format(time.RFC3339)
	},
}

func (r *Row) Write(buffer []byte) error {
	for i, col := range *r.Columns {
		val := r.Get(i)

		// Write null flag
		isNull := writeNullFlag(buffer, col.NullFlagPos, val)
		if isNull == 0 {
			if err := writeValue(buffer, col, val); err != nil {
				return err
			}
		}
	}
	return nil
}

func writeNullFlag(buffer []byte, nullFlagPos int, val interface{}) byte {
	var isNull byte
	if val == nil {
		isNull = 1
	}
	buffer[nullFlagPos] = isNull
	return isNull
}

func writeValue(buffer []byte, col Column, val interface{}) error {
	switch col.Type {
	case Int8Type:
		buffer[col.StartPosition] = uint8(val.(int64))
	case Int16Type:
		binary.LittleEndian.PutUint16(buffer[col.StartPosition:], uint16(val.(int64)))
	case Int32Type:
		binary.LittleEndian.PutUint32(buffer[col.StartPosition:], uint32(val.(int64)))
	case Int64Type:
		binary.LittleEndian.PutUint64(buffer[col.StartPosition:], uint64(val.(int64)))
	case Uint8Type:
		buffer[col.StartPosition] = uint8(val.(int64))
	case Uint16Type:
		binary.LittleEndian.PutUint16(buffer[col.StartPosition:], uint16(val.(int64)))
	case Uint32Type:
		binary.LittleEndian.PutUint32(buffer[col.StartPosition:], uint32(val.(int64)))
	case Uint64Type:
		binary.LittleEndian.PutUint64(buffer[col.StartPosition:], uint64(val.(int64)))
	case Float32Type:
		binary.LittleEndian.PutUint32(buffer[col.StartPosition:], math.Float32bits(val.(float32)))
	case Float64Type:
		binary.LittleEndian.PutUint64(buffer[col.StartPosition:], math.Float64bits(val.(float64)))
	case StringType:
		str := val.(string)
		if len(str) > col.Length {
			return fmt.Errorf("string length exceeds maximum length of %d", col.Length)
		}
		copy(buffer[col.StartPosition:], str)
		for j := len(str); j < col.Length; j++ {
			buffer[col.StartPosition+j] = 0
		}
	case TimestampType:
		binary.LittleEndian.PutUint64(buffer[col.StartPosition:], uint64(val.(int64)))
	default:
		return fmt.Errorf("unsupported type %s for column %s", col.Type.String(), col.Name)
	}
	return nil
}

func (r *Row) Read(buffer []byte) error {
	for i, col := range *r.Columns {
		isNull := buffer[col.NullFlagPos]
		if isNull == 1 {
			r.Set(i, nil)
			continue
		}

		startPos := col.StartPosition
		parser, ok := parseTypes[col.Type]
		if !ok {
			return fmt.Errorf("unsupported data type: %s", col.Type)
		}
		value := parser(buffer[startPos : startPos+col.Length])
		if value == nil {
			return fmt.Errorf("failed to parse value for column %s", col.Name)
		}
		r.Set(i, value)
	}

	return nil
}
