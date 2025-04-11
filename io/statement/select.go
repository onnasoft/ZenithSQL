package statement

import (
	"fmt"
	"strings"

	"github.com/asaskevich/govalidator"
	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/vmihailenco/msgpack/v5"
)

// SelectStatement representa una consulta SELECT para una sola tabla
type SelectStatement struct {
	Database  string   `msgpack:"database" valid:"required,alphanumunderscore"`
	Schema    string   `msgpack:"schema" valid:"required,alphanumunderscore"`
	TableName string   `msgpack:"table_name" valid:"required,alphanumunderscore"`
	Columns   []string `msgpack:"columns" valid:"required"`
	Where     string   `msgpack:"where"`
	Limit     uint64   `msgpack:"limit"`
	Offset    uint64   `msgpack:"offset"`
	OrderBy   []string `msgpack:"order_by"`
}

type SelectStatementConfig struct {
	Database  string
	Schema    string
	TableName string
	Columns   []string
	Where     string
	Limit     uint64
	Offset    uint64
	OrderBy   []string
}

func NewSelectStatement(cfg SelectStatementConfig) (*SelectStatement, error) {
	stmt := &SelectStatement{
		Database:  strings.TrimSpace(cfg.Database),
		Schema:    strings.TrimSpace(cfg.Schema),
		TableName: strings.TrimSpace(cfg.TableName),
		Columns:   cleanColumns(cfg.Columns),
		Where:     strings.TrimSpace(cfg.Where),
		Limit:     cfg.Limit,
		Offset:    cfg.Offset,
		OrderBy:   cleanOrderBy(cfg.OrderBy),
	}

	if err := stmt.validate(); err != nil {
		return nil, err
	}

	return stmt, nil
}

// validate realiza la validación completa de la estructura
func (s *SelectStatement) validate() error {
	if _, err := govalidator.ValidateStruct(s); err != nil {
		return fmt.Errorf("invalid statement: %w", err)
	}

	if len(s.Columns) == 0 {
		return fmt.Errorf("at least one column must be specified")
	}

	for _, col := range s.Columns {
		if !govalidator.IsAlphanumeric(col) && !strings.Contains(col, ".") {
			return fmt.Errorf("invalid column name: %s", col)
		}
	}

	return nil
}

// cleanColumns limpia y normaliza los nombres de columna
func cleanColumns(columns []string) []string {
	cleaned := make([]string, 0, len(columns))
	for _, col := range columns {
		trimmed := strings.TrimSpace(col)
		if trimmed != "" {
			cleaned = append(cleaned, trimmed)
		}
	}
	return cleaned
}

// cleanOrderBy limpia y normaliza las cláusulas ORDER BY
func cleanOrderBy(orderBy []string) []string {
	cleaned := make([]string, 0, len(orderBy))
	for _, ob := range orderBy {
		trimmed := strings.TrimSpace(ob)
		if trimmed != "" {
			cleaned = append(cleaned, trimmed)
		}
	}
	return cleaned
}

// Protocol implementa la interfaz Message
func (s SelectStatement) Protocol() protocol.MessageType {
	return protocol.Select
}

// ToBytes serializa el statement a bytes
func (s SelectStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(s)
}

// FromBytes deserializa el statement desde bytes
func (s *SelectStatement) FromBytes(data []byte) error {
	if err := msgpack.Unmarshal(data, s); err != nil {
		return err
	}
	return s.validate()
}

// String representa el statement como string
func (s SelectStatement) String() string {
	return fmt.Sprintf(
		"SELECT %s FROM %s.%s.%s WHERE %s ORDER BY %s LIMIT %d OFFSET %d",
		strings.Join(s.Columns, ", "),
		s.Database,
		s.Schema,
		s.TableName,
		s.Where,
		strings.Join(s.OrderBy, ", "),
		s.Limit,
		s.Offset,
	)
}

// GetFullTableName devuelve el nombre completo de la tabla con esquema
func (s *SelectStatement) GetFullTableName() string {
	return fmt.Sprintf("%s.%s.%s", s.Database, s.Schema, s.TableName)
}
