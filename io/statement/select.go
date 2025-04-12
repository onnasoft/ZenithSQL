package statement

import (
	"fmt"
	"strings"

	"github.com/asaskevich/govalidator"
	"github.com/onnasoft/ZenithSQL/io/filters"
	"github.com/onnasoft/ZenithSQL/io/protocol"
	"github.com/onnasoft/ZenithSQL/model/aggregate"
	"github.com/vmihailenco/msgpack/v5"
)

type Aggregation struct {
	Function aggregate.AggregateType `msgpack:"function" valid:"required,matches(^(SUM|AVG|COUNT|MAX|MIN|GROUP_CONCAT)$)"`
	Column   string                  `msgpack:"column" valid:"required"`
	Alias    string                  `msgpack:"alias"`
}

type SelectStatement struct {
	Database     string          `msgpack:"database" valid:"required,alphanumunderscore"`
	Schema       string          `msgpack:"schema" valid:"required,alphanumunderscore"`
	TableName    string          `msgpack:"table_name" valid:"required,alphanumunderscore"`
	Columns      []string        `msgpack:"columns"`
	Aggregations []Aggregation   `msgpack:"aggregations"`
	Where        *filters.Filter `msgpack:"where"`
	GroupBy      []string        `msgpack:"group_by"`
	Having       string          `msgpack:"having"`
	Limit        uint64          `msgpack:"limit"`
	Offset       uint64          `msgpack:"offset"`
	OrderBy      []string        `msgpack:"order_by"`
}

type SelectStatementConfig struct {
	Database     string
	Schema       string
	TableName    string
	Columns      []string
	Aggregations []Aggregation
	Where        *filters.Filter
	GroupBy      []string
	Having       string
	Limit        uint64
	Offset       uint64
	OrderBy      []string
}

func NewSelectStatement(cfg SelectStatementConfig) (*SelectStatement, error) {
	stmt := &SelectStatement{
		Database:     strings.TrimSpace(cfg.Database),
		Schema:       strings.TrimSpace(cfg.Schema),
		TableName:    strings.TrimSpace(cfg.TableName),
		Columns:      cleanStrings(cfg.Columns),
		Aggregations: cfg.Aggregations,
		Where:        cfg.Where,
		GroupBy:      cleanStrings(cfg.GroupBy),
		Having:       strings.TrimSpace(cfg.Having),
		Limit:        cfg.Limit,
		Offset:       cfg.Offset,
		OrderBy:      cleanStrings(cfg.OrderBy),
	}

	if err := stmt.validate(); err != nil {
		return nil, err
	}

	return stmt, nil
}

func (s *SelectStatement) validate() error {
	if _, err := govalidator.ValidateStruct(s); err != nil {
		return fmt.Errorf("invalid statement: %w", err)
	}

	if len(s.Columns) == 0 && len(s.Aggregations) == 0 {
		return fmt.Errorf("must specify columns or aggregations")
	}

	for _, agg := range s.Aggregations {
		if _, err := govalidator.ValidateStruct(agg); err != nil {
			return fmt.Errorf("invalid aggregation: %w", err)
		}
	}

	return nil
}

func cleanStrings(items []string) []string {
	cleaned := make([]string, 0, len(items))
	for _, item := range items {
		trimmed := strings.TrimSpace(item)
		if trimmed != "" {
			cleaned = append(cleaned, trimmed)
		}
	}
	return cleaned
}

func (s SelectStatement) Protocol() protocol.MessageType {
	return protocol.Select
}

func (s SelectStatement) ToBytes() ([]byte, error) {
	return msgpack.Marshal(s)
}

func (s *SelectStatement) FromBytes(data []byte) error {
	if err := msgpack.Unmarshal(data, s); err != nil {
		return err
	}
	return s.validate()
}

func (s SelectStatement) String() string {
	var sb strings.Builder
	sb.WriteString("SELECT ")

	if len(s.Columns) > 0 {
		sb.WriteString(strings.Join(s.Columns, ", "))
	}

	if len(s.Aggregations) > 0 {
		if len(s.Columns) > 0 {
			sb.WriteString(", ")
		}
		for i, agg := range s.Aggregations {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(fmt.Sprintf("%s(%s)", agg.Function, agg.Column))
			if agg.Alias != "" {
				sb.WriteString(" AS " + agg.Alias)
			}
		}
	}

	sb.WriteString(fmt.Sprintf(" FROM %s.%s.%s", s.Database, s.Schema, s.TableName))

	if s.Where != nil {
		sql, _, err := s.Where.Build()
		if err == nil && sql != "" {
			sb.WriteString(" WHERE " + sql)
		}
	}

	if len(s.GroupBy) > 0 {
		sb.WriteString(" GROUP BY " + strings.Join(s.GroupBy, ", "))
		if s.Having != "" {
			sb.WriteString(" HAVING " + s.Having)
		}
	}

	if len(s.OrderBy) > 0 {
		sb.WriteString(" ORDER BY " + strings.Join(s.OrderBy, ", "))
	}

	if s.Limit > 0 {
		sb.WriteString(fmt.Sprintf(" LIMIT %d", s.Limit))
	}

	if s.Offset > 0 {
		sb.WriteString(fmt.Sprintf(" OFFSET %d", s.Offset))
	}

	return sb.String()
}

func (s *SelectStatement) GetFullTableName() string {
	return fmt.Sprintf("%s.%s.%s", s.Database, s.Schema, s.TableName)
}
