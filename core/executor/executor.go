package executor

import (
	"context"
	"errors"

	"github.com/onnasoft/ZenithSQL/io/statement"
	"github.com/onnasoft/ZenithSQL/model/catalog"
)

var (
	ErrUnsupportedStatement = errors.New("unsupported statement")
)

type Executor interface {
	Execute(ctx context.Context, stmt statement.Statement) (any, error)
}

type DefaultExecutor struct {
	catalog *catalog.Catalog
}

func New(catalog *catalog.Catalog) *DefaultExecutor {
	return &DefaultExecutor{
		catalog: catalog,
	}
}

func (e *DefaultExecutor) Execute(ctx context.Context, stmt statement.Statement) (any, error) {
	switch s := stmt.(type) {
	case *statement.CreateTableStatement:
		return e.executeCreateTable(ctx, s)
	case *statement.DropTableStatement:
		return e.executeDropTable(ctx, s)
	case *statement.TruncateTableStatement:
		return e.executeTruncateTable(ctx, s)
	case *statement.ImportStatement:
		return e.executeImport(ctx, s)
	case *statement.InsertStatement:
		return e.executeInsert(ctx, s)
	case *statement.UpdateStatement:
		return e.executeUpdate(ctx, s)
	case *statement.SelectStatement:
		return e.executeSelect(ctx, s)
	}

	return nil, ErrUnsupportedStatement
}
