package executor

import (
	"context"
	"fmt"

	"github.com/onnasoft/ZenithSQL/io/statement"
)

func (e *DefaultExecutor) executeUpdate(ctx context.Context, stmt *statement.UpdateStatement) (any, error) {
	return nil, fmt.Errorf("not implemented")
}
