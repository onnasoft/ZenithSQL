package executor

import (
	"context"

	"github.com/onnasoft/ZenithSQL/io/response"
	"github.com/onnasoft/ZenithSQL/io/statement"
)

func (e *DefaultExecutor) executeUpdate(ctx context.Context, stmt *statement.UpdateStatement) response.Response {
	return response.NewUpdateResponse(false, "not implemented")
}
