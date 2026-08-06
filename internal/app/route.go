package app

import (
	"context"

	"github.com/caduceus/caduceus/internal/tasks"
)

func (a *App) ExplainRoute(ctx context.Context, req tasks.Request) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return a.Node.ExplainRoute(req)
}
