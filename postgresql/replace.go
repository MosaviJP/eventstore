package postgresql

import (
	"context"

	"github.com/nbd-wtf/go-nostr"
)

func (b *PostgresBackend) ReplaceEvent(ctx context.Context, evt *nostr.Event) error {
	// 内部直接调用 SaveEvents（保持语义，简化分支）
	return b.SaveEvents(ctx, []*nostr.Event{evt})
}
