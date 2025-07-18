package postgresql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/nbd-wtf/go-nostr"
)

func (b *PostgresBackend) QueryEvents(ctx context.Context, filter nostr.Filter) (ch chan *nostr.Event, err error) {
	// 尝试从 context 中获取用户 pubkey
	userPubkey := ""
	if pubkey, ok := ctx.Value("userPubkey").(string); ok {
		userPubkey = pubkey
	}
	
	query, params, err := b.queryEventsSql(filter, false, userPubkey)
	if err != nil {
		return nil, err
	}

	rows, err := b.DB.QueryContext(ctx, query, params...)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to fetch events using query %q: %w", query, err)
	}

	ch = make(chan *nostr.Event)
	go func() {
		defer rows.Close()
		defer close(ch)
		for rows.Next() {
			var evt nostr.Event
			var timestamp int64
			err := rows.Scan(&evt.ID, &evt.PubKey, &timestamp,
				&evt.Kind, &evt.Tags, &evt.Content, &evt.Sig)
			if err != nil {
				return
			}
			evt.CreatedAt = nostr.Timestamp(timestamp)
			select {
			case ch <- &evt:
			case <-ctx.Done():
				return
			}
		}
	}()

	return ch, nil
}

func (b *PostgresBackend) CountEvents(ctx context.Context, filter nostr.Filter) (int64, error) {
	query, params, err := b.queryEventsSql(filter, true, "")
	if err != nil {
		return 0, err
	}

	var count int64
	if err = b.DB.QueryRowContext(ctx, query, params...).Scan(&count); err != nil && err != sql.ErrNoRows {
		return 0, fmt.Errorf("failed to fetch events using query %q: %w", query, err)
	}
	return count, nil
}

func makePlaceHolders(n int) string {
	return strings.TrimRight(strings.Repeat("?,", n), ",")
}

var (
	TooManyIDs       = errors.New("too many ids")
	TooManyAuthors   = errors.New("too many authors")
	TooManyKinds     = errors.New("too many kinds")
	TooManyTagValues = errors.New("too many tag values")
	EmptyTagSet      = errors.New("empty tag set")
)

func (b *PostgresBackend) queryEventsSql(filter nostr.Filter, doCount bool, userPubkey string) (string, []any, error) {
	conditions := make([]string, 0, 7)
	params := make([]any, 0, 20)

	// 先检查是否需要联表查询，这会影响 kinds 条件的处理方式
	needDisappearingJoin := false
	hasDisappearingKinds := false
	
	if !doCount && userPubkey != "" && len(filter.Kinds) > 0 {
		for _, kind := range filter.Kinds {
			if kind == 1404 || (kind == 1059) {
				hasDisappearingKinds = true
				break
			}
		}
		
		// 检查表是否存在，如果不存在则不进行联表查询
		if hasDisappearingKinds {
			// 简单的表存在性检查
			var exists bool
			err := b.DB.QueryRow(`
				SELECT EXISTS (
					SELECT FROM information_schema.tables 
					WHERE table_schema = 'moss_api' 
					AND table_name = 'dismsg_user_status'
				)`).Scan(&exists)
			
			if err == nil && exists {
				needDisappearingJoin = true
			}
			// 如果表不存在或查询失败，则不进行联表查询，回退到普通查询
		}
	}

	// 如果需要联表查询，首先添加 userPubkey 参数
	if needDisappearingJoin {
		params = append(params, userPubkey)
	}

	if len(filter.IDs) > 0 {
		if len(filter.IDs) > b.QueryIDsLimit {
			// too many ids, fail everything
			return "", nil, TooManyIDs
		}

		for _, v := range filter.IDs {
			params = append(params, v)
		}
		conditions = append(conditions, ` event.id IN (`+makePlaceHolders(len(filter.IDs))+`)`)
	}

	if len(filter.Authors) > 0 {
		if len(filter.Authors) > b.QueryAuthorsLimit {
			// too many authors, fail everything
			return "", nil, TooManyAuthors
		}

		for _, v := range filter.Authors {
			params = append(params, v)
		}
		conditions = append(conditions, ` event.pubkey IN (`+makePlaceHolders(len(filter.Authors))+`)`)
	}

	// 只有在不需要联表查询时才添加普通的 kind 条件
	// 如果需要联表查询，kinds 条件将在后面的 extraConditions 中处理
	if len(filter.Kinds) > 0 && !needDisappearingJoin {
		if len(filter.Kinds) > b.QueryKindsLimit {
			// too many kinds, fail everything
			return "", nil, TooManyKinds
		}

		for _, v := range filter.Kinds {
			params = append(params, v)
		}
		conditions = append(conditions, `event.kind IN (`+makePlaceHolders(len(filter.Kinds))+`)`)
	} else if len(filter.Kinds) > 0 && needDisappearingJoin {
		// 检查 kinds 数量限制，但不添加到条件中（将在后面处理）
		if len(filter.Kinds) > b.QueryKindsLimit {
			return "", nil, TooManyKinds
		}
	}

	totalTags := 0
	for _, values := range filter.Tags {
		if len(values) == 0 {
			// any tag set to [] is wrong
			return "", nil, EmptyTagSet
		}

		for _, tagValue := range values {
			params = append(params, tagValue)
		}

		// each separate tag key is an independent condition
		conditions = append(conditions, `event.tagvalues && ARRAY[`+makePlaceHolders(len(values))+`]`)

		totalTags += len(values)
	}

	if filter.Since != nil {
		conditions = append(conditions, `event.created_at >= ?`)
		params = append(params, filter.Since)
	}
	if filter.Until != nil {
		conditions = append(conditions, `event.created_at <= ?`)
		params = append(params, filter.Until)
	}
	if filter.Search != "" {
		conditions = append(conditions, `event.content LIKE ?`)
		params = append(params, `%`+strings.ReplaceAll(filter.Search, `%`, `\%`)+`%`)
	}

	if len(conditions) == 0 {
		// fallback
		conditions = append(conditions, `true`)
	}

	// 构建基础 FROM 子句和额外的条件
	fromClause := "event"
	extraConditions := make([]string, 0)
	
	if needDisappearingJoin {
		fromClause = "event LEFT JOIN moss_api.dismsg_user_status dus ON event.id = dus.event_id AND dus.user_pubkey = ?"
		
		// 对于混合类型查询，我们需要更精确的条件：
		// 1. 非阅后即焚事件（如 kind 4）：正常显示
		// 2. 阅后即焚事件：只显示未过期的
		disappearingConditions := make([]string, 0)
		normalConditions := make([]string, 0)
		
		for _, kind := range filter.Kinds {
			if kind == 1404 {
				// 1404 事件：必须未过期
				disappearingConditions = append(disappearingConditions, 
					"(event.kind = 1404 AND (dus.burn_at IS NULL OR dus.burn_at > NOW()))")
			} else if kind == 1059 {
				// 1059 事件且 k=3048：必须未过期
				disappearingConditions = append(disappearingConditions, 
					"(event.kind = 1059 AND event.tagvalues && ARRAY['3048'] AND (dus.burn_at IS NULL OR dus.burn_at > NOW()))")
			} else {
				// 普通事件：正常显示
				normalConditions = append(normalConditions, fmt.Sprintf("event.kind = %d", kind))
			}
		}
		
		// 组合条件：普通事件 OR 未过期的阅后即焚事件
		combinedConditions := make([]string, 0)
		if len(normalConditions) > 0 {
			combinedConditions = append(combinedConditions, "("+strings.Join(normalConditions, " OR ")+")")
		}
		if len(disappearingConditions) > 0 {
			combinedConditions = append(combinedConditions, "("+strings.Join(disappearingConditions, " OR ")+")")
		}
		
		if len(combinedConditions) > 0 {
			extraConditions = append(extraConditions, "("+strings.Join(combinedConditions, " OR ")+")")
		}
	}

	// 合并所有条件
	allConditions := append(conditions, extraConditions...)

	if filter.Limit < 1 || filter.Limit > b.QueryLimit {
		params = append(params, b.QueryLimit)
	} else {
		params = append(params, filter.Limit)
	}

	var query string
	if doCount {
		query = sqlx.Rebind(sqlx.BindType("postgres"), `SELECT
          COUNT(*)
        FROM `+fromClause+` WHERE `+
			strings.Join(allConditions, " AND ")+
			" LIMIT ?")
	} else {
		query = sqlx.Rebind(sqlx.BindType("postgres"), `SELECT
          event.id, event.pubkey, event.created_at, event.kind, event.tags, event.content, event.sig
        FROM `+fromClause+` WHERE `+
			strings.Join(allConditions, " AND ")+
			" ORDER BY event.created_at DESC, event.id LIMIT ?")
	}

	return query, params, nil
}
