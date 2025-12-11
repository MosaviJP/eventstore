package postgresql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
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

	// 打印实际执行的SQL语句（嵌入参数）
	log.Printf("Executing SQL Query: %s", formatSQLWithParams(query, params))
	// log.Printf("filters: Kinds=%v, tags=%v", filter.Kinds, filter.Tags)

	if ctx == nil {
		fmt.Printf("QueryEvents: context is nil for filter: %v\n", filter)
	} else if ctx.Err() != nil {
		fmt.Printf("QueryEvents: context error: %v for filter: %v\n", ctx.Err(), filter)
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

	fmt.Printf("RO-DB has %d connections\n", b.DB.Stats().OpenConnections)
	return ch, nil
}

func (b *PostgresBackend) CountEvents(ctx context.Context, filter nostr.Filter) (int64, error) {
	query, params, err := b.queryEventsSql(filter, true, "")
	if err != nil {
		return 0, err
	}

	// 打印实际执行的SQL语句（嵌入参数）
	log.Printf("Executing Count SQL Query: %s", formatSQLWithParams(query, params))

	var count int64
	if err = b.DB.QueryRowContext(ctx, query, params...).Scan(&count); err != nil && err != sql.ErrNoRows {
		return 0, fmt.Errorf("failed to fetch events using query %q: %w", query, err)
	}
	return count, nil
}

func makePlaceHolders(n int) string {
	return strings.TrimRight(strings.Repeat("?,", n), ",")
}

// formatSQLWithParams 将参数嵌入到SQL语句中，用于调试输出
func formatSQLWithParams(query string, params []any) string {
	result := query
	for i, param := range params {
		placeholder := "$" + fmt.Sprintf("%d", i+1)
		var valueStr string
		
		switch v := param.(type) {
		case string:
			valueStr = fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "''"))
		case int, int64, int32:
			valueStr = fmt.Sprintf("%d", v)
		case float64, float32:
			valueStr = fmt.Sprintf("%f", v)
		case bool:
			valueStr = fmt.Sprintf("%t", v)
		case *nostr.Timestamp:
			if v != nil {
				valueStr = fmt.Sprintf("%d", int64(*v))
			} else {
				valueStr = "NULL"
			}
		case nostr.Timestamp:
			valueStr = fmt.Sprintf("%d", int64(v))
		default:
			valueStr = fmt.Sprintf("'%v'", v)
		}
		
		result = strings.Replace(result, placeholder, valueStr, 1)
	}
	return result
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

	// 判断 tagValue 中是否包含 "3048"
	has3048 := false
	for _, values := range filter.Tags {
		for _, tagValue := range values {
			if tagValue == "3048" {
				has3048 = true
				break
			}
		}
		if has3048 {
			break
		}
	}

	// 精细判断是否需要联表：存在 kind==1404/1405，或 kind==1059 且 tagValue 包含3048，都必须联表
	needDisappearingJoin := false
	if len(filter.Kinds) > 0 {
		for _, kind := range filter.Kinds {
			if kind == 1404 || kind == 1405 || (kind == 1059 && has3048) {
				if userPubkey == "" {
					return "", nil, fmt.Errorf("user pubkey required for kinds 1404/1405 (or 1059 with k=3048)")
				}

				var exists bool
				if err := b.DB.QueryRow(`
					SELECT EXISTS (
						SELECT FROM information_schema.tables 
						WHERE table_schema = 'moss_api' 
						AND table_name = 'dismsg_user_status'
					)`).Scan(&exists); err != nil {
					return "", nil, fmt.Errorf("failed to check moss_api.dismsg_user_status existence: %w", err)
				}
				if !exists {
					return "", nil, fmt.Errorf("table moss_api.dismsg_user_status not found; required for kinds 1404/1405")
				}

				needDisappearingJoin = true
				break
			}
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

	   normalTagConditions := make([]string, 0)
	   kTagValues := []string{}
	   for key, values := range filter.Tags {
			   if len(values) == 0 {
					   return "", nil, EmptyTagSet
			   }
			   if key == "k" {
					   for _, tagValue := range values {
							   kTagValues = append(kTagValues, tagValue)
					   }
			   } else {
					   tagPlaceholders := make([]string, 0, len(values))
					   for _, tagValue := range values {
							   params = append(params, tagValue)
							   tagPlaceholders = append(tagPlaceholders, "?")
					   }
					   if len(tagPlaceholders) > 0 {
							   normalTagConditions = append(normalTagConditions, `event.tagvalues && ARRAY[`+strings.Join(tagPlaceholders, ",")+`]`)
					   }
			   }
	   }

	   // k标签处理逻辑
	   kTagCondition := ""
	   if len(kTagValues) > 0 {
			if needDisappearingJoin && has3048 {
				// 有3048，非3048的k标签 OR (3048且未过期)
				non3048 := make([]string, 0)
				for _, v := range kTagValues {
					if v != "3048" {
						non3048 = append(non3048, v)
					}
				}
				orParts := make([]string, 0)
				if len(non3048) > 0 {
					non3048Quoted := make([]string, 0, len(non3048))
					for _, v := range non3048 {
						non3048Quoted = append(non3048Quoted, "'"+v+"'")
					}
					orParts = append(orParts, `event.tagvalues && ARRAY[`+strings.Join(non3048Quoted, ",")+`]`)
				}
				// 3048且未过期
				orParts = append(orParts, `(event.tagvalues && ARRAY['3048'] AND (dus.burn_at IS NULL OR dus.burn_at > NOW()))`)
				kTagCondition = "(" + strings.Join(orParts, " OR ") + ")"
			} else {
					// 没有3048，所有k标签和其他标签一样AND
					tagPlaceholders := make([]string, 0, len(kTagValues))
					for _, tagValue := range kTagValues {
							params = append(params, tagValue)
							tagPlaceholders = append(tagPlaceholders, "?")
					}
					if len(tagPlaceholders) > 0 {
							kTagCondition = `event.tagvalues && ARRAY[` + strings.Join(tagPlaceholders, ",") + "]"
					}
			}
	   }
	// 合并k标签和其他标签
	conditions = append(conditions, normalTagConditions...)
	if kTagCondition != "" {
			conditions = append(conditions, kTagCondition)
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

	// 过滤已过期的事件（expiration标签）
	// 只选择没有expiration标签，或者expiration标签值大于当前时间戳的事件
	conditions = append(conditions, `(
		NOT EXISTS (
			SELECT 1 FROM jsonb_array_elements(event.tags) AS tag_elem
			WHERE jsonb_array_length(tag_elem) >= 2 
			AND tag_elem->>0 = 'expiration'
			AND (tag_elem->>1)::bigint <= EXTRACT(EPOCH FROM NOW())
		)
	)`)

	if len(conditions) == 0 {
		// fallback
		conditions = append(conditions, `true`)
	}

	// 构建基础 FROM 子句和额外的条件
	fromClause := "event"
	extraConditions := make([]string, 0)
	
	if needDisappearingJoin {
		fromClause = "event LEFT JOIN moss_api.dismsg_user_status dus ON event.id = dus.event_id AND dus.user_pubkey = ?"
	}

	   normalKindConditions := make([]string, 0)
	   disappearingConditions := make([]string, 0)
	   for _, kind := range filter.Kinds {
			   switch kind {
			   case 1404:
					   disappearingConditions = append(disappearingConditions,
							   "(event.kind = 1404 AND (dus.burn_at IS NULL OR dus.burn_at > NOW()))")
			   case 1405:
					   disappearingConditions = append(disappearingConditions,
							   "(event.kind = 1405 AND (dus.burn_at IS NULL OR dus.burn_at > NOW()))")
			   case 1059:
					   // 1059的tag条件已在上面处理，这里只拼kind
					   normalKindConditions = append(normalKindConditions, "event.kind = 1059")
			   default:
					   normalKindConditions = append(normalKindConditions, fmt.Sprintf("event.kind = %d", kind))
			   }
	   }

	combinedConditions := make([]string, 0)
	if len(normalKindConditions) > 0 {
		combinedConditions = append(combinedConditions, "("+strings.Join(normalKindConditions, " OR ")+")")
	}
	if len(disappearingConditions) > 0 {
		combinedConditions = append(combinedConditions, "("+strings.Join(disappearingConditions, " OR ")+")")
	}
	if len(combinedConditions) > 0 {
		extraConditions = append(extraConditions, "("+strings.Join(combinedConditions, " OR ")+")")
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
