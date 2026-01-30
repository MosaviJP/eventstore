package postgresql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/nbd-wtf/go-nostr"
)

func (b *PostgresBackend) QueryEvents(ctx context.Context, filter nostr.Filter) (ch chan *nostr.Event, err error) {
	// 尝试从 context 中获取用户 pubkey
	userPubkey := ""
	if pubkey, ok := ctx.Value("userPubkey").(string); ok {
		userPubkey = pubkey
	}

	// 检查是否需要对比查询（仅对kind=1059且有k标签的查询）
	isKind1059Only := len(filter.Kinds) == 1 && filter.Kinds[0] == 1059
	kTagValues := filter.Tags["k"]
	needComparison := b.EnableQueryComparison && isKind1059Only && len(kTagValues) > 0

	if needComparison {
		// 执行双重查询并对比
		return b.queryWithComparison(ctx, filter, userPubkey)
	}

	// 普通查询（使用优化路径）
	query, params, err := b.queryEventsSql(filter, false, userPubkey, true)
	if err != nil {
		return nil, err
	}

	// 打印实际执行的SQL语句（嵌入参数）
	log.Printf("Executing SQL Query: %s%s", formatSQLWithParams(query, params), traceSuffix(ctx))
	// log.Printf("filters: Kinds=%v, tags=%v", filter.Kinds, filter.Tags)

	if ctx == nil {
		fmt.Printf("QueryEvents: context is nil for filter: %v%s\n", filter, traceSuffix(ctx))
	} else if ctx.Err() != nil {
		fmt.Printf("QueryEvents: context error: %v for filter: %v%s\n", ctx.Err(), filter, traceSuffix(ctx))
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
				log.Printf("Error reading data from database: %s", err)
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

// queryWithComparison 并行执行新旧查询，立即返回配置的查询结果，异步对比
func (b *PostgresBackend) queryWithComparison(ctx context.Context, filter nostr.Filter, userPubkey string) (ch chan *nostr.Event, err error) {
	// 生成新旧查询
	newQuery, newParams, err := b.queryEventsSql(filter, false, userPubkey, true)
	if err != nil {
		return nil, fmt.Errorf("new query generation failed: %w", err)
	}

	oldQuery, oldParams, err := b.queryEventsSql(filter, false, userPubkey, false)
	if err != nil {
		return nil, fmt.Errorf("old query generation failed: %w", err)
	}

	log.Printf("Starting parallel query execution%s", traceSuffix(ctx))
	log.Printf("NEW: %s%s", formatSQLWithParams(newQuery, newParams), traceSuffix(ctx))
	log.Printf("OLD: %s%s", formatSQLWithParams(oldQuery, oldParams), traceSuffix(ctx))

	// 创建结果通道
	ch = make(chan *nostr.Event)

	// 并行执行两个查询
	type queryResult struct {
		events []*nostr.Event
		err    error
		isNew  bool
	}

	resultCh := make(chan queryResult, 2)

	// 启动新查询
	go func() {
		events, err := b.executeQuery(ctx, newQuery, newParams)
		resultCh <- queryResult{events: events, err: err, isNew: true}
	}()

	// 启动旧查询
	go func() {
		events, err := b.executeQuery(ctx, oldQuery, oldParams)
		resultCh <- queryResult{events: events, err: err, isNew: false}
	}()

	// 等待配置的查询完成并立即返回
	go func() {
		defer close(ch)
		
		var primaryResult queryResult
		var secondaryResult queryResult
		var receivedPrimary bool = false
		var receivedSecondary bool = false

		// 等待两个结果
		for i := 0; i < 2; i++ {
			result := <-resultCh
			
			// 确定哪个是主要结果（需要立即返回的）
			isPrimary := (b.UseOldQuery && !result.isNew) || (!b.UseOldQuery && result.isNew)
			
			if isPrimary {
				primaryResult = result
				receivedPrimary = true
				
				// 立即发送主要结果
				if result.err != nil {
					log.Printf("Primary query failed: %v%s", result.err, traceSuffix(ctx))
					return
				}
				
				for _, evt := range result.events {
					select {
					case ch <- evt:
					case <-ctx.Done():
						return
					}
				}
			} else {
				secondaryResult = result
				receivedSecondary = true
			}
			
			// 如果两个结果都收到了，异步处理对比
			if receivedPrimary && receivedSecondary {
				// 异步对比，不阻塞响应
				go func() {
					if secondaryResult.err != nil {
						log.Printf("Secondary query failed: %v%s", secondaryResult.err, traceSuffix(ctx))
						return
					}
					
					var newEvents, oldEvents []*nostr.Event
					if primaryResult.isNew {
						newEvents = primaryResult.events
						oldEvents = secondaryResult.events
					} else {
						newEvents = secondaryResult.events
						oldEvents = primaryResult.events
					}
					
					if err := b.compareQueryResults(newEvents, oldEvents, ctx); err != nil {
						log.Printf("Async query comparison failed: %v%s", err, traceSuffix(ctx))
					} else {
						log.Printf("Async query comparison successful: both queries returned %d events%s", len(newEvents), traceSuffix(ctx))
					}
				}()
				return
			}
		}
	}()

	return ch, nil
}

// executeQuery 执行单个查询并返回事件列表
func (b *PostgresBackend) executeQuery(ctx context.Context, query string, params []any) ([]*nostr.Event, error) {
	rows, err := b.DB.QueryContext(ctx, query, params...)
	if err != nil {
		if err == sql.ErrNoRows {
			return []*nostr.Event{}, nil
		}
		return nil, err
	}
	defer rows.Close()

	events := make([]*nostr.Event, 0)
	for rows.Next() {
		var evt nostr.Event
		var timestamp int64
		err := rows.Scan(&evt.ID, &evt.PubKey, &timestamp, &evt.Kind, &evt.Tags, &evt.Content, &evt.Sig)
		if err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		
		evt.CreatedAt = nostr.Timestamp(timestamp)
		events = append(events, &evt)
	}
	
	return events, nil
}

// compareQueryResults 对比两个查询结果是否一致
func (b *PostgresBackend) compareQueryResults(newEvents, oldEvents []*nostr.Event, ctx context.Context) error {
	if len(newEvents) != len(oldEvents) {
		log.Printf("[QUERY_COMPARISON_MISMATCH] Query result count mismatch: new=%d, old=%d%s", len(newEvents), len(oldEvents), traceSuffix(ctx))
		return fmt.Errorf("result count mismatch: new query returned %d events, old query returned %d events", len(newEvents), len(oldEvents))
	}

	// 创建 ID 到事件的映射，用于对比
	newEventMap := make(map[string]*nostr.Event)
	oldEventMap := make(map[string]*nostr.Event)

	for _, evt := range newEvents {
		newEventMap[evt.ID] = evt
	}

	for _, evt := range oldEvents {
		oldEventMap[evt.ID] = evt
	}

	// 检查是否有缺失的事件
	for id := range newEventMap {
		if _, exists := oldEventMap[id]; !exists {
			log.Printf("[QUERY_COMPARISON_MISMATCH] Event %s found in new query but missing in old query%s", id, traceSuffix(ctx))
			return fmt.Errorf("event %s found in new query but missing in old query", id)
		}
	}

	for id := range oldEventMap {
		if _, exists := newEventMap[id]; !exists {
			log.Printf("[QUERY_COMPARISON_MISMATCH] Event %s found in old query but missing in new query%s", id, traceSuffix(ctx))
			return fmt.Errorf("event %s found in old query but missing in new query", id)
		}
	}

	log.Printf("Query comparison successful: both queries returned %d identical events%s", len(newEvents), traceSuffix(ctx))
	return nil
}

func (b *PostgresBackend) CountEvents(ctx context.Context, filter nostr.Filter) (int64, error) {
	query, params, err := b.queryEventsSql(filter, true, "", true)
	if err != nil {
		return 0, err
	}

	// 打印实际执行的SQL语句（嵌入参数）
	log.Printf("Executing Count SQL Query: %s%s", formatSQLWithParams(query, params), traceSuffix(ctx))

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

func (b *PostgresBackend) queryEventsSql(filter nostr.Filter, doCount bool, userPubkey string, useOptimizedPath bool) (string, []any, error) {
	conditions := make([]string, 0, 7)
	params := make([]any, 0, 20)
	buildOverlapCondition := func(placeholders []string) string {
		return `event.tagvalues && ARRAY[` + strings.Join(placeholders, ",") + `]`
	}
	buildOrContainsCondition := func(placeholders []string) string {
		if len(placeholders) == 1 {
			return `event.tagvalues @> ARRAY[` + placeholders[0] + `]`
		}
		parts := make([]string, 0, len(placeholders))
		for _, ph := range placeholders {
			parts = append(parts, `event.tagvalues @> ARRAY[`+ph+`]`)
		}
		return "(" + strings.Join(parts, " OR ") + ")"
	}

	disappearingKSet := map[string]struct{}{
		"3048": {},
		"3049": {},
		"3050": {},
	}
	var nowEpoch int64
	nowEpochReady := false
	getNowEpoch := func() int64 {
		if !nowEpochReady {
			nowEpoch = time.Now().Unix()
			nowEpochReady = true
		}
		return nowEpoch
	}

	// 判断 k 标签中是否包含阅后即焚标识
	hasDisappearingK := false
	if kValues, ok := filter.Tags["k"]; ok {
		for _, tagValue := range kValues {
			if _, exists := disappearingKSet[tagValue]; exists {
				hasDisappearingK = true
				break
			}
		}
	}

	// 精细判断是否需要联表：存在 kind==106/1404/1405，或 kind==1059 且 k 标签包含阅后即焚标识，都必须联表
	needDisappearingJoin := false
	if len(filter.Kinds) > 0 {
		for _, kind := range filter.Kinds {
			if kind == 106 || kind == 1404 || kind == 1405 || (kind == 1059 && hasDisappearingK) {
				if userPubkey == "" {
					return "", nil, fmt.Errorf("user pubkey required for kinds 106/1404/1405 (or 1059 with k=3048/3049/3050)")
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
					return "", nil, fmt.Errorf("table moss_api.dismsg_user_status not found; required for kinds 106/1404/1405")
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
	pTagValues := []string{}
	
	// 检查是否为 kind=1059 的优化查询路径
	isKind1059Only := len(filter.Kinds) == 1 && filter.Kinds[0] == 1059
	
	for key, values := range filter.Tags {
		if len(values) == 0 {
			return "", nil, EmptyTagSet
		}
		if key == "k" {
			for _, tagValue := range values {
				kTagValues = append(kTagValues, tagValue)
			}
		} else if key == "p" && isKind1059Only {
			// 对于 kind=1059，单独处理 p 标签以便使用专门索引
			for _, tagValue := range values {
				pTagValues = append(pTagValues, tagValue)
			}
		} else {
			tagPlaceholders := make([]string, 0, len(values))
			for _, tagValue := range values {
				params = append(params, tagValue)
				tagPlaceholders = append(tagPlaceholders, "?")
			}
			if len(tagPlaceholders) > 0 {
				normalTagConditions = append(normalTagConditions, buildOverlapCondition(tagPlaceholders))
			}
		}
	}

	// k标签处理逻辑
	kTagCondition := ""
	splitKTags := len(kTagValues) > 1
	
	// 针对 kind=1059 的优化路径：使用专门的函数索引
	if useOptimizedPath && isKind1059Only && len(kTagValues) > 0 {
		// 构建 k 标签条件
		kConditions := make([]string, 0, len(kTagValues))
		for _, kValue := range kTagValues {
			// 检查是否为阅后即焚标签，需要特殊处理
			if _, isDisappearing := disappearingKSet[kValue]; isDisappearing && needDisappearingJoin {
				params = append(params, kValue)
				expClause := ""
				switch kValue {
				case "3048":
					expClause = " AND event.expiration_at > ?"
					params = append(params, getNowEpoch())
				case "3049", "3050":
					expClause = " AND (event.expiration_at IS NULL OR event.expiration_at > ?)"
					params = append(params, getNowEpoch())
				}
				
				kConditions = append(kConditions, 
					`(event.ktag = ? AND (dus.burn_at IS NULL OR dus.burn_at > NOW())`+expClause+`)`)
			} else {
				// 普通的 k 标签查询
				params = append(params, kValue)
				kConditions = append(kConditions, `event.ktag = ?`)
			}
		}
		
		// 如果有 p 标签，使用 ptag 列（适合每个事件只有1个p标签的场景）
		if len(pTagValues) > 0 {
			pPlaceholders := make([]string, 0, len(pTagValues))
			for _, pValue := range pTagValues {
				params = append(params, pValue)
				pPlaceholders = append(pPlaceholders, "?")
			}
			pCondition := `event.ptag IN (` + strings.Join(pPlaceholders, ",") + `)`
			
			// 组合 k 和 p 条件：任一 k 标签 AND 任一 p 标签
			if len(kConditions) == 1 {
				conditions = append(conditions, kConditions[0]+" AND "+pCondition)
			} else {
				conditions = append(conditions, "("+strings.Join(kConditions, " OR ")+") AND "+pCondition)
			}
		} else {
			// 只有 k 标签
			if len(kConditions) == 1 {
				conditions = append(conditions, kConditions[0])
			} else {
				conditions = append(conditions, "("+strings.Join(kConditions, " OR ")+")")
			}
		}
		
		// kind=1059优化路径处理完毕，不需要执行后续的k标签逻辑
		kTagCondition = "" // 确保不会重复添加k标签条件
	} else {
		// 原来的 k 标签处理逻辑
		if len(kTagValues) > 0 && !splitKTags {
			disappearingKValues := make([]string, 0)
			otherKValues := make([]string, 0)
			for _, v := range kTagValues {
				if _, ok := disappearingKSet[v]; ok {
					disappearingKValues = append(disappearingKValues, v)
					continue
				}
				otherKValues = append(otherKValues, v)
			}

			if needDisappearingJoin && len(disappearingKValues) > 0 {
				// 有阅后即焚k标签，非阅后即焚的k标签 OR (阅后即焚k标签且未过期)
				orParts := make([]string, 0)
				if len(otherKValues) > 0 {
					tagPlaceholders := make([]string, 0, len(otherKValues))
					for _, v := range otherKValues {
						params = append(params, v)
						tagPlaceholders = append(tagPlaceholders, "?")
					}
					orParts = append(orParts, buildOrContainsCondition(tagPlaceholders))
				}
				seen := make(map[string]struct{})
				for _, v := range disappearingKValues {
					if _, exists := seen[v]; exists {
						continue
					}
					seen[v] = struct{}{}
					expClause := ""
					switch v {
					case "3048":
						expClause = " AND event.expiration_at > ?"
						params = append(params, getNowEpoch())
					case "3049", "3050":
						expClause = " AND (event.expiration_at IS NULL OR event.expiration_at > ?)"
						params = append(params, getNowEpoch())
					}
					orParts = append(orParts, `(event.tagvalues @> ARRAY['`+v+`'] AND (dus.burn_at IS NULL OR dus.burn_at > NOW())`+expClause+`)`)
				}
				kTagCondition = "(" + strings.Join(orParts, " OR ") + ")"
			} else {
				// 没有阅后即焚k标签，所有k标签和其他标签一样AND
				tagPlaceholders := make([]string, 0, len(kTagValues))
				for _, tagValue := range kTagValues {
					params = append(params, tagValue)
					tagPlaceholders = append(tagPlaceholders, "?")
				}
				if len(tagPlaceholders) > 0 {
					kTagCondition = buildOrContainsCondition(tagPlaceholders)
				}
			}
		}
	}
	
	// 合并k标签和其他标签
	conditions = append(conditions, normalTagConditions...)
	if kTagCondition != "" {
		conditions = append(conditions, kTagCondition)
	}
	
	// 对于传统逻辑，如果有p标签（kind=1059），需要添加重叠条件
	if !useOptimizedPath && len(pTagValues) > 0 {
		pPlaceholders := make([]string, 0, len(pTagValues))
		for _, pValue := range pTagValues {
			params = append(params, pValue)
			pPlaceholders = append(pPlaceholders, "?")
		}
		// 使用重叠条件：event.tagvalues && ARRAY[p1, p2, p3, ...]
		conditions = append(conditions, buildOverlapCondition(pPlaceholders))
	}

	if filter.Since != nil && filter.Until != nil {
		conditions = append(conditions, `event.created_at BETWEEN ? AND ?`)
		params = append(params, filter.Since, filter.Until)
	} else {
		if filter.Since != nil {
			conditions = append(conditions, `event.created_at >= ?`)
			params = append(params, filter.Since)
		}
		if filter.Until != nil {
			conditions = append(conditions, `event.created_at <= ?`)
			params = append(params, filter.Until)
		}
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
	}

	normalKindConditions := make([]string, 0)
	disappearingConditions := make([]string, 0)
	var expNullKinds []int
	var expRequiredKinds []int
	for _, kind := range filter.Kinds {
		switch kind {
		case 106:
			expNullKinds = append(expNullKinds, 106)
		case 1404, 1405:
			expRequiredKinds = append(expRequiredKinds, kind)
		case 1059:
			// 1059的tag条件已在上面处理，这里只拼kind
			normalKindConditions = append(normalKindConditions, "event.kind = 1059")
		default:
			normalKindConditions = append(normalKindConditions, fmt.Sprintf("event.kind = %d", kind))
		}
	}
	if len(expNullKinds) > 0 {
		parts := make([]string, 0, len(expNullKinds))
		for _, kind := range expNullKinds {
			parts = append(parts, fmt.Sprintf("event.kind = %d", kind))
		}
		disappearingConditions = append(disappearingConditions,
			"("+strings.Join(parts, " OR ")+") AND (dus.burn_at IS NULL OR dus.burn_at > NOW()) AND (event.expiration_at IS NULL OR event.expiration_at > ?)")
		params = append(params, getNowEpoch())
	}
	if len(expRequiredKinds) > 0 {
		parts := make([]string, 0, len(expRequiredKinds))
		for _, kind := range expRequiredKinds {
			parts = append(parts, fmt.Sprintf("event.kind = %d", kind))
		}
		disappearingConditions = append(disappearingConditions,
			"("+strings.Join(parts, " OR ")+") AND (dus.burn_at IS NULL OR dus.burn_at > NOW()) AND event.expiration_at > ?")
		params = append(params, getNowEpoch())
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
	baseWhere := strings.Join(allConditions, " AND ")
	baseParams := append([]any(nil), params...)

	var query string
	if splitKTags {
		seenK := make(map[string]struct{})
		subQueries := make([]string, 0, len(kTagValues))
		params = params[:0]
		for _, k := range kTagValues {
			if _, exists := seenK[k]; exists {
				continue
			}
			seenK[k] = struct{}{}

			kCondition := `event.tagvalues @> ARRAY[?]`
			kParams := []any{k}
			if needDisappearingJoin {
				if _, ok := disappearingKSet[k]; ok {
					kCondition += ` AND (dus.burn_at IS NULL OR dus.burn_at > NOW())`
					switch k {
					case "3048":
						kCondition += " AND event.expiration_at > ?"
						kParams = append(kParams, getNowEpoch())
					case "3049", "3050":
						kCondition += " AND (event.expiration_at IS NULL OR event.expiration_at > ?)"
						kParams = append(kParams, getNowEpoch())
					}
				}
			}

			subWhere := baseWhere + " AND " + kCondition
			if doCount {
				subQueries = append(subQueries, `SELECT 1 FROM `+fromClause+` WHERE `+subWhere)
			} else {
				subQueries = append(subQueries, `SELECT event.id, event.pubkey, event.created_at, event.kind, event.tags, event.content, event.sig FROM `+fromClause+` WHERE `+subWhere)
			}

			params = append(params, baseParams...)
			params = append(params, kParams...)
		}

		if doCount {
			query = sqlx.Rebind(sqlx.BindType("postgres"), `SELECT COUNT(*) FROM (`+strings.Join(subQueries, " UNION ALL ")+`) AS ev LIMIT ?`)
		} else {
			query = sqlx.Rebind(sqlx.BindType("postgres"), `SELECT ev.id, ev.pubkey, ev.created_at, ev.kind, ev.tags, ev.content, ev.sig FROM (`+strings.Join(subQueries, " UNION ALL ")+`) AS ev ORDER BY ev.created_at DESC, ev.id LIMIT ?`)
		}
	} else {
		if doCount {
			query = sqlx.Rebind(sqlx.BindType("postgres"), `SELECT
		  COUNT(*)
		FROM `+fromClause+` WHERE `+
				baseWhere+
				" LIMIT ?")
		} else {
			query = sqlx.Rebind(sqlx.BindType("postgres"), `SELECT
		  event.id, event.pubkey, event.created_at, event.kind, event.tags, event.content, event.sig
		FROM `+fromClause+` WHERE `+
				baseWhere+
				" ORDER BY event.created_at DESC, event.id LIMIT ?")
		}
		params = baseParams
	}

	if filter.Limit < 1 || filter.Limit > b.QueryLimit {
		params = append(params, b.QueryLimit)
	} else {
		params = append(params, filter.Limit)
	}

	return query, params, nil
}
