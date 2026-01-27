package postgresql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/MosaviJP/eventstore"
	"github.com/jmoiron/sqlx"
	"github.com/nbd-wtf/go-nostr"
)

func (b *PostgresBackend) SaveEvent(ctx context.Context, evt *nostr.Event) error {
	// 薄包装调用 SaveEvents
	return b.SaveEvents(ctx, []*nostr.Event{evt})
}

// func that saves a list of events into DB as a transaction, if event is a replace event, it will be replaced
func (b *PostgresBackend) SaveEvents(ctx context.Context, events []*nostr.Event) error {
	if len(events) == 0 {
		return nil
	}

	// 读取 tx：从 ctx 获取事务，如果没有则使用 DB
	tx, ok := eventstore.TxFrom(ctx)
	var exec sqlx.ExtContext = b.DB
	var needCommit bool

	if ok {
		// 使用外部传入的事务
		exec = tx
		needCommit = false
	} else {
		// 创建新事务
		newTx, err := b.DB.BeginTxx(ctx, nil)
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}
		tx = newTx
		exec = tx
		needCommit = true
		defer func() {
			if needCommit {
				tx.Rollback() // 确保在错误情况下回滚
			}
		}()
	}

	fmt.Printf("TX: SaveEvents: starting transaction for %d events%s\n", len(events), traceSuffix(ctx))

	for _, evt := range events {
		if nostr.IsReplaceableKind(evt.Kind) || nostr.IsAddressableKind(evt.Kind) {
			shouldStore := true
			var prevCreatedAt nostr.Timestamp

			query := `SELECT id, created_at FROM event WHERE pubkey=$1 AND kind=$2`
			args := []interface{}{evt.PubKey, evt.Kind}

			if nostr.IsAddressableKind(evt.Kind) {
				// 可寻址事件需要额外的 d 标签条件（NIP-33）
				// 在 jsonb 数组 [["k","v"], ...] 中精确匹配键为 'd' 且值等于给定 dTag 的项
				dTag := evt.Tags.GetD()
				query += ` AND EXISTS (
					SELECT 1 FROM jsonb_array_elements(tags) AS tag_elem
					WHERE jsonb_array_length(tag_elem) >= 2
					  AND tag_elem->>0 = 'd'
					  AND tag_elem->>1 = $3
				)`
				args = append(args, dTag)
			}
			query += ` ORDER BY created_at DESC, id DESC LIMIT 1`

			var prevID string
			row := tx.QueryRowxContext(ctx, query, args...)
			err := row.Scan(&prevID, &prevCreatedAt)
			if err == nil {
				// 找到了之前的事件，比较时间戳
				if prevCreatedAt >= evt.CreatedAt {
					fmt.Printf("SaveEvents: event %s is older than existing, skipping%s\n", evt.ID, traceSuffix(ctx))
					shouldStore = false
				} else {
					// 删除旧事件
					if _, err := exec.ExecContext(ctx, `DELETE FROM event WHERE id = $1`, prevID); err != nil {
						if needCommit {
							tx.Rollback()
						}
						return fmt.Errorf("failed to delete event for replacing: %w", err)
					}
					fmt.Printf("SaveEvents: deleted older event %s for replacement%s\n", prevID, traceSuffix(ctx))
				}
			}
			// 如果 err != nil，说明没有找到之前的事件，继续插入

			if !shouldStore {
				continue
			}
		}

		sql, params, _ := saveEventSql(ctx, evt)
		res, err := exec.ExecContext(ctx, sql, params...)
		if err != nil {
			fmt.Printf("SaveEvents: failed to execute SQL: %v, ctx.Err: %v%s\n", err, ctx.Err(), traceSuffix(ctx))
			if needCommit {
				tx.Rollback()
			}
			return fmt.Errorf("failed to execute SQL: %w", err)
		}

		nr, err := res.RowsAffected()
		if err != nil {
			if needCommit {
				tx.Rollback()
			}
			return fmt.Errorf("failed to get rows affected: %w", err)
		}

		if nr == 0 {
			fmt.Printf("SaveEvents: event %s was not inserted (maybe duplicate), continuing%s\n", evt.ID, traceSuffix(ctx))
			continue
		}

		fmt.Printf("SaveEvents: event %s saved successfully, rows: %d%s\n", evt.ID, nr, traceSuffix(ctx))
	}

	// 只有在我们创建了事务的情况下才提交
	if needCommit {
		if err := tx.Commit(); err != nil {
			fmt.Printf("SaveEvents: failed to commit transaction: %v%s\n", err, traceSuffix(ctx))
			return fmt.Errorf("failed to commit transaction: %w", err)
		}
		fmt.Printf("SaveEvents: all %d events saved successfully%s\n", len(events), traceSuffix(ctx))
	}

	return nil
}
func (b *PostgresBackend) BeforeSave(ctx context.Context, evt *nostr.Event) {
	// do nothing
}

func (b *PostgresBackend) AfterSave(evt *nostr.Event) {
	if b.KeepRecentEvents {
		return
	}
	// delete all but the 100 most recent ones for each key
	b.DB.Exec(`DELETE FROM event WHERE pubkey = $1 AND kind = $2 AND created_at < (
      SELECT created_at FROM event WHERE pubkey = $1
      ORDER BY created_at DESC, id OFFSET 100 LIMIT 1
    )`, evt.PubKey, evt.Kind)
}

func saveEventSql(ctx context.Context, evt *nostr.Event) (string, []any, error) {
	// 对于 kind=1059 事件，提取 k 和 p 标签值并存储到专门的列中
	if evt.Kind == 1059 {
		const query = `INSERT INTO event (
		id, pubkey, created_at, kind, tags, expiration_at, content, sig, ktag, ptag)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (id) DO NOTHING`

		var (
			tagsj, _ = json.Marshal(evt.Tags)
			expAt    = extractExpirationAt(ctx, evt.ID, evt.Tags)
			kTag     = extractFirstTagValue(evt.Tags, "k")
			pTag     = extractFirstTagValue(evt.Tags, "p")
			params   = []any{evt.ID, evt.PubKey, evt.CreatedAt, evt.Kind, tagsj, expAt, evt.Content, evt.Sig, kTag, pTag}
		)

		return query, params, nil
	} else {
		// 对于非 kind=1059 事件，使用原有逻辑
		const query = `INSERT INTO event (
		id, pubkey, created_at, kind, tags, expiration_at, content, sig)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (id) DO NOTHING`

		var (
			tagsj, _ = json.Marshal(evt.Tags)
			expAt    = extractExpirationAt(ctx, evt.ID, evt.Tags)
			params   = []any{evt.ID, evt.PubKey, evt.CreatedAt, evt.Kind, tagsj, expAt, evt.Content, evt.Sig}
		)

		return query, params, nil
	}
}

// 提取指定标签的第一个值
func extractFirstTagValue(tags nostr.Tags, tagName string) sql.NullString {
	for _, tag := range tags {
		if len(tag) >= 2 && tag[0] == tagName {
			return sql.NullString{String: tag[1], Valid: true}
		}
	}
	return sql.NullString{Valid: false}
}

func extractExpirationAt(ctx context.Context, eventID string, tags nostr.Tags) sql.NullInt64 {
	for _, tag := range tags {
		if len(tag) < 2 || tag[0] != "expiration" {
			continue
		}
		value, err := strconv.ParseInt(tag[1], 10, 64)
		if err != nil {
			fmt.Printf("SaveEvents: invalid expiration tag value for event %s: %q (err=%v)%s\n", eventID, tag[1], err, traceSuffix(ctx))
			return sql.NullInt64{Valid: false}
		}
		return sql.NullInt64{Int64: value, Valid: true}
	}
	return sql.NullInt64{Valid: false}
}

// UpsertDisappearing 插入或更新消失消息记录
func (b *PostgresBackend) UpsertDisappearing(
	ctx context.Context,
	eventID string, ttlSeconds int64, expiration time.Time, createdAt time.Time,
) error {
	// 同样用 tx := TxFrom(ctx) 决定 exec，在同一事务里执行
	tx, ok := eventstore.TxFrom(ctx)
	var exec sqlx.ExtContext = b.DB
	var needCommit bool

	if ok {
		// 使用外部传入的事务
		exec = tx
		needCommit = false
	} else {
		// 创建新事务
		newTx, err := b.DB.BeginTxx(ctx, nil)
		if err != nil {
			return fmt.Errorf("failed to begin transaction: %w", err)
		}
		tx = newTx
		exec = tx
		needCommit = true
		defer func() {
			if needCommit {
				tx.Rollback()
			}
		}()
	}

	query := `INSERT INTO moss_api.dismsg_messages(event_id, ttl_seconds, expiration, created_at)
VALUES ($1,$2,$3,$4)
ON CONFLICT (event_id) DO UPDATE
  SET ttl_seconds=EXCLUDED.ttl_seconds,
      expiration=EXCLUDED.expiration,
      created_at=EXCLUDED.created_at`

	_, err := exec.ExecContext(ctx, query, eventID, ttlSeconds, expiration, createdAt)
	if err != nil {
		if needCommit {
			tx.Rollback()
		}
		return fmt.Errorf("failed to upsert disappearing message: %w", err)
	}

	if needCommit {
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit disappearing transaction: %w", err)
		}
	}

	fmt.Printf("UpsertDisappearing: event %s upserted successfully%s\n", eventID, traceSuffix(ctx))
	return nil
}

// EnsureDisappearingSchema 确保消失消息的 schema 和表存在
func (b *PostgresBackend) EnsureDisappearingSchema() error {
	query := `
	CREATE SCHEMA IF NOT EXISTS moss_api;
	
	CREATE TABLE IF NOT EXISTS moss_api.dismsg_messages (
		id int8 GENERATED BY DEFAULT AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1 NO CYCLE) NOT NULL,
		event_id varchar NOT NULL,
		ttl_seconds int8 NOT NULL,
		expiration timestamptz NOT NULL,
		created_at timestamptz NOT NULL,
		CONSTRAINT dismsg_messages_pkey PRIMARY KEY (id)
	);
	
	-- 创建索引（与现有表结构一致）
	CREATE INDEX IF NOT EXISTS disappearingmessage_created_at ON moss_api.dismsg_messages USING btree (created_at);
	CREATE INDEX IF NOT EXISTS disappearingmessage_event_id ON moss_api.dismsg_messages USING btree (event_id);
	CREATE INDEX IF NOT EXISTS disappearingmessage_expiration ON moss_api.dismsg_messages USING btree (expiration);
	CREATE UNIQUE INDEX IF NOT EXISTS dismsg_messages_event_id_key ON moss_api.dismsg_messages USING btree (event_id);
	`

	if _, err := b.DB.Exec(query); err != nil {
		return fmt.Errorf("failed to create schema and table: %w", err)
	}

	fmt.Println("EnsureDisappearingSchema: schema and table created successfully")
	return nil
}

// EnsureGroupCurrentMembers 确保当前群成员表存在
func (b *PostgresBackend) EnsureGroupCurrentMembers() error {
	query := `
	CREATE SCHEMA IF NOT EXISTS moss_api;

	CREATE TABLE IF NOT EXISTS moss_api.group_current_members (
		id int8 GENERATED BY DEFAULT AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1 NO CYCLE) NOT NULL,
		group_id varchar NOT NULL,
		member_pubkey varchar NOT NULL,
		updated_at timestamptz NOT NULL,
		created_at timestamptz NOT NULL,
		CONSTRAINT group_current_members_pkey PRIMARY KEY (id)
	);

	CREATE INDEX IF NOT EXISTS groupcurrentmember_group_id ON moss_api.group_current_members USING btree (group_id);
	CREATE UNIQUE INDEX IF NOT EXISTS groupcurrentmember_group_id_member_pubkey ON moss_api.group_current_members USING btree (group_id, member_pubkey);
	`

	if _, err := b.DB.Exec(query); err != nil {
		return fmt.Errorf("failed to ensure group_current_members table: %w", err)
	}

	fmt.Println("EnsureGroupCurrentMembers: table created successfully")
	return nil
}
