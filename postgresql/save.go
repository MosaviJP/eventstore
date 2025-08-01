package postgresql

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/MosaviJP/eventstore"
	"github.com/MosaviJP/eventstore/internal"
	"github.com/nbd-wtf/go-nostr"
)

func (b *PostgresBackend) SaveEvent(ctx context.Context, evt *nostr.Event) error {
	deadline, hasDeadline := ctx.Deadline()
	fmt.Printf("SaveEvent: event id: %s\n", evt.ID)
	if hasDeadline {
		fmt.Printf("SaveEvent: context deadline: %s\n", deadline.String())
	} else {
		fmt.Printf("SaveEvent: context has no deadline\n")
	}
	fmt.Printf("SaveEvent: ctx canceled before SQL? %v\n", ctx.Err())
	sql, params, _ := saveEventSql(evt)
	paramsJson, _ := json.Marshal(params)
	tmpCtx := context.Background()
	fmt.Printf("SaveEvent: params: %s\n", string(paramsJson))
	res, err := b.DB.ExecContext(tmpCtx, sql, params...)
	if err != nil {
		fmt.Printf("SaveEvent: failed to execute SQL: %v, ctx.Err: %v\n", err, ctx.Err())
		return err
	}

	nr, err := res.RowsAffected()
	if err != nil {
		fmt.Printf("SaveEvent: failed to get rows affected: %v\n", err)
		return err
	}

	if nr == 0 {
		return eventstore.ErrDupEvent
	}

	fmt.Printf("SaveEvent: event saved successfully, rows: %d\n", nr)
	return nil
}

// func that saves a list of events into DB as a transaction, if event is a replace event, it will be replaced
func (b *PostgresBackend) SaveEvents(ctx context.Context, events []*nostr.Event) error {
	if len(events) == 0 {
		return nil
	}
	tx, err := b.DB.BeginTx(ctx, nil)
	fmt.Printf("TX: SaveEvents: starting transaction for %d events\n", len(events))
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	for _, evt := range events {
		if nostr.IsReplaceableKind(evt.Kind) {
			filter := nostr.Filter{Limit: 1, Kinds: []int{evt.Kind}, Authors: []string{evt.PubKey}}
			if nostr.IsAddressableKind(evt.Kind) {
				filter.Tags = nostr.TagMap{"d": []string{evt.Tags.GetD()}}
			}
			ch, err := b.QueryEvents(ctx, filter)
			if err != nil {
				tx.Rollback()
				return fmt.Errorf("failed to query before replacing: %w", err)
			}

			shouldStore := true
			for previous := range ch {
				if internal.IsOlder(previous, evt) {
					if _, err := tx.ExecContext(ctx, `DELETE FROM event WHERE id = $1`, previous.ID); err != nil {
						tx.Rollback()
						return fmt.Errorf("failed to delete event for replacing: %w", err)
					}
				} else {
					shouldStore = false
				}
			}

			if !shouldStore {
				fmt.Printf("SaveEvents: event %s is older than existing, skipping\n", evt.ID)
				continue
			}
		}

		sql, params, _ := saveEventSql(evt)
		paramsJson, err := json.Marshal(params)
		if err == nil {
			fmt.Printf("SaveEvents: params: %s in tx: %p\n", string(paramsJson), tx)
		}
		res, err := tx.ExecContext(ctx, sql, params...)
		if err != nil {
			fmt.Printf("SaveEvents: failed to execute SQL: %v, ctx.Err: %v\n", err, ctx.Err())
			tx.Rollback()
			return fmt.Errorf("failed to execute SQL: %w", err)
		}
		nr, err := res.RowsAffected()
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("failed to get rows affected: %w", err)
		}
		if nr == 0 {
			fmt.Printf("SaveEvents: event %s was not inserted (maybe duplicate), continuing\n", evt.ID)
			continue // 而不是 return
		}
		fmt.Printf("SaveEvents: event %s saved successfully, rows: %d\n",
			evt.ID, nr)
	}
	if err := tx.Commit(); err != nil {
		fmt.Printf("SaveEvents: failed to commit transaction: %v\n", err)
		tx.Rollback()
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	fmt.Printf("SaveEvents: all %d events saved successfully\n", len(events))
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

func saveEventSql(evt *nostr.Event) (string, []any, error) {
	const query = `INSERT INTO event (
	id, pubkey, created_at, kind, tags, content, sig)
	VALUES ($1, $2, $3, $4, $5, $6, $7)
	ON CONFLICT (id) DO NOTHING`

	var (
		tagsj, _ = json.Marshal(evt.Tags)
		params   = []any{evt.ID, evt.PubKey, evt.CreatedAt, evt.Kind, tagsj, evt.Content, evt.Sig}
	)

	return query, params, nil
}
