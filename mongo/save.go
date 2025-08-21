package mongo

import (
	"context"
	"github.com/MosaviJP/eventstore"
	"github.com/nbd-wtf/go-nostr"
)

func (m *MongoDBBackend) SaveEvent(ctx context.Context, event *nostr.Event) error {
	result, err := m.Client.Database("events").Collection("events").InsertOne(ctx, event)
	if err != nil {
		return err
	}
	if result.InsertedID == nil {
		return eventstore.ErrDupEvent
	}
	return err
}

func (m *MongoDBBackend) SaveEvents(ctx context.Context, events []*nostr.Event) error {
	if len(events) == 0 {
		return nil
	}

	for _, evt := range events {
		if err := m.SaveEvent(ctx, evt); err != nil && err != eventstore.ErrDupEvent {
			return err
		}
	}
	return nil
}
