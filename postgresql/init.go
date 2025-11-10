package postgresql

import (
	"os"
	"strconv"
	"time"

	"github.com/MosaviJP/eventstore"
	"github.com/jmoiron/sqlx"
	"github.com/jmoiron/sqlx/reflectx"
	_ "github.com/lib/pq"
)

const (
	queryLimit        = 100
	queryIDsLimit     = 500
	queryAuthorsLimit = 500
	queryKindsLimit   = 10
	queryTagsLimit    = 10
)

var _ eventstore.Store = (*PostgresBackend)(nil)

// SetDefaultLimits sets default query limits if not set
func (b *PostgresBackend) SetDefaultLimits() {
	if b.QueryLimit == 0 {
		b.QueryLimit = queryLimit
	}
	if b.QueryIDsLimit == 0 {
		b.QueryIDsLimit = queryIDsLimit
	}
	if b.QueryAuthorsLimit == 0 {
		b.QueryAuthorsLimit = queryAuthorsLimit
	}
	if b.QueryKindsLimit == 0 {
		b.QueryKindsLimit = queryKindsLimit
	}
	if b.QueryTagsLimit == 0 {
		b.QueryTagsLimit = queryTagsLimit
	}
}

// InitReadOnly initializes a read-only PostgresBackend (no DDL)
func (b *PostgresBackend) InitReadOnly() error {
	var err error
	var db *sqlx.DB
	if b.DB == nil {
		db, err = sqlx.Connect("postgres", b.DatabaseURL)
		if err != nil {
			return err
		}
		b.DB = db
	}
	// 连接池参数改为可配置，默认更保守，减少等待膨胀
	maxOpen := 40
	maxIdle := 20
	idleSec := 120
	if v := os.Getenv("RELAY_DB_MAX_OPEN_CONNS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxOpen = n
		}
	}
	if v := os.Getenv("RELAY_DB_MAX_IDLE_CONNS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			maxIdle = n
		}
	}
	if v := os.Getenv("RELAY_DB_CONN_MAX_IDLE_SECONDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			idleSec = n
		}
	}
	b.DB.SetMaxOpenConns(maxOpen)
	b.DB.SetMaxIdleConns(maxIdle)
	b.DB.SetConnMaxIdleTime(time.Duration(idleSec) * time.Second)
	b.DB.Mapper = reflectx.NewMapperFunc("json", sqlx.NameMapper)
	b.SetDefaultLimits()
	return nil
}

func (b *PostgresBackend) Init() error {
	var err error
	var db *sqlx.DB

	if b.DB == nil {
		db, err = sqlx.Connect("postgres", b.DatabaseURL)
		if err != nil {
			return err
		}
		b.DB = db
	}
	// 连接池参数改为可配置，默认更保守
	maxOpen := 40
	maxIdle := 20
	idleSec := 120
	if v := os.Getenv("RELAY_DB_MAX_OPEN_CONNS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			maxOpen = n
		}
	}
	if v := os.Getenv("RELAY_DB_MAX_IDLE_CONNS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			maxIdle = n
		}
	}
	if v := os.Getenv("RELAY_DB_CONN_MAX_IDLE_SECONDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			idleSec = n
		}
	}
	b.DB.SetMaxOpenConns(maxOpen)
	b.DB.SetMaxIdleConns(maxIdle)
	b.DB.SetConnMaxIdleTime(time.Duration(idleSec) * time.Second)

	b.DB.Mapper = reflectx.NewMapperFunc("json", sqlx.NameMapper)

	_, err = b.DB.Exec(`
CREATE OR REPLACE FUNCTION tags_to_tagvalues(jsonb) RETURNS text[]
	AS 'SELECT array_agg(t->>1) FROM (SELECT jsonb_array_elements($1) AS t)s WHERE length(t->>0) = 1;'
	LANGUAGE SQL
	IMMUTABLE
	RETURNS NULL ON NULL INPUT;

CREATE TABLE IF NOT EXISTS event (
  id text NOT NULL,
  pubkey text NOT NULL,
  created_at integer NOT NULL,
  kind integer NOT NULL,
  tags jsonb NOT NULL,
  content text NOT NULL,
  sig text NOT NULL,

  tagvalues text[] GENERATED ALWAYS AS (tags_to_tagvalues(tags)) STORED
);

CREATE UNIQUE INDEX IF NOT EXISTS ididx ON event USING btree (id text_pattern_ops);
CREATE INDEX IF NOT EXISTS pubkeyprefix ON event USING btree (pubkey text_pattern_ops);
CREATE INDEX IF NOT EXISTS timeidx ON event (created_at DESC);
CREATE INDEX IF NOT EXISTS kindidx ON event (kind);
CREATE INDEX IF NOT EXISTS kindtimeidx ON event(kind,created_at DESC);
CREATE INDEX IF NOT EXISTS arbitrarytagvalues ON event USING gin (tagvalues);
	`)

	if err != nil {
		return err
	}

	// Initialize disappearing messages schema
	err = b.EnsureDisappearingSchema()
	if err != nil {
		return err
	}

	if err := b.EnsureGroupCurrentMembers(); err != nil {
		return err
	}

	b.SetDefaultLimits()
	return nil
}
