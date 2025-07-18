package checks

import (
	"github.com/MosaviJP/eventstore"
	"github.com/MosaviJP/eventstore/badger"
	"github.com/MosaviJP/eventstore/bluge"
	"github.com/MosaviJP/eventstore/edgedb"
	"github.com/MosaviJP/eventstore/lmdb"
	"github.com/MosaviJP/eventstore/mongo"
	"github.com/MosaviJP/eventstore/mysql"
	"github.com/MosaviJP/eventstore/postgresql"
	"github.com/MosaviJP/eventstore/sqlite3"
	"github.com/MosaviJP/eventstore/strfry"
)

// compile-time checks to ensure all backends implement Store
var (
	_ eventstore.Store = (*badger.BadgerBackend)(nil)
	_ eventstore.Store = (*lmdb.LMDBBackend)(nil)
	_ eventstore.Store = (*edgedb.EdgeDBBackend)(nil)
	_ eventstore.Store = (*postgresql.PostgresBackend)(nil)
	_ eventstore.Store = (*mongo.MongoDBBackend)(nil)
	_ eventstore.Store = (*sqlite3.SQLite3Backend)(nil)
	_ eventstore.Store = (*strfry.StrfryBackend)(nil)
	_ eventstore.Store = (*bluge.BlugeBackend)(nil)
	_ eventstore.Store = (*mysql.MySQLBackend)(nil)
)
