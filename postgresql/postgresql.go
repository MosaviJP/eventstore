package postgresql

import (
	"sync"

	"github.com/jmoiron/sqlx"
)

type PostgresBackend struct {
	sync.Mutex
	*sqlx.DB
	DatabaseURL       string
	QueryLimit        int
	QueryIDsLimit     int
	QueryAuthorsLimit int
	QueryKindsLimit   int
	QueryTagsLimit    int
	KeepRecentEvents  bool
	
	// 查询对比配置
	EnableQueryComparison bool // 是否启用查询对比
	UseOldQuery          bool // 对比时返回哪种查询结果，false=新查询，true=旧查询
}

func (b *PostgresBackend) Close() {
	b.DB.Close()
}
