package postgresql

import (
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"
)

// groupMgmtSchemaDefault 是未配置时的回落值。
//
// 取 moss_api 是为了保持 mosavi 现网行为不变：该 schema 名在本包里原本是
// 硬编码的，不设环境变量时必须逐字节等价于改造前。
const groupMgmtSchemaDefault = "moss_api"

// schemaNamePattern 限定 schema 名的字符集。
//
// schema 名会被拼进 SQL —— 标识符不能用占位符绑定，只能字符串插值，
// 所以必须在入口处收紧。不匹配时直接 panic 而非静默回落到默认值：
// 回落会让配置错误表现为「连到了另一个 schema」，比启动失败难查得多。
var schemaNamePattern = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

var (
	groupMgmtSchemaOnce  sync.Once
	groupMgmtSchemaValue string
)

// groupMgmtSchema 返回群管理相关表（dismsg_messages / dismsg_user_status /
// group_current_members）所在的 schema。
//
// 优先级与 nostr-relay 的 main.go 一致，两处同名同义，改一处要同步另一处：
//
//	GROUP_MGMT_SCHEMA → RELAY_GROUP_SCHEMA → moss_api
//
// 背景：这些表名原本硬编码为 moss_api。founa 体系复用同一套代码但库是
// 独立的，硬编码会让 founa 的库里长出一个名字来自旧品牌的 schema，
// 且与服务侧 GROUP_MGMT_SCHEMA 指向的 schema 不是同一个地方 ——
// 建表走硬编码、而服务侧以为在另一个 schema，两边读写会错开。
func groupMgmtSchema() string {
	groupMgmtSchemaOnce.Do(func() {
		v := strings.TrimSpace(os.Getenv("GROUP_MGMT_SCHEMA"))
		if v == "" {
			v = strings.TrimSpace(os.Getenv("RELAY_GROUP_SCHEMA"))
		}
		if v == "" {
			v = groupMgmtSchemaDefault
		}
		if !schemaNamePattern.MatchString(v) {
			panic(fmt.Sprintf("eventstore: 非法的 schema 名 %q，只允许 [a-z_][a-z0-9_]{0,62}", v))
		}
		groupMgmtSchemaValue = v
	})
	return groupMgmtSchemaValue
}
