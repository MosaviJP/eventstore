package postgresql

import (
	"os"
	"strings"
	"sync"
	"testing"
)

// reset 让每个用例都能重新读一次环境变量（生产路径是 sync.Once，只读一次）。
func reset() {
	groupMgmtSchemaOnce = sync.Once{}
	groupMgmtSchemaValue = ""
}

func TestGroupMgmtSchema(t *testing.T) {
	cases := []struct {
		name  string
		group string
		relay string
		want  string
	}{
		{"两者皆未设时回落默认，保持 mosavi 现网行为", "", "", "moss_api"},
		{"GROUP_MGMT_SCHEMA 生效", "founa_api", "", "founa_api"},
		{"仅 RELAY_GROUP_SCHEMA 时用它", "", "legacy_api", "legacy_api"},
		{"GROUP_MGMT_SCHEMA 优先于 RELAY_GROUP_SCHEMA", "founa_api", "legacy_api", "founa_api"},
		{"空白串视同未设", "   ", "", "moss_api"},
		{"两侧空白被裁掉", "  founa_api  ", "", "founa_api"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			reset()
			t.Setenv("GROUP_MGMT_SCHEMA", c.group)
			t.Setenv("RELAY_GROUP_SCHEMA", c.relay)
			if got := groupMgmtSchema(); got != c.want {
				t.Fatalf("got %q, want %q", got, c.want)
			}
		})
	}
}

// schema 名会被拼进 SQL，非法输入必须在入口处炸掉，
// 不能静默回落 —— 回落会让配置错误表现为「连到了另一个 schema」。
func TestGroupMgmtSchemaRejectsInjection(t *testing.T) {
	bad := []string{
		`public; DROP TABLE event; --`,
		`"moss_api"`,
		`moss api`,
		`moss-api`,
		`MossAPI`,
		`1abc`,
		strings.Repeat("a", 64),
	}
	for _, v := range bad {
		t.Run(v, func(t *testing.T) {
			reset()
			os.Setenv("GROUP_MGMT_SCHEMA", v)
			defer os.Unsetenv("GROUP_MGMT_SCHEMA")
			defer func() {
				if recover() == nil {
					t.Fatalf("%q 应当 panic 而非被接受", v)
				}
			}()
			_ = groupMgmtSchema()
		})
	}
}

// 只读一次：生产路径下改环境变量不应影响已解析的值。
func TestGroupMgmtSchemaCachedOnce(t *testing.T) {
	reset()
	t.Setenv("GROUP_MGMT_SCHEMA", "founa_api")
	first := groupMgmtSchema()
	t.Setenv("GROUP_MGMT_SCHEMA", "other_api")
	if second := groupMgmtSchema(); second != first {
		t.Fatalf("应缓存首次结果 %q，却得到 %q", first, second)
	}
}
