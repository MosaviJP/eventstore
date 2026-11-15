package postgresql

import (
	"strings"
	"testing"

	"github.com/nbd-wtf/go-nostr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type kind1059Case struct {
	name   string
	filter nostr.Filter
}

const (
	pA = "aaaa000000000000000000000000000000000000000000000000000000000001"
	pB = "bbbb000000000000000000000000000000000000000000000000000000000002"
	au = "cccc000000000000000000000000000000000000000000000000000000000003"
)

// Every filter carries at most one tag key besides k/p so that the
// generated SQL does not depend on map iteration order.
func kind1059Cases() []kind1059Case {
	since := nostr.Timestamp(1700000000)
	until := nostr.Timestamp(1800000000)
	return []kind1059Case{
		{"1059_p_without_k", nostr.Filter{Kinds: []int{1059}, Tags: nostr.TagMap{"p": {pA}}, Limit: 3}},
		{"1059_two_p_without_k", nostr.Filter{Kinds: []int{1059}, Tags: nostr.TagMap{"p": {pA, pB}}, Limit: 3}},
		{"1059_p_without_k_authors_since_until", nostr.Filter{Kinds: []int{1059}, Authors: []string{au}, Tags: nostr.TagMap{"p": {pA}}, Since: &since, Until: &until, Limit: 3}},
		{"1059_p_with_one_k", nostr.Filter{Kinds: []int{1059}, Tags: nostr.TagMap{"p": {pA}, "k": {"14"}}, Limit: 3}},
		{"1059_p_with_two_k", nostr.Filter{Kinds: []int{1059}, Tags: nostr.TagMap{"p": {pA}, "k": {"14", "15"}}, Limit: 3}},
		{"1059_k_without_p", nostr.Filter{Kinds: []int{1059}, Tags: nostr.TagMap{"k": {"14"}}, Limit: 3}},
		{"1059_no_tags", nostr.Filter{Kinds: []int{1059}, Limit: 3}},
		{"1059_and_1_with_p", nostr.Filter{Kinds: []int{1059, 1}, Tags: nostr.TagMap{"p": {pA}}, Limit: 3}},
		{"kind1_with_p", nostr.Filter{Kinds: []int{1}, Tags: nostr.TagMap{"p": {pA}}, Limit: 3}},
	}
}

func findCase(t *testing.T, name string) nostr.Filter {
	for _, c := range kind1059Cases() {
		if c.name == name {
			return c.filter
		}
	}
	t.Fatalf("case %q not found", name)
	return nostr.Filter{}
}

// QueryEvents and CountEvents both call queryEventsSql with
// useOptimizedPath=true; doCount distinguishes them.
var queryAndCount = []struct {
	name    string
	doCount bool
}{{"QueryEvents", false}, {"CountEvents", true}}

// Regression: kind 1059 with #p but without #k used to drop the #p
// condition entirely on the optimized path, returning the latest 1059
// events of every user.
func TestKind1059PTagWithoutKIsApplied(t *testing.T) {
	for _, qc := range queryAndCount {
		t.Run(qc.name, func(t *testing.T) {
			q, params, err := defaultBackend.queryEventsSql(findCase(t, "1059_p_without_k"), qc.doCount, "", true)
			require.NoError(t, err)
			assert.Contains(t, normSQL(q), "event.tagvalues && ARRAY[$1]")
			assert.Contains(t, normSQL(q), "(event.kind = 1059)")
			assert.Equal(t, []any{pA, 3}, params)

			q, params, err = defaultBackend.queryEventsSql(findCase(t, "1059_two_p_without_k"), qc.doCount, "", true)
			require.NoError(t, err)
			assert.Contains(t, normSQL(q), "event.tagvalues && ARRAY[$1,$2]")
			assert.Equal(t, []any{pA, pB, 3}, params)

			// p must stay parameterized: the value never appears in the SQL text
			assert.NotContains(t, q, pA)
		})
	}
}

func TestKind1059PTagWithoutKWithOtherConditions(t *testing.T) {
	for _, qc := range queryAndCount {
		t.Run(qc.name, func(t *testing.T) {
			q, params, err := defaultBackend.queryEventsSql(findCase(t, "1059_p_without_k_authors_since_until"), qc.doCount, "", true)
			require.NoError(t, err)
			assert.Contains(t, normSQL(q), "event.pubkey IN ($1) AND event.tagvalues && ARRAY[$2] AND event.created_at BETWEEN $3 AND $4")
			require.Len(t, params, 5)
			assert.Equal(t, au, params[0])
			assert.Equal(t, pA, params[1])
		})
	}
}

// The optimized path and the legacy path must now apply the same p
// condition for the no-#k shape (the legacy path was already correct).
func TestKind1059PTagWithoutKMatchesLegacyPath(t *testing.T) {
	for _, qc := range queryAndCount {
		t.Run(qc.name, func(t *testing.T) {
			for _, name := range []string{"1059_p_without_k", "1059_two_p_without_k", "1059_p_without_k_authors_since_until"} {
				f := findCase(t, name)
				qOpt, pOpt, err := defaultBackend.queryEventsSql(f, qc.doCount, "", true)
				require.NoError(t, err)
				qOld, pOld, err := defaultBackend.queryEventsSql(f, qc.doCount, "", false)
				require.NoError(t, err)
				assert.Equal(t, normSQL(qOld), normSQL(qOpt), name)
				assert.Equal(t, pOld, pOpt, name)
			}
		})
	}
}

// Shapes that were not affected must produce byte-identical SQL and
// params to the code before the fix (expected values captured from
// eventstore 6983251).
func TestKind1059OtherShapesUnchanged(t *testing.T) {
	type want struct {
		query  string
		params []any
	}
	expect := map[string]map[bool]want{
		"1059_p_with_one_k": {
			false: {"SELECT event.id, event.pubkey, event.created_at, event.kind, event.tags, event.content, event.sig FROM event WHERE event.ktag = $1 AND event.ptag IN ($2) AND ((event.kind = 1059)) ORDER BY event.created_at DESC, event.id LIMIT $3", []any{"14", pA, 3}},
			true:  {"SELECT COUNT(*) FROM event WHERE event.ktag = $1 AND event.ptag IN ($2) AND ((event.kind = 1059)) LIMIT $3", []any{"14", pA, 3}},
		},
		"1059_p_with_two_k": {
			false: {"SELECT ev.id, ev.pubkey, ev.created_at, ev.kind, ev.tags, ev.content, ev.sig FROM (SELECT event.id, event.pubkey, event.created_at, event.kind, event.tags, event.content, event.sig FROM event WHERE (event.ktag = $1 OR event.ktag = $2) AND event.ptag IN ($3) AND ((event.kind = 1059)) AND event.tagvalues @> ARRAY[$4] UNION ALL SELECT event.id, event.pubkey, event.created_at, event.kind, event.tags, event.content, event.sig FROM event WHERE (event.ktag = $5 OR event.ktag = $6) AND event.ptag IN ($7) AND ((event.kind = 1059)) AND event.tagvalues @> ARRAY[$8]) AS ev ORDER BY ev.created_at DESC, ev.id LIMIT $9", []any{"14", "15", pA, "14", "14", "15", pA, "15", 3}},
			true:  {"SELECT COUNT(*) FROM (SELECT 1 FROM event WHERE (event.ktag = $1 OR event.ktag = $2) AND event.ptag IN ($3) AND ((event.kind = 1059)) AND event.tagvalues @> ARRAY[$4] UNION ALL SELECT 1 FROM event WHERE (event.ktag = $5 OR event.ktag = $6) AND event.ptag IN ($7) AND ((event.kind = 1059)) AND event.tagvalues @> ARRAY[$8]) AS ev LIMIT $9", []any{"14", "15", pA, "14", "14", "15", pA, "15", 3}},
		},
		"1059_k_without_p": {
			false: {"SELECT event.id, event.pubkey, event.created_at, event.kind, event.tags, event.content, event.sig FROM event WHERE event.ktag = $1 AND ((event.kind = 1059)) ORDER BY event.created_at DESC, event.id LIMIT $2", []any{"14", 3}},
			true:  {"SELECT COUNT(*) FROM event WHERE event.ktag = $1 AND ((event.kind = 1059)) LIMIT $2", []any{"14", 3}},
		},
		"1059_no_tags": {
			false: {"SELECT event.id, event.pubkey, event.created_at, event.kind, event.tags, event.content, event.sig FROM event WHERE true AND ((event.kind = 1059)) ORDER BY event.created_at DESC, event.id LIMIT $1", []any{3}},
			true:  {"SELECT COUNT(*) FROM event WHERE true AND ((event.kind = 1059)) LIMIT $1", []any{3}},
		},
		"1059_and_1_with_p": {
			false: {"SELECT event.id, event.pubkey, event.created_at, event.kind, event.tags, event.content, event.sig FROM event WHERE event.tagvalues && ARRAY[$1] AND ((event.kind = 1059 OR event.kind = 1)) ORDER BY event.created_at DESC, event.id LIMIT $2", []any{pA, 3}},
			true:  {"SELECT COUNT(*) FROM event WHERE event.tagvalues && ARRAY[$1] AND ((event.kind = 1059 OR event.kind = 1)) LIMIT $2", []any{pA, 3}},
		},
		"kind1_with_p": {
			false: {"SELECT event.id, event.pubkey, event.created_at, event.kind, event.tags, event.content, event.sig FROM event WHERE event.tagvalues && ARRAY[$1] AND ((event.kind = 1)) ORDER BY event.created_at DESC, event.id LIMIT $2", []any{pA, 3}},
			true:  {"SELECT COUNT(*) FROM event WHERE event.tagvalues && ARRAY[$1] AND ((event.kind = 1)) LIMIT $2", []any{pA, 3}},
		},
	}
	for name, byCount := range expect {
		for doCount, w := range byCount {
			q, params, err := defaultBackend.queryEventsSql(findCase(t, name), doCount, "", true)
			require.NoError(t, err)
			assert.Equal(t, w.query, normSQL(q), "%s count=%v", name, doCount)
			assert.Equal(t, w.params, params, "%s count=%v", name, doCount)
		}
	}
}

// normSQL collapses whitespace so expected SQL can be written on one line.
func normSQL(s string) string { return strings.Join(strings.Fields(s), " ") }
