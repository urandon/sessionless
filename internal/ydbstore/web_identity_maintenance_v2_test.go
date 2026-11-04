package ydbstore

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ydbclient"
	ydbmigrations "gitcode.com/urandon/sessionless/migrations/ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

type maintenanceSchemaTest struct{ bad bool }

func (d maintenanceSchemaTest) DescribeTable(_ context.Context, name string, _ ...options.DescribeTableOption) (*options.Description, error) {
	for _, table := range identityMaintenanceTables() {
		if path.Base(name) != table.name {
			continue
		}
		out := &options.Description{PrimaryKey: slices.Clone(table.keys)}
		for _, c := range table.cols {
			var typ types.Type
			switch c.kind {
			case "Uint32":
				typ = types.TypeUint32
			case "Uint64":
				typ = types.TypeUint64
			case "Timestamp":
				typ = types.TypeTimestamp
			case "Utf8":
				typ = types.TypeUTF8
			case "JsonDocument":
				typ = types.TypeJSONDocument
			}
			out.Columns = append(out.Columns, options.Column{Name: c.name, Type: types.Optional(typ)})
		}
		if table.name == "oidc_login_challenges" {
			ttl := options.NewTTLSettings().ColumnDateType("delete_after").ExpireAfter(0)
			out.TimeToLiveSettings = &ttl
		}
		if table.name == "web_sessions" {
			ttl := options.NewTTLSettings().ColumnDateType("absolute_expires_at").ExpireAfter(0)
			out.TimeToLiveSettings = &ttl
		}
		if d.bad {
			out.PrimaryKey = []string{"foreign"}
		}
		return out, nil
	}
	return nil, errors.New("unknown schema")
}

type maintenanceSQLTest struct {
	at                                           time.Time
	data, pending                                map[string][][]driver.Value
	statements                                   []string
	writes, commits, rollbacks                   int
	failWrite, lostCommit, badMetadata, failRead bool
	failWriteAt                                  int
	beforeExact                                  func(*maintenanceSQLTest)
	metadataAlter                                func(string, [][]driver.Value) [][]driver.Value
}
type maintenanceConnTest struct{ f *maintenanceSQLTest }
type maintenanceTxTest struct{ f *maintenanceSQLTest }
type maintenanceDriverTest struct{}
type maintenanceRowsTest struct {
	data  [][]driver.Value
	width int
	index int
}

func (r *maintenanceRowsTest) Columns() []string {
	out := make([]string, r.width)
	for i := range out {
		out[i] = "cell"
	}
	return out
}
func (*maintenanceRowsTest) Close() error { return nil }
func (r *maintenanceRowsTest) Next(out []driver.Value) error {
	if r.index >= len(r.data) {
		return io.EOF
	}
	copy(out, r.data[r.index])
	r.index++
	return nil
}
func (*maintenanceDriverTest) Open(string) (driver.Conn, error) {
	return nil, errors.New("requires connector")
}
func (f *maintenanceSQLTest) Driver() driver.Driver { return &maintenanceDriverTest{} }
func (f *maintenanceSQLTest) Connect(context.Context) (driver.Conn, error) {
	return &maintenanceConnTest{f}, nil
}
func (*maintenanceConnTest) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("explicit SQL only")
}
func (*maintenanceConnTest) Close() error { return nil }
func (*maintenanceConnTest) Begin() (driver.Tx, error) {
	return nil, errors.New("explicit serializable only")
}
func maintenanceCloneTest(in map[string][][]driver.Value) map[string][][]driver.Value {
	out := map[string][][]driver.Value{}
	for k, rows := range in {
		for _, row := range rows {
			out[k] = append(out[k], slices.Clone(row))
		}
	}
	return out
}
func (c *maintenanceConnTest) BeginTx(ctx context.Context, o driver.TxOptions) (driver.Tx, error) {
	if ctx.Err() != nil || o.Isolation != driver.IsolationLevel(sql.LevelSerializable) {
		return nil, errors.New("wrong transaction")
	}
	c.f.pending = maintenanceCloneTest(c.f.data)
	return &maintenanceTxTest{c.f}, nil
}
func (tx *maintenanceTxTest) Commit() error {
	tx.f.data = tx.f.pending
	tx.f.pending = nil
	tx.f.commits++
	if tx.f.lostCommit {
		tx.f.lostCommit = false
		return errors.New("private driver lost commit detail")
	}
	return nil
}
func (tx *maintenanceTxTest) Rollback() error { tx.f.pending = nil; tx.f.rollbacks++; return nil }
func (c *maintenanceConnTest) QueryContext(ctx context.Context, s string, args []driver.NamedValue) (driver.Rows, error) {
	f := c.f
	f.statements = append(f.statements, s)
	if ctx.Err() != nil || f.failRead {
		return nil, errors.New("private SQL detail")
	}
	row := func(width int, rows ...[]driver.Value) (driver.Rows, error) {
		return &maintenanceRowsTest{data: rows, width: width}, nil
	}
	metadata := func(width int, rows ...[]driver.Value) (driver.Rows, error) {
		if f.metadataAlter != nil {
			rows = f.metadataAlter(s, rows)
		}
		return row(width, rows...)
	}
	if s == `SELECT CurrentUtcTimestamp()` {
		return row(1, []driver.Value{f.at})
	}
	if s == identityMaintenanceChecksumsSQL {
		state := "applied"
		if f.badMetadata {
			state = "pending"
		}
		return metadata(6, []driver.Value{int64(130), "00130_add_identity_challenge_delete_after.sql", "1417c1e749d489d77d47ebebb773cfb13d4be747abbc4b11b1be2b71f6907859", state, f.at.Add(-time.Hour), f.at.Add(-time.Minute)}, []driver.Value{int64(131), "00131_set_identity_challenge_delete_after_ttl.sql", "20bd275a6389db46ba4378957a0e07a50ec23a1f5b6611f0991da501cf216632", state, f.at.Add(-time.Hour), f.at.Add(-time.Minute)})
	}
	if s == identityMaintenanceGooseSQL {
		return metadata(3, []driver.Value{int64(130), true, f.at.Add(-time.Minute)}, []driver.Value{int64(131), true, f.at.Add(-time.Minute)})
	}
	if s == webIdentityRowSQLV2 {
		for _, cells := range f.pending["external_identities"] {
			if cells[0] == args[0].Value && cells[1] == args[1].Value && cells[2] == args[2].Value {
				return row(1, []driver.Value{cells[6]})
			}
		}
		return row(1)
	}
	var table identityMaintenanceTable
	for _, t := range identityMaintenanceTables() {
		if strings.Contains(s, " FROM "+t.name+" ") {
			table = t
			break
		}
	}
	if table.name == "" {
		return nil, errors.New("unexpected query")
	}
	if strings.Contains(s, "SELECT 1 ") {
		return row(1)
	}
	if strings.Contains(s, " LIMIT 2") && f.beforeExact != nil {
		callback := f.beforeExact
		f.beforeExact = nil
		callback(f)
	}
	var selected [][]driver.Value
	for _, cells := range f.pending[table.name] {
		if len(args) == 0 || cells[0] != args[0].Value {
			continue
		}
		include := true
		keys := []string{}
		for _, name := range table.keys[1:] {
			for i, col := range table.cols {
				if col.name == name {
					keys = append(keys, cells[i].(string))
				}
			}
		}
		if strings.Contains(s, " LIMIT 3") {
			if table.name != "external_identities_by_user" || cells[1] != args[1].Value {
				continue
			}
			selected = append(selected, []driver.Value{cells[2], cells[3], cells[4]})
			continue
		}
		if len(args) > 1 {
			if strings.Contains(s, " > (") {
				var cursor []string
				for _, arg := range args[1:] {
					cursor = append(cursor, arg.Value.(string))
				}
				include = slices.Compare(keys, cursor) > 0
			} else {
				for i, arg := range args[1:] {
					if keys[i] != arg.Value {
						include = false
					}
				}
			}
		}
		if include {
			selected = append(selected, slices.Clone(cells))
		}
	}
	slices.SortFunc(selected, func(a, b []driver.Value) int {
		if len(a) != len(table.cols) {
			return strings.Compare(a[0].(string), b[0].(string))
		}
		var ak, bk []string
		for _, name := range table.keys[1:] {
			for i, col := range table.cols {
				if col.name == name {
					ak = append(ak, a[i].(string))
					bk = append(bk, b[i].(string))
				}
			}
		}
		return slices.Compare(ak, bk)
	})
	limit := 17
	width := len(table.cols)
	if strings.Contains(s, " LIMIT 2") {
		limit = 2
	}
	if strings.Contains(s, " LIMIT 3") {
		limit = 3
		width = 3
	}
	if len(selected) > limit {
		selected = selected[:limit]
	}
	return &maintenanceRowsTest{data: selected, width: width}, nil
}
func (c *maintenanceConnTest) ExecContext(ctx context.Context, s string, args []driver.NamedValue) (driver.Result, error) {
	f := c.f
	f.writes++
	if ctx.Err() != nil || f.failWrite || f.failWriteAt == f.writes {
		return nil, errors.New("private write detail")
	}
	f.statements = append(f.statements, s)
	switch {
	case strings.HasPrefix(s, "UPDATE external_identities "):
		for _, row := range f.pending["external_identities"] {
			if row[0] == args[1].Value && row[1] == args[2].Value && row[2] == args[3].Value {
				row[6] = args[0].Value
				return driver.RowsAffected(1), nil
			}
		}
	case strings.HasPrefix(s, "UPDATE web_sessions "):
		for _, row := range f.pending["web_sessions"] {
			if row[0] == args[2].Value && row[1] == args[3].Value {
				row[13] = args[0].Value
				row[12] = args[1].Value
				return driver.RowsAffected(1), nil
			}
		}
	case strings.HasPrefix(s, "DELETE FROM oidc_login_challenges "):
		rows := f.pending["oidc_login_challenges"]
		for i, row := range rows {
			if row[0] == args[0].Value && row[1] == args[1].Value {
				f.pending["oidc_login_challenges"] = append(rows[:i], rows[i+1:]...)
				return driver.RowsAffected(1), nil
			}
		}
	}
	return nil, errors.New("unexpected write")
}
func maintenanceRequestTest(mode string) IdentityMaintenanceRequestV1 {
	return IdentityMaintenanceRequestV1{Mode: mode, EnvironmentDigest: domain.DigestSecret("maintenance-test-environment"), ManifestDigest: domain.DigestSecret("maintenance-test-manifest"), SourceCommit: strings.Repeat("a", 40)}
}
func maintenanceFixtureTest(t *testing.T) (context.Context, *sql.DB, *maintenanceSQLTest) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	f := &maintenanceSQLTest{at: time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC), data: map[string][][]driver.Value{}}
	db := sql.OpenDB(f)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if e := db.Close(); e != nil {
			t.Error(e)
		}
	})
	return ctx, db, f
}
func maintenanceLegacyIdentityTest(f *maintenanceSQLTest, provider domain.IdentityProvider, subject, user string) {
	created := f.at.Add(-time.Hour).Add(789 * time.Nanosecond)
	v := domain.ExternalIdentity{Subject: domain.ExternalSubject{Provider: provider, Subject: subject}, UserID: domain.UserID(user), CreatedAt: created, UpdatedAt: created.Add(time.Second)}
	raw, _ := json.Marshal(v)
	bucket, _ := webBucket(v.Subject.String())
	ub, _ := webBucket(user)
	f.data["external_identities"] = append(f.data["external_identities"], []driver.Value{int64(bucket), string(provider), subject, user, v.CreatedAt.Truncate(time.Microsecond), v.UpdatedAt.Truncate(time.Microsecond), string(raw)})
	f.data["external_identities_by_user"] = append(f.data["external_identities_by_user"], []driver.Value{int64(ub), user, string(provider), subject, v.CreatedAt.Truncate(time.Microsecond)})
}
func maintenanceLegacyCookieTest(f *maintenanceSQLTest) string {
	key := string(domain.DigestSecret("private-maintenance-cookie"))
	bucket, _ := webBucket(key)
	s := domain.WebSession{SessionDigest: domain.SecretDigest(key), CSRFTokenDigest: domain.DigestSecret("csrf"), UserID: "maintenance-owner", ActiveTenantID: "maintenance-tenant", AuthenticatedSubject: domain.ExternalSubject{Provider: domain.IdentityProviderYandex, Subject: "maintenance-subject"}, MembershipSecurityVersion: 1, IssuedAt: f.at.Add(-time.Hour), LastSeenAt: f.at.Add(-time.Minute), IdleExpiresAt: f.at.Add(time.Hour), AbsoluteExpiresAt: f.at.Add(2 * time.Hour)}
	raw, _ := json.Marshal(s)
	f.data["web_sessions"] = [][]driver.Value{{int64(bucket), key, string(s.CSRFTokenDigest), string(s.UserID), string(s.ActiveTenantID), string(s.AuthenticatedSubject.Provider), s.AuthenticatedSubject.Subject, int64(1), s.IssuedAt, s.LastSeenAt, s.IdleExpiresAt, s.AbsoluteExpiresAt, time.Unix(0, 0).UTC(), string(raw)}}
	return key
}
func maintenanceLegacyChallengeTest(f *maintenanceSQLTest) string {
	key := string(domain.DigestSecret("private-maintenance-state"))
	bucket, _ := webBucket(key)
	v := domain.OIDCLoginChallenge{StateDigest: domain.SecretDigest(key), BrowserBindingDigest: domain.DigestSecret("private-browser"), PKCEVerifier: strings.Repeat("p", 64), Nonce: "private-nonce", RedirectPath: "/", CreatedAt: f.at.Add(-time.Minute), ExpiresAt: f.at.Add(time.Minute)}
	raw, _ := json.Marshal(v)
	f.data["oidc_login_challenges"] = [][]driver.Value{{int64(bucket), key, string(v.BrowserBindingDigest), v.PKCEVerifier, v.Nonce, v.RedirectPath, v.CreatedAt, v.ExpiresAt, time.Unix(0, 0).UTC(), string(raw), nil}}
	return key
}
func maintenanceRunTest(t *testing.T, ctx context.Context, db *sql.DB, r IdentityMaintenanceRequestV1) (IdentityMaintenanceResultV1, error) {
	t.Helper()
	return maintainIdentityV1(ctx, db, maintenanceSchemaTest{}, "/isolated-test", r)
}
func maintenanceReportTest(t *testing.T, r IdentityMaintenanceResultV1) identityMaintenanceReport {
	t.Helper()
	var v identityMaintenanceReport
	if json.Unmarshal(r.Report, &v) != nil {
		t.Fatal("invalid redacted report")
	}
	return v
}

func TestIdentityMaintenanceV1ConvertAtomicGroupThenExactV2Join(t *testing.T) {
	ctx, db, f := maintenanceFixtureTest(t)
	maintenanceLegacyIdentityTest(f, domain.IdentityProviderYandex, "private-yandex", "maintenance-owner")
	maintenanceLegacyIdentityTest(f, domain.IdentityProviderTelegram, "private-telegram", "maintenance-owner")
	original := maintenanceCloneTest(f.data)
	r := maintenanceRequestTest("prepare")
	r.Kind = "identity"
	r.Selector = "maintenance-owner"
	prepared, e := maintenanceRunTest(t, ctx, db, r)
	if e != nil {
		t.Fatal("valid two-method legacy baseline", e)
	}
	if f.writes != 0 || !reflect.DeepEqual(f.data, original) {
		t.Fatal("prepare mutated rows")
	}
	var c identityMaintenanceCandidate
	if identityMaintenanceStrict(prepared.Candidate, &c, 16384) != nil || c.PrecisionProjected != 2 {
		t.Fatal("missing immutable projected candidate")
	}
	r = maintenanceRequestTest("apply")
	r.Candidate = prepared.Candidate
	got, e := maintenanceRunTest(t, ctx, db, r)
	if e != nil || maintenanceReportTest(t, got).Outcome != "applied" || f.writes != 2 {
		t.Fatalf("apply group writes=%d err=%v report=%s", f.writes, e, got.Report)
	}
	if !reflect.DeepEqual(f.data["external_identities_by_user"], original["external_identities_by_user"]) {
		t.Fatal("reverse timestamps changed")
	}
	tx, e := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if e != nil {
		t.Fatal(e)
	}
	active, e := readActiveExternalIdentitiesV2(ctx, tx, "maintenance-owner")
	_ = tx.Rollback()
	if e != nil || len(active) != 2 {
		t.Fatalf("genuine existing V2 active-set join count=%d err=%v", len(active), e)
	}
	for _, identity := range active {
		if identity.Revision != 1 || identity.CreatedAt.Nanosecond()%1000 != 0 {
			t.Fatal("wrong conversion postimage")
		}
	}
	before := f.writes
	again, e := maintenanceRunTest(t, ctx, db, r)
	if e != nil || maintenanceReportTest(t, again).Outcome != "alreadyDesiredObserved" || f.writes != before {
		t.Fatal("idempotence failed", e)
	}
	inventory, e := maintenanceRunTest(t, ctx, db, maintenanceRequestTest("inventory"))
	if e != nil || !maintenanceReportTest(t, inventory).Ready {
		t.Fatal("zero-V1 final predicate", e)
	}
}
func TestIdentityMaintenanceV1RetireNullChallengeAndUnknownDeleteActor(t *testing.T) {
	ctx, db, f := maintenanceFixtureTest(t)
	key := maintenanceLegacyChallengeTest(f)
	r := maintenanceRequestTest("prepare")
	r.Kind = "challenge"
	r.Selector = key
	prepared, e := maintenanceRunTest(t, ctx, db, r)
	if e != nil {
		t.Fatal("valid NULL-retention V1 baseline", e)
	}
	r = maintenanceRequestTest("apply")
	r.Candidate = prepared.Candidate
	f.lostCommit = true
	got, e := maintenanceRunTest(t, ctx, db, r)
	if !errors.Is(e, ErrIdentityMaintenancePossiblyCommittedV1) || maintenanceReportTest(t, got).Outcome != "possiblyCommitted" {
		t.Fatal("lost DELETE commit fiction", e)
	}
	writes := f.writes
	r.Mode = "reconcile"
	got, e = maintenanceRunTest(t, ctx, db, r)
	if e != nil || maintenanceReportTest(t, got).Outcome != "desiredAbsentObservedActorUnknown" || f.writes != writes {
		t.Fatal("absence attributed to this actor", e)
	}
	r.Mode = "apply"
	got, e = maintenanceRunTest(t, ctx, db, r)
	if e != nil || maintenanceReportTest(t, got).Outcome != "desiredAbsentObservedActorUnknown" || f.writes != writes {
		t.Fatal("absence blindly reapplied", e)
	}
}
func TestIdentityMaintenanceV1CookiePreparedTimeAndOriginalExpiry(t *testing.T) {
	ctx, db, f := maintenanceFixtureTest(t)
	key := maintenanceLegacyCookieTest(f)
	original := slices.Clone(f.data["web_sessions"][0])
	r := maintenanceRequestTest("prepare")
	r.Kind = "cookie"
	r.Selector = key
	prepared, e := maintenanceRunTest(t, ctx, db, r)
	if e != nil {
		t.Fatal(e)
	}
	sample := f.at
	f.at = f.at.Add(time.Minute)
	r = maintenanceRequestTest("apply")
	r.Candidate = prepared.Candidate
	got, e := maintenanceRunTest(t, ctx, db, r)
	if e != nil || maintenanceReportTest(t, got).Outcome != "applied" {
		t.Fatal(e)
	}
	row := f.data["web_sessions"][0]
	for i := 0; i < 12; i++ {
		if !reflect.DeepEqual(row[i], original[i]) {
			t.Fatalf("cookie fact %d changed", i)
		}
	}
	if row[12] != sample {
		t.Fatal("revocation intent renewed")
	}
	r.Mode = "reconcile"
	got, e = maintenanceRunTest(t, ctx, db, r)
	if e != nil || maintenanceReportTest(t, got).Outcome != "exactPostimageObserved" || f.writes != 1 {
		t.Fatal("cookie replay", e)
	}
}
func TestIdentityMaintenanceV1AnomaliesNeverSelectFavorableSubset(t *testing.T) {
	for _, name := range []string{"missing_reverse", "foreign_owner", "duplicate_provider", "bad_precision", "unknown_json", "duplicate_json", "null_v2", "mixed_v1_v2", "pending_checksum", "metadata_error", "stale_candidate", "write_error"} {
		t.Run(name, func(t *testing.T) {
			ctx, db, f := maintenanceFixtureTest(t)
			maintenanceLegacyIdentityTest(f, domain.IdentityProviderYandex, "private-subject", "maintenance-owner")
			r := maintenanceRequestTest("prepare")
			r.Kind = "identity"
			r.Selector = "maintenance-owner"
			baseline, e := maintenanceRunTest(t, ctx, db, r)
			if e != nil {
				t.Fatal("coherent baseline", e)
			}
			original := maintenanceCloneTest(f.data)
			switch name {
			case "missing_reverse":
				f.data["external_identities_by_user"] = nil
			case "foreign_owner":
				f.data["external_identities_by_user"][0][1] = "foreign-owner"
			case "duplicate_provider":
				maintenanceLegacyIdentityTest(f, domain.IdentityProviderYandex, "second-subject", "maintenance-owner")
			case "bad_precision":
				f.data["external_identities"][0][4] = f.at.Add(-time.Hour - time.Microsecond)
			case "unknown_json":
				f.data["external_identities"][0][6] = strings.Replace(f.data["external_identities"][0][6].(string), "{", "{\"unknown\":true,", 1)
			case "duplicate_json":
				f.data["external_identities"][0][6] = strings.Replace(f.data["external_identities"][0][6].(string), "{", "{\"user_id\":\"maintenance-owner\",", 1)
			case "null_v2":
				maintenanceLegacyChallengeTest(f)
				f.data["oidc_login_challenges"][0][9] = `{"version":2}`
			case "mixed_v1_v2":
				maintenanceLegacyIdentityTest(f, domain.IdentityProviderTelegram, "second-subject", "maintenance-owner")
				var old domain.ExternalIdentity
				_ = json.Unmarshal([]byte(f.data["external_identities"][1][6].(string)), &old)
				v := domain.ExternalIdentityV2{Version: 2, Subject: old.Subject, UserID: old.UserID, CreatedAt: old.CreatedAt.Truncate(time.Microsecond), UpdatedAt: old.UpdatedAt.Truncate(time.Microsecond), State: domain.ExternalIdentityActiveV2, Revision: 1}
				raw, _ := domain.EncodeExternalIdentityV2(v)
				f.data["external_identities"][1][6] = string(raw)
			case "pending_checksum":
				f.badMetadata = true
			case "metadata_error":
				f.failRead = true
			case "stale_candidate":
				f.data["external_identities"][0][5] = f.at
			case "write_error":
				f.failWrite = true
			}
			r = maintenanceRequestTest("apply")
			r.Candidate = baseline.Candidate
			got, e := maintenanceRunTest(t, ctx, db, r)
			if e == nil || maintenanceReportTest(t, got).Outcome == "applied" {
				t.Fatalf("%s accepted", name)
			}
			if name == "write_error" {
				if !reflect.DeepEqual(f.data, original) {
					t.Fatal("partial group committed")
				}
			} else if f.writes != 0 {
				t.Fatal("anomaly mutated rows")
			}
			if strings.Contains(string(got.Report), "private-subject") || strings.Contains(string(got.Report), "private SQL") {
				t.Fatal("report exposed private material")
			}
		})
	}
}
func TestIdentityMaintenanceV1CursorAndBudgetCaps(t *testing.T) {
	ctx, db, f := maintenanceFixtureTest(t)
	bucket := uint32(0)
	const searchCap = 4096
	for n := 0; n < searchCap && len(f.data["external_identities"]) < 18; n++ {
		if err := ctx.Err(); err != nil {
			t.Fatalf("bounded fixture key search cancelled after %d candidates, found=%d/18: %v", n, len(f.data["external_identities"]), err)
		}
		subject := fmt.Sprintf("subject-%04x", n)
		b, _ := webBucket("yandex:" + subject)
		if b == bucket {
			maintenanceLegacyIdentityTest(f, domain.IdentityProviderYandex, subject, "owner-"+subject)
		}
	}
	if found := len(f.data["external_identities"]); found != 18 {
		t.Fatalf("bounded fixture key search exhausted %d fixed-size candidates: bucket=%d found=%d want=18", searchCap, bucket, found)
	}
	got, e := maintenanceRunTest(t, ctx, db, maintenanceRequestTest("inventory"))
	if e != nil || maintenanceReportTest(t, got).Rows != 36 {
		t.Fatalf("sentinel dropped row report=%s err=%v", got.Report, e)
	}
	pages := maintenanceReportTest(t, got).Pages
	if pages != 65 {
		t.Fatalf("last-accepted cursor pages=%d want65", pages)
	}
	for _, which := range []string{"material", "statements", "transactions", "pages"} {
		t.Run(which, func(t *testing.T) {
			b := &identityMaintenanceBudget{}
			switch which {
			case "material":
				b.material = 128 << 20
			case "statements":
				b.statements = 160
			case "transactions":
				b.tx = 134
			case "pages":
				b.pages = 128
			}
			var err error
			if which == "material" {
				err = b.charge(1)
			} else if which == "statements" {
				err = b.statement()
			} else if which == "transactions" {
				_, _, _, err = b.begin(ctx, db)
			} else {
				_, err = identityMaintenanceInventory(ctx, db, b, f.at)
			}
			if err == nil {
				t.Fatal("cap accepted")
			}
			if which != "pages" && b.charge(0) == nil {
				t.Fatal("failure reset")
			}
		})
	}
}
func TestIdentityMaintenanceV1StrictArtifactsAndV2NoNormalization(t *testing.T) {
	ctx, db, f := maintenanceFixtureTest(t)
	maintenanceLegacyIdentityTest(f, domain.IdentityProviderYandex, "subject", "maintenance-owner")
	r := maintenanceRequestTest("prepare")
	r.Kind = "identity"
	r.Selector = "maintenance-owner"
	prepared, e := maintenanceRunTest(t, ctx, db, r)
	if e != nil {
		t.Fatal(e)
	}
	for _, raw := range []string{strings.Replace(string(prepared.Candidate), `"contract":`, `"CONTRACT":`, 1), strings.Replace(string(prepared.Candidate), "{", `{"contract":"identity-maintenance.v1",`, 1), string(prepared.Candidate) + "{}", strings.Replace(string(prepared.Candidate), "maintenance-owner", `bad\ud800`, 1)} {
		var c identityMaintenanceCandidate
		if identityMaintenanceStrict([]byte(raw), &c, 16384) == nil {
			t.Fatal("malformed artifact accepted")
		}
	}
	r = maintenanceRequestTest("apply")
	r.Candidate = prepared.Candidate
	if _, e := maintenanceRunTest(t, ctx, db, r); e != nil {
		t.Fatal(e)
	}
	var v domain.ExternalIdentityV2
	_ = json.Unmarshal([]byte(f.data["external_identities"][0][6].(string)), &v)
	v.CreatedAt = v.CreatedAt.Add(time.Nanosecond)
	raw, _ := domain.EncodeExternalIdentityV2(v)
	f.data["external_identities"][0][6] = string(raw)
	before := f.writes
	if _, e := maintenanceRunTest(t, ctx, db, maintenanceRequestTest("inventory")); e == nil || f.writes != before {
		t.Fatal("V2 timestamp was normalized")
	}
}

func TestIdentityMaintenanceV1TombstonesRemainWithoutActiveProjection(t *testing.T) {
	ctx, db, f := maintenanceFixtureTest(t)
	v := domain.ExternalIdentityV2{Version: 2, Subject: domain.ExternalSubject{Provider: domain.IdentityProviderTelegram, Subject: "private-tombstone"}, UserID: "prior-owner", CreatedAt: f.at.Add(-time.Hour), UpdatedAt: f.at.Add(-time.Minute), State: domain.ExternalIdentityUnlinkedV2, Revision: 7}
	if v.Validate() != nil {
		t.Fatal("invalid tombstone baseline")
	}
	raw, _ := domain.EncodeExternalIdentityV2(v)
	bucket, _ := webBucket(v.Subject.String())
	f.data["external_identities"] = [][]driver.Value{{int64(bucket), string(v.Subject.Provider), v.Subject.Subject, string(v.UserID), v.CreatedAt, v.UpdatedAt, string(raw)}}
	before := maintenanceCloneTest(f.data)
	got, e := maintenanceRunTest(t, ctx, db, maintenanceRequestTest("inventory"))
	if e != nil || !maintenanceReportTest(t, got).Ready || !reflect.DeepEqual(f.data, before) || f.writes != 0 {
		t.Fatal("valid tombstone-only group not retained", e)
	}
	ub, _ := webBucket(string(v.UserID))
	f.data["external_identities_by_user"] = [][]driver.Value{{int64(ub), string(v.UserID), string(v.Subject.Provider), v.Subject.Subject, v.CreatedAt}}
	if _, e := maintenanceRunTest(t, ctx, db, maintenanceRequestTest("inventory")); e == nil {
		t.Fatal("tombstone reactivation projection accepted")
	}
}

func TestIdentityMaintenanceV1CoherentChangedPreimageAndAtomicRollback(t *testing.T) {
	for _, change := range []string{"before_apply", "during_exact", "lost_identity_commit", "second_write_rollback"} {
		t.Run(change, func(t *testing.T) {
			ctx, db, f := maintenanceFixtureTest(t)
			maintenanceLegacyIdentityTest(f, domain.IdentityProviderYandex, "first-subject", "maintenance-owner")
			maintenanceLegacyIdentityTest(f, domain.IdentityProviderTelegram, "second-subject", "maintenance-owner")
			r := maintenanceRequestTest("prepare")
			r.Kind = "identity"
			r.Selector = "maintenance-owner"
			prepared, e := maintenanceRunTest(t, ctx, db, r)
			if e != nil {
				t.Fatal("coherent two-row baseline", e)
			}
			mutate := func(rows map[string][][]driver.Value) {
				row := rows["external_identities"][0]
				var old domain.ExternalIdentity
				if json.Unmarshal([]byte(row[6].(string)), &old) != nil {
					t.Fatal("bad original")
				}
				old.UpdatedAt = f.at.Add(-time.Second)
				raw, _ := json.Marshal(old)
				row[6] = string(raw)
				row[5] = old.UpdatedAt
			}
			if change == "before_apply" {
				mutate(f.data)
			}
			if change == "during_exact" {
				f.beforeExact = func(f *maintenanceSQLTest) { mutate(f.pending) }
			}
			if change == "lost_identity_commit" {
				f.lostCommit = true
			}
			if change == "second_write_rollback" {
				f.failWriteAt = 2
			}
			r = maintenanceRequestTest("apply")
			r.Candidate = prepared.Candidate
			got, e := maintenanceRunTest(t, ctx, db, r)
			if e == nil || maintenanceReportTest(t, got).Outcome == "applied" {
				t.Fatal("contradiction accepted", change)
			}
			if change == "lost_identity_commit" {
				r.Mode = "reconcile"
				got, e = maintenanceRunTest(t, ctx, db, r)
				if e != nil || maintenanceReportTest(t, got).Outcome != "exactPostimageObserved" || f.writes != 2 {
					t.Fatal("lost identity commit replayed", e)
				}
			} else {
				for _, row := range f.data["external_identities"] {
					if _, decodeErr := decodeExternalIdentityV1([]byte(row[6].(string))); decodeErr != nil {
						t.Fatal("failed group partially converted", decodeErr)
					}
				}
			}
		})
	}
}

func TestIdentityMaintenanceV1MetadataPhysicalTypesBeforeInventory(t *testing.T) {
	type contradiction struct {
		name, query string
		column      int
		value       driver.Value
		rows        int
	}
	cases := []contradiction{
		{"checksum_text_version", identityMaintenanceChecksumsSQL, 0, "130", 0},
		{"goose_text_version", identityMaintenanceGooseSQL, 0, "130", 0},
		{"goose_numeric_bool", identityMaintenanceGooseSQL, 1, int64(1), 0},
		{"goose_text_bool", identityMaintenanceGooseSQL, 1, "true", 0},
		{"checksum_missing", identityMaintenanceChecksumsSQL, 0, nil, 1},
		{"goose_missing", identityMaintenanceGooseSQL, 0, nil, 1},
		{"checksum_excess", identityMaintenanceChecksumsSQL, 0, nil, 3},
		{"goose_excess", identityMaintenanceGooseSQL, 0, nil, 3},
		{"wrong_filename", identityMaintenanceChecksumsSQL, 1, "00130_foreign.sql", 0},
		{"wrong_hash", identityMaintenanceChecksumsSQL, 2, strings.Repeat("0", 64), 0},
		{"pending", identityMaintenanceChecksumsSQL, 3, "pending", 0},
		{"bad_utf8", identityMaintenanceChecksumsSQL, 3, "\xff", 0},
		{"long_filename", identityMaintenanceChecksumsSQL, 1, strings.Repeat("n", 257), 0},
		{"long_hash", identityMaintenanceChecksumsSQL, 2, strings.Repeat("a", 65), 0},
		{"long_state", identityMaintenanceChecksumsSQL, 3, strings.Repeat("a", 17), 0},
		{"false_goose", identityMaintenanceGooseSQL, 1, false, 0},
		{"checksum_text_time", identityMaintenanceChecksumsSQL, 4, "2026-11-04T11:00:00Z", 0},
		{"goose_text_time", identityMaintenanceGooseSQL, 2, "2026-11-04T11:00:00Z", 0},
	}
	for _, query := range []string{identityMaintenanceChecksumsSQL, identityMaintenanceGooseSQL} {
		width := 6
		label := "checksum"
		if query == identityMaintenanceGooseSQL {
			width = 3
			label = "goose"
		}
		for i := 0; i < width; i++ {
			cases = append(cases, contradiction{label + "_null_" + strconv.Itoa(i), query, i, nil, 0})
		}
	}
	for _, name := range []string{"future_checksum", "reversed_checksum", "future_goose", "submicro_goose"} {
		query := identityMaintenanceChecksumsSQL
		column := 5
		if strings.HasSuffix(name, "goose") {
			query = identityMaintenanceGooseSQL
			column = 2
		}
		cases = append(cases, contradiction{name, query, column, nil, -1})
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			ctx, db, f := maintenanceFixtureTest(t)
			request := maintenanceRequestTest("inventory")
			baseline, err := maintenanceRunTest(t, ctx, db, request)
			if err != nil || !maintenanceReportTest(t, baseline).Complete || f.writes != 0 {
				t.Fatal("valid physical metadata baseline", err)
			}
			frontier := len(f.statements)
			mutated := false
			f.metadataAlter = func(query string, rows [][]driver.Value) [][]driver.Value {
				if query != test.query {
					return rows
				}
				mutated = true
				for i := range rows {
					rows[i] = slices.Clone(rows[i])
				}
				switch test.rows {
				case 1:
					return rows[:1]
				case 3:
					return append(rows, slices.Clone(rows[1]))
				case -1:
					switch test.name {
					case "future_checksum", "future_goose":
						rows[0][test.column] = f.at.Add(time.Microsecond)
					case "reversed_checksum":
						rows[0][4] = f.at
					case "submicro_goose":
						rows[0][test.column] = f.at.Add(-time.Minute + time.Nanosecond)
					}
				default:
					rows[0][test.column] = test.value
				}
				return rows
			}
			got, err := maintenanceRunTest(t, ctx, db, request)
			if !mutated || err == nil || maintenanceReportTest(t, got).Complete || len(got.Candidate) != 0 || f.writes != 0 {
				t.Fatalf("metadata contradiction accepted name=%s mutated=%v err=%v", test.name, mutated, err)
			}
			for _, query := range f.statements[frontier:] {
				if query != `SELECT CurrentUtcTimestamp()` && query != identityMaintenanceChecksumsSQL && query != identityMaintenanceGooseSQL {
					t.Fatal("metadata denial reached inventory/DML", query)
				}
			}
		})
	}
}

// This adapter NEVER supplies a database result, receipt, commit or SQL error
// instead of the real SDK. It can run a synchronous peer transaction after an
// actual bounded read closes, or lose one ACK AFTER the real Commit returned nil.
type maintenanceNativeControl struct {
	mu                                                                sync.Mutex
	query                                                             string
	afterRead                                                         func(context.Context) error
	fired, observedRows, writeAttempts, realWriteCommits, lostReplies int
	hookErr                                                           error
	loseReply                                                         bool
	nativeErrors                                                      []maintenanceNativeError
	errorOverflow                                                     bool
}
type maintenanceNativeError struct {
	phase string
	err   error
}

func (c *maintenanceNativeControl) observe(phase string, err error) {
	if err == nil || errors.Is(err, io.EOF) {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.nativeErrors) >= 16 {
		c.errorOverflow = true
		return
	}
	c.nativeErrors = append(c.nativeErrors, maintenanceNativeError{phase: phase, err: err})
}

// These exported helpers/status are from pinned SDK errors.go, not error text.
// TLI is the SDK's ABORTED/issue2001 case. A timeout, transport or generic
// authorization/storage denial cannot substitute for a real native conflict.
func maintenanceNativeConflictClass(err error) string {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || ydb.IsTimeoutError(err) || ydb.IsTransportError(err) {
		return "not_conflict"
	}
	if ydb.IsOperationErrorTransactionLocksInvalidated(err) {
		return "transaction_locks_invalidated"
	}
	if ydb.IsOperationError(err, Ydb.StatusIds_ABORTED) {
		return "aborted"
	}
	return "not_conflict"
}

type maintenanceNativeConnector struct {
	actual  driver.Connector
	control *maintenanceNativeControl
}

func (c *maintenanceNativeConnector) Driver() driver.Driver { return c.actual.Driver() }
func (c *maintenanceNativeConnector) Connect(ctx context.Context) (driver.Conn, error) {
	actual, err := c.actual.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &maintenanceNativeConn{Conn: actual, control: c.control}, nil
}

type maintenanceNativeConn struct {
	driver.Conn
	control *maintenanceNativeControl
	active  *maintenanceNativeTx
}

func (c *maintenanceNativeConn) CheckNamedValue(v *driver.NamedValue) error {
	checker, ok := c.Conn.(driver.NamedValueChecker)
	if !ok {
		return driver.ErrSkip
	}
	return checker.CheckNamedValue(v)
}
func (c *maintenanceNativeConn) BeginTx(ctx context.Context, o driver.TxOptions) (driver.Tx, error) {
	begin, ok := c.Conn.(driver.ConnBeginTx)
	if !ok {
		return nil, errors.New("native SDK lacks explicit transaction interface")
	}
	actual, err := begin.BeginTx(ctx, o)
	if err != nil {
		return nil, err
	}
	tx := &maintenanceNativeTx{actual: actual, conn: c}
	c.control.mu.Lock()
	c.active = tx
	c.control.mu.Unlock()
	return tx, nil
}
func (c *maintenanceNativeConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	q, ok := c.Conn.(driver.QueryerContext)
	if !ok {
		return nil, errors.New("native SDK lacks context query")
	}
	rows, err := q.QueryContext(ctx, query, args)
	c.control.observe("query", err)
	if err != nil {
		return nil, err
	}
	c.control.mu.Lock()
	selected := query == c.control.query && c.control.afterRead != nil && c.control.fired == 0
	c.control.mu.Unlock()
	return &maintenanceNativeRows{Rows: rows, control: c.control, ctx: ctx, selected: selected}, nil
}
func (c *maintenanceNativeConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	exec, ok := c.Conn.(driver.ExecerContext)
	if !ok {
		return nil, errors.New("native SDK lacks context exec")
	}
	c.control.mu.Lock()
	c.control.writeAttempts++
	if c.active != nil {
		c.active.wrote = true
	}
	c.control.mu.Unlock()
	result, err := exec.ExecContext(ctx, query, args)
	c.control.observe("exec", err)
	return result, err
}

type maintenanceNativeRows struct {
	driver.Rows
	control  *maintenanceNativeControl
	ctx      context.Context
	once     sync.Once
	mu       sync.Mutex
	count    int
	err      error
	selected bool
}

func (r *maintenanceNativeRows) Next(dest []driver.Value) error {
	err := r.Rows.Next(dest)
	r.control.observe("rows.next", err)
	if err == nil {
		r.mu.Lock()
		r.count++
		r.mu.Unlock()
	}
	return err
}
func (r *maintenanceNativeRows) Close() error {
	r.once.Do(func() {
		r.err = r.Rows.Close()
		r.control.observe("rows.close", r.err)
		if r.err != nil {
			return
		}
		if !r.selected {
			return
		}
		r.mu.Lock()
		count := r.count
		r.mu.Unlock()
		r.control.mu.Lock()
		hook := r.control.afterRead
		if r.control.fired != 0 {
			hook = nil
		} else {
			r.control.fired++
			r.control.observedRows = count
		}
		r.control.mu.Unlock()
		if hook != nil {
			r.err = hook(r.ctx)
			r.control.mu.Lock()
			r.control.hookErr = r.err
			r.control.mu.Unlock()
		}
	})
	return r.err
}

type maintenanceNativeTx struct {
	actual driver.Tx
	conn   *maintenanceNativeConn
	wrote  bool
}

func (tx *maintenanceNativeTx) Rollback() error {
	err := tx.actual.Rollback()
	tx.conn.control.mu.Lock()
	tx.conn.active = nil
	tx.conn.control.mu.Unlock()
	return err
}
func (tx *maintenanceNativeTx) Commit() error {
	err := tx.actual.Commit()
	c := tx.conn.control
	c.observe("commit", err)
	c.mu.Lock()
	defer c.mu.Unlock()
	tx.conn.active = nil
	if err != nil {
		return err
	}
	if tx.wrote {
		c.realWriteCommits++
		if c.loseReply {
			c.loseReply = false
			c.lostReplies++
			return errors.New("test lost reply after acknowledged actual SDK commit")
		}
	}
	return nil
}

func maintenanceNativeConflict(t *testing.T, ctx context.Context, db, wrapped *sql.DB, client *ydbclient.Client, base string, control *maintenanceNativeControl, kind string) {
	t.Helper()
	request := maintenanceRequestTest("prepare")
	request.Kind = "identity"
	request.Selector = "native-maintenance-owner"
	prepared, err := maintainIdentityV1(ctx, db, client.Table(), base, request)
	if err != nil {
		t.Fatal("valid native candidate before conflict", kind, err)
	}
	identityBucket, _ := webBucket("yandex:native-subject")
	userBucket, _ := webBucket("native-maintenance-owner")
	peerCommitted := false
	var removedCreated time.Time
	query := webIdentityActiveSetSQLV2
	expectedRows := 2
	if kind == "point" {
		table := identityMaintenanceTables()[0]
		query = "SELECT " + identityMaintenanceProjection(table) + " FROM external_identities WHERE shard_bucket=$1 AND provider=$2 AND subject=$3 LIMIT 2"
		expectedRows = 1
	}
	control.mu.Lock()
	control.query = query
	control.fired = 0
	control.hookErr = nil
	control.nativeErrors = nil
	control.errorOverflow = false
	commitsBefore := control.realWriteCommits
	control.afterRead = func(call context.Context) error {
		// The competing work is synchronous inside the SAME original 5s
		// query/transaction context; it gets no fresh longer lifetime.
		peer, e := db.BeginTx(call, &sql.TxOptions{Isolation: sql.LevelSerializable})
		if e != nil {
			return e
		}
		defer peer.Rollback()
		if kind == "point" {
			var raw string
			var at time.Time
			if e := peer.QueryRowContext(call, webIdentityRowSQLV2, identityBucket, domain.IdentityProviderYandex, "native-subject").Scan(&raw); e != nil {
				return e
			}
			old, e := decodeExternalIdentityV1([]byte(raw))
			if e != nil {
				return e
			}
			if e := peer.QueryRowContext(call, `SELECT CurrentUtcTimestamp()`).Scan(&at); e != nil {
				return e
			}
			old.UpdatedAt = at
			next, e := json.Marshal(old)
			if e != nil {
				return e
			}
			if _, e := peer.ExecContext(call, `UPDATE external_identities SET updated_at=$1,record=CAST($2 AS JsonDocument) WHERE shard_bucket=$3 AND provider=$4 AND subject=$5`, at, string(next), identityBucket, domain.IdentityProviderYandex, "native-subject"); e != nil {
				return e
			}
		} else {
			// A real competing storage mutation of the OWNED reverse projection,
			// not a fabricated provider grant or a proposed valid unlink flow.
			if e := peer.QueryRowContext(call, `SELECT created_at FROM external_identities_by_user WHERE user_bucket=$1 AND user_id=$2 AND provider=$3 AND subject=$4`, userBucket, "native-maintenance-owner", domain.IdentityProviderTelegram, "native-tg-subject").Scan(&removedCreated); e != nil {
				return e
			}
			if _, e := peer.ExecContext(call, `DELETE FROM external_identities_by_user WHERE user_bucket=$1 AND user_id=$2 AND provider=$3 AND subject=$4`, userBucket, "native-maintenance-owner", domain.IdentityProviderTelegram, "native-tg-subject"); e != nil {
				return e
			}
		}
		if e := peer.Commit(); e != nil {
			return e
		}
		control.mu.Lock()
		peerCommitted = true
		control.mu.Unlock()
		return nil
	}
	control.mu.Unlock()
	request = maintenanceRequestTest("apply")
	request.Candidate = prepared.Candidate
	got, applyErr := maintainIdentityV1(ctx, wrapped, client.Table(), base, request)
	control.mu.Lock()
	fired, observed, hookErr, committed := control.fired, control.observedRows, control.hookErr, peerCommitted
	nativeErrors := slices.Clone(control.nativeErrors)
	overflow, commitsAfter := control.errorOverflow, control.realWriteCommits
	control.afterRead = nil
	control.query = ""
	control.mu.Unlock()
	if fired != 1 || observed != expectedRows || hookErr != nil || !committed {
		t.Fatalf("native conflict barrier kind=%s fired=%d rows=%d peerCommitted=%v hookErr=%v", kind, fired, observed, committed, hookErr)
	}
	if applyErr == nil || maintenanceReportTest(t, got).Outcome == "applied" {
		t.Fatalf("actual native %s serializable conflict did not deny: err=%v report=%s", kind, applyErr, got.Report)
	}
	if overflow || commitsAfter != commitsBefore || len(nativeErrors) == 0 {
		t.Fatalf("native conflict evidence kind=%s errors=%d overflow=%v primarySuccessfulCommits=%d", kind, len(nativeErrors), overflow, commitsAfter-commitsBefore)
	}
	conflict := false
	for _, observedError := range nativeErrors {
		class := maintenanceNativeConflictClass(observedError.err)
		if class == "not_conflict" {
			t.Fatalf("native %s denial substituted non-conflict at phase=%s", kind, observedError.phase)
		}
		conflict = true
	}
	if !conflict {
		t.Fatal("missing real SDK conflict/lock-invalidated error", kind)
	}
	// Native readback, not fixture bookkeeping, proves the original conversion
	// did not partly commit across its two canonical rows.
	for _, subject := range []domain.ExternalSubject{{Provider: domain.IdentityProviderYandex, Subject: "native-subject"}, {Provider: domain.IdentityProviderTelegram, Subject: "native-tg-subject"}} {
		bucket, _ := webBucket(subject.String())
		var raw string
		if e := db.QueryRowContext(ctx, webIdentityRowSQLV2, bucket, subject.Provider, subject.Subject).Scan(&raw); e != nil {
			t.Fatal(e)
		}
		if _, e := decodeExternalIdentityV1([]byte(raw)); e != nil {
			t.Fatal("native conflicting candidate partially committed", kind, e)
		}
	}
	if kind == "range" {
		// Restore only the exact projection captured from this owned namespace,
		// by absence-checked INSERT (no overwrite/current-owner reassignment).
		peer, e := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
		if e != nil {
			t.Fatal(e)
		}
		defer peer.Rollback()
		var existing time.Time
		e = peer.QueryRowContext(ctx, `SELECT created_at FROM external_identities_by_user WHERE user_bucket=$1 AND user_id=$2 AND provider=$3 AND subject=$4`, userBucket, "native-maintenance-owner", domain.IdentityProviderTelegram, "native-tg-subject").Scan(&existing)
		if !errors.Is(e, sql.ErrNoRows) {
			t.Fatal("owned reverse absence not established before restore", e)
		}
		if _, e := peer.ExecContext(ctx, `INSERT INTO external_identities_by_user (user_bucket,user_id,provider,subject,created_at) VALUES ($1,$2,$3,$4,$5)`, userBucket, "native-maintenance-owner", domain.IdentityProviderTelegram, "native-tg-subject", removedCreated); e != nil {
			t.Fatal(e)
		}
		if e := peer.Commit(); e != nil {
			t.Fatal(e)
		}
	}
	request = maintenanceRequestTest("prepare")
	request.Kind = "identity"
	request.Selector = "native-maintenance-owner"
	if _, e := maintainIdentityV1(ctx, db, client.Table(), base, request); e != nil {
		t.Fatal("native post-conflict actual correspondence/restoration", kind, e)
	}
}

// The entire inventory MUST NOT touch shared test tables. The only namespace
// adapter lives here: SQL bindings and native Describe point at one unique
// directory; production retains fixed table names and its actual database.
// Keep the concrete optional timestamp type at the real database/sql binder
// boundary. A bare nil becomes YDB Void, not the intentionally NULL legacy
// delete_after Timestamp. This must not replace NULL with a non-NULL sentinel.
func maintenanceNativeSeedArgTest(column identityMaintenanceColumn, value driver.Value) any {
	if value == nil && column.kind == "Timestamp" {
		return nullableTime(nil)
	}
	if value != nil {
		switch column.kind {
		case "Uint32":
			return uint32(value.(int64))
		case "Uint64":
			return uint64(value.(int64))
		}
	}
	return value
}

func TestIdentityMaintenanceV1NativeSeedTypedNullTimestamp(t *testing.T) {
	got := maintenanceNativeSeedArgTest(identityMaintenanceColumn{kind: "Timestamp"}, nil)
	timestamp, typed := got.(*time.Time)
	if !typed || timestamp != nil {
		t.Fatalf("native NULL timestamp argument = %T(%v), want typed nil *time.Time", got, got)
	}
	at := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		kind  string
		input driver.Value
		want  any
	}{
		{kind: "Timestamp", input: at, want: at},
		{kind: "Uint32", input: int64(7), want: uint32(7)},
		{kind: "Uint64", input: int64(9), want: uint64(9)},
		{kind: "Utf8", input: "original", want: "original"},
	} {
		if got := maintenanceNativeSeedArgTest(identityMaintenanceColumn{kind: test.kind}, test.input); !reflect.DeepEqual(got, test.want) {
			t.Errorf("native %s argument = %T(%v), want %T(%v)", test.kind, got, got, test.want, test.want)
		}
	}
}

func TestWebIdentityV2YDBMaintenanceIsolatedConversionAndRetirement(t *testing.T) {
	if os.Getenv("SESSIONLESS_WEB_IDENTITY_V2_YDB_GATE") != "1" {
		t.Skip("explicit disposable CI identity gate not requested")
	}
	u, e := url.Parse(os.Getenv("YDB_CONNECTION_STRING"))
	if e != nil || os.Getenv("GITHUB_ACTIONS") != "true" || os.Getenv("SESSIONLESS_WEB_IDENTITY_V2_YDB_DISPOSABLE") != "1" || (u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost" && u.Hostname() != "::1") || u.Query().Get("go_query_mode") != "data" || u.Query().Has("go_fake_tx") {
		t.Fatal("requires disposable CI real-data loopback database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	client, e := ydbclient.Open(ctx, u.String())
	if e != nil {
		t.Fatal(e)
	}
	base := path.Join(client.DatabasePath(), "identity-maintenance-"+rand.Text())
	ownedDirectory := false
	var created []string
	// Ownership armed BEFORE the first directory/table mutation. Only confirmed
	// newly created exact paths may be cleaned; never drop a shared table.
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 20*time.Second)
		defer stop()
		for i := len(created) - 1; i >= 0; i-- {
			statement := "DROP TABLE IF EXISTS `" + path.Join(base, created[i]) + "`"
			ddl, e := ydbclient.OpenScripting(cleanup, u.String())
			if e != nil {
				t.Error(e)
				continue
			}
			_, e = ddl.DB.ExecContext(cleanup, statement)
			_ = ddl.Close(cleanup)
			if e != nil {
				t.Error(e)
			}
		}
		if ownedDirectory {
			if e := client.Scheme().RemoveDirectory(cleanup, base); e != nil {
				t.Error(e)
			}
		}
		if e := client.Close(cleanup); e != nil {
			t.Error(e)
		}
	})
	if _, e := client.Scheme().DescribePath(ctx, base); e == nil || (!ydb.IsOperationErrorSchemeError(e) && !ydb.IsOperationErrorNotFoundError(e)) {
		t.Fatal("unique namespace absence not established", e)
	}
	if e := client.Scheme().MakeDirectory(ctx, base); e != nil {
		t.Fatal(e)
	}
	ownedDirectory = true
	ddl, e := ydbclient.OpenScripting(ctx, u.String())
	if e != nil {
		t.Fatal(e)
	}
	defer ddl.Close(ctx)
	for _, filename := range []string{"00049_create_external_identities.sql", "00050_create_external_identities_by_user.sql", "00053_create_oidc_login_challenges.sql", "00054_create_web_sessions.sql", "00130_add_identity_challenge_delete_after.sql", "00131_set_identity_challenge_delete_after_ttl.sql"} {
		raw, e := fs.ReadFile(ydbmigrations.Files, filename)
		if e != nil {
			t.Fatal(e)
		}
		up, _, _ := strings.Cut(string(raw), "-- +goose Down")
		var lines []string
		for _, line := range strings.Split(up, "\n") {
			if !strings.HasPrefix(strings.TrimSpace(line), "--") {
				lines = append(lines, line)
			}
		}
		statement := strings.Join(lines, "\n")
		for _, table := range identityMaintenanceTables() {
			statement = strings.ReplaceAll(statement, "`"+table.name+"`", "`"+path.Join(base, table.name)+"`")
		}
		statement = strings.Replace(statement, "CREATE TABLE IF NOT EXISTS", "CREATE TABLE", 1)
		if strings.HasPrefix(filename, "000") {
			for _, table := range identityMaintenanceTables() {
				if strings.Contains(string(raw), "`"+table.name+"`") {
					created = append(created, table.name)
				}
			}
		}
		if _, e := ddl.DB.ExecContext(ctx, statement); e != nil {
			t.Fatal("owned migration source DDL; exact attempted child already armed for cleanup", e)
		}
	}
	// Existing migrator/Goose YDB metadata schema; no new migration/product table.
	for name, columns := range map[string]string{"schema_migration_checksums": "version Uint64,filename Utf8,sha256 Utf8,state Utf8,recorded_at Timestamp,applied_at Timestamp,PRIMARY KEY(version)", "sessionless_goose_versions": "version_id Uint64,is_applied Bool,tstamp Timestamp,PRIMARY KEY(version_id)"} {
		created = append(created, name)
		if _, e := ddl.DB.ExecContext(ctx, "CREATE TABLE `"+path.Join(base, name)+"` ("+columns+")"); e != nil {
			t.Fatal("owned metadata CREATE; attempted child cleanup armed", e)
		}
	}
	// Explicit SDK table-prefix binding is test-only; no production DSN/namespace
	// option or serving installer is added by this fixture.
	native, e := ydb.Open(ctx, u.String())
	if e != nil {
		t.Fatal(e)
	}
	connector, e := ydb.Connector(native, ydb.WithQueryService(false), ydb.WithAutoDeclare(), ydb.WithNumericArgs(), ydb.WithTablePathPrefix(base))
	if e != nil {
		_ = native.Close(ctx)
		t.Fatal(e)
	}
	db := sql.OpenDB(connector)
	control := &maintenanceNativeControl{}
	wrapped := sql.OpenDB(&maintenanceNativeConnector{actual: connector, control: control})
	t.Cleanup(func() {
		// Both pools use this driver: pool connections must join before its
		// Close. The earlier namespace-client cleanup runs after this callback.
		if e := wrapped.Close(); e != nil {
			t.Error(e)
		}
		if e := db.Close(); e != nil {
			t.Error(e)
		}
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if e := native.Close(cleanup); e != nil {
			t.Error(e)
		}
	})
	var at time.Time
	if e := db.QueryRowContext(ctx, `SELECT CurrentUtcTimestamp()`).Scan(&at); e != nil {
		t.Fatal(e)
	}
	setup := &maintenanceSQLTest{at: at.UTC(), data: map[string][][]driver.Value{}}
	maintenanceLegacyIdentityTest(setup, domain.IdentityProviderYandex, "native-subject", "native-maintenance-owner")
	maintenanceLegacyIdentityTest(setup, domain.IdentityProviderTelegram, "native-tg-subject", "native-maintenance-owner")
	challenge := maintenanceLegacyChallengeTest(setup)
	cookie := maintenanceLegacyCookieTest(setup)
	var nativeChallenge domain.OIDCLoginChallenge
	if e := json.Unmarshal([]byte(setup.data["oidc_login_challenges"][0][9].(string)), &nativeChallenge); e != nil {
		t.Fatal(e)
	}
	nativeChallenge.ExpiresAt = at.Add(6 * time.Minute)
	challengeRaw, _ := json.Marshal(nativeChallenge)
	setup.data["oidc_login_challenges"][0][7] = nativeChallenge.ExpiresAt
	setup.data["oidc_login_challenges"][0][9] = string(challengeRaw)
	tx, e := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if e != nil {
		t.Fatal(e)
	}
	defer tx.Rollback()
	for _, table := range identityMaintenanceTables() {
		for _, row := range setup.data[table.name] {
			names := make([]string, len(table.cols))
			holders := make([]string, len(table.cols))
			args := make([]any, len(row))
			for i, col := range table.cols {
				names[i] = col.name
				holders[i] = "$" + strconv.Itoa(i+1)
				if col.name == "record" {
					holders[i] = "CAST(" + holders[i] + " AS JsonDocument)"
				}
				args[i] = maintenanceNativeSeedArgTest(col, row[i])
			}
			if _, e := tx.ExecContext(ctx, "INSERT INTO "+table.name+" ("+strings.Join(names, ",")+") VALUES ("+strings.Join(holders, ",")+")", args...); e != nil {
				t.Fatal(e)
			}
		}
	}
	for version, name := range map[uint64]string{130: "00130_add_identity_challenge_delete_after.sql", 131: "00131_set_identity_challenge_delete_after_ttl.sql"} {
		raw, _ := fs.ReadFile(ydbmigrations.Files, name)
		sum := sha256.Sum256(raw)
		hash := hex.EncodeToString(sum[:])
		if _, e := tx.ExecContext(ctx, `INSERT INTO schema_migration_checksums (version,filename,sha256,state,recorded_at,applied_at) VALUES ($1,$2,$3,$4,$5,$6)`, version, name, hash, "applied", at, at); e != nil {
			t.Fatal(e)
		}
		if _, e := tx.ExecContext(ctx, `INSERT INTO sessionless_goose_versions (version_id,is_applied,tstamp) VALUES ($1,$2,$3)`, version, true, at); e != nil {
			t.Fatal(e)
		}
	}
	if e := tx.Commit(); e != nil {
		t.Fatal(e)
	}
	var originalRetention sql.NullTime
	if e := db.QueryRowContext(ctx,
		`SELECT delete_after FROM oidc_login_challenges WHERE shard_bucket = $1 AND state_digest = $2`,
		uint32(setup.data["oidc_login_challenges"][0][0].(int64)), challenge,
	).Scan(&originalRetention); e != nil || originalRetention.Valid {
		t.Fatalf("native legacy challenge must retain actual SQL NULL delete_after: valid=%t err=%v", originalRetention.Valid, e)
	}
	maintenanceNativeConflict(t, ctx, db, wrapped, client, base, control, "point")
	maintenanceNativeConflict(t, ctx, db, wrapped, client, base, control, "range")
	for _, item := range []struct{ kind, key string }{{"identity", "native-maintenance-owner"}, {"cookie", cookie}, {"challenge", challenge}} {
		r := maintenanceRequestTest("prepare")
		r.Kind = item.kind
		r.Selector = item.key
		prepared, e := maintainIdentityV1(ctx, db, client.Table(), base, r)
		if e != nil {
			t.Fatal("native coherent complete inventory/prepare", item.kind, e)
		}
		r = maintenanceRequestTest("apply")
		r.Candidate = prepared.Candidate
		var got IdentityMaintenanceResultV1
		if item.kind == "identity" {
			got, e = maintainIdentityV1(ctx, db, client.Table(), base, r)
			if e != nil || maintenanceReportTest(t, got).Outcome != "applied" {
				t.Fatal("native exact apply", item.kind, e)
			}
		} else {
			control.mu.Lock()
			lostBefore, writesBefore := control.lostReplies, control.writeAttempts
			control.loseReply = true
			control.mu.Unlock()
			got, e = maintainIdentityV1(ctx, wrapped, client.Table(), base, r)
			control.mu.Lock()
			lostAfter, writesAfter, realCommits := control.lostReplies, control.writeAttempts, control.realWriteCommits
			control.mu.Unlock()
			if !errors.Is(e, ErrIdentityMaintenancePossiblyCommittedV1) || maintenanceReportTest(t, got).Outcome != "possiblyCommitted" || lostAfter != lostBefore+1 || writesAfter != writesBefore+1 || realCommits != lostAfter {
				t.Fatalf("real native commit/lost reply kind=%s err=%v lost=%d->%d writes=%d->%d realCommits=%d", item.kind, e, lostBefore, lostAfter, writesBefore, writesAfter, realCommits)
			}
			r.Mode = "reconcile"
			got, e = maintainIdentityV1(ctx, wrapped, client.Table(), base, r)
			want := "exactPostimageObserved"
			if item.kind == "challenge" {
				want = "desiredAbsentObservedActorUnknown"
			}
			if e != nil || maintenanceReportTest(t, got).Outcome != want {
				t.Fatal("native fresh reconcile after actual committed mutation", item.kind, e)
			}
			r.Mode = "apply"
			got, e = maintainIdentityV1(ctx, wrapped, client.Table(), base, r)
			want = "alreadyDesiredObserved"
			if item.kind == "challenge" {
				want = "desiredAbsentObservedActorUnknown"
			}
			control.mu.Lock()
			noReplay := control.writeAttempts == writesAfter
			control.mu.Unlock()
			if e != nil || maintenanceReportTest(t, got).Outcome != want || !noReplay {
				t.Fatal("native retained/missing result allowed replay or actor fiction", item.kind, e)
			}
		}
		if item.kind == "identity" {
			read, e := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
			if e != nil {
				t.Fatal(e)
			}
			active, e := readActiveExternalIdentitiesV2(ctx, read, "native-maintenance-owner")
			_ = read.Rollback()
			if e != nil || len(active) != 2 {
				t.Fatal("native existing exact V2 active-set join", e)
			}
		}
	}
	got, e := maintainIdentityV1(ctx, db, client.Table(), base, maintenanceRequestTest("inventory"))
	if e != nil || !maintenanceReportTest(t, got).Ready {
		t.Fatal("native complete zero-V1 readiness", e)
	}
}
