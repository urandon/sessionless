package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"io/fs"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	ydbmigrations "gitcode.com/urandon/sessionless/migrations/ydb"
)

// Source/controlled-driver checks only. These tests neither apply DDL nor
// establish native YDB ALTER/replay, TTL timing or retained-data cutover proof.
func identityTTLSourceUp(t *testing.T, name string) string {
	t.Helper()
	raw, err := fs.ReadFile(ydbmigrations.Files, name)
	if err != nil {
		t.Fatal(err)
	}
	up, down, found := strings.Cut(string(raw), "-- +goose Down")
	if !found || !strings.Contains(up, "-- +goose Up") {
		t.Fatalf("%s lacks explicit Goose Up/Down boundaries", name)
	}
	var lines []string
	for _, line := range strings.Split(up, "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "--") {
			lines = append(lines, line)
		}
	}
	for _, line := range strings.Split(down, "\n") {
		if value := strings.TrimSpace(line); value != "" && !strings.HasPrefix(value, "--") {
			t.Fatalf("%s has executable Down data %q", name, value)
		}
	}
	statement := strings.Join(strings.Fields(strings.Join(lines, "\n")), " ")
	if strings.Count(statement, ";") != 1 {
		t.Fatalf("%s must own exactly one executable statement: %s", name, statement)
	}
	return statement
}

func TestWebIdentityTTLForwardSourcePreservesCreateAndTwoSteps(t *testing.T) {
	add := identityTTLSourceUp(t, "00130_add_identity_challenge_delete_after.sql")
	if want := "ALTER TABLE `oidc_login_challenges` ADD COLUMN `delete_after` Timestamp;"; add != want {
		t.Errorf("add-column source=%q want=%q", add, want)
	}
	set := identityTTLSourceUp(t, "00131_set_identity_challenge_delete_after_ttl.sql")
	if want := `ALTER TABLE ` + "`oidc_login_challenges`" + ` SET (TTL = Interval("PT0S") ON delete_after);`; set != want {
		t.Errorf("TTL source=%q want=%q", set, want)
	}
	create := identityTTLSourceUp(t, "00053_create_oidc_login_challenges.sql")
	if !strings.Contains(create, `TTL = Interval("PT0S") ON expires_at`) || strings.Contains(create, "delete_after") || !strings.Contains(create, "PRIMARY KEY (shard_bucket, state_digest)") {
		t.Errorf("historical CREATE lost its original retention/primary-key lineage: %s", create)
	}
	if strings.Contains(add, "IF NOT EXISTS") || strings.Contains(add, "UPDATE") || strings.Contains(set, "UPDATE") || strings.Contains(set, "ADD COLUMN") {
		t.Error("source guessed ADD replay support or combined/backfilled independent schema actions")
	}
}

type identityTTLConnector struct {
	statement string
	args      []driver.NamedValue
	begins    int
	rollbacks int
	closed    bool
}
type identityTTLConn struct{ fixture *identityTTLConnector }
type identityTTLTx struct{ fixture *identityTTLConnector }
type identityTTLDriver struct{}

func (*identityTTLDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("isolated TTL fixture requires its Connector")
}
func (fixture *identityTTLConnector) Driver() driver.Driver { return &identityTTLDriver{} }
func (fixture *identityTTLConnector) Connect(context.Context) (driver.Conn, error) {
	return &identityTTLConn{fixture: fixture}, nil
}
func (*identityTTLConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("TTL fixture requires explicit ExecContext")
}
func (conn *identityTTLConn) Close() error { conn.fixture.closed = true; return nil }
func (*identityTTLConn) Begin() (driver.Tx, error) {
	return nil, errors.New("TTL fixture requires explicit serializable BeginTx")
}
func (conn *identityTTLConn) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	if ctx.Err() != nil || options.Isolation != driver.IsolationLevel(sql.LevelSerializable) || conn.fixture.begins != 0 {
		return nil, errors.New("TTL fixture rejects unexpected transaction boundary")
	}
	conn.fixture.begins++
	return &identityTTLTx{fixture: conn.fixture}, nil
}
func (*identityTTLTx) Commit() error      { return errors.New("source-only TTL fixture never commits data") }
func (tx *identityTTLTx) Rollback() error { tx.fixture.rollbacks++; return nil }
func (conn *identityTTLConn) ExecContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Result, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if _, bounded := ctx.Deadline(); !bounded || conn.fixture.statement != "" || conn.fixture.begins != 1 {
		return nil, errors.New("TTL fixture rejects unbounded, extra or out-of-transaction write")
	}
	conn.fixture.statement = strings.Join(strings.Fields(statement), " ")
	conn.fixture.args = append([]driver.NamedValue(nil), args...)
	return driver.RowsAffected(1), nil
}

func TestWebIdentityTTLV1WriterUsesOriginalPhysicalExpiryOnly(t *testing.T) {
	for _, consumed := range []bool{false, true} {
		name := "pending"
		if consumed {
			name = "consumed"
		}
		t.Run(name, func(t *testing.T) {
			fixture := &identityTTLConnector{}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			t.Cleanup(cancel)
			db := sql.OpenDB(fixture)
			db.SetMaxOpenConns(1)
			t.Cleanup(func() {
				if err := db.Close(); err != nil {
					t.Errorf("close source-only TTL fixture: %v", err)
				}
			})
			tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := tx.Rollback(); err != nil && !errors.Is(err, sql.ErrTxDone) {
					t.Errorf("rollback TTL fixture: %v", err)
				}
			})
			created := time.Date(2026, 10, 26, 16, 0, 0, 0, time.UTC)
			challenge := domain.OIDCLoginChallenge{Provider: domain.IdentityProviderTelegram, Issuer: "https://oauth.telegram.org", Audience: "123456", StateDigest: domain.DigestSecret("v1-state"), BrowserBindingDigest: domain.DigestSecret("v1-browser"), PKCEVerifier: strings.Repeat("p", 64), Nonce: "synthetic-v1-nonce", RedirectPath: "/", CreatedAt: created, ExpiresAt: created.Add(10 * time.Minute)}
			if consumed {
				at := created.Add(time.Minute)
				challenge.ConsumedAt = &at
			}
			if err := challenge.Validate(); err != nil {
				t.Fatal(err)
			}
			const bucket = uint32(5)
			if err := writeChallenge(ctx, tx, bucket, challenge); err != nil {
				t.Fatalf("actual V1 writeChallenge: %v", err)
			}
			wantSQL := `UPSERT INTO oidc_login_challenges (shard_bucket, state_digest, browser_binding_digest, pkce_verifier, nonce, redirect_path, created_at, expires_at, consumed_at, record, delete_after) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, CAST($10 AS JsonDocument), $11)`
			if fixture.statement != wantSQL || len(fixture.args) != 11 {
				t.Fatalf("physical TTL write shape=%q args=%d want=%q/11", fixture.statement, len(fixture.args), wantSQL)
			}
			for index, arg := range fixture.args {
				if arg.Ordinal != index+1 || arg.Name != "" {
					t.Errorf("argument %d is not exact positional binding: %+v", index, arg)
				}
			}
			if fixture.args[0].Value != int64(bucket) || fixture.args[1].Value != string(challenge.StateDigest) || fixture.args[7].Value != challenge.ExpiresAt || fixture.args[10].Value != challenge.ExpiresAt {
				t.Errorf("physical key/expiry changed: bucket=%v state=%v expires=%v delete_after=%v", fixture.args[0].Value, fixture.args[1].Value, fixture.args[7].Value, fixture.args[10].Value)
			}
			raw, ok := fixture.args[9].Value.(string)
			if !ok {
				t.Fatalf("V1 record argument type=%T want string", fixture.args[9].Value)
			}
			var decoded domain.OIDCLoginChallenge
			if err := json.Unmarshal([]byte(raw), &decoded); err != nil || !reflect.DeepEqual(decoded, challenge) {
				t.Errorf("V1 JSON contract changed: decoded=%+v err=%v", decoded, err)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal([]byte(raw), &fields); err != nil {
				t.Fatal(err)
			}
			for _, forbidden := range []string{"version", "purpose", "delete_after"} {
				if _, present := fields[forbidden]; present {
					t.Errorf("physical retention field silently reinterpreted V1 JSON as %s", forbidden)
				}
			}
			if err := tx.Rollback(); err != nil || fixture.rollbacks != 1 {
				t.Errorf("original source-only transaction cleanup err=%v rollbacks=%d", err, fixture.rollbacks)
			}
		})
	}
}
