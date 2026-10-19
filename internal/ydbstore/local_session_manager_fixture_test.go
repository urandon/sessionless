package ydbstore

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// TEST ONLY: declare an already admitted/running canonical invocation whose
// original worker lease is replayed by Manager. This is not queued admission,
// Web ingress, native YDB, source approval or a fabricated private local owner.
// Every effect/entry/send/cleanup/completion still executes the original Store.
func (s *LocalTerminalPublicationSessionConnectedSeed) InstallManagerReadSQL(t *testing.T, cleanupReady ...<-chan struct{}) *LocalTerminalCompletionSQLFixture {
	t.Helper()
	if len(cleanupReady) > 1 {
		t.Fatal("one original cleanup boundary only")
	}
	var ready <-chan struct{}
	if len(cleanupReady) == 1 {
		ready = cleanupReady[0]
	}
	s.DeclareWorkerPreflightWebOrigin(t)
	state := s.WorkerPreflightState(t)
	var lease domain.Lease
	if json.Unmarshal([]byte(s.parent.sql.leaseRow[16].(string)), &lease) != nil {
		t.Fatal("original manager lease DATA")
	}
	h := sha256.New()
	for _, value := range []string{string(state.Run.ID), string(state.Attempt.ID)} {
		_, _ = h.Write([]byte(value))
		_, _ = h.Write([]byte{0})
	}
	lease.ID = domain.LeaseID("lea_" + hex.EncodeToString(h.Sum(nil)[:16]))
	body, err := json.Marshal(lease)
	if err != nil || lease.ValidateForAttempt(state.Run, state.Attempt) != nil {
		t.Fatal("valid existing manager lease DATA", err)
	}
	s.Request.LeaseID, s.parent.request.LeaseID = lease.ID, lease.ID
	s.parent.sql.leaseRow[2], s.parent.sql.leaseRow[9], s.parent.sql.leaseRow[16] = string(lease.ID), string(lease.ID), string(body)
	f := s.InstallTerminalCompletionSQL(t)
	keys := map[string]string{
		"SELECT payload FROM worker_jobs WHERE tenant_id = $1 AND run_id = $2":                      string(state.Run.ID),
		"SELECT payload FROM attempts WHERE tenant_id = $1 AND attempt_id = $2":                     string(state.Attempt.ID),
		"SELECT payload FROM quota_reservations WHERE tenant_id = $1 AND quota_reservation_id = $2": string(state.Reservation.ID),
		"SELECT payload FROM artifact_manifests WHERE tenant_id = $1 AND artifact_manifest_id = $2": string(state.InputManifest.ID),
	}
	db := sql.OpenDB(localManagerReadConnector{f: f, keys: keys, cleanupReady: ready})
	db.SetMaxOpenConns(1)
	if ready != nil {
		// One watchdog connection waits BEFORE entering the scripted SQL
		// transaction; ordinary bounded work uses the other connection. The
		// cleanup owner is outside SQL when releasing this gate. Therefore no
		// concurrent scripted transaction or fake permission is introduced.
		db.SetMaxOpenConns(2)
	}
	t.Cleanup(func() { _ = db.Close() })
	s.DB = db
	return f
}

type localManagerReadConnector struct {
	f            *LocalTerminalCompletionSQLFixture
	keys         map[string]string
	cleanupReady <-chan struct{}
}
type localManagerReadConn struct {
	*terminalCompletionConn
	keys         map[string]string
	cleanupReady <-chan struct{}
}

func (c localManagerReadConnector) Driver() driver.Driver {
	return (terminalCompletionConnector{f: c.f}).Driver()
}
func (c localManagerReadConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := (terminalCompletionConnector{f: c.f}).Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &localManagerReadConn{terminalCompletionConn: conn.(*terminalCompletionConn), keys: c.keys, cleanupReady: c.cleanupReady}, nil
}
func (c *localManagerReadConn) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	if _, bounded := ctx.Deadline(); c.cleanupReady != nil && !bounded {
		// Only the original watchdog uses WithoutCancel and has no deadline.
		// The test's actual RunOnce caller and every ordinary SQL call have a
		// deadline. Do not begin/hold a fake transaction while cleanup advances.
		select {
		case <-c.cleanupReady:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return c.terminalCompletionConn.BeginTx(ctx, options)
}
func (c *localManagerReadConn) QueryContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Rows, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	s := c.f.cleanup.seed
	state := s.parent.sql.leaseRow
	row := func(v ...driver.Value) (driver.Rows, error) { return &explanationFixtureRows{values: v}, nil }
	if expected, found := c.keys[completionSQL(statement)]; found {
		if len(args) != 2 || args[0].Value != state[0] || args[1].Value != expected {
			return nil, errors.New("manager payload read requires original exact tenant/key")
		}
		table := strings.Fields(completionSQL(statement))[3]
		return row(s.parent.graph.rows[table]...)
	}
	switch completionSQL(statement) {
	case "SELECT lease_id, attempt_id, worker_id, fence_token, expires_at FROM lease_heads WHERE tenant_id = $1 AND run_id = $2":
		if len(args) != 2 || args[0].Value != state[0] || args[1].Value != state[1] || !c.f.state.LeasePresent {
			return nil, errors.New("manager stable claim requires exact existing lease head")
		}
		return row(state[2], state[3], state[4], state[5], state[6])
	case "SELECT payload FROM leases WHERE tenant_id = $1 AND lease_id = $2":
		if len(args) != 2 || args[0].Value != state[0] || args[1].Value != state[2] {
			return nil, errors.New("manager stable claim requires original lease")
		}
		return row(state[16])
	case "SELECT payload FROM checkpoints WHERE tenant_id = $1 AND attempt_id = $2 ORDER BY sequence DESC LIMIT 1":
		if len(args) != 2 || args[0].Value != state[0] || args[1].Value != state[3] {
			return nil, errors.New("manager checkpoint requires original attempt")
		}
		return &compactionSourceRows{}, nil
	}
	return c.terminalCompletionConn.QueryContext(ctx, statement, args)
}
