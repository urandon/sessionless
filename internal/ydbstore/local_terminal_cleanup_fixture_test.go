package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// Only this new historical read is added to the scripted storage model. All
// existing authority, retirement and snapshot SQL still executes its original
// fixture. Install before the real NewLocalV1 constructor, never on its Store.
type LocalTerminalCleanupSQLFixture struct {
	seed *LocalTerminalPublicationSessionConnectedSeed
	mode string
}

// SetFault mutates scripted durable DATA before synchronous cleanup, not any
// production private owner, permission or receipt.
func (f *LocalTerminalCleanupSQLFixture) SetFault(mode string) {
	f.mode = mode
	if mode == "retirement_lost_ack" {
		f.seed.parent.graph.commitModes = []string{"ambiguous"}
	}
}

type terminalCleanupConnector struct {
	fixture *LocalTerminalCleanupSQLFixture
}
type terminalCleanupConn struct {
	*terminalPublicationConn
	fixture *LocalTerminalCleanupSQLFixture
}

func (c terminalCleanupConnector) Driver() driver.Driver {
	return terminalPublicationConnector{c.fixture.seed.publication}.Driver()
}
func (c terminalCleanupConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := (terminalPublicationConnector{c.fixture.seed.publication}).Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &terminalCleanupConn{terminalPublicationConn: conn.(*terminalPublicationConn), fixture: c.fixture}, nil
}
func (c *terminalCleanupConn) QueryContext(ctx context.Context, statement string, args []driver.NamedValue) (driver.Rows, error) {
	if statement == `SELECT state FROM session_deletions WHERE tenant_id = $1 AND session_id = $2` {
		var run domain.Run
		if err := json.Unmarshal([]byte(c.fixture.seed.parent.graph.rows["runs"][0].(string)), &run); err != nil ||
			len(args) != 2 || args[0].Value != string(run.TenantID) || args[1].Value != string(run.SessionID) || ctx.Err() != nil {
			return nil, errors.New("cleanup deletion read requires original tenant and Session")
		}
		switch c.fixture.mode {
		case "deletion_registered":
			return &compactionSourceRows{rows: [][]driver.Value{{"requested"}}}, nil
		case "deletion_read_failure":
			return nil, errors.New("owned deletion read failure")
		case "deletion_close_failure":
			return &compactionSourceRows{failClose: true}, nil
		case "changed_historical_run":
			run.CreatedAt = run.CreatedAt.Add(-time.Microsecond)
			if err := run.Validate(); err != nil {
				return nil, err
			}
			body, err := json.Marshal(run)
			if err != nil {
				return nil, err
			}
			c.fixture.seed.parent.graph.rows["runs"] = []driver.Value{string(body)}
		}
		return &compactionSourceRows{}, nil
	}
	return c.terminalPublicationConn.QueryContext(ctx, statement, args)
}
func (s *LocalTerminalPublicationSessionConnectedSeed) InstallTerminalCleanupSQL(t *testing.T) *LocalTerminalCleanupSQLFixture {
	t.Helper()
	f := &LocalTerminalCleanupSQLFixture{seed: s}
	db := sql.OpenDB(terminalCleanupConnector{fixture: f})
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	s.DB = db
	return f
}

// TEST ONLY raw scripted SQL DATA, read synchronously after the real retirement
// transaction and before physical Delete. This grants no allocation authority.
func (s *LocalTerminalPublicationSessionConnectedSeed) TerminalCleanupAllocation(t *testing.T) (domain.RegisteredAllocationV2, domain.RegisteredAllocationV2) {
	t.Helper()
	row := s.sql.f.row
	if len(row) != 12 {
		t.Fatal("missing scripted original allocation row")
	}
	original, e := domain.DecodeRegisteredAllocationV2([]byte(row[6].(string)))
	if e != nil {
		t.Fatal(e)
	}
	current, e := domain.DecodeRegisteredAllocationV2([]byte(row[8].(string)))
	if e != nil {
		t.Fatal(e)
	}
	return original, current
}
