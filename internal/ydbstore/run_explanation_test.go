package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/runexplanation"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

// Test-owned connector exercises the actual resource SQL path without a live
// service. Transaction inputs form one immutable snapshot; every query and
// mutation is counted, and range/probe/secret reads are rejected.
type explanationSQLFixture struct {
	rows           map[string][]driver.Value
	queries        []string
	isolation      driver.IsolationLevel
	deadline       bool
	commitFailures int
	commits        int
	begins         int
}

func (f *explanationSQLFixture) Connect(context.Context) (driver.Conn, error) { return f, nil }
func (f *explanationSQLFixture) Driver() driver.Driver                        { return explanationFixtureDriver{} }

type explanationFixtureDriver struct{}

func (explanationFixtureDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("connector required")
}
func (f *explanationSQLFixture) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepare forbidden")
}
func (f *explanationSQLFixture) Close() error { return nil }
func (f *explanationSQLFixture) Begin() (driver.Tx, error) {
	return nil, errors.New("BeginTx required")
}
func (f *explanationSQLFixture) BeginTx(_ context.Context, opts driver.TxOptions) (driver.Tx, error) {
	f.isolation = opts.Isolation
	f.begins++
	return explanationFixtureTx{fixture: f}, nil
}

type explanationFixtureTx struct{ fixture *explanationSQLFixture }

func (tx explanationFixtureTx) Commit() error {
	tx.fixture.commits++
	if tx.fixture.commitFailures > 0 {
		tx.fixture.commitFailures--
		return retry.RetryableError(errors.New("test-owned serializable commit conflict"), retry.WithBackoff(retry.TypeNoBackoff))
	}
	return nil
}
func (explanationFixtureTx) Rollback() error { return nil }
func (f *explanationSQLFixture) QueryContext(ctx context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	f.queries = append(f.queries, query)
	_, f.deadline = ctx.Deadline()
	if strings.Contains(query, "CurrentUtcTimestamp") {
		return &explanationFixtureRows{values: f.rows["clock"]}, nil
	}
	for table, value := range f.rows {
		if strings.Contains(query, "FROM "+table+" ") {
			return &explanationFixtureRows{values: value}, nil
		}
	}
	return &explanationFixtureRows{}, nil
}
func (f *explanationSQLFixture) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	return nil, errors.New("read resource attempted mutation")
}

type explanationFixtureRows struct {
	values []driver.Value
	done   bool
}

func (r *explanationFixtureRows) Columns() []string {
	if len(r.values) == 0 {
		return []string{"absent"}
	}
	return make([]string, len(r.values))
}
func (r *explanationFixtureRows) Close() error { return nil }
func (r *explanationFixtureRows) Next(out []driver.Value) error {
	if r.done || len(r.values) == 0 {
		return io.EOF
	}
	copy(out, r.values)
	r.done = true
	return nil
}

func explanationReadFixture(t *testing.T) (*Store, *explanationSQLFixture, ports.RunExplanationAuthorizationV1, domain.Run) {
	t.Helper()
	at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	auth := ports.RunExplanationAuthorizationV1{TenantID: "tenant-test", UserID: "user-test", MembershipSecurityVersion: 1, WebSessionDigest: domain.DigestSecret("session-test")}
	session := domain.WebSession{SessionDigest: auth.WebSessionDigest, CSRFTokenDigest: domain.DigestSecret("csrf"), UserID: auth.UserID, ActiveTenantID: auth.TenantID, AuthenticatedSubject: domain.ExternalSubject{Provider: domain.IdentityProviderTelegram, Subject: "123"}, MembershipSecurityVersion: 1, IssuedAt: at.Add(-time.Hour), LastSeenAt: at.Add(-time.Hour), IdleExpiresAt: at.Add(time.Hour), AbsoluteExpiresAt: at.Add(24 * time.Hour)}
	membership := domain.TenantMembership{TenantID: auth.TenantID, UserID: auth.UserID, Role: domain.TenantMembershipViewer, Status: domain.TenantMembershipActive, SecurityVersion: 1, CreatedAt: at.Add(-time.Hour), UpdatedAt: at.Add(-time.Hour)}
	run := domain.Run{ID: "run-test", TenantID: auth.TenantID, SessionID: "session-test", TriggerEventID: "event-test", SubscriptionConnectionID: "subscription-test", IdempotencyKey: "key-test", Status: domain.RunCreated, CreatedAt: at.Add(-time.Minute), UpdatedAt: at.Add(-time.Minute)}
	canonical := domain.Session{ID: run.SessionID, TenantID: auth.TenantID, CreatedBy: "other-owner", Status: domain.SessionActive, CreatedAt: at.Add(-time.Hour), UpdatedAt: at.Add(-time.Minute)}
	participant := domain.SessionParticipant{TenantID: auth.TenantID, SessionID: run.SessionID, UserID: auth.UserID, Role: domain.SessionParticipantViewer, Status: domain.SessionParticipantActive, CreatedAt: at.Add(-time.Hour), UpdatedAt: at.Add(-time.Hour)}
	f := &explanationSQLFixture{rows: map[string][]driver.Value{"clock": {at}}}
	for table, value := range map[string]any{"web_sessions": session, "tenant_memberships": membership, "runs": run, "sessions": canonical, "session_participants": participant} {
		encoded, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		f.rows[table] = []driver.Value{string(encoded)}
	}
	db := sql.OpenDB(f)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	store, err := New(db, Options{})
	if err != nil {
		t.Fatal(err)
	}
	return store, f, auth, run
}

func TestRunExplanationResourceHistoricalReadIsAuthorizedBoundedAndReadOnly(t *testing.T) {
	store, f, auth, run := explanationReadFixture(t)
	value, err := store.ReadRunExplanationV1(context.Background(), auth, run.ID)
	if err != nil {
		t.Fatalf("read historical authorized Run: %v", err)
	}
	if value.Attempt.Availability != runexplanation.Unknown || value.Admission.Availability != runexplanation.Unknown || value.RunID != run.ID {
		t.Fatalf("historical projection=%+v", value)
	}
	if f.isolation != driver.IsolationLevel(sql.LevelSerializable) || !f.deadline || len(f.queries) != 7 {
		t.Fatalf("resource isolation=%v deadline=%t actual statements=%d; want serializable, deadline,7", f.isolation, f.deadline, len(f.queries))
	}
	for _, query := range f.queries {
		if strings.Contains(query, "ORDER BY") || strings.Contains(query, "worker_jobs") || strings.Contains(query, "audit") || strings.Contains(query, "secret_digest") {
			t.Errorf("forbidden resource query: %s", query)
		}
	}
}

func TestRunExplanationResourceRechecksSecurityBeforeTarget(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*explanationSQLFixture, *ports.RunExplanationAuthorizationV1)
		want   error
	}{
		{name: "security version preserving READ", change: func(_ *explanationSQLFixture, a *ports.RunExplanationAuthorizationV1) { a.MembershipSecurityVersion++ }, want: domain.ErrMembershipVersionChanged},
		{name: "revoked session", change: func(f *explanationSQLFixture, _ *ports.RunExplanationAuthorizationV1) { delete(f.rows, "web_sessions") }, want: domain.ErrWebSessionRevoked},
		{name: "membership removed", change: func(f *explanationSQLFixture, _ *ports.RunExplanationAuthorizationV1) {
			delete(f.rows, "tenant_memberships")
		}, want: domain.ErrMembershipDenied},
		{name: "identity changed", change: func(_ *explanationSQLFixture, a *ports.RunExplanationAuthorizationV1) { a.UserID = "another-user" }, want: domain.ErrWebSessionRevoked},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, f, auth, run := explanationReadFixture(t)
			test.change(f, &auth)
			_, err := store.ReadRunExplanationV1(context.Background(), auth, run.ID)
			if !errors.Is(err, test.want) {
				t.Fatalf("error=%v want=%v", err, test.want)
			}
			for _, query := range f.queries {
				if strings.Contains(query, "FROM runs ") {
					t.Fatalf("target read before authorization: %s", query)
				}
			}
		})
	}
}

func TestRunExplanationResourceAbsentAndNonparticipantAreOpaque(t *testing.T) {
	for _, table := range []string{"runs", "sessions", "session_participants"} {
		t.Run(table, func(t *testing.T) {
			store, f, auth, run := explanationReadFixture(t)
			delete(f.rows, table)
			_, err := store.ReadRunExplanationV1(context.Background(), auth, run.ID)
			if !errors.Is(err, ports.ErrRunExplanationNotFound) {
				t.Fatalf("missing %s error=%v want opaque not_found", table, err)
			}
		})
	}
}

func TestRunExplanationResourceRejectsOversizeBeforeDecode(t *testing.T) {
	store, f, auth, run := explanationReadFixture(t)
	f.rows["run_explanation_heads_v1"] = []driver.Value{strings.Repeat("x", runexplanation.MaxProjectionBytes+1)}
	_, err := store.ReadRunExplanationV1(context.Background(), auth, run.ID)
	if !errors.Is(err, ports.ErrRunExplanationUnavailable) {
		t.Fatalf("oversize error=%v", err)
	}
}

func explanationAttachedReadFixture(t *testing.T) (*Store, *explanationSQLFixture, ports.RunExplanationAuthorizationV1, domain.Run) {
	t.Helper()
	store, f, auth, run := explanationReadFixture(t)
	run.Status = domain.RunQueued
	at := run.CreatedAt
	attempt := domain.Attempt{ID: "attempt-test", TenantID: auth.TenantID, RunID: run.ID, Number: 1, Status: domain.AttemptCreated, CreatedAt: at, UpdatedAt: at}
	head := runexplanation.HeadV1{Version: 1, TenantID: auth.TenantID, RunID: run.ID, SessionID: run.SessionID, SelectedAttemptID: attempt.ID, SelectedAt: at, Revision: 1, Placement: runexplanation.PlacementSelector{Kind: domain.ExecutionPlacementAttachedWorker, OwnerUserID: "private-owner", WorkerID: "private-worker"}}
	lease := domain.Lease{ID: "lease-test", TenantID: auth.TenantID, RunID: run.ID, AttemptID: attempt.ID, WorkerID: "private-worker", FenceToken: 7, AcquiredAt: at, ExpiresAt: at.Add(time.Hour)}
	fence, err := domain.NewAttachedWorkerFenceTokenV1(auth.TenantID, head.Placement.OwnerUserID, head.Placement.WorkerID, run.ID, attempt.ID, lease.ID, 7)
	if err != nil {
		t.Fatal(err)
	}
	receipt := domain.AttachedWorkerAttemptV1{Version: 1, TenantID: auth.TenantID, OwnerUserID: head.Placement.OwnerUserID, WorkerID: head.Placement.WorkerID, ConnectionID: "private-connection", RunID: run.ID, AttemptID: attempt.ID, ReservationID: "reservation-test", LeaseID: lease.ID, LeaseGeneration: 7, FenceToken: fence, EnrollmentGeneration: 2, ConnectionGeneration: 3, ContextDigest: domain.AttachedWorkerContextDigest(domain.DigestAttachedWorkerCapability([]byte("context"))), CapabilityDigest: domain.DigestAttachedWorkerCapability([]byte("capability")), PolicyDigest: domain.AttachedWorkerPolicyDigest(domain.DigestAttachedWorkerCapability([]byte("policy"))), State: domain.AttachedWorkerAttemptOffered, PlatformAttemptSequence: 1, LeaseExpiresAt: lease.ExpiresAt, CreatedAt: at, UpdatedAt: at, Revision: 1}
	if err := receipt.Validate(); err != nil {
		t.Fatal(err)
	}
	for table, value := range map[string]any{"runs": run, "attempts": attempt, "run_explanation_heads_v1": head, "leases": lease, "attached_worker_attempt_heads": receipt} {
		encoded, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		f.rows[table] = []driver.Value{string(encoded)}
	}
	f.rows["attached_workers"] = []driver.Value{int64(2), int64(3), string(domain.AttachedWorkerDesiredActive), string(domain.AttachedWorkerObservedOnline), int64(1), at, at}
	f.rows["attached_worker_connections"] = []driver.Value{"private-connection", int64(2), int64(3), int64(1), string(domain.AttachedWorkerConnectionOnline), at, at, at}
	return store, f, auth, run
}

func TestRunExplanationResourceActualAttachedStatementBudgetAndNoPrivateResponse(t *testing.T) {
	store, f, auth, run := explanationAttachedReadFixture(t)
	value, err := store.ReadRunExplanationV1(context.Background(), auth, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if value.Attached.Availability != runexplanation.Recorded || len(f.queries) != 12 || len(f.queries) > runexplanation.MaxPointStatements {
		t.Fatalf("attached=%+v actual statements=%d; want recorded,12<=20", value.Attached, len(f.queries))
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) > runexplanation.MaxResponseBytes || strings.Contains(string(encoded), "private-") {
		t.Fatalf("response bytes=%d private selector disclosure=%t", len(encoded), strings.Contains(string(encoded), "private-"))
	}
	for _, query := range f.queries {
		if strings.Contains(query, "attached_worker_connections") && (strings.Contains(query, "record") || strings.Contains(query, "secret")) {
			t.Fatalf("connection read exported internal record/secret: %s", query)
		}
	}
}

func TestRunExplanationResourceExactJoinMismatchIsUnknown(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*explanationSQLFixture)
	}{
		{name: "numeric generation", change: func(f *explanationSQLFixture) {
			var lease domain.Lease
			if err := json.Unmarshal([]byte(f.rows["leases"][0].(string)), &lease); err != nil {
				panic(err)
			}
			lease.FenceToken++
			encoded, _ := json.Marshal(lease)
			f.rows["leases"][0] = string(encoded)
		}},
		{name: "replacement attached target", change: func(f *explanationSQLFixture) {
			var receipt domain.AttachedWorkerAttemptV1
			if err := json.Unmarshal([]byte(f.rows["attached_worker_attempt_heads"][0].(string)), &receipt); err != nil {
				panic(err)
			}
			receipt.RunID = "replacement-run"
			encoded, _ := json.Marshal(receipt)
			f.rows["attached_worker_attempt_heads"][0] = string(encoded)
		}},
		{name: "selected Attempt mismatch", change: func(f *explanationSQLFixture) {
			var attempt domain.Attempt
			if err := json.Unmarshal([]byte(f.rows["attempts"][0].(string)), &attempt); err != nil {
				panic(err)
			}
			attempt.RunID = "replacement-run"
			encoded, _ := json.Marshal(attempt)
			f.rows["attempts"][0] = string(encoded)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, f, auth, run := explanationAttachedReadFixture(t)
			test.change(f)
			value, err := store.ReadRunExplanationV1(context.Background(), auth, run.ID)
			if err != nil {
				t.Fatal(err)
			}
			if value.Attached.Availability != runexplanation.Unknown {
				t.Fatalf("mismatched attached=%+v want unknown", value.Attached)
			}
			encoded, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(encoded), "replacement") {
				t.Fatalf("replacement target leaked: %s", encoded)
			}
		})
	}
}

func TestRunExplanationResourceAggregateBudgetIncludesSerializableRetries(t *testing.T) {
	for _, test := range []struct {
		name        string
		attached    bool
		failures    int
		wantQueries int
		wantError   bool
	}{
		{name: "short read retry succeeds", failures: 1, wantQueries: 14},
		{name: "short read repeated conflicts exhaust budget", failures: 3, wantQueries: 20, wantError: true},
		{name: "attached retry cannot spend another12", attached: true, failures: 1, wantQueries: 20, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			var store *Store
			var f *explanationSQLFixture
			var auth ports.RunExplanationAuthorizationV1
			var run domain.Run
			if test.attached {
				store, f, auth, run = explanationAttachedReadFixture(t)
			} else {
				store, f, auth, run = explanationReadFixture(t)
			}
			f.commitFailures = test.failures
			_, err := store.ReadRunExplanationV1(context.Background(), auth, run.ID)
			if test.wantError && !errors.Is(err, ports.ErrRunExplanationUnavailable) || !test.wantError && err != nil {
				t.Fatalf("retry error=%v want unavailable=%t", err, test.wantError)
			}
			if len(f.queries) != test.wantQueries || len(f.queries) > runexplanation.MaxPointStatements || f.begins < 2 {
				t.Fatalf("aggregate SQL=%d transaction attempts=%d commits=%d wantSQL=%d <=20 and retried transaction", len(f.queries), f.begins, f.commits, test.wantQueries)
			}
		})
	}
}
