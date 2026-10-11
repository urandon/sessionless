package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
)

// This connector drives the actual Store.Transact retry/commit boundary. It
// supplies one immutable existing reservation, not production YDB evidence.
type effectCommitFixture struct {
	record        attemptEffectRecordV1
	active        bool
	begins        int
	commits       int
	commitFailure error
	failRetryRead bool
	mintFailure   bool
	grantCalls    int
	premature     int
	issuer        *serverlessharness.CapabilityIssuer
}

func (f *effectCommitFixture) Connect(context.Context) (driver.Conn, error) { return f, nil }
func (f *effectCommitFixture) Driver() driver.Driver                        { return effectCommitDriver{} }

type effectCommitDriver struct{}

func (effectCommitDriver) Open(string) (driver.Conn, error) { return nil, errors.New("use connector") }
func (f *effectCommitFixture) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}
func (f *effectCommitFixture) Close() error { return nil }
func (f *effectCommitFixture) Begin() (driver.Tx, error) {
	return nil, errors.New("serializable BeginTx required")
}
func (f *effectCommitFixture) BeginTx(_ context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if opts.Isolation != driver.IsolationLevel(sql.LevelSerializable) || f.active {
		return nil, errors.New("invalid transaction isolation or overlapping transaction")
	}
	f.active = true
	f.begins++
	return effectCommitTx{f}, nil
}

type effectCommitTx struct{ f *effectCommitFixture }

func (tx effectCommitTx) Commit() error {
	tx.f.active = false
	tx.f.commits++
	if tx.f.commits == 1 {
		return tx.f.commitFailure
	}
	return nil
}
func (tx effectCommitTx) Rollback() error { tx.f.active = false; return nil }
func (f *effectCommitFixture) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if !f.active || !strings.Contains(query, "FROM attempt_effect_reservations") {
		return nil, errors.New("unexpected effect query outside reservation transaction")
	}
	if f.failRetryRead && f.begins > 1 {
		return nil, errors.New("test-owned retry read failure")
	}
	if len(args) != 3 || args[0].Value != string(f.record.Reservation.TenantID) || args[1].Value != string(f.record.Reservation.AttemptID) || args[2].Value != string(domain.ProviderEffectTurnV1) {
		return nil, errors.New("unexpected reservation scope")
	}
	payload, err := marshal(f.record)
	if err != nil {
		return nil, err
	}
	return &effectCommitRows{payload: payload}, nil
}

type effectCommitRows struct {
	payload string
	done    bool
}

func (*effectCommitRows) Columns() []string { return []string{"record"} }
func (*effectCommitRows) Close() error      { return nil }
func (r *effectCommitRows) Next(out []driver.Value) error {
	if r.done {
		return io.EOF
	}
	r.done = true
	out[0] = r.payload
	return nil
}
func (f *effectCommitFixture) MintAttemptEffectOwnershipGrant(a domain.ServerlessInvocationAuthorityV1, r domain.AttemptEffectReservationV1, expires time.Time) (ports.AttemptEffectOwnershipGrantV1, error) {
	f.grantCalls++
	if f.active || f.commits == 0 {
		f.premature++
	}
	if f.mintFailure {
		return ports.AttemptEffectOwnershipGrantV1{}, errors.New("test-owned post-commit mint failure")
	}
	return f.issuer.MintAttemptEffectOwnershipGrant(a, r, expires)
}
func (f *effectCommitFixture) MintAttemptEffectObservationGrant(a domain.ServerlessInvocationAuthorityV1, r domain.AttemptEffectReservationV1, at, expires time.Time) (ports.AttemptEffectObservationGrantV1, error) {
	f.grantCalls++
	if f.active || f.commits == 0 {
		f.premature++
	}
	if f.mintFailure {
		return ports.AttemptEffectObservationGrantV1{}, errors.New("test-owned post-commit mint failure")
	}
	return f.issuer.MintAttemptEffectObservationGrant(a, r, at, expires)
}

func TestAttemptEffectGrantsRequireConfirmedCommit(t *testing.T) {
	for _, name := range []string{"success", "commit_failure", "retry_success", "retry_read_failure", "mint_failure", "missing_issuer"} {
		t.Run(name, func(t *testing.T) {
			at := time.Unix(20, 0).UTC()
			managed, err := sessionlessharness.NewDeterministicFixtureManagedAuthorityV2("tenant-test", "user-test", "run-test", "attempt-test", "subscription-test", time.Unix(10, 0).UTC())
			if err != nil {
				t.Fatal(err)
			}
			a := domain.ServerlessInvocationAuthorityV1{
				Version:        domain.ServerlessInvocationAuthorityVersionV1,
				HarnessBinding: managed.HarnessBinding, ExecutionPlacementV2: managed.ExecutionPlacementV2,
				SubstrateBinding: managed.SubstrateBinding, AdmissionCostCeiling: managed.AdmissionCostCeiling,
				Lease:                 domain.Lease{ID: "lease-test", TenantID: "tenant-test", RunID: "run-test", AttemptID: "attempt-test", WorkerID: "worker-test", FenceToken: 1, AcquiredAt: at, ExpiresAt: at.Add(time.Hour)},
				ContextManifestDigest: strings.Repeat("b", 64), InputManifestDigest: strings.Repeat("c", 64), InvocationDeadline: at.Add(50 * time.Minute),
			}
			r, err := domain.BuildAttemptEffectReservationV1(a, "original-claim", nil, at)
			if err != nil {
				t.Fatal(err)
			}
			issuer, err := serverlessharness.NewCapabilityIssuer(func() time.Time { return at }, strings.NewReader(strings.Repeat("k", 32)))
			if err != nil {
				t.Fatal(err)
			}
			f := &effectCommitFixture{record: attemptEffectRecordV1{Authority: a, Reservation: r}, issuer: issuer}
			if name == "commit_failure" {
				f.commitFailure = errors.New("test-owned unconfirmed commit")
			}
			if strings.HasPrefix(name, "retry_") {
				f.commitFailure = retry.RetryableError(errors.New("test-owned serialization conflict"), retry.WithBackoff(retry.TypeNoBackoff))
			}
			f.failRetryRead = name == "retry_read_failure"
			f.mintFailure = name == "mint_failure"
			db := sql.OpenDB(f)
			db.SetMaxOpenConns(1)
			t.Cleanup(func() {
				if err := db.Close(); err != nil {
					t.Error(err)
				}
			})
			options := Options{AttemptEffectGrantIssuer: f}
			if name == "missing_issuer" {
				options.AttemptEffectGrantIssuer = nil
			}
			store, err := New(db, options)
			if err != nil {
				t.Fatal(err)
			}
			store.attachedWorkerNow = func(context.Context, rowQuery) (time.Time, error) { return at, nil }
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			t.Cleanup(cancel)
			request := ports.ReserveAttemptEffectRequestV1{TenantID: r.TenantID, RunID: r.RunID, AttemptID: r.AttemptID, LeaseID: r.LeaseID, FenceToken: r.FenceToken, PhysicalInvocationClaimID: "contending-claim"}
			result, err := store.ReserveAttemptEffect(ctx, request)
			wantSuccess := name == "success" || name == "retry_success"
			if (err == nil) != wantSuccess {
				t.Fatalf("scenario=%s error=%v wantSuccess=%t", name, err, wantSuccess)
			}
			wantCalls := 0
			if name == "mint_failure" {
				wantCalls = 1
			}
			if wantSuccess {
				wantCalls = 1
				if result.Status != ports.AttemptEffectReconcileOnlyV1 || result.Grant != nil || result.ObservationGrant == nil || issuer.VerifyObservationGrant(*result.ObservationGrant) != nil {
					t.Fatalf("invalid committed reconcile-only result: %+v", result)
				}
			} else if result.Status != "" || result.Grant != nil || result.ObservationGrant != nil || result.Reservation.AttemptID != "" {
				t.Errorf("unconfirmed transaction leaked a result: %+v", result)
			}
			if f.grantCalls != wantCalls || f.premature != 0 {
				t.Errorf("scenario=%s grantCalls=%d premature=%d wantCalls=%d all-after-commit", name, f.grantCalls, f.premature, wantCalls)
			}
			wantBegins := 1
			if name == "missing_issuer" {
				wantBegins = 0
			}
			if strings.HasPrefix(name, "retry_") {
				wantBegins = 2
			}
			if f.begins != wantBegins {
				t.Errorf("transaction attempts=%d want%d", f.begins, wantBegins)
			}
		})
	}
}
