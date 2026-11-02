package ydbstore

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localoperator"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
)

// Raw test dependencies only. The external test constructs the ACTUAL session;
// no private bootstrap, bridge, keeper, grant or running-state constructor leaks.
type LocalCanonicalSessionSeed struct {
	Source    *localoperator.Source
	DB        *sql.DB
	Options   Options
	Prepare   *serverlessharness.CapabilityIssuer
	Operate   *serverlessharness.OperationCapabilityIssuer
	Backend   domain.HarnessBackendDescriptorV1
	Substrate domain.SubstrateBindingV1
	Loaded    ports.WorkerJobState
	Claim     ports.WorkerLeaseRequest
	peer      *canonicalSessionSQL
}

type LocalCanonicalSessionStats struct {
	Parents, Burns, Allocations, Writes int
	LeasePresent                        bool
}

func (s *LocalCanonicalSessionSeed) Stats() LocalCanonicalSessionStats {
	return LocalCanonicalSessionStats{s.peer.parent.inserts, s.peer.parent.burnUpdates, s.peer.alloc.writes, s.peer.base.writes, len(s.peer.base.rows["leases"]) != 0}
}
func (s *LocalCanonicalSessionSeed) Now() time.Time {
	return time.Now().UTC().Truncate(time.Microsecond)
}

// This reuses ONLY SQL-row machinery, never rich seed constructors. Parent,
// burn and allocation start absent; lease projections come from actual writes.
type canonicalSessionSQL struct {
	*localWorkerLifecycleSQL
	base    *localCanonicalJoinSQL
	parent  *ownedAttemptEffectParentSQLFixture
	alloc   *allocationSQLFixture
	queries int
}

func (f *canonicalSessionSQL) Connect(context.Context) (driver.Conn, error) { return f, nil }
func (f *canonicalSessionSQL) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	f.localWorkerLifecycleSQL.beginPhase("canonical facade callback")
	return (allocationConn{f.alloc}).BeginTx(ctx, opts)
}
func (f *canonicalSessionSQL) QueryContext(ctx context.Context, s string, args []driver.NamedValue) (driver.Rows, error) {
	f.queries++
	if f.queries > 1024 {
		return nil, errors.New("canonical session finite SQL inventory exhausted")
	}
	if s == "SELECT CurrentUtcTimestamp()" {
		f.base.at = time.Now().UTC().Truncate(time.Microsecond)
	}
	if s == ownedAttemptEffectLeaseSQL {
		if !f.exactArgs(args, string(f.base.request.TenantID), string(f.base.request.RunID)) {
			return nil, errors.New("foreign actual lease join")
		}
		head := f.base.rows["lease_heads"]
		var lease domain.Lease
		if len(head) != 8 || len(f.base.rows["leases"]) != 1 || json.Unmarshal([]byte(f.base.rows["leases"][0].(string)), &lease) != nil {
			return &explanationFixtureRows{}, nil
		}
		f.base.recordQuery(s, args)
		return &explanationFixtureRows{values: append(append([]driver.Value(nil), head...), string(lease.TenantID), string(lease.ID), string(lease.RunID), string(lease.AttemptID), lease.WorkerID, int64(lease.FenceToken), lease.AcquiredAt, lease.ExpiresAt, f.base.rows["leases"][0])}, nil
	}
	switch s {
	case ownedAttemptEffectParentSQL, ownedAttemptPreparationBurnSQL, ownedOperationAccountingSQL, ownedAllocationRegistrationSQL:
		if len(args) < 2 || args[0].Value != string(f.base.request.TenantID) || args[1].Value != string(f.base.request.AttemptID) {
			return nil, errors.New("foreign original effect/allocation key")
		}
		return (allocationConn{f.alloc}).QueryContext(ctx, s, args)
	case "SELECT CurrentUtcTimestamp()", ownedAdmissionPrincipalSQL, ownedAdmissionSourcesJoinSQL, ownedProfileCurrentSQL, ownedAdmissionSQL:
		return f.base.QueryContext(ctx, s, args)
	}
	for statement := range ownedCanonicalJSONReads {
		bounded, _, _ := ownedCanonicalJSONSelect(statement)
		if s != bounded {
			continue
		}
		table := strings.Fields(statement)[3]
		key := string(f.base.request.RunID)
		switch table {
		case "attempts":
			key = string(f.base.request.AttemptID)
		case "quota_reservations":
			key = "reservation-local-lifecycle"
		case "artifact_manifests":
			key = string(f.base.request.ManifestID)
		case "sessions", "session_deletions":
			key = string(f.base.request.SessionID)
		default:
			if table != "runs" && table != "worker_jobs" {
				continue
			}
		}
		if !f.exactArgs(args, string(f.base.request.TenantID), key) {
			return nil, fmt.Errorf("foreign canonical session point key: %s", table)
		}
		f.base.recordQuery(s, args)
		return &explanationFixtureRows{values: append([]driver.Value(nil), f.base.rows[table]...)}, nil
	}
	// Exact ordinary claim/start/load inventory remains the reviewed real-lease
	// peer. Its per-call observation counters are not another production budget.
	return f.localWorkerLifecycleSQL.QueryContext(ctx, s, args)
}
func (f *canonicalSessionSQL) ExecContext(ctx context.Context, s string, args []driver.NamedValue) (driver.Result, error) {
	if strings.Contains(s, "INSERT INTO attempt_effect_reservations") || s == ownedAttemptPreparationBurnUpdateSQL || s == ownedOperationAccountingWriteSQL || s == ownedAllocationRegistrationCreateSQL || s == ownedAllocationRegistrationTransitionSQL {
		result, err := (allocationConn{f.alloc}).ExecContext(ctx, s, args)
		if err == nil && strings.Contains(s, "INSERT INTO attempt_effect_reservations") && f.parent.burnRow == nil {
			// INSERT's existing physical defaults, not an approval/burn fact.
			f.parent.burnRow = []driver.Value{uint64(0), uint64(0), time.Unix(0, 0).UTC(), ""}
		}
		return result, err
	}
	return f.localWorkerLifecycleSQL.ExecContext(ctx, s, args)
}

func NewLocalCanonicalSessionSeed(t *testing.T, endpoint string) *LocalCanonicalSessionSeed {
	t.Helper()
	scope := domain.AttemptExecutionSourceScopeV1{TenantID: "tenant-local-connected", OwnerUserID: "owner-local-connected", SessionID: "session-local-connected", RunID: "run-local-connected", AttemptID: "attempt-local-connected"}
	actual := newLocalRuntimeActualFixture(t, true, true, localRuntimeActualPrebuild{sourceOnly: true, authenticFunding: true, scope: scope, endpoint: endpoint, compiledCompaction: true, setupDuration: 10 * time.Second, networkOperation: 5 * time.Second})
	if actual.issuer != nil || !reflect.DeepEqual(actual.grant, ports.AttemptEffectOwnershipGrantV1{}) || actual.authority.Lease.ID != "" || actual.parent.RunID != "" {
		t.Fatal("source-only constructor minted invocation authority")
	}
	original := &originalLocalRecoveryFixture{root: actual.installationRoot, exe: actual.executable, source: actual.source, store: actual.installationStore, sql: actual.installationSQL, receipt: actual.receipt, scope: scope, rows: actual.execution.Spending}
	store, base, _, request := newLocalCanonicalJoin(t, original)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	t.Cleanup(cancel)
	if result, err := store.CommitCanonicalUserEvent(ctx, request); err != nil || !result.Created {
		t.Fatalf("genuine empty canonical commit: created=%t err=%v", result.Created, err)
	}
	base.canonical = false
	admission := ports.DispatchAdmissionRequest{TenantID: scope.TenantID, RunID: scope.RunID, AttemptID: scope.AttemptID, OutboxID: request.DispatchID, ReservationID: "reservation-local-lifecycle", Now: base.at, HoldUntil: base.at.Add(time.Minute), Limits: domain.ProductLimits{MaxTenantQueueDepth: 8, MaxActiveRuns: 1, MaxRuntime: time.Minute, MaxTurns: 8, MaxInputBytes: 1 << 20, MaxContextBytes: 1 << 20, MaxArtifacts: 8, MaxContextEvents: 100, MaxToolEvents: 20, MaxToolEventBytes: 1 << 18}, Workload: domain.WorkloadShape{Runtime: time.Second, Turns: 1}}
	if result, err := store.AdmitDispatch(ctx, admission); err != nil || !result.Admitted {
		t.Fatalf("genuine canonical admission: admitted=%t err=%v", result.Admitted, err)
	}
	parent := &ownedAttemptEffectParentSQLFixture{base: base.ownedFinalScript, graph: base.managedAdmissionGraphFixture}
	alloc := &allocationSQLFixture{parent: parent}
	peer := &canonicalSessionSQL{localWorkerLifecycleSQL: &localWorkerLifecycleSQL{localCanonicalJoinSQL: base, leaseID: "lease-local-connected", worker: "worker-local-connected"}, base: base, parent: parent, alloc: alloc}
	db := sql.OpenDB(peer)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	store.db = db
	loaded, found, err := store.LoadWorkerJob(ctx, scope.TenantID, scope.RunID)
	if err != nil || !found || loaded.Run.Status != domain.RunQueued || loaded.Attempt.Status != domain.AttemptCreated {
		t.Fatalf("genuine queued load: found=%t err=%v", found, err)
	}
	prepare, err := serverlessharness.NewCapabilityIssuer(func() time.Time { return time.Now().UTC().Truncate(time.Microsecond) }, bytes.NewReader(bytes.Repeat([]byte{19}, 32)))
	if err != nil {
		t.Fatal(err)
	}
	operate, err := serverlessharness.NewOperationCapabilityIssuer(func() time.Time { return time.Now().UTC().Truncate(time.Microsecond) }, bytes.NewReader(bytes.Repeat([]byte{23}, 32)), 16)
	if err != nil {
		t.Fatal(err)
	}
	return &LocalCanonicalSessionSeed{Source: actual.source, DB: db, Options: Options{OwnedAttemptEffectSourceVerifier: actual.source, OwnedProfilePublicationVerifier: actual.source, AttemptSpendingPublicationVerifier: actual.source}, Prepare: prepare, Operate: operate, Backend: loaded.Job.HarnessBinding.Backend, Substrate: *loaded.Job.SubstrateBinding, Loaded: loaded, Claim: ports.WorkerLeaseRequest{TenantID: scope.TenantID, RunID: scope.RunID, AttemptID: scope.AttemptID, LeaseID: peer.leaseID, WorkerID: peer.worker}, peer: peer}
}
