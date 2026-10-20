package ydbstore

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localoperator"
	"gitcode.com/urandon/sessionless/internal/operationbody"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
)

// This exact-query SQL overlay models only operation 1/call 1 over the existing
// original parent/accounting/allocation/proxy fixtures. It is not native YDB
// isolation. Unknown operation SQL is explicitly rejected, never approved.
type loopbackOperationSQL struct {
	*localProxySQL
	operation, pendingOperation, call, pendingCall []driver.Value
	operationWrites, callWrites                    int
	observationWrites                              int
	// Explicit factory/real-network test arm; old fixtures retain fixed time.
	sampleWallClock bool
}
type loopbackOperationConnector struct{ f *loopbackOperationSQL }
type loopbackOperationDriver struct{ f *loopbackOperationSQL }
type loopbackOperationTx struct {
	driver.Tx
	f *loopbackOperationSQL
}

func (c loopbackOperationConnector) Connect(context.Context) (driver.Conn, error) { return c.f, nil }
func (c loopbackOperationConnector) Driver() driver.Driver                        { return loopbackOperationDriver{c.f} }
func (d loopbackOperationDriver) Open(string) (driver.Conn, error)                { return d.f, nil }
func (f *loopbackOperationSQL) BeginTx(ctx context.Context, o driver.TxOptions) (driver.Tx, error) {
	tx, e := f.localProxySQL.BeginTx(ctx, o)
	if e != nil {
		return nil, e
	}
	f.pendingOperation, f.pendingCall = nil, nil
	return loopbackOperationTx{tx, f}, nil
}
func (f *loopbackOperationSQL) QueryContext(ctx context.Context, s string, args []driver.NamedValue) (driver.Rows, error) {
	if f.sampleWallClock && s == "SELECT CurrentUtcTimestamp()" {
		f.f.parent.graph.at = time.Now().UTC().Truncate(time.Microsecond)
	}
	switch s {
	case ownedOperationReadSQL, ownedOperationCallSQL:
		f.f.parent.base.recordQuery(s, args)
		if len(args) != 3 {
			return nil, errors.New("connected operation point read requires three exact keys")
		}
		row := f.operation
		if f.pendingOperation != nil {
			row = f.pendingOperation
		}
		if s == ownedOperationCallSQL {
			row = f.call
			if f.pendingCall != nil {
				row = f.pendingCall
			}
		}
		if row != nil {
			var reservation domain.AttemptOperationReservationV1
			opRow := f.operation
			if f.pendingOperation != nil {
				opRow = f.pendingOperation
			}
			if len(opRow) != 8 || json.Unmarshal([]byte(opRow[0].(string)), &reservation) != nil {
				return nil, errors.New("connected operation row invalid")
			}
			op := reservation.Operation
			if args[0].Value != string(op.Scope.TenantID) || args[1].Value != string(op.Scope.AttemptID) || (s == ownedOperationReadSQL && args[2].Value != int64(op.Sequence)) || (s == ownedOperationCallSQL && args[2].Value != op.CallID) {
				return nil, errors.New("connected operation point read crossed original identity")
			}
		}
		return &explanationFixtureRows{values: append([]driver.Value(nil), row...)}, nil
	default:
		if strings.Contains(s, "attempt_operations_v1") || strings.Contains(s, "attempt_operation_calls_v1") {
			return nil, errors.New("unmodeled connected operation query")
		}
		return f.localProxySQL.QueryContext(ctx, s, args)
	}
}

const loopbackOperationInsertSQL = `INSERT INTO attempt_operations_v1 (tenant_id,attempt_id,sequence,record,digest,operation_digest,call_id) VALUES ($1,$2,$3,$4,$5,$6,$7)`
const loopbackCallInsertSQL = `INSERT INTO attempt_operation_calls_v1 (tenant_id,attempt_id,call_id,sequence,operation_digest) VALUES ($1,$2,$3,$4,$5)`
const loopbackObservationUpdateSQL = `UPDATE attempt_operations_v1 SET observation_record=$1,observation_digest=$2 WHERE tenant_id=$3 AND attempt_id=$4 AND sequence=$5 AND digest=$6 AND observation_record IS NULL AND observation_digest IS NULL`

func (f *loopbackOperationSQL) ExecContext(ctx context.Context, s string, args []driver.NamedValue) (driver.Result, error) {
	switch s {
	case loopbackOperationInsertSQL:
		if len(args) != 7 || f.operation != nil || f.pendingOperation != nil || f.row == nil {
			return nil, errors.New("connected operation requires absent row and committed actual proxy")
		}
		var r domain.AttemptOperationReservationV1
		if json.Unmarshal([]byte(args[3].Value.(string)), &r) != nil || r.Validate() != nil {
			return nil, errors.New("connected operation write invalid")
		}
		d, _ := r.Digest()
		if args[0].Value != string(r.Operation.Scope.TenantID) || args[1].Value != string(r.Operation.Scope.AttemptID) || args[2].Value != int64(r.Operation.Sequence) || args[4].Value != string(d) || args[5].Value != string(r.OperationDigest) || args[6].Value != r.Operation.CallID {
			return nil, errors.New("connected operation write lost exact reservation")
		}
		f.pendingOperation = []driver.Value{args[3].Value, args[4].Value, args[5].Value, args[6].Value, uint64(0), uint64(0), "", ""}
		f.operationWrites++
	case loopbackCallInsertSQL:
		if len(args) != 5 || f.call != nil || f.pendingCall != nil || f.pendingOperation == nil {
			return nil, errors.New("connected call index requires sole pending operation")
		}
		var r domain.AttemptOperationReservationV1
		if json.Unmarshal([]byte(f.pendingOperation[0].(string)), &r) != nil || args[0].Value != string(r.Operation.Scope.TenantID) || args[1].Value != string(r.Operation.Scope.AttemptID) || args[2].Value != r.Operation.CallID || args[3].Value != int64(r.Operation.Sequence) || args[4].Value != string(r.OperationDigest) {
			return nil, errors.New("connected call index crossed immutable operation")
		}
		f.pendingCall = []driver.Value{args[3].Value, args[4].Value}
		f.callWrites++
	case loopbackObservationUpdateSQL:
		row := f.operation
		if f.pendingOperation != nil {
			row = f.pendingOperation
		}
		if len(args) != 6 || len(row) != 8 || row[4] != uint64(0) || row[5] != uint64(0) || row[6] != "" || row[7] != "" {
			return nil, errors.New("connected observation CAS requires exact unobserved original row")
		}
		var reservation domain.AttemptOperationReservationV1
		var observation domain.AttemptOperationObservationV1
		if json.Unmarshal([]byte(row[0].(string)), &reservation) != nil || json.Unmarshal([]byte(args[0].Value.(string)), &observation) != nil || observation.ValidateForReservation(reservation) != nil {
			return nil, errors.New("connected observation CAS has invalid original reservation/observation")
		}
		digest, _ := observation.Digest()
		canonical, _ := json.Marshal(observation)
		if string(canonical) != args[0].Value || string(digest) != args[1].Value || args[2].Value != string(reservation.Operation.Scope.TenantID) || args[3].Value != string(reservation.Operation.Scope.AttemptID) || args[4].Value != int64(reservation.Operation.Sequence) || args[5].Value != row[1] {
			return nil, errors.New("connected observation CAS crossed original identity/digest")
		}
		f.pendingOperation = append([]driver.Value(nil), row...)
		f.pendingOperation[4], f.pendingOperation[5] = uint64(1), uint64(1)
		f.pendingOperation[6], f.pendingOperation[7] = args[0].Value, args[1].Value
		f.observationWrites++
	default:
		if strings.Contains(s, "attempt_operations_v1") || strings.Contains(s, "attempt_operation_calls_v1") {
			return nil, errors.New("unmodeled connected operation write")
		}
		return f.localProxySQL.ExecContext(ctx, s, args)
	}
	f.f.parent.base.recordQuery(s, args)
	return driver.RowsAffected(1), nil
}
func (tx loopbackOperationTx) Commit() error {
	e := tx.Tx.Commit()
	if e == nil {
		if tx.f.pendingOperation != nil {
			tx.f.operation = append([]driver.Value(nil), tx.f.pendingOperation...)
		}
		if tx.f.pendingCall != nil {
			tx.f.call = append([]driver.Value(nil), tx.f.pendingCall...)
		}
	}
	tx.f.pendingOperation, tx.f.pendingCall = nil, nil
	return e
}
func (tx loopbackOperationTx) Rollback() error {
	tx.f.pendingOperation, tx.f.pendingCall = nil, nil
	return tx.Tx.Rollback()
}

// A bounded immutable test body keeper, not a provider/price/capability issuer.
type loopbackOperationBody struct {
	scope domain.AttemptOperationScopeV1
	ref   string
	body  []byte
}

func (b *loopbackOperationBody) ReadAttemptOperationBody(ctx context.Context, s domain.AttemptOperationScopeV1, ref string, ceiling uint64) ([]byte, error) {
	if ctx.Err() != nil || s != b.scope || ref != b.ref || ceiling == 0 || ceiling > 1<<20 || uint64(len(b.body)) > ceiling {
		return nil, domain.ErrMembershipDenied
	}
	return bytes.Clone(b.body), nil
}

// LocalLoopbackConnectedFixture is TEST-ONLY navigation across the package
// cycle: actual Source/private receipt, same bootstrap/Store/issuers and real
// runtime/allocation, with SCRIPTED SQL, external funding and effect verifier.
// The external test supplies an actual connector, never an approval callback.
type LocalLoopbackConnectedFixture struct {
	Source             *localoperator.Source
	OperationIssuer    *serverlessharness.OperationCapabilityIssuer
	Bootstrap          *LocalOwnedStoreBootstrapV1
	Allocation         domain.RegisteredAllocationV2
	ExecutionSource    domain.AttemptExecutionSourceV1
	Operation          domain.AttemptOperationV1
	Frozen             *configuredinference.FrozenRequestV1
	Context            context.Context
	actual             localRuntimeActualFixture
	parent             *ownedAttemptEffectParentFixture
	sql                *loopbackOperationSQL
	store              *Store
	bridge             *serverlessharness.BoundedOwnedAllocationPreparationV1
	body               *loopbackOperationBody
	operationBodies    *operationbody.StoreV1
	observationGrant   ports.AttemptOperationObservationGrantV1
	payload            []byte
	sourceCloseCommits int
	sourceCloseFired   bool
}

func NewLocalLoopbackConnectedFixture(t *testing.T, endpoint string) *LocalLoopbackConnectedFixture {
	t.Helper()
	state := currentWorkerAuthorityFixture(t)
	scope := domain.AttemptExecutionSourceScopeV1{TenantID: state.Run.TenantID, OwnerUserID: state.Job.CredentialOwnerUserID, SessionID: state.Run.SessionID, RunID: state.Run.ID, AttemptID: state.Attempt.ID}
	actual := newLocalRuntimeActualFixture(t, true, true, localRuntimeActualPrebuild{authenticFunding: true, scope: scope, setupDuration: 10 * time.Second, endpoint: endpoint})
	p := newOwnedAttemptEffectParentFixture(t, ownedFinalPrebuild{at: actual.at, sources: &ownedFinalSources{profile: actual.profile, spending: actual.execution.Spending}})
	issuer, e := serverlessharness.NewOperationCapabilityIssuer(func() time.Time { return p.graph.at }, strings.NewReader(strings.Repeat("C", 32)), 16)
	if e != nil {
		t.Fatal(e)
	}
	b, e := NewLocalOwnedStoreBootstrapV1(actual.source, p.issuer.issuer, issuer)
	if e != nil {
		t.Fatal(e)
	}
	frozen, e := configuredinference.FreezeRequestV1(actual.config, actual.config.Limits, configuredinference.RequestV1{
		Messages: []configuredinference.MessageV1{{Role: configuredinference.RoleUserV1, Text: "owned test peer, not a model"}}, MaxOutputTokens: 1,
	})
	if e != nil {
		t.Fatal(e)
	}
	return &LocalLoopbackConnectedFixture{Source: actual.source, OperationIssuer: issuer, Bootstrap: b, ExecutionSource: p.source.Clone(), Context: actual.ctx, Frozen: frozen, actual: actual, parent: p, payload: frozen.Body()}
}
func (f *LocalLoopbackConnectedFixture) Complete(t *testing.T, keeper ports.OwnedProxyPublicationVerifierV2) {
	t.Helper()
	f.complete(t, keeper, nil)
}

// CompleteWithBlobStore selects the real production body adapter explicitly.
// Only storage bytes are fake; scope/limits come from the ORIGINAL admitted
// parent, and the same concrete adapter is retained by the original Store.
func (f *LocalLoopbackConnectedFixture) CompleteWithBlobStore(t *testing.T, keeper ports.OwnedProxyPublicationVerifierV2, blobs ports.BlobStore) {
	t.Helper()
	if blobs == nil {
		t.Fatal("connected production-body arm needs explicit owned blob storage")
	}
	f.complete(t, keeper, blobs)
}

func (f *LocalLoopbackConnectedFixture) complete(t *testing.T, keeper ports.OwnedProxyPublicationVerifierV2, blobs ports.BlobStore) {
	t.Helper()
	f.sql = &loopbackOperationSQL{localProxySQL: &localProxySQL{allocationConn: allocationConn{&allocationSQLFixture{parent: f.parent.sql}}}}
	db := sql.OpenDB(loopbackOperationConnector{f.sql})
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if e := db.Close(); e != nil {
			t.Error(e)
		}
	})
	scope := ownedOperationScope(f.parent.parent)
	requestRef := "connected-local-request"
	var bodyReader ports.AttemptOperationBodyStoreV1
	if blobs == nil {
		f.body = &loopbackOperationBody{scope: scope, ref: requestRef, body: bytes.Clone(f.payload)}
		bodyReader = f.body
	} else {
		adapter, err := operationbody.NewV1(scope, f.parent.parent.Authority.HarnessBinding.AgentLoopPolicy.Limits, blobs)
		if err != nil {
			t.Fatalf("original scoped production body adapter: %v", err)
		}
		reference, err := adapter.Write(f.Context, f.payload)
		if err != nil || reference.Digest != ports.OwnedFixtureBytesDigestV1(f.payload) || reference.Bytes != uint64(len(f.payload)) {
			t.Fatalf("production body write/readback before original Store setup: %v", err)
		}
		requestRef, bodyReader = reference.Ref, adapter
		f.operationBodies = adapter
	}
	store, e := f.Bootstrap.CreateStore(db, Options{OwnedAttemptEffectSourceVerifier: f.Source, OwnedProfilePublicationVerifier: f.Source, AttemptOperationBodies: bodyReader}, keeper)
	if e != nil {
		t.Fatal(e)
	}
	if store.attemptOperationBodies != bodyReader {
		t.Fatal("original Store replaced production body custody")
	}
	f.store, f.parent.store = store, store
	bridge, e := serverlessharness.NewBoundedOwnedLocalAllocationPreparationV1(f.parent.issuer.issuer, f.parent.parent.Authority.HarnessBinding.Backend, f.parent.parent.Authority.SubstrateBinding, f.Source, f.Bootstrap)
	if e != nil {
		t.Fatal(e)
	}
	f.bridge = bridge
	t.Cleanup(func() {
		if e := bridge.Close(); e != nil {
			t.Error(e)
		}
	})
	if e = f.Bootstrap.Complete(bridge); e != nil {
		t.Fatal(e)
	}
	reserved := f.parent.reserve(t, f.parent.request)
	if reserved.Grant == nil {
		t.Fatal("connected original Store reserve returned no grant")
	}
	publication, e := bridge.PrepareAllocationPublication(f.Context, reserved.Grant.Clone())
	if e != nil {
		t.Fatalf("connected original Store burn/runtime: %v", e)
	}
	registered, e := store.publishBoundedOwnedAllocation(t.Context(), publication)
	if e != nil {
		t.Fatalf("connected allocation registration: %v", e)
	}
	if e = bridge.EnterOwnedLocalAllocation(f.Context, publication); e != nil {
		t.Fatalf("connected original allocation entry: %v", e)
	}
	f.Allocation = registered.Current.Clone()
	duration := min(20*time.Second, f.actual.config.Limits.OperationTimeout)
	deadline := time.Now().UTC().Truncate(time.Microsecond).Add(duration)
	if deadline.After(f.Allocation.Window.ExpiresAt) {
		t.Fatal("connected operation does not fit original active allocation")
	}
	cost := f.parent.parent.Authority.AdmissionCostCeiling
	cd, e := cost.Digest()
	if e != nil {
		t.Fatal(e)
	}
	f.Operation = domain.AttemptOperationV1{Version: 1, Scope: scope, Sequence: 1, Kind: domain.AttemptOperationModelV1, CallID: "connected-local-model", RequestRef: requestRef, RequestDigest: ports.OwnedFixtureBytesDigestV1(f.payload), Deadline: deadline, CleanupDuration: f.actual.config.Limits.CleanupTimeout, RequestBytes: uint64(len(f.payload)), ResultBytes: 512, ContextBytes: 1, Currency: cost.Currency, PriceRevision: cost.PriceRevision, AdmissionCostCeilingDigest: cd, Debit: domain.AttemptOperationDebitV1{Operations: 1, ModelCalls: 1, InputTokens: 1, OutputTokens: 1, BodyBytes: uint64(len(f.payload)) + 512, ResponseBytes: 512, ActiveDuration: duration}}
	if e = f.Operation.ValidateForParent(f.parent.parent.Authority, f.parent.parent.Reservation); e != nil {
		t.Fatalf("connected immutable model operation: %v", e)
	}
	if f.parent.sql.inserts != 1 || f.parent.sql.burnUpdates != 1 || f.sql.f.writes != 1 {
		t.Fatal("connected setup did not use one original reserve/burn/allocation")
	}
}
func (f *LocalLoopbackConnectedFixture) Payload() []byte { return bytes.Clone(f.payload) }

// Independent test wire oracle, not a real model or measured token usage.
func (f *LocalLoopbackConnectedFixture) Response(t *testing.T) []byte {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"model": f.actual.config.ModelID,
		"choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": "owned peer result"}, "finish_reason": "stop"}},
		"usage":   map[string]uint64{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2}})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// Existing test transaction hook only: custody/profile preparation commits
// first, then all before-send current joins commit. Revoke the actual Source
// after that final commit and before the original issuer consumes capability.
func (f *LocalLoopbackConnectedFixture) ArmSourceCloseAfterCurrentCommit(t *testing.T) {
	t.Helper()
	if f.parent.graph.afterCommit != nil {
		t.Fatal("connected source-close hook already occupied")
	}
	f.parent.graph.afterCommit = func() {
		f.sourceCloseCommits++
		if f.sourceCloseCommits == 2 {
			f.parent.graph.afterCommit = nil
			f.sourceCloseFired = true
			if err := f.Source.Close(); err != nil {
				t.Errorf("actual Source close after current commit: %v", err)
			}
		}
	}
}
func (f *LocalLoopbackConnectedFixture) AssertSourceClosedAfterCurrent(t *testing.T) {
	t.Helper()
	if !f.sourceCloseFired || f.sourceCloseCommits != 2 {
		t.Fatalf("source-close control did not reach custody and final current transactions: commits=%d fired=%t", f.sourceCloseCommits, f.sourceCloseFired)
	}
	if _, err := f.Source.ExecutionLifetime(); err == nil {
		t.Fatal("original Source remained alive after post-current close")
	}
	f.AssertSingleOriginalWrites(t)
}

// Only the scripted current head changes; original private Source, allocation,
// publication and operation remain untouched and cannot authorize this update.
func (f *LocalLoopbackConnectedFixture) RevokeCurrentProfile(t *testing.T) {
	t.Helper()
	r := f.parent.base.reader
	r.profile = r.profile.Clone()
	r.profile.State = domain.OwnedAgentCurrentRevokedV1
	r.profile.RevocationGeneration++
	r.profile.UpdatedAt = time.Now().UTC().Truncate(time.Microsecond)
	r.sync(t)
	f.parent.graph.at = r.profile.UpdatedAt
}
func (f *LocalLoopbackConnectedFixture) RegisterAndReserve(t *testing.T, proof ports.OwnedProxyPublicationV2) (domain.RegisteredProxyAttestationV2, ports.AttemptOperationCapabilityV1) {
	t.Helper()
	f.parent.graph.at = time.Now().UTC().Truncate(time.Microsecond)
	registered, replayed, e := f.store.PublishOwnedProxy(t.Context(), f.parent.request, proof)
	if e != nil || replayed {
		t.Fatalf("connected genuine socket publication: err=%v replay=%t", e, replayed)
	}
	f.parent.graph.at = time.Now().UTC().Truncate(time.Microsecond)
	r, e := f.store.ReserveAttemptOperation(t.Context(), ports.ReserveAttemptOperationRequestV1{Operation: f.Operation.Clone()})
	if e != nil || r.Capability == nil {
		ctx, cancel := ownedAuthorityCallContext(t.Context())
		defer cancel()
		_, priceErr := f.store.prepareOwnedOperationEvidence(ctx, f.Operation)
		t.Fatalf("connected real Store operation reservation: err=%v capability=%t evidence=%v", e, r.Capability != nil, priceErr)
	}
	if r.ObservationGrant.Validate() != nil {
		t.Fatal("connected actual operation reserve omitted its original observation grant")
	}
	f.observationGrant = r.ObservationGrant.Clone()
	return registered, r.Capability.Clone()
}

// Select a larger normalized DATA slot before freezing the whole operation for
// preflight/publication. Existing 512-byte connected fixtures remain unchanged.
func (f *LocalLoopbackConnectedFixture) UseNormalizedModelResultBound(t *testing.T) {
	t.Helper()
	if f.operationBodies == nil || f.sql.operation != nil || f.sql.row != nil {
		t.Fatal("normalized slot must use original production body custody before proxy/operation reservation")
	}
	f.Operation.ResultBytes = 4096
	f.Operation.Debit.BodyBytes = f.Operation.RequestBytes + 4096
	f.Operation.Debit.ResponseBytes = 4096
	if err := f.Operation.ValidateForParent(f.parent.parent.Authority, f.parent.parent.Reservation); err != nil {
		t.Fatalf("normalized slot exceeds original admitted parent: %v", err)
	}
}

func (f *LocalLoopbackConnectedFixture) WriteOperationBody(t *testing.T, body []byte) operationbody.ReferenceV1 {
	t.Helper()
	if f.operationBodies == nil {
		t.Fatal("normalized response needs the original concrete production body adapter")
	}
	ref, err := f.operationBodies.Write(f.Context, body)
	if err != nil {
		t.Fatalf("production response body custody: %v", err)
	}
	return ref
}

// This calls the SAME actual issuer with the observation grant returned by the
// real Store reservation. It authenticates DATA, not a caller approval callback.
func (f *LocalLoopbackConnectedFixture) AuthenticateCompletedModelObservation(t *testing.T, ref operationbody.ReferenceV1, usage domain.AttemptOperationUsageV1) ports.AuthenticatedAttemptOperationObservationV1 {
	t.Helper()
	if f.observationGrant.Validate() != nil {
		t.Fatal("missing actual original observation grant")
	}
	at := time.Now().UTC().Truncate(time.Microsecond)
	f.parent.graph.at = at
	reservation := f.observationGrant.Reservation
	digest, err := reservation.Digest()
	if err != nil {
		t.Fatal(err)
	}
	o := domain.AttemptOperationObservationV1{Version: 1, OperationDigest: reservation.OperationDigest, ReservationDigest: digest,
		Outcome: domain.AttemptOperationCompletedV1, Reason: domain.AttemptOperationReasonNoneV1,
		ResponseRef: ref.Ref, ResponseDigest: ref.Digest, ResponseBytes: ref.Bytes, ObservedAt: at, Usage: usage.Clone()}
	authenticated, err := f.OperationIssuer.AuthenticateAttemptOperationObservation(f.observationGrant.Clone(), o)
	if err != nil {
		t.Fatalf("same original issuer observation authentication: %v", err)
	}
	return authenticated
}

func (f *LocalLoopbackConnectedFixture) RecordAuthenticatedModelObservation(t *testing.T, observation ports.AuthenticatedAttemptOperationObservationV1) error {
	t.Helper()
	f.parent.graph.at = time.Now().UTC().Truncate(time.Microsecond)
	return f.store.RecordAttemptOperationObservation(f.Context, ports.RecordAttemptOperationObservationRequestV1{ObservationGrant: f.observationGrant.Clone(), Observation: observation.Clone()})
}

func (f *LocalLoopbackConnectedFixture) ReadOriginalModelObservation(t *testing.T) (ports.AuthenticatedAttemptOperationObservationV1, error) {
	t.Helper()
	f.parent.graph.at = time.Now().UTC().Truncate(time.Microsecond)
	return f.store.ReadAttemptOperationObservation(f.Context, f.observationGrant.Clone())
}

// ValidateRetainedNormalizedModelData exposes only the existing private DATA
// validator to this connected test. It uses the original bootstrap configuration
// clone and body adapter, but authenticates no Source, grant or observation and
// grants no effect/continuation permission.
func (f *LocalLoopbackConnectedFixture) ValidateRetainedNormalizedModelData(t *testing.T, op domain.AttemptOperationV1, observation domain.AttemptOperationObservationV1) error {
	t.Helper()
	if f.store == nil || f.Bootstrap == nil || f.store.localBootstrap != f.Bootstrap || f.operationBodies == nil {
		t.Fatal("retained normalized DATA validation requires the original Store/bootstrap/body adapter")
	}
	return f.store.validateLocalNormalizedModel(f.Context, op.Clone(), observation.Clone())
}

func (f *LocalLoopbackConnectedFixture) AssertObservationWrites(t *testing.T, want int) {
	t.Helper()
	if f.sql.observationWrites != want {
		t.Fatalf("observation SQL writes=%d, want %d", f.sql.observationWrites, want)
	}
}

func (f *LocalLoopbackConnectedFixture) ReserveOriginalOperationAgain(t *testing.T) (ports.ReserveAttemptOperationResultV1, error) {
	t.Helper()
	f.parent.graph.at = time.Now().UTC().Truncate(time.Microsecond)
	return f.store.ReserveAttemptOperation(f.Context, ports.ReserveAttemptOperationRequestV1{Operation: f.Operation.Clone()})
}
func (f *LocalLoopbackConnectedFixture) AssertSingleOriginalWrites(t *testing.T) {
	t.Helper()
	if f.parent.sql.inserts != 1 || f.parent.sql.burnUpdates != 1 || f.sql.f.writes != 1 || f.sql.writes != 1 || f.sql.operationWrites != 1 || f.sql.callWrites != 1 {
		t.Fatalf("unexpected connected writes: parent=%d burn=%d allocation=%d proxy=%d operation=%d call=%d", f.parent.sql.inserts, f.parent.sql.burnUpdates, f.sql.f.writes, f.sql.writes, f.sql.operationWrites, f.sql.callWrites)
	}
}
func (f *LocalLoopbackConnectedFixture) AssertReplayNoNewCapability(t *testing.T) {
	t.Helper()
	r, e := f.store.ReserveAttemptOperation(t.Context(), ports.ReserveAttemptOperationRequestV1{Operation: f.Operation.Clone()})
	if e != nil || r.Capability != nil || r.Status != ports.AttemptOperationReconcileOnlyV1 {
		t.Fatalf("connected reservation replay minted send authority: err=%v capability=%t status=%s", e, r.Capability != nil, r.Status)
	}
	f.AssertSingleOriginalWrites(t)
}

// Keep the SAME original issuer inside its real valid send window. A denial
// below must not be explained by expiry, malformed data or a different issuer.
func (f *LocalLoopbackConnectedFixture) originalCapabilityWindow(t *testing.T, cap ports.AttemptOperationCapabilityV1) {
	t.Helper()
	at := time.Now().UTC().Truncate(time.Microsecond)
	od, err := f.Operation.Digest()
	if err != nil || cap.Validate() != nil || cap.Reservation.OperationDigest != od || f.store.attemptOperationIssuer != f.OperationIssuer || at.Before(f.parent.graph.at) || at.Before(cap.IssuedAt) || !at.Before(cap.ExpiresAt) {
		t.Fatalf("original issuer consumption control is not valid/current: malformed=%v sameIssuer=%t insideWindow=%t", err, f.store.attemptOperationIssuer == f.OperationIssuer, !at.Before(cap.IssuedAt) && at.Before(cap.ExpiresAt))
	}
	f.parent.graph.at = at
}

func (f *LocalLoopbackConnectedFixture) AssertOriginalCapabilityConsumed(t *testing.T, cap ports.AttemptOperationCapabilityV1) {
	t.Helper()
	f.originalCapabilityWindow(t, cap)
	if err := f.OperationIssuer.ConsumeAttemptOperationCapability(cap.Clone()); err == nil {
		t.Fatal("successful original Store-authorized send left actual issuer capability unconsumed")
	}
}

// TEST-ONLY terminal burn of a denied fixture's capability. This never passes
// through a connector or sends bytes; success once then denial proves the
// actual original issuer had not consumed it on the failed current boundary.
func (f *LocalLoopbackConnectedFixture) AssertOriginalCapabilityUntouchedThenBurn(t *testing.T, cap ports.AttemptOperationCapabilityV1) {
	t.Helper()
	f.originalCapabilityWindow(t, cap)
	if err := f.OperationIssuer.ConsumeAttemptOperationCapability(cap.Clone()); err != nil {
		t.Fatalf("denied current boundary already consumed genuine original capability: %v", err)
	}
	if err := f.OperationIssuer.ConsumeAttemptOperationCapability(cap.Clone()); err == nil {
		t.Fatal("test-only terminal original capability burn was not one-use")
	}
}

// The same concrete retained Store, with the original independent actual.ctx
// (NOT Source.ExecutionLifetime or the connector's Source-linked child). This
// isolates the post-transaction Source custody recheck from ctx.Err denial.
func (f *LocalLoopbackConnectedFixture) AuthorizeOriginalStoreBeforeSend(t *testing.T, cap ports.AttemptOperationCapabilityV1) error {
	t.Helper()
	if f.Context.Err() != nil {
		t.Fatal("independent original Store caller already cancelled")
	}
	f.originalCapabilityWindow(t, cap)
	err := f.store.AuthorizeAttemptOperationBeforeSend(f.Context, cap.Clone())
	if f.Context.Err() != nil {
		t.Fatal("Source closure cancelled independent original Store caller")
	}
	return err
}
