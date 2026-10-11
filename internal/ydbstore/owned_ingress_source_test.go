package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Drives the actual canonical adapter/SDK callback, not native YDB isolation,
// source publication or authenticated funding. Each fixture has an explicit
// finite table inventory. A write is counted and stopped before any effect.
type ingressSourceSQLFixture struct {
	*ratedSQLFixture
	session domain.SessionID
	tenant  domain.TenantID
}

var errIngressSourceFirstWrite = errors.New("source fixture reached first SQL write")

func (f *ingressSourceSQLFixture) Connect(context.Context) (driver.Conn, error) { return f, nil }
func (f *ingressSourceSQLFixture) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	f.statements = append(f.statements, query)
	f.arguments = append(f.arguments, append([]driver.NamedValue(nil), args...))
	if _, ok := ctx.Deadline(); !ok {
		return nil, errors.New("source fixture requires caller deadline")
	}
	for table, row := range f.rows {
		if strings.Contains(query, "FROM "+table+" ") || strings.Contains(query, "FROM "+table+"\n") {
			if table == "sessions" || table == "session_participants" || table == "session_deletions" {
				if len(args) < 2 || args[0].Value != string(f.tenant) || args[1].Value != string(f.session) {
					return nil, errors.New("source fixture rejects foreign physical Session lookup")
				}
			}
			return &explanationFixtureRows{values: append([]driver.Value(nil), row...)}, nil
		}
	}
	return nil, errors.New("source fixture rejects query outside explicit inventory")
}
func (f *ingressSourceSQLFixture) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	f.statements = append(f.statements, query)
	f.arguments = append(f.arguments, append([]driver.NamedValue(nil), args...))
	f.writes++
	return nil, errIngressSourceFirstWrite
}

func ingressSourceFixture(t *testing.T, foreignCanonicalSession bool) (*Store, *ingressSourceSQLFixture, ports.CanonicalUserEventCommit, domain.OwnedAgentExecutionPackageV1) {
	t.Helper()
	r := newOwnedReaderFixture(t)
	pkg, err := domain.BuildOwnedAgentExecutionPackageV1(domain.AttemptExecutionSourceScopeV1{
		TenantID: r.scope.tenant, SessionID: r.scope.session, OwnerUserID: r.scope.owner, RunID: r.scope.run, AttemptID: r.scope.attempt,
	}, r.profile, r.spending, r.driver.at)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := domain.EncodeAttemptExecutionSourceV1(pkg.Source)
	if err != nil {
		t.Fatal(err)
	}
	at := pkg.Source.IssuedAt
	session := r.scope.session
	if foreignCanonicalSession {
		session = "actually-authorized-other-session"
	}
	f := &ingressSourceSQLFixture{ratedSQLFixture: &ratedSQLFixture{rows: make(map[string][]driver.Value), slots: make(map[uint32][]driver.Value)}, session: session, tenant: r.scope.tenant}
	put := func(table string, value any) { t.Helper(); ratedFixturePut(t, f.ratedSQLFixture, table, value) }
	put("tenant_memberships", domain.TenantMembership{TenantID: r.scope.tenant, UserID: r.scope.owner, Role: domain.TenantMembershipOwner, Status: domain.TenantMembershipActive, SecurityVersion: 1, CreatedAt: at, UpdatedAt: at})
	put("sessions", domain.Session{ID: session, TenantID: r.scope.tenant, CreatedBy: r.scope.owner, Status: domain.SessionActive, CreatedAt: at, UpdatedAt: at})
	put("session_participants", domain.SessionParticipant{TenantID: r.scope.tenant, SessionID: session, UserID: r.scope.owner, Role: domain.SessionParticipantOwner, Status: domain.SessionParticipantActive, CreatedAt: at, UpdatedAt: at})
	binding := domain.FrontendBinding{ID: "binding-189", TenantID: r.scope.tenant, Frontend: "synthetic", ExternalConversationID: "conversation-189", SessionID: session, Revision: 1, CreatedAt: at, UpdatedAt: at}
	put("frontend_bindings", binding)
	f.rows["frontend_ingress_idempotency"], f.rows["session_deletions"], f.rows["session_material_current_v1"] = nil, nil, nil
	request := ports.CanonicalUserEventCommit{
		TenantID: r.scope.tenant, UserID: r.scope.owner, SessionID: r.scope.session,
		BindingID: binding.ID, ExpectedBindingRevision: binding.Revision,
		Origin:         domain.FrontendEventOrigin{Frontend: binding.Frontend, ExternalConversationID: binding.ExternalConversationID, ExternalEventID: "delivery-189", BindingID: binding.ID, BindingRevision: binding.Revision},
		IdempotencyKey: "delivery-189", EventID: "event-189", RunID: r.scope.run, AttemptID: r.scope.attempt,
		SubscriptionConnectionID: "fixture-connection", ManifestID: "manifest-189", DispatchID: "dispatch-189",
		Payload:              domain.BlobRef{TenantID: r.scope.tenant, Key: domain.SessionEventObjectPrefix(r.scope.tenant, session, "event-189") + "uploads/source/message.json", Size: 1, SHA256: strings.Repeat("a", 64)},
		ExecutionPlacementV2: pkg.Placement, HarnessBinding: pkg.Binding, SubstrateBinding: &pkg.Substrate, AdmissionCostCeiling: &pkg.Cost,
		OwnedExecutionSourceRecord: string(raw), CommittedAt: at, ExpireAt: at.Add(time.Hour),
	}
	if err := validateCanonicalCommit(request); err != nil {
		t.Fatalf("valid separately scoped source request: %v", err)
	}
	db := sql.OpenDB(f)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return &Store{db: db}, f, request, pkg
}

func ingressSourceContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestOwnedIngressFencesActualCanonicalSessionBeforeWrites(t *testing.T) {
	t.Parallel()
	t.Run("same_session_reaches_real_first_write", func(t *testing.T) {
		store, f, request, _ := ingressSourceFixture(t, false)
		result, err := store.CommitCanonicalUserEvent(ingressSourceContext(t), request)
		if !errors.Is(err, errIngressSourceFirstWrite) || result.Created || f.writes != 1 || f.commits != 0 || f.rollbacks != 1 {
			t.Fatalf("valid original source must reach first write then rollback: err=%v result=%+v writes=%d commits=%d rollbacks=%d", err, result, f.writes, f.commits, f.rollbacks)
		}
	})
	t.Run("coherent_other_authorized_session_denied", func(t *testing.T) {
		store, f, request, _ := ingressSourceFixture(t, true)
		result, err := store.CommitCanonicalUserEvent(ingressSourceContext(t), request)
		if !errors.Is(err, domain.ErrMembershipDenied) || result.Created || f.writes != 0 || f.commits != 0 || f.rollbacks != 1 {
			t.Fatalf("source assertion must not replace actual authorized Session: err=%v result=%+v writes=%d commits=%d rollbacks=%d", err, result, f.writes, f.commits, f.rollbacks)
		}
		if len(f.statements) != 6 || len(f.isolations) != 1 || f.isolations[0] != driver.IsolationLevel(sql.LevelSerializable) {
			t.Fatalf("source fence escaped expected serializable pre-write reads: SQL=%d isolation=%v", len(f.statements), f.isolations)
		}
	})
}

func TestOwnedIngressConcurrentMissRecoveryPreservesExactSourceBytes(t *testing.T) {
	t.Parallel()
	for _, changed := range []bool{false, true} {
		name := "exact_original"
		if changed {
			name = "changed_valid_original_snapshot"
		}
		t.Run(name, func(t *testing.T) {
			store, f, request, pkg := ingressSourceFixture(t, false)
			f.rows["frontend_ingress_idempotency"] = []driver.Value{string(request.SessionID), int64(1), string(request.EventID), string(request.RunID), ""}
			ratedFixturePut(t, f.ratedSQLFixture, "session_events", domain.SessionEvent{ID: request.EventID, TenantID: request.TenantID, SessionID: request.SessionID, Sequence: 1, AuthorUserID: &request.UserID, RunID: &request.RunID, IdempotencyKey: request.IdempotencyKey})
			ratedFixturePut(t, f.ratedSQLFixture, "runs", domain.Run{ID: request.RunID, TenantID: request.TenantID, SessionID: request.SessionID, TriggerEventID: request.EventID})
			ratedFixturePut(t, f.ratedSQLFixture, "dispatch_outbox", domain.DispatchOutbox{ID: request.DispatchID, TenantID: request.TenantID, RunID: request.RunID, AttemptID: request.AttemptID,
				CredentialOwnerUserID: request.UserID, ExecutionPlacementV2: pkg.Placement, HarnessBinding: pkg.Binding, SubstrateBinding: &pkg.Substrate, AdmissionCostCeiling: &pkg.Cost, OwnedExecutionSourceRecord: request.OwnedExecutionSourceRecord})
			if changed {
				// Mutation correlation changes while all completed execution digests,
				// scope, generation, price, amount and original windows remain valid.
				pkg.Source.Spending[3].MutationID = "other-valid-source-mutation"
				raw, err := domain.EncodeAttemptExecutionSourceV1(pkg.Source)
				if err != nil {
					t.Fatal(err)
				}
				request.OwnedExecutionSourceRecord = string(raw)
				if err := validateCanonicalCommit(request); err != nil {
					t.Fatalf("counterexample must remain independently valid: %v", err)
				}
			}
			result, err := store.CommitCanonicalUserEvent(ingressSourceContext(t), request)
			if changed {
				if !errors.Is(err, domain.ErrEventIdempotencyConflict) || !reflect.DeepEqual(result, ports.CanonicalUserEventResult{}) || f.commits != 0 || f.rollbacks != 1 {
					t.Fatalf("changed source must not recover/rebind original Attempt: err=%v result=%+v commits=%d rollbacks=%d", err, result, f.commits, f.rollbacks)
				}
			} else if err != nil || result.Created || result.SessionID != request.SessionID || result.RunID != request.RunID || f.commits != 1 {
				t.Fatalf("exact historical source recovery: err=%v result=%+v commits=%d", err, result, f.commits)
			}
			if f.writes != 0 {
				t.Fatalf("historical recovery wrote %d SQL effects", f.writes)
			}
		})
	}
}

func TestOwnedDispatchSourceCannotBypassClosedFinalAdmission(t *testing.T) {
	t.Parallel()
	store, f, request, pkg := ingressSourceFixture(t, false)
	ratedFixturePut(t, f.ratedSQLFixture, "dispatch_outbox", domain.DispatchOutbox{ID: request.DispatchID, TenantID: request.TenantID, RunID: request.RunID, AttemptID: request.AttemptID,
		CredentialOwnerUserID: request.UserID, ExecutionPlacementV2: pkg.Placement, HarnessBinding: pkg.Binding, SubstrateBinding: &pkg.Substrate, AdmissionCostCeiling: &pkg.Cost, OwnedExecutionSourceRecord: request.OwnedExecutionSourceRecord})
	limits := domain.ProductLimits{MaxTenantQueueDepth: 8, MaxActiveRuns: 1, MaxRuntime: time.Minute, MaxTurns: 10, MaxInputBytes: 1 << 20, MaxContextBytes: 1 << 20, MaxContextEvents: 100, MaxArtifacts: 10, MaxToolEvents: 20, MaxToolEventBytes: 1 << 18}
	admit := ports.DispatchAdmissionRequest{TenantID: request.TenantID, OutboxID: request.DispatchID, RunID: request.RunID, AttemptID: request.AttemptID, ReservationID: "reservation-189", Now: request.CommittedAt, HoldUntil: request.CommittedAt.Add(time.Minute), Limits: limits, Workload: domain.WorkloadShape{Runtime: time.Second, Turns: 1}}
	if err := validateAdmissionRequest(admit); err != nil {
		t.Fatal(err)
	}
	result, err := store.AdmitDispatch(ingressSourceContext(t), admit)
	if !errors.Is(err, domain.ErrMembershipDenied) || result.Admitted || f.writes != 0 || f.commits != 0 || f.rollbacks != 1 || len(f.statements) != 1 {
		t.Fatalf("source package is not current/final admission: err=%v admitted=%v SQL=%d writes=%d commits=%d rollbacks=%d", err, result.Admitted, len(f.statements), f.writes, f.commits, f.rollbacks)
	}
	// Quoted String transport survives JSON payload storage without decoding
	// authority through float64, refreshing time or changing canonical bytes.
	encoded, err := json.Marshal(domain.DispatchOutbox{OwnedExecutionSourceRecord: request.OwnedExecutionSourceRecord})
	if err != nil {
		t.Fatal(err)
	}
	var roundtrip domain.DispatchOutbox
	if err := json.Unmarshal(encoded, &roundtrip); err != nil || roundtrip.OwnedExecutionSourceRecord != request.OwnedExecutionSourceRecord {
		t.Fatalf("source String transport changed exact bytes: %v", err)
	}
}
