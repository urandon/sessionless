package ydbstore

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
	"gitcode.com/urandon/sessionless/internal/sessionmaterial"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
)

// The original installed Source supplies all authority. This SQL peer supplies
// canonical DATA and exercises real adapter/SDK callbacks, not Web HTTP/auth,
// native isolation, model execution, or the remainder of issue #218.
type localCanonicalJoinSQL struct {
	*localCancellationSQL
	request ports.CanonicalUserEventCommit
	reads   map[string]struct {
		table string
		args  []driver.Value
	}
	canonical       bool
	participantRows [][]driver.Value
}

func (f *localCanonicalJoinSQL) Connect(context.Context) (driver.Conn, error) { return f, nil }

func canonicalJoinSQL(s string) string { return strings.Join(strings.Fields(s), " ") }

func (f *localCanonicalJoinSQL) QueryContext(ctx context.Context, s string, args []driver.NamedValue) (driver.Rows, error) {
	flat := canonicalJoinSQL(s)
	values := make([]driver.Value, len(args))
	for i := range args {
		values[i] = args[i].Value
	}
	if entry, ok := f.reads[flat]; ok {
		if !reflect.DeepEqual(values, entry.args) {
			return nil, fmt.Errorf("canonical join foreign key: %s", flat)
		}
		f.recordQuery(s, args)
		if entry.table == "active_session_participants" {
			rows := f.participantRows
			if rows == nil {
				rows = [][]driver.Value{f.rows[entry.table]}
			}
			return &localCanonicalJoinRows{rows: rows}, nil
		}
		return &explanationFixtureRows{values: append([]driver.Value(nil), f.rows[entry.table]...)}, nil
	}
	if s == ownedAdmissionPrincipalSQL {
		bucket, _ := ydbpartition.BucketV1(string(f.request.UserID))
		if !reflect.DeepEqual(values, []driver.Value{string(f.request.TenantID), string(f.request.SessionID), string(f.request.UserID), int64(bucket)}) {
			return nil, errors.New("canonical join foreign principal")
		}
		var session domain.Session
		if json.Unmarshal([]byte(f.rows["sessions"][0].(string)), &session) != nil {
			return nil, errors.New("canonical join missing original Session")
		}
		f.session = []driver.Value{f.rows["sessions"][0], string(session.CreatedBy), string(session.Status), int64(session.LastEventSequence), int64(0)}
	}
	if s == ownedAdmissionSourcesJoinSQL {
		want := append([]driver.Value(nil), f.reader.driver.profileArgs...)
		for _, key := range f.reader.driver.spendingArgs {
			want = append(want, key...)
		}
		if !reflect.DeepEqual(values, want) {
			return nil, errors.New("canonical join foreign original source key")
		}
	}
	if f.canonical {
		switch s {
		case `SELECT CurrentUtcTimestamp()`, ownedAdmissionPrincipalSQL, ownedAdmissionSourcesJoinSQL, ownedProfileCurrentSQL:
		default:
			return nil, fmt.Errorf("canonical join unregistered SQL: %s", flat)
		}
	}
	return f.localCancellationSQL.QueryContext(ctx, s, args)
}

type localCanonicalJoinRows struct{ rows [][]driver.Value }

func (*localCanonicalJoinRows) Columns() []string {
	return []string{"record", "tenant", "session", "user", "status"}
}
func (*localCanonicalJoinRows) Close() error { return nil }
func (r *localCanonicalJoinRows) Next(dest []driver.Value) error {
	if len(r.rows) == 0 {
		return io.EOF
	}
	copy(dest, r.rows[0])
	r.rows = r.rows[1:]
	return nil
}

// These are the exact existing canonical writer forms, not an arbitrary table
// or SQL fallback. The parent fixture supplies transactional rollback snapshots.
var localCanonicalJoinWrites = map[string]struct {
	table   string
	payload int
}{
	"INSERT INTO session_events (tenant_id, session_id, sequence, event_id, kind, author_user_id, run_id, idempotency_key, blob_key, created_at, record) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, CAST($11 AS JsonDocument))":        {"session_events", 10},
	"INSERT INTO session_event_idempotency (tenant_id, session_id, idempotency_key, sequence, event_id, created_at) VALUES ($1, $2, $3, $4, $5, $6)":                                                                                         {"", -1},
	"INSERT INTO session_material_boundaries_v1 (tenant_id,session_id,sequence,version,through_sequence,last_event_id,prefix_digest,record) VALUES ($1,$2,$3,$4,$5,$6,$7,CAST($8 AS String))":                                                {"", -1},
	"UPSERT INTO session_material_current_v1 (tenant_id,session_id,version,through_sequence,last_event_id,prefix_digest,record) VALUES ($1,$2,$3,$4,$5,$6,CAST($7 AS String))":                                                               {"", -1},
	"DELETE FROM session_activity WHERE tenant_id = $1 AND user_id = $2 AND status = $3 AND activity_bucket = $4 AND updated_at = $5 AND session_id = $6":                                                                                    {"", -1},
	"UPDATE sessions SET status = $1, last_event_sequence = $2, updated_at = $3, archived_at = $4, record = CAST($5 AS JsonDocument) WHERE tenant_id = $6 AND session_id = $7":                                                               {"sessions", 4},
	"UPSERT INTO session_activity (tenant_id, user_id, status, activity_bucket, updated_at, session_id) VALUES ($1, $2, $3, $4, $5, $6)":                                                                                                     {"", -1},
	"UPSERT INTO runs (tenant_id, run_id, session_id, trigger_event_id, subscription_connection_id, status, created_at, updated_at, payload) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CAST($9 AS JsonDocument))":                              {"runs", 8},
	"UPSERT INTO runs_by_session (tenant_id, session_id, created_at, run_id, trigger_event_id, status) VALUES ($1, $2, $3, $4, $5, $6)":                                                                                                      {"", -1},
	"UPSERT INTO run_idempotency (tenant_id, idempotency_key, run_id, created_at, expire_at) VALUES ($1, $2, $3, $4, $5)":                                                                                                                    {"", -1},
	"UPSERT INTO session_displays (tenant_id, session_id, updated_at, record) VALUES ($1, $2, $3, CAST($4 AS JsonDocument))":                                                                                                                 {"session_displays", 3},
	"UPSERT INTO attempts (tenant_id, attempt_id, run_id, attempt_number, status, worker_id, created_at, updated_at, retention_expire_at, payload) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, CAST($10 AS JsonDocument))":                   {"attempts", 9},
	"UPSERT INTO artifact_manifests (tenant_id, artifact_manifest_id, run_id, created_at, payload) VALUES ($1, $2, $3, $4, CAST($5 AS JsonDocument))":                                                                                        {"artifact_manifests", 4},
	"UPSERT INTO artifact_manifests_by_run (tenant_id, run_id, artifact_manifest_id) VALUES ($1, $2, $3)":                                                                                                                                    {"", -1},
	"UPSERT INTO run_explanation_heads_v1 (tenant_id,run_id,revision,record) VALUES ($1,$2,$3,CAST($4 AS JsonDocument))":                                                                                                                     {"run_explanation_heads_v1", 3},
	"DELETE FROM dispatch_ready_v2 WHERE shard_bucket = $1 AND available_at = $2 AND tenant_id = $3 AND dispatch_outbox_id = $4":                                                                                                             {"", -1},
	"UPSERT INTO dispatch_outbox (tenant_id, dispatch_outbox_id, run_id, attempt_id, status, created_at, updated_at, retention_expire_at, payload) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CAST($9 AS JsonDocument))":                        {"dispatch_outbox", 8},
	"UPSERT INTO dispatch_ready_v2 (shard_bucket, available_at, tenant_id, dispatch_outbox_id, run_id, attempt_id) VALUES ($1, $2, $3, $4, $5, $6)":                                                                                          {"", -1},
	"INSERT INTO frontend_ingress_idempotency (tenant_id, binding_id, idempotency_key, session_id, sequence, event_id, run_id, origin_digest, mutation_digest, created_at, expire_at) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)": {"", -1},
}

func (f *localCanonicalJoinSQL) ExecContext(ctx context.Context, s string, args []driver.NamedValue) (driver.Result, error) {
	if !f.canonical {
		return f.localCancellationSQL.ExecContext(ctx, s, args)
	}
	entry, ok := localCanonicalJoinWrites[canonicalJoinSQL(s)]
	if !ok {
		return nil, fmt.Errorf("canonical join unexpected write: %s", canonicalJoinSQL(s))
	}
	if entry.payload >= len(args) {
		return nil, errors.New("canonical join missing payload")
	}
	result, err := f.managedAdmissionGraphFixture.ExecContext(ctx, s, args)
	if err != nil {
		return nil, err
	}
	if entry.table != "" {
		f.rows[entry.table] = []driver.Value{args[entry.payload].Value}
	}
	if strings.HasPrefix(s, "UPSERT INTO run_idempotency") {
		f.rows["run_idempotency"] = []driver.Value{args[2].Value}
	}
	if strings.HasPrefix(s, "INSERT INTO frontend_ingress_idempotency") {
		f.rows["frontend_ingress_idempotency"] = []driver.Value{args[3].Value, args[4].Value, args[5].Value, args[6].Value, args[8].Value}
	}
	if strings.HasPrefix(s, "UPSERT INTO session_material_current_v1") {
		f.rows["session_material_current_v1"] = []driver.Value{args[6].Value, args[2].Value, args[3].Value, args[0].Value, args[1].Value, args[4].Value, args[5].Value}
	}
	if strings.HasPrefix(s, "INSERT INTO session_material_boundaries_v1") {
		f.rows["session_material_boundaries_v1"] = []driver.Value{args[7].Value, args[3].Value, args[4].Value, args[0].Value, args[1].Value, args[5].Value, args[6].Value}
	}
	return result, nil
}

func newLocalCanonicalJoin(t *testing.T) (*Store, *localCanonicalJoinSQL, *originalLocalRecoveryFixture, ports.CanonicalUserEventCommit) {
	t.Helper()
	original := newOriginalLocalRecoveryFixture(t, 4)
	binder, err := NewLocalHarnessBinder(original.store, original.source, original.receipt)
	if err != nil {
		t.Fatal("same-Source binder constructor", err)
	}
	authority, err := binder.BindHarness(t.Context(), localBinderRequest(original.scope, original.sql.at))
	if err != nil {
		t.Fatal("same-Source actual binder", err)
	}
	source, err := domain.DecodeAttemptExecutionSourceV1([]byte(authority.OwnedExecutionSourceRecord))
	if err != nil || original.source.VerifyOwnedAttemptEffectSource(source) != nil {
		t.Fatal("binder original authenticated Source", err)
	}
	at, scope := source.IssuedAt, source.Scope
	rated := &ratedSQLFixture{rows: make(map[string][]driver.Value), slots: make(map[uint32][]driver.Value)}
	graph := &managedAdmissionGraphFixture{canonicalBudgetFixture: &canonicalBudgetFixture{ratedSQLFixture: rated, at: at}}
	reader := &ownedReaderFixture{driver: &ownedCurrentSQLFixture{ratedSQLFixture: &ratedSQLFixture{}, at: at}, profile: original.receipt.Profile.Clone(), spending: source.Spending, ref: source.Profile}
	for i, v := range reader.spending {
		reader.refs[i].locator = v.Locator
	}
	reader.sync(t)
	script := &ownedFinalScript{managedAdmissionGraphFixture: graph, reader: reader}
	f := &localCanonicalJoinSQL{localCancellationSQL: &localCancellationSQL{ownedFinalScript: script}, canonical: true}
	session := domain.Session{ID: scope.SessionID, TenantID: scope.TenantID, CreatedBy: scope.OwnerUserID, Status: domain.SessionActive, CreatedAt: at, UpdatedAt: at}
	member := domain.TenantMembership{TenantID: scope.TenantID, UserID: scope.OwnerUserID, Role: domain.TenantMembershipOwner, Status: domain.TenantMembershipActive, SecurityVersion: 1, CreatedAt: at, UpdatedAt: at}
	participant := domain.SessionParticipant{TenantID: scope.TenantID, SessionID: scope.SessionID, UserID: scope.OwnerUserID, Role: domain.SessionParticipantOwner, Status: domain.SessionParticipantActive, CreatedAt: at, UpdatedAt: at}
	binding := domain.FrontendBinding{ID: "binding-local-canonical", TenantID: scope.TenantID, Frontend: domain.FrontendWeb, ExternalConversationID: "conversation-local-canonical", SessionID: scope.SessionID, Revision: 1, CreatedAt: at, UpdatedAt: at}
	for table, value := range map[string]any{"sessions": session, "tenant_memberships": member, "session_participants": participant, "frontend_bindings": binding} {
		ratedFixturePut(t, rated, table, value)
	}
	script.membership = []driver.Value{rated.rows["tenant_memberships"][0], string(member.Role), string(member.Status), int64(member.SecurityVersion)}
	script.participant = []driver.Value{rated.rows["session_participants"][0], string(participant.Role), string(participant.Status), int64(0)}
	rated.rows["active_session_participants"] = []driver.Value{rated.rows["session_participants"][0], string(participant.TenantID), string(participant.SessionID), string(participant.UserID), string(participant.Status)}
	genesis, err := sessioncontext.NewSessionMaterialGenesisV1(scope.TenantID, scope.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	rawGenesis, err := sessioncontext.EncodeSessionMaterialPrefixV1(genesis)
	if err != nil {
		t.Fatal(err)
	}
	rated.rows["session_material_current_v1"] = []driver.Value{string(rawGenesis), int64(1), int64(0), string(scope.TenantID), string(scope.SessionID), "", genesis.PrefixDigest}
	origin := domain.FrontendEventOrigin{BindingID: binding.ID, BindingRevision: 1, Frontend: domain.FrontendWeb, ExternalConversationID: binding.ExternalConversationID, ExternalEventID: "delivery-local-canonical"}
	payload, err := json.Marshal(struct {
		Version uint32                     `json:"version"`
		Origin  domain.FrontendEventOrigin `json:"origin"`
		Text    string                     `json:"text"`
	}{1, origin, "public canonical local text"})
	if err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(payload)
	eventID := domain.SessionEventID("event-local-canonical")
	ref := domain.BlobRef{TenantID: scope.TenantID, Key: domain.SessionEventObjectPrefix(scope.TenantID, scope.SessionID, eventID) + "uploads/original/message.json", Size: int64(len(payload)), SHA256: hex.EncodeToString(hash[:])}
	artifacts := []domain.Artifact{{Name: "message.json", MediaType: "application/json", Blob: ref}}
	prepared, err := sessionmaterial.PrepareV1(domain.SessionEvent{ID: eventID, TenantID: scope.TenantID, SessionID: scope.SessionID, Kind: domain.SessionEventUserMessage, AuthorUserID: &scope.OwnerUserID, RunID: &scope.RunID, IdempotencyKey: "delivery-local-canonical", Payload: ref}, payload, nil)
	if err != nil {
		t.Fatal("actual prepared canonical material", err)
	}
	t.Cleanup(prepared.Release)
	request := ports.CanonicalUserEventCommit{TenantID: scope.TenantID, UserID: scope.OwnerUserID, SessionID: scope.SessionID, BindingID: binding.ID, ExpectedBindingRevision: 1, Origin: origin, IdempotencyKey: "delivery-local-canonical", EventID: eventID, RunID: scope.RunID, AttemptID: scope.AttemptID, ManifestID: "manifest-local-canonical", DispatchID: "dispatch-local-canonical", Payload: ref, PreparedMaterial: prepared, Artifacts: artifacts, DisplayText: "public canonical local text", LocalCapacityAssociation: authority.Clone().LocalCapacityAssociation, ExecutionPlacementV2: authority.ExecutionPlacementV2, HarnessBinding: authority.HarnessBinding.Clone(), SubstrateBinding: authority.Clone().SubstrateBinding, AdmissionCostCeiling: authority.Clone().AdmissionCostCeiling, OwnedExecutionSourceRecord: authority.OwnedExecutionSourceRecord, CommittedAt: at, ExpireAt: at.Add(time.Hour)}
	f.request = request
	f.reads = make(map[string]struct {
		table string
		args  []driver.Value
	})
	add := func(s, table string, args ...driver.Value) {
		f.reads[canonicalJoinSQL(s)] = struct {
			table string
			args  []driver.Value
		}{table, args}
	}
	tenant, sessionID, user := string(scope.TenantID), string(scope.SessionID), string(scope.OwnerUserID)
	bucket, _ := ydbpartition.BucketV1(user)
	for statement := range ownedCanonicalJSONReads {
		bounded, _, _ := ownedCanonicalJSONSelect(statement)
		switch strings.Fields(statement)[3] {
		case "sessions", "session_deletions":
			add(bounded, strings.Fields(statement)[3], tenant, sessionID)
		case "session_participants":
			add(bounded, "session_participants", tenant, sessionID, user)
		case "tenant_memberships":
			add(bounded, "tenant_memberships", int64(bucket), user, tenant)
		case "runs":
			add(bounded, "runs", tenant, string(scope.RunID))
		case "attempts":
			add(bounded, "attempts", tenant, string(scope.AttemptID))
		case "artifact_manifests":
			add(bounded, "artifact_manifests", tenant, string(request.ManifestID))
		case "worker_jobs":
			add(bounded, "worker_jobs", tenant, string(scope.RunID))
		case "frontend_bindings":
			add(bounded, "frontend_bindings", tenant, string(binding.ID))
		case "session_displays":
			add(bounded, "session_displays", tenant, sessionID)
		case "session_events":
			add(bounded, "session_events", tenant, sessionID, int64(1))
		case "dispatch_outbox":
			add(bounded, "dispatch_outbox", tenant, string(request.DispatchID))
		}
	}
	add("SELECT SUBSTRING(CAST(record AS String),0,32769) FROM frontend_bindings WHERE tenant_id = $1 AND binding_id = $2", "frontend_bindings", tenant, string(binding.ID))
	add("SELECT SUBSTRING(CAST(record AS String),0,32769) FROM session_displays WHERE tenant_id = $1 AND session_id = $2", "session_displays", tenant, sessionID)
	add("SELECT SUBSTRING(CAST(record AS String),0,32769) FROM session_events WHERE tenant_id = $1 AND session_id = $2 AND sequence = $3", "session_events", tenant, sessionID, int64(1))
	add("SELECT SUBSTRING(CAST(payload AS String),0,131073) FROM dispatch_outbox WHERE tenant_id=$1 AND dispatch_outbox_id=$2", "dispatch_outbox", tenant, string(request.DispatchID))
	add("SELECT session_id, sequence, event_id, run_id, mutation_digest FROM frontend_ingress_idempotency WHERE tenant_id = $1 AND binding_id = $2 AND idempotency_key = $3 AND expire_at > CurrentUtcTimestamp()", "frontend_ingress_idempotency", tenant, string(binding.ID), string(request.IdempotencyKey))
	add(ownedCanonicalIngressIdempotencySQL, "frontend_ingress_idempotency", tenant, string(binding.ID), string(request.IdempotencyKey))
	add(sessionMaterialCurrentSQL, "session_material_current_v1", tenant, sessionID)
	add(sessionMaterialBoundarySQL, "session_material_boundaries_v1", tenant, sessionID, int64(0))
	add("SELECT record FROM session_participants WHERE tenant_id = $1 AND session_id = $2 AND status = $3 LIMIT $4", "session_participants", tenant, sessionID, string(domain.SessionParticipantActive), int64(maxSessionDeletionRows+1))
	add(ownedActiveSessionParticipantsSQL, "active_session_participants", tenant, sessionID, string(domain.SessionParticipantActive), int64(maxSessionDeletionRows+1))
	add("SELECT SUBSTRING(CAST(run_id AS String),0,161) FROM run_idempotency WHERE tenant_id = $1 AND idempotency_key = $2", "run_idempotency", tenant, string(request.IdempotencyKey))
	add("SELECT SUBSTRING(CAST(record AS String), 0, 8193) FROM run_explanation_heads_v1 WHERE tenant_id=$1 AND run_id=$2", "run_explanation_heads_v1", tenant, string(scope.RunID))
	add("SELECT payload FROM runs WHERE tenant_id = $1 AND run_id = $2", "runs", tenant, string(scope.RunID))
	add("SELECT payload FROM worker_jobs WHERE tenant_id = $1 AND run_id = $2", "worker_jobs", tenant, string(scope.RunID))
	add("SELECT payload FROM attempts WHERE tenant_id = $1 AND attempt_id = $2", "attempts", tenant, string(scope.AttemptID))
	add("SELECT payload FROM artifact_manifests WHERE tenant_id = $1 AND artifact_manifest_id = $2", "artifact_manifests", tenant, string(request.ManifestID))
	add("SELECT payload FROM checkpoints WHERE tenant_id = $1 AND attempt_id = $2 ORDER BY sequence DESC LIMIT 1", "checkpoints", tenant, string(scope.AttemptID))
	db := sql.OpenDB(f)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	store, err := New(db, Options{OwnedProfilePublicationVerifier: original.source, AttemptSpendingPublicationVerifier: original.source, OwnedAttemptEffectSourceVerifier: original.source})
	if err != nil {
		t.Fatal(err)
	}
	return store, f, original, request
}

func TestLocalCanonicalCommitThenActualAdmission(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, f, _, request := newLocalCanonicalJoin(t)
		for _, table := range []string{"runs", "attempts", "dispatch_outbox", "worker_jobs", "quota_reservations"} {
			if len(f.rows[table]) != 0 {
				t.Fatalf("preseeded request authority: %s", table)
			}
		}
		result, err := store.CommitCanonicalUserEvent(t.Context(), request)
		if err != nil || !result.Created || result.Sequence != 1 || result.RunID != request.RunID {
			t.Fatalf("actual canonical commit: result=%+v err=%v statements=%d writes=%d trace=%q", result, err, len(f.statements), f.writes, f.statements)
		}
		canonicalStatements := len(f.statements)
		if canonicalStatements > 32 {
			t.Fatalf("canonical statements=%d >32", canonicalStatements)
		}
		prefix, err := sessioncontext.DecodeSessionMaterialPrefixV1([]byte(f.rows["session_material_current_v1"][0].(string)))
		if err != nil || prefix.ThroughSequence != 1 || prefix.LastEventID != request.EventID || prefix.Coverage != sessioncontext.SessionMaterialCompleteV1 || prefix.VerifiedPayloadBytes != uint64(request.Payload.Size) || !reflect.DeepEqual(f.rows["session_material_current_v1"], f.rows["session_material_boundaries_v1"]) {
			t.Fatalf("actual prepared canonical material fold: prefix=%+v err=%v", prefix, err)
		}
		f.canonical = false
		admission := ports.DispatchAdmissionRequest{TenantID: request.TenantID, RunID: request.RunID, AttemptID: request.AttemptID, OutboxID: request.DispatchID, ReservationID: "reservation-local-canonical", Now: f.at, HoldUntil: f.at.Add(30 * time.Second), Limits: domain.ProductLimits{MaxTenantQueueDepth: 8, MaxActiveRuns: 1, MaxRuntime: time.Minute, MaxTurns: 8, MaxInputBytes: 1 << 20, MaxContextBytes: 1 << 20, MaxArtifacts: 8, MaxContextEvents: 100, MaxToolEvents: 20, MaxToolEventBytes: 1 << 18}, Workload: domain.WorkloadShape{Runtime: time.Second, Turns: 1}}
		admitted, err := store.AdmitDispatch(t.Context(), admission)
		if err != nil || !admitted.Admitted || admitted.Delivery != ports.DispatchDeliveryManagedQueue {
			t.Fatalf("canonical-prefix actual admission: result=%+v err=%v statements=%d", admitted, err, len(f.statements)-canonicalStatements)
		}
		if len(f.statements)-canonicalStatements > 32 {
			t.Fatalf("admission statements=%d >32", len(f.statements)-canonicalStatements)
		}
		var run domain.Run
		var job domain.WorkerJob
		var reservation domain.QuotaReservation
		var outbox domain.DispatchOutbox
		for table, target := range map[string]any{"runs": &run, "worker_jobs": &job, "quota_reservations": &reservation, "dispatch_outbox": &outbox} {
			if len(f.rows[table]) == 0 || json.Unmarshal([]byte(f.rows[table][0].(string)), target) != nil {
				t.Fatalf("committed graph missing %s", table)
			}
		}
		var final domain.AttemptExecutionAdmissionV1
		if len(f.final) == 0 || json.Unmarshal([]byte(f.final[0].(string)), &final) != nil || !ownedAdmissionMatches(&final, final.Source, request.OwnedExecutionSourceRecord, reservation, job) || job.ValidateForRun(run) != nil || reservation.ValidateForRun(run) != nil || job.OwnedExecutionSourceRecord != request.OwnedExecutionSourceRecord || outbox.OwnedExecutionSourceRecord != request.OwnedExecutionSourceRecord || run.SubscriptionConnectionID != "" || reservation.SubscriptionConnectionID != "" || !equalLocalCapacityV1(run.LocalCapacityAssociation, request.LocalCapacityAssociation) || outbox.Status != domain.DispatchPending {
			t.Fatalf("actual admitted graph lost original local Source/hold/selected arm: final=%t jobValid=%t quotaValid=%t outboxStatus=%s", len(f.final) != 0, job.ValidateForRun(run) == nil, reservation.ValidateForRun(run) == nil, outbox.Status)
		}
		admissionStatements := len(f.statements) - canonicalStatements
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		loaded, found, err := store.LoadWorkerJob(ctx, request.TenantID, request.RunID)
		if err != nil || !found || !reflect.DeepEqual(loaded.Run, run) || !reflect.DeepEqual(loaded.Job, job) || !reflect.DeepEqual(loaded.Reservation, reservation) || loaded.InputManifest.ID != request.ManifestID || loaded.Checkpoint != nil {
			t.Fatalf("actual loaded-state after canonical admission: found=%t err=%v", found, err)
		}
		t.Logf("actual canonical=%d admission=%d load=%d statements; original limits unchanged", canonicalStatements, admissionStatements, len(f.statements)-canonicalStatements-admissionStatements)
	})
}

func TestLocalCanonicalCommitThreeCallbacksRetainOriginalBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, f, _, request := newLocalCanonicalJoin(t)
		f.commitModes = []string{"rollback", "rollback"}
		ctx, cancel := ownedAuthorityCallContext(t.Context())
		defer cancel()
		result, err := store.CommitCanonicalUserEvent(ctx, request)
		budget := ctx.Value(ownedAuthorityCallContextKey{}).(*ownedAuthorityBudget)
		if err != nil || !result.Created || f.commits != 3 || budget.callbacks != 3 || budget.statements != 96 || budget.callbackStatements != 32 || budget.bytes > ownedAuthorityMaxBytes || budget.failed || len(f.statements) != 96 {
			t.Fatalf("three actual canonical callbacks: created=%t err=%v commits=%d callbacks=%d statements=%d last=%d bytes=%d failed=%t", result.Created, err, f.commits, budget.callbacks, budget.statements, budget.callbackStatements, budget.bytes, budget.failed)
		}
		for _, query := range []string{ownedAdmissionPrincipalSQL, ownedAdmissionSourcesJoinSQL} {
			count := 0
			for _, statement := range f.statements {
				if statement == query {
					count++
				}
			}
			if count != 3 {
				t.Fatalf("fresh current join each callback: got=%d want=3 query=%s", count, query)
			}
		}
		if len(f.rows["session_events"]) != 1 || len(f.rows["frontend_ingress_idempotency"]) != 5 || len(f.rows["worker_jobs"]) != 0 || len(f.final) != 0 {
			t.Fatal("canonical callback manufactured admission or lost committed event/index")
		}
		t.Logf("canonical retries: %d callbacks, %d statements, %d metadata bytes", budget.callbacks, budget.statements, budget.bytes)
	})
}

func TestLocalCanonicalCommitExactSecondDeliveryIsReadOnly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, f, original, request := newLocalCanonicalJoin(t)
		first, err := store.CommitCanonicalUserEvent(t.Context(), request)
		if err != nil || !first.Created {
			t.Fatalf("original canonical commit: result=%+v err=%v", first, err)
		}
		before := cloneLocalInstallationRows(f.rows)
		heads, audits := cloneLocalInstallationRows(original.sql.fundingHeads), cloneLocalInstallationRows(original.sql.fundingAudits)
		writes, statements := f.writes, len(f.statements)
		replay := request
		replay.PreparedMaterial = nil
		replay.DisplayText = "redelivery must not replace the canonical text"
		replay.CommittedAt = request.CommittedAt.Add(-time.Second)
		second, err := store.CommitCanonicalUserEvent(t.Context(), replay)
		want := first
		want.Created = false
		if err != nil || second != want || f.writes != writes || !reflect.DeepEqual(before, f.rows) || !reflect.DeepEqual(heads, original.sql.fundingHeads) || !reflect.DeepEqual(audits, original.sql.fundingAudits) {
			t.Fatalf("same original Source canonical redelivery: result=%+v want=%+v err=%v newWrites=%d newStatements=%d recordsEqual=%t", second, want, err, f.writes-writes, len(f.statements)-statements, reflect.DeepEqual(before, f.rows))
		}
		t.Logf("original canonical redelivery: %d read statements, zero writes; event/Source/time/material unchanged", len(f.statements)-statements)
	})
}

func TestLocalCanonicalCommitCommittedLostACKReturnsOriginalReadback(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, f, _, request := newLocalCanonicalJoin(t)
		f.commitModes = []string{"ambiguous"}
		var committed map[string][]driver.Value
		var writes, statements int
		f.afterCommit = func() {
			if committed == nil {
				committed = cloneLocalInstallationRows(f.rows)
				writes, statements = f.writes, len(f.statements)
			}
		}
		ctx, cancel := ownedAuthorityCallContext(t.Context())
		defer cancel()
		result, err := store.CommitCanonicalUserEvent(ctx, request)
		budget := ctx.Value(ownedAuthorityCallContextKey{}).(*ownedAuthorityBudget)
		if err != nil || result.Created || result.SessionID != request.SessionID || result.EventID != request.EventID || result.RunID != request.RunID || result.Sequence != 1 || budget.callbacks != 2 || budget.statements > 96 || statements != 32 || f.writes != writes || !reflect.DeepEqual(committed, f.rows) {
			t.Fatalf("committed/lost-ACK canonical recovery: result=%+v err=%v callbacks=%d statements=%d first=%d newWrites=%d recordsEqual=%t", result, err, budget.callbacks, budget.statements, statements, f.writes-writes, reflect.DeepEqual(committed, f.rows))
		}
		t.Logf("committed/lost-ACK canonical recovery: %d callbacks/%d statements; zero retry writes, event/Source/time/material unchanged", budget.callbacks, budget.statements)
	})
}

func TestLocalCanonicalCommitFreshCallbackObservesWithdrawal(t *testing.T) {
	for _, fault := range []string{"original Source closed", "canonical membership revoked", "current profile revoked"} {
		t.Run(fault, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				store, f, original, request := newLocalCanonicalJoin(t)
				f.commitModes = []string{"rollback"}
				changed := false
				f.afterCommit = func() {
					if changed {
						return
					}
					changed = true
					switch fault {
					case "original Source closed":
						if err := original.source.Close(); err != nil {
							t.Fatal(err)
						}
					case "canonical membership revoked":
						var member domain.TenantMembership
						if json.Unmarshal([]byte(f.rows["tenant_memberships"][0].(string)), &member) != nil {
							t.Fatal("withdrawal original member")
						}
						member.Status = domain.TenantMembershipRevoked
						ratedFixturePut(t, f.ratedSQLFixture, "tenant_memberships", member)
						f.membership[0], f.membership[2] = f.rows["tenant_memberships"][0], string(member.Status)
					case "current profile revoked":
						f.reader.profile.State = domain.OwnedAgentCurrentRevokedV1
						f.reader.sync(t)
					}
				}
				ctx, cancel := ownedAuthorityCallContext(t.Context())
				defer cancel()
				result, err := store.CommitCanonicalUserEvent(ctx, request)
				budget := ctx.Value(ownedAuthorityCallContextKey{}).(*ownedAuthorityBudget)
				if !changed || !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(result, ports.CanonicalUserEventResult{}) || budget.callbacks != 2 || budget.statements > ownedAuthorityMaxTotalStatements || f.persistedWrites != 0 {
					t.Fatalf("fresh callback ignored withdrawal: fault=%s changed=%t err=%v callbacks=%d statements=%d committedWrites=%d", fault, changed, err, budget.callbacks, budget.statements, f.persistedWrites)
				}
				for _, table := range []string{"runs", "attempts", "dispatch_outbox", "worker_jobs", "quota_reservations", "session_events", "frontend_ingress_idempotency"} {
					if len(f.rows[table]) != 0 {
						t.Fatalf("withdrawn canonical callback persisted %s", table)
					}
				}
			})
		})
	}
}

func TestLocalCanonicalCurrentProfileCacheInvalidatesBeforeFailedWrite(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, f, _, request := newLocalCanonicalJoin(t)
		source, err := domain.DecodeAttemptExecutionSourceV1([]byte(request.OwnedExecutionSourceRecord))
		if err != nil {
			t.Fatal(err)
		}
		err = store.ownedAuthorityTx(t.Context(), func(ctx context.Context, q *ownedAuthorityQuery) error {
			q.metadataCache = make(map[string]ownedMetadataCacheEntry)
			if _, err := readOwnedAdmissionSources(ctx, q, source); err != nil {
				return err
			}
			beforeStatements, beforeBytes := q.budget.statements, q.budget.bytes
			if _, err := readOwnedProfileCurrent(ctx, q, source.Profile); err != nil {
				return err
			}
			if q.budget.statements != beforeStatements || q.budget.bytes != beforeBytes {
				return errors.New("already charged exact profile reread SQL or double debit")
			}
			// The peer rejects this non-inventory write. Production invalidation
			// must precede dispatch even when no write succeeds.
			if _, err := q.ExecContext(ctx, "UPDATE owned_agent_profile_current_v1 SET state=$1 WHERE tenant_id=$2", "revoked", request.TenantID); err == nil {
				return errors.New("unexpected fixture write accepted")
			}
			f.reader.profile.State = domain.OwnedAgentCurrentRevokedV1
			f.reader.sync(t)
			if _, err := readOwnedProfileCurrent(ctx, q, source.Profile); !errors.Is(err, domain.ErrMembershipDenied) {
				return errors.New("failed write retained stale current profile")
			}
			if q.budget.statements != beforeStatements+2 {
				return fmt.Errorf("invalidated profile SQL statements=%d want=%d", q.budget.statements, beforeStatements+2)
			}
			return domain.ErrMembershipDenied // diagnostic transaction never commits
		})
		if !errors.Is(err, domain.ErrMembershipDenied) || f.commits != 0 || f.persistedWrites != 0 {
			t.Fatalf("profile cache invalidation transaction: err=%v commits=%d", err, f.commits)
		}
	})
}

func TestLocalCanonicalPrincipalCacheInvalidatesBeforeFailedWrite(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		store, f, _, request := newLocalCanonicalJoin(t)
		err := store.ownedAuthorityTx(t.Context(), func(ctx context.Context, q *ownedAuthorityQuery) error {
			q.metadataCache = make(map[string]ownedMetadataCacheEntry)
			if _, _, err := readOwnedSessionPrincipal(ctx, q, request.TenantID, request.SessionID, request.UserID); err != nil {
				return err
			}
			beforeStatements, beforeBytes := q.budget.statements, q.budget.bytes
			if err := authorizeTenantWriteTx(ctx, q.canonicalState(store, request.TenantID), request.UserID); err != nil {
				return err
			}
			if q.budget.statements != beforeStatements || q.budget.bytes != beforeBytes {
				return errors.New("already charged exact membership reread SQL or double debit")
			}
			if _, err := q.ExecContext(ctx, "UPDATE tenant_memberships SET status=$1 WHERE tenant_id=$2", "revoked", request.TenantID); err == nil {
				return errors.New("unexpected fixture membership write accepted")
			}
			var member domain.TenantMembership
			if json.Unmarshal([]byte(f.rows["tenant_memberships"][0].(string)), &member) != nil {
				return errors.New("original canonical member missing")
			}
			member.Status = domain.TenantMembershipRevoked
			ratedFixturePut(t, f.ratedSQLFixture, "tenant_memberships", member)
			if err := authorizeTenantWriteTx(ctx, q.canonicalState(store, request.TenantID), request.UserID); !errors.Is(err, domain.ErrMembershipDenied) {
				return errors.New("failed write retained stale canonical membership")
			}
			if q.budget.statements != beforeStatements+2 {
				return fmt.Errorf("invalidated membership SQL statements=%d want=%d", q.budget.statements, beforeStatements+2)
			}
			return domain.ErrMembershipDenied
		})
		if !errors.Is(err, domain.ErrMembershipDenied) || f.commits != 0 || f.persistedWrites != 0 {
			t.Fatalf("principal cache invalidation transaction: err=%v commits=%d", err, f.commits)
		}
	})
}

func TestLocalCanonicalOwnedProjectionsChargeBeforeParsing(t *testing.T) {
	for _, reader := range []string{"ingress idempotency", "material current", "material boundary", "active participant"} {
		for _, fault := range []string{"valid", "cap plus one", "invalid first scalar", "original byte budget exhausted"} {
			t.Run(reader+"/"+fault, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					store, f, _, request := newLocalCanonicalJoin(t)
					table := "session_material_current_v1"
					switch reader {
					case "ingress idempotency":
						table = "frontend_ingress_idempotency"
						f.rows[table] = []driver.Value{string(request.SessionID), int64(1), string(request.EventID), string(request.RunID), strings.Repeat("a", 64)}
					case "material boundary":
						table = "session_material_boundaries_v1"
						f.rows[table] = append([]driver.Value(nil), f.rows["session_material_current_v1"]...)
					case "active participant":
						table = "active_session_participants"
					}
					if fault == "cap plus one" {
						cap := 8193
						if reader == "ingress idempotency" {
							cap = 161
						}
						f.rows[table][0] = strings.Repeat("x", cap)
					}
					if fault == "invalid first scalar" {
						f.rows[table][0] = true
					}
					wantBytes := uint64(0)
					for _, cell := range f.rows[table] {
						switch cell := cell.(type) {
						case nil:
						case string:
							wantBytes += uint64(len(cell))
						default:
							wantBytes += 8
						}
					}
					observed := false
					err := store.ownedAuthorityTx(t.Context(), func(ctx context.Context, q *ownedAuthorityQuery) error {
						if fault == "original byte budget exhausted" {
							if err := q.budget.materialized(q.callback, ownedAuthorityMaxBytes-q.budget.bytes-1); err != nil {
								return err
							}
						}
						before := q.budget.bytes
						state := q.canonicalState(store, request.TenantID)
						var readErr error
						switch reader {
						case "ingress idempotency":
							var session, event, run string
							var sequence uint64
							var mutation sql.NullString
							readErr = scanCanonicalIngressIdempotencyTx(ctx, state, request.BindingID, request.IdempotencyKey, &session, &sequence, &event, &run, &mutation)
						case "material current":
							_, _, readErr = readSessionMaterialPrefixV1(ctx, q, sessionMaterialCurrentSQL, request.TenantID, request.SessionID, 0, nil)
						case "material boundary":
							_, _, readErr = readSessionMaterialPrefixV1(ctx, q, sessionMaterialBoundarySQL, request.TenantID, request.SessionID, 0, nil)
						case "active participant":
							_, readErr = activeSessionParticipants(ctx, state, request.SessionID, maxSessionDeletionRows)
						}
						observed = true
						if fault == "valid" {
							if readErr != nil || q.budget.failed {
								return fmt.Errorf("valid bounded %s read: %w", reader, readErr)
							}
						} else if readErr == nil || !q.budget.failed {
							return fmt.Errorf("invalid bounded %s read accepted: fault=%s error=%v sticky=%t", reader, fault, readErr, q.budget.failed)
						}
						if fault != "original byte budget exhausted" && q.budget.bytes != before+wantBytes {
							return fmt.Errorf("%s not all cells charged before parsing: got=%d want=%d", reader, q.budget.bytes-before, wantBytes)
						}
						if q.budget.statements != 2 {
							return fmt.Errorf("%s expanded/reset original SQL budget: statements=%d", reader, q.budget.statements)
						}
						return readErr
					})
					if !observed || (fault == "valid") != (err == nil) || fault != "valid" && (f.commits != 0 || f.persistedWrites != 0) {
						t.Fatalf("bounded projection transaction: reader=%s fault=%s observed=%t err=%v commits=%d", reader, fault, observed, err, f.commits)
					}
				})
			})
		}
	}
}

func TestLocalCanonicalOwnedParticipantPhysicalKeysAndRowOverflow(t *testing.T) {
	for _, fault := range []string{"foreign tenant", "foreign session", "mismatched user", "wrong status", "row limit plus one"} {
		t.Run(fault, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				store, f, _, request := newLocalCanonicalJoin(t)
				row := f.rows["active_session_participants"]
				maxRows := maxSessionDeletionRows
				switch fault {
				case "foreign tenant":
					row[1] = "foreign-tenant"
				case "foreign session":
					row[2] = "foreign-session"
				case "mismatched user":
					row[3] = "foreign-user"
				case "wrong status":
					row[4] = "removed"
				case "row limit plus one":
					maxRows = 1
					f.participantRows = [][]driver.Value{row, row}
				}
				if maxRows == 1 {
					entry := f.reads[canonicalJoinSQL(ownedActiveSessionParticipantsSQL)]
					entry.args = append([]driver.Value(nil), entry.args...)
					entry.args[3] = int64(2)
					f.reads[canonicalJoinSQL(ownedActiveSessionParticipantsSQL)] = entry
				}
				err := store.ownedAuthorityTx(t.Context(), func(ctx context.Context, q *ownedAuthorityQuery) error {
					_, err := activeSessionParticipants(ctx, q.canonicalState(store, request.TenantID), request.SessionID, maxRows)
					return err
				})
				if !errors.Is(err, domain.ErrMembershipDenied) || f.commits != 0 || f.persistedWrites != 0 {
					t.Fatalf("participant projection corruption: fault=%s err=%v commits=%d", fault, err, f.commits)
				}
			})
		})
	}
}
