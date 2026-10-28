package ydbstore

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/retry"

	"gitcode.com/urandon/sessionless/internal/attachedresourcegeneration"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// This drives the actual Store methods, database/sql Begin/Commit/Rollback and
// original membership/worker/connection/protocol readers. It is deterministic
// SQL fault evidence, not native YDB isolation/query syntax or a real bearer.
type generationSQLFixture struct {
	rows                               map[string][]driver.Value
	keys                               map[string][]driver.Value
	queries                            []string
	mode                               string
	pending                            string
	writeAt                            time.Time
	begins, commits, rollbacks, writes int
	deadlines                          []time.Time
	isolations                         []driver.IsolationLevel
	afterCommit                        func()
	failQuery                          string
}

func (f *generationSQLFixture) Connect(context.Context) (driver.Conn, error) { return f, nil }
func (*generationSQLFixture) Driver() driver.Driver                          { return explanationFixtureDriver{} }
func (*generationSQLFixture) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("generation fixture forbids Prepare")
}
func (*generationSQLFixture) Close() error { return nil }
func (*generationSQLFixture) Begin() (driver.Tx, error) {
	return nil, errors.New("generation fixture requires explicit BeginTx")
}
func (*generationSQLFixture) CheckNamedValue(value *driver.NamedValue) error {
	if _, ok := value.Value.(uint64); ok {
		return nil
	}
	return driver.ErrSkip
}
func (f *generationSQLFixture) BeginTx(ctx context.Context, options driver.TxOptions) (driver.Tx, error) {
	deadline, found := ctx.Deadline()
	if !found {
		return nil, errors.New("generation transaction has no deadline")
	}
	f.begins++
	f.pending = ""
	f.deadlines = append(f.deadlines, deadline)
	f.isolations = append(f.isolations, options.Isolation)
	return generationFixtureTx{f}, nil
}

type generationFixtureTx struct{ f *generationSQLFixture }

func (tx generationFixtureTx) Commit() error {
	f := tx.f
	f.commits++
	if f.pending != "" && f.mode != "rollback" {
		f.rows[generationSQLKey(attachedGenerationResourceSQL)][3] = f.pending
	}
	f.pending = ""
	if f.afterCommit != nil {
		f.afterCommit()
	}
	if f.mode == "ambiguous" || f.mode == "rollback" {
		return retry.RetryableError(errors.New("synthetic commit acknowledgement unavailable"), retry.WithBackoff(retry.TypeNoBackoff))
	}
	return nil
}
func (tx generationFixtureTx) Rollback() error { tx.f.rollbacks++; tx.f.pending = ""; return nil }
func generationSQLKey(statement string) string { return strings.Join(strings.Fields(statement), " ") }
func (f *generationSQLFixture) QueryContext(ctx context.Context, statement string, arguments []driver.NamedValue) (driver.Rows, error) {
	statement = generationSQLKey(statement)
	f.queries = append(f.queries, statement)
	if strings.Contains(statement, "attached_resource_generation_v1") && !strings.Contains(statement, "SUBSTRING(CAST(attached_resource_generation_v1 AS String),0,4097)") {
		return nil, errors.New("generation source lost its prospective SQL size bound")
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if statement == f.failQuery {
		return nil, errors.New("synthetic DB read unavailable")
	}
	want, known := f.keys[statement]
	if !known {
		return nil, errors.New("generation fixture rejects non-manifest/nonpoint query")
	}
	got := make([]driver.Value, len(arguments))
	for i := range arguments {
		got[i] = arguments[i].Value
	}
	if !reflect.DeepEqual(got, want) {
		return nil, errors.New("generation fixture rejects foreign query keys")
	}
	return &explanationFixtureRows{values: append([]driver.Value(nil), f.rows[statement]...)}, nil
}
func (f *generationSQLFixture) ExecContext(ctx context.Context, statement string, arguments []driver.NamedValue) (driver.Result, error) {
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if statement != attachedGenerationUpdateSQL || len(arguments) != 4 {
		return nil, errors.New("generation fixture forbids unrelated writes")
	}
	for _, forbidden := range []string{"entitlement", "quota", "observed_at", "credential_ref", "INSERT", "UPSERT"} {
		if strings.Contains(statement, forbidden) {
			return nil, errors.New("generation metadata tried to acquire unrelated resource ownership")
		}
	}
	if f.mode == "write-fail" {
		return nil, errors.New("synthetic metadata update failed before commit")
	}
	keys := f.keys[generationSQLKey(attachedGenerationResourceSQL)]
	if arguments[0].Value != keys[0] || arguments[1].Value != keys[1] {
		return nil, errors.New("generation fixture rejects foreign update key")
	}
	raw, ok := arguments[2].Value.(string)
	if !ok || len(raw) > domain.AttachedResourceGenerationMetadataMaxBytesV1 {
		return nil, errors.New("unbounded generation update")
	}
	if _, err := domain.DecodeAttachedResourceGenerationHeadV1([]byte(raw)); err != nil {
		return nil, err
	}
	at, ok := arguments[3].Value.(time.Time)
	if !ok || at != f.rows["SELECT CurrentUtcTimestamp()"][0] {
		return nil, errors.New("generation update did not use its DB timestamp")
	}
	f.pending, f.writeAt = raw, at
	f.writes++
	return driver.RowsAffected(1), nil
}

type generationStoreFixture struct {
	store      *Store
	sql        *generationSQLFixture
	auth       ports.AttachedResourceGenerationAuthorizationV1
	baseline   domain.AttachedResourceGenerationBaselineV1
	worker     domain.AttachedWorker
	connection domain.AttachedWorkerConnection
	manifest   domain.AttachedWorkerCapabilityManifest
	member     domain.TenantMembership
	at         time.Time
}

func newGenerationStoreFixture(t *testing.T) *generationStoreFixture {
	t.Helper()
	original := newComputeChoiceAttachedFixture(t)
	at := original.source.at
	worker, connection := original.worker, original.connection
	connection.ManifestIdentityKey = domain.DigestAttachedWorkerIdentityKey(worker.IdentityPublicKey)
	snapshot, err := attachedworkerprotocol.DecodeMachineSnapshotV1(connection.ProtocolSnapshot)
	if err != nil || snapshot.Manifest == nil {
		t.Fatalf("original synthetic protocol manifest: %v", err)
	}
	payload, err := json.Marshal(snapshot.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifest := domain.AttachedWorkerCapabilityManifest{Version: 1, TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, WorkerID: worker.ID, EnrollmentGeneration: worker.EnrollmentGeneration, ManifestRevision: connection.ManifestRevision, Digest: connection.CapabilityDigest, ProtocolVersion: connection.ProtocolVersion, IdentityKeyDigest: connection.ManifestIdentityKey, ManifestPayload: payload}
	binding, evidence := computeChoiceEvidenceFixture(t, at, original.pin)
	baseline := domain.AttachedResourceGenerationBaselineV1{Version: 1, TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, SubscriptionConnectionID: original.pin.subscriptionID, WorkerID: worker.ID, EnrollmentGeneration: worker.EnrollmentGeneration, BaselineRevision: binding.Resource.Revision, CapabilityDigest: connection.CapabilityDigest, GenerationOnlyDelegation: true, Binding: binding, Evidence: evidence}
	registry, err := attachedresourcegeneration.NewRegistry([]domain.AttachedResourceGenerationBaselineV1{baseline})
	if err != nil {
		t.Fatalf("trusted synthetic baseline: %v", err)
	}
	f := &generationStoreFixture{sql: &generationSQLFixture{rows: make(map[string][]driver.Value), keys: make(map[string][]driver.Value)}, baseline: baseline, worker: worker, connection: connection, manifest: manifest, member: original.source.member, at: at}
	f.auth = ports.AttachedResourceGenerationAuthorizationV1{TenantID: worker.TenantID, OwnerUserID: worker.OwnerUserID, SubscriptionConnectionID: baseline.SubscriptionConnectionID, WorkerID: worker.ID, EnrollmentGeneration: worker.EnrollmentGeneration, BaselineRevision: baseline.BaselineRevision, ConnectionID: connection.ID, ConnectionGeneration: connection.ConnectionGeneration, PresentedSecretDigest: connection.SecretDigest}
	f.syncRows(t)
	db := sql.OpenDB(f.sql)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	f.store, err = New(db, Options{AttachedResourceGenerations: registry})
	if err != nil {
		t.Fatal(err)
	}
	if f.store.attachedResourceGenerations != registry {
		t.Fatal("Store copied/replaced installed registry pointer")
	}
	return f
}

func generationJSON(t *testing.T, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}
func (f *generationStoreFixture) syncRows(t *testing.T) {
	t.Helper()
	q := f.sql
	bucket, err := webBucket(string(f.auth.OwnerUserID))
	if err != nil {
		t.Fatal(err)
	}
	membership := generationSQLKey(`SELECT record FROM tenant_memberships WHERE user_bucket = $1 AND user_id = $2 AND tenant_id = $3`)
	worker := generationSQLKey(`SELECT record FROM attached_workers WHERE tenant_id = $1 AND owner_user_id = $2 AND worker_id = $3`)
	connection := generationSQLKey(`SELECT record FROM attached_worker_connections WHERE tenant_id=$1 AND owner_user_id=$2 AND worker_id=$3`)
	manifest := generationSQLKey(`SELECT record FROM attached_worker_capability_manifests WHERE tenant_id=$1 AND owner_user_id=$2 AND worker_id=$3 AND capability_digest=$4`)
	q.rows["SELECT CurrentUtcTimestamp()"], q.keys["SELECT CurrentUtcTimestamp()"] = []driver.Value{f.at}, []driver.Value{}
	q.rows[membership], q.keys[membership] = []driver.Value{generationJSON(t, f.member)}, []driver.Value{int64(bucket), string(f.auth.OwnerUserID), string(f.auth.TenantID)}
	var prior driver.Value
	if old := q.rows[generationSQLKey(attachedGenerationResourceSQL)]; len(old) == 4 {
		prior = old[3]
	}
	q.rows[generationSQLKey(attachedGenerationResourceSQL)] = []driver.Value{"canonical-actor-not-user", "codex", "", prior}
	q.keys[generationSQLKey(attachedGenerationResourceSQL)] = []driver.Value{string(f.auth.TenantID), string(f.auth.SubscriptionConnectionID)}
	q.rows[generationSQLKey(attachedGenerationActorSQL)], q.keys[generationSQLKey(attachedGenerationActorSQL)] = []driver.Value{string(f.auth.OwnerUserID)}, []driver.Value{string(f.auth.TenantID), "canonical-actor-not-user"}
	q.rows[generationSQLKey(attachedGenerationOwnerSQL)], q.keys[generationSQLKey(attachedGenerationOwnerSQL)] = []driver.Value{string(f.auth.SubscriptionConnectionID)}, []driver.Value{string(f.auth.TenantID), string(f.auth.OwnerUserID), string(f.auth.SubscriptionConnectionID)}
	identity := []driver.Value{string(f.auth.TenantID), string(f.auth.OwnerUserID), string(f.auth.WorkerID)}
	q.rows[worker], q.keys[worker] = []driver.Value{generationJSON(t, f.worker)}, identity
	q.rows[connection], q.keys[connection] = []driver.Value{generationJSON(t, f.connection)}, identity
	q.rows[manifest], q.keys[manifest] = []driver.Value{generationJSON(t, f.manifest)}, append(append([]driver.Value(nil), identity...), string(f.connection.CapabilityDigest))
}

func (f *generationStoreFixture) head(generation uint64, revoked bool) domain.AttachedResourceGenerationHeadV1 {
	return domain.AttachedResourceGenerationHeadV1{Version: 1, OwnerUserID: f.auth.OwnerUserID, WorkerID: f.auth.WorkerID, EnrollmentGeneration: f.auth.EnrollmentGeneration, BaselineRevision: f.auth.BaselineRevision, CredentialGeneration: generation, Revoked: revoked}
}
func (f *generationStoreFixture) publish(t *testing.T, expected, generation uint64, revoked bool) ports.AttachedResourceGenerationResultV1 {
	t.Helper()
	result, err := f.store.PublishAttachedResourceGenerationV1(t.Context(), ports.AttachedResourceGenerationPublicationV1{Authorization: f.auth, ExpectedCredentialGeneration: expected, Target: f.head(generation, revoked)})
	if err != nil {
		t.Fatalf("publish expected=%d target=%d revoked=%t: %v", expected, generation, revoked, err)
	}
	return result
}

func TestAttachedResourceGenerationStoreOriginalTransactionsAndReadback(t *testing.T) {
	f := newGenerationStoreFixture(t)
	result := f.publish(t, 0, 1, false)
	if result.Status != ports.AttachedWorkerExecutionApplied || result.Head != f.head(1, false) || f.sql.begins != 1 || f.sql.commits != 1 || f.sql.writes != 1 || f.sql.rollbacks != 0 || len(f.sql.queries) != 8 || f.sql.isolations[0] != driver.IsolationLevel(sql.LevelSerializable) || f.sql.writeAt != f.at {
		t.Fatalf("original single-transaction initial publish: result=%+v begin/commit/write/rollback=%d/%d/%d/%d queries=%d", result, f.sql.begins, f.sql.commits, f.sql.writes, f.sql.rollbacks, len(f.sql.queries))
	}
	if !f.sql.deadlines[0].After(time.Now()) || time.Until(f.sql.deadlines[0]) > ports.AttachedResourceGenerationOperationTimeoutV1 {
		t.Fatal("original bounded transaction deadline absent or widened")
	}
	if got := f.publish(t, 0, 1, false); got.Status != ports.AttachedWorkerExecutionReplayed || got.Head != result.Head || f.sql.writes != 1 {
		t.Fatalf("duplicate allocated another mutation: %+v writes=%d", got, f.sql.writes)
	}
	if got := f.publish(t, 1, 1, false); got.Status != ports.AttachedWorkerExecutionReplayed || f.sql.writes != 1 {
		t.Fatalf("unchanged head mutated: %+v", got)
	}
	if got := f.publish(t, 1, 2, false); got.Status != ports.AttachedWorkerExecutionApplied || got.Head != f.head(2, false) || f.sql.writes != 2 {
		t.Fatalf("exact successor not applied: %+v", got)
	}
	for _, invalid := range []struct{ expected, target uint64 }{{1, 3}, {2, 4}, {0, 2}} {
		if got := f.publish(t, invalid.expected, invalid.target, false); got.Status != ports.AttachedWorkerExecutionConflict || got.Head != (domain.AttachedResourceGenerationHeadV1{}) {
			t.Fatalf("invalid active transition returned head: %+v", got)
		}
	}
	if f.sql.writes != 2 {
		t.Fatal("conflicts performed mutations")
	}
	result, err := f.store.ReadAttachedResourceGenerationV1(t.Context(), f.auth)
	if err != nil || result.Head != f.head(2, false) || result.Status != ports.AttachedWorkerExecutionApplied || f.sql.writes != 2 {
		t.Fatalf("authenticated readback: %+v err=%v", result, err)
	}
	head, found, err := f.store.LoadAttachedResourceGenerationHeadV1(t.Context(), f.auth.TenantID, f.auth.OwnerUserID, f.auth.SubscriptionConnectionID)
	if err != nil || !found || head != result.Head || f.sql.writes != 2 {
		t.Fatalf("authenticated owner preflight: found=%t err=%v", found, err)
	}
}

func TestAttachedResourceGenerationStoreUnknownCommitNeverRetriesMutation(t *testing.T) {
	for _, mode := range []string{"ambiguous", "rollback"} {
		t.Run(mode, func(t *testing.T) {
			f := newGenerationStoreFixture(t)
			f.sql.mode = mode
			result, err := f.store.PublishAttachedResourceGenerationV1(t.Context(), ports.AttachedResourceGenerationPublicationV1{Authorization: f.auth, Target: f.head(1, false)})
			if !errors.Is(err, ErrAttachedResourceGenerationUnavailable) || !reflect.DeepEqual(result, ports.AttachedResourceGenerationResultV1{}) || f.sql.begins != 1 || f.sql.commits != 1 || f.sql.writes != 1 {
				t.Fatalf("ambiguous mutation retried/acknowledged: result=%+v err=%v begins=%d commits=%d writes=%d", result, err, f.sql.begins, f.sql.commits, f.sql.writes)
			}
			f.sql.mode = ""
			read, err := f.store.ReadAttachedResourceGenerationV1(t.Context(), f.auth)
			want := ports.AttachedWorkerExecutionApplied
			if mode == "rollback" {
				want = ports.AttachedWorkerExecutionNotFound
			}
			if err != nil || read.Status != want || f.sql.begins != 2 || f.sql.writes != 1 {
				t.Fatalf("one exact reconciliation unexpectedly mutated/retried: %+v err=%v", read, err)
			}
			if mode == "ambiguous" && read.Head != f.head(1, false) || mode == "rollback" && read.Head != (domain.AttachedResourceGenerationHeadV1{}) {
				t.Fatalf("readback invented/lost target: %+v", read)
			}
		})
	}
}

func TestAttachedResourceGenerationStoreRevocationOutlivesEvidenceNotIdentity(t *testing.T) {
	f := newGenerationStoreFixture(t)
	if got := f.publish(t, 0, 1, false); got.Status != ports.AttachedWorkerExecutionApplied {
		t.Fatal(got.Status)
	}
	f.at = f.at.Add(2 * time.Second)
	f.syncRows(t) // Explicit authoritative DB time crosses the original evidence expiry.
	if got := f.publish(t, 1, 2, false); got.Status != ports.AttachedWorkerExecutionDenied || got.Head != (domain.AttachedResourceGenerationHeadV1{}) {
		t.Fatalf("expired evidence renewed active generation: %+v", got)
	}
	if got := f.publish(t, 0, 1, false); got.Status != ports.AttachedWorkerExecutionReplayed {
		t.Fatalf("historical duplicate required renewed positive evidence: %+v", got)
	}
	if got := f.publish(t, 1, 7, true); got.Status != ports.AttachedWorkerExecutionApplied || got.Head != f.head(7, true) {
		t.Fatalf("higher revoke after missed intermediate ACK denied: %+v", got)
	}
	if got := f.publish(t, 1, 7, true); got.Status != ports.AttachedWorkerExecutionReplayed {
		t.Fatalf("revoked duplicate: %+v", got)
	}
	if got := f.publish(t, 7, 8, false); got.Status != ports.AttachedWorkerExecutionConflict {
		t.Fatalf("revoked endpoint resurrected: %+v", got)
	}
	result, err := f.store.ReadAttachedResourceGenerationV1(t.Context(), f.auth)
	if err != nil || result.Head != f.head(7, true) {
		t.Fatalf("revoked/expired metadata read failed: %+v %v", result, err)
	}
	f.at = f.connection.AuthExpiresAt
	f.syncRows(t)
	result, err = f.store.ReadAttachedResourceGenerationV1(t.Context(), f.auth)
	if err != nil || result.Status != ports.AttachedWorkerExecutionDenied || result.Head != (domain.AttachedResourceGenerationHeadV1{}) {
		t.Fatalf("expired bearer read head: %+v err=%v", result, err)
	}
}

func TestAttachedResourceGenerationStoreAbsentDenialAndFinalOverflow(t *testing.T) {
	f := newGenerationStoreFixture(t)
	if got := f.publish(t, 0, math.MaxUint64, true); got.Status != ports.AttachedWorkerExecutionApplied || got.Head != f.head(math.MaxUint64, true) {
		t.Fatalf("offline initial final denial: %+v", got)
	}
	if got := f.publish(t, 0, 1, false); got.Status != ports.AttachedWorkerExecutionConflict {
		t.Fatalf("delayed initial active bypassed offline revocation: %+v", got)
	}
	if got := f.publish(t, math.MaxUint64, 1, false); got.Status != ports.AttachedWorkerExecutionConflict {
		t.Fatalf("wrapped generation accepted: %+v", got)
	}
}

func TestAttachedResourceGenerationStoreRejectsOriginalAuthorityLoss(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*testing.T, *generationStoreFixture)
	}{
		{"membership", func(t *testing.T, f *generationStoreFixture) {
			f.member.Status = domain.TenantMembershipRevoked
			f.syncRows(t)
		}},
		{"actor owner", func(_ *testing.T, f *generationStoreFixture) {
			f.sql.rows[generationSQLKey(attachedGenerationActorSQL)] = []driver.Value{"foreign-user"}
		}},
		{"missing owner projection", func(_ *testing.T, f *generationStoreFixture) {
			f.sql.rows[generationSQLKey(attachedGenerationOwnerSQL)] = nil
		}},
		{"wrong owner projection", func(_ *testing.T, f *generationStoreFixture) {
			f.sql.rows[generationSQLKey(attachedGenerationOwnerSQL)] = []driver.Value{"foreign-resource"}
		}},
		{"local provider is not server provider", func(_ *testing.T, f *generationStoreFixture) {
			f.sql.rows[generationSQLKey(attachedGenerationResourceSQL)][1] = "openai"
		}},
		{"credential custody", func(_ *testing.T, f *generationStoreFixture) {
			f.sql.rows[generationSQLKey(attachedGenerationResourceSQL)][2] = "unexpected"
		}},
		{"revoked worker", func(t *testing.T, f *generationStoreFixture) {
			f.worker.DesiredState = domain.AttachedWorkerDesiredRevoked
			f.worker.RevokedAt = f.at
			f.syncRows(t)
		}},
		{"worker enrollment advanced", func(t *testing.T, f *generationStoreFixture) { f.worker.EnrollmentGeneration++; f.syncRows(t) }},
		{"worker connection advanced", func(t *testing.T, f *generationStoreFixture) { f.worker.ConnectionGeneration++; f.syncRows(t) }},
		{"superseded connection", func(t *testing.T, f *generationStoreFixture) {
			f.connection.State = domain.AttachedWorkerConnectionSuperseded
			f.syncRows(t)
		}},
		{"connection id", func(_ *testing.T, f *generationStoreFixture) { f.auth.ConnectionID = "foreign-connection" }},
		{"connection generation", func(_ *testing.T, f *generationStoreFixture) { f.auth.ConnectionGeneration++ }},
		{"connection secret", func(_ *testing.T, f *generationStoreFixture) {
			f.auth.PresentedSecretDigest = domain.DigestAttachedWorkerConnectionSecret([]byte("another-test-only-secret"))
		}},
		{"capability", func(t *testing.T, f *generationStoreFixture) {
			f.connection.CapabilityDigest = domain.DigestAttachedWorkerCapability([]byte("foreign"))
			f.syncRows(t)
		}},
		{"protocol snapshot", func(t *testing.T, f *generationStoreFixture) {
			f.connection.ProtocolSnapshot = []byte(`{}`)
			f.syncRows(t)
		}},
		{"manifest owner", func(t *testing.T, f *generationStoreFixture) { f.manifest.OwnerUserID = "foreign-user"; f.syncRows(t) }},
		{"manifest identity", func(t *testing.T, f *generationStoreFixture) {
			f.manifest.IdentityKeyDigest = domain.AttachedWorkerIdentityKeyDigest(strings.Repeat("a", 64))
			f.syncRows(t)
		}},
		{"manifest revision", func(t *testing.T, f *generationStoreFixture) { f.manifest.ManifestRevision++; f.syncRows(t) }},
		{"manifest payload", func(t *testing.T, f *generationStoreFixture) {
			f.manifest.ManifestPayload = []byte(`{}`)
			f.syncRows(t)
		}},
		{"expired auth", func(t *testing.T, f *generationStoreFixture) { f.at = f.connection.AuthExpiresAt; f.syncRows(t) }},
		{"wrong baseline revision", func(_ *testing.T, f *generationStoreFixture) { f.auth.BaselineRevision++ }},
		{"foreign generation owner", func(t *testing.T, f *generationStoreFixture) {
			head := f.head(1, false)
			head.OwnerUserID = "foreign-user"
			raw, err := domain.EncodeAttachedResourceGenerationHeadV1(head)
			if err != nil {
				t.Fatal(err)
			}
			f.sql.rows[generationSQLKey(attachedGenerationResourceSQL)][3] = string(raw)
		}},
		{"oversize generation column", func(_ *testing.T, f *generationStoreFixture) {
			f.sql.rows[generationSQLKey(attachedGenerationResourceSQL)][3] = strings.Repeat(" ", 4097)
		}},
		{"column null is not head", func(_ *testing.T, f *generationStoreFixture) {
			f.sql.rows[generationSQLKey(attachedGenerationResourceSQL)][3] = "null"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newGenerationStoreFixture(t)
			tc.mutate(t, f)
			result, err := f.store.PublishAttachedResourceGenerationV1(t.Context(), ports.AttachedResourceGenerationPublicationV1{Authorization: f.auth, Target: f.head(1, false)})
			if result.Head != (domain.AttachedResourceGenerationHeadV1{}) || result.Status == ports.AttachedWorkerExecutionApplied || result.Status == ports.AttachedWorkerExecutionReplayed || f.sql.writes != 0 || f.sql.commits != 0 {
				t.Fatalf("lost original authority performed/acknowledged mutation: %+v err=%v writes=%d commits=%d", result, err, f.sql.writes, f.sql.commits)
			}
			read, readErr := f.store.ReadAttachedResourceGenerationV1(t.Context(), f.auth)
			if read.Head != (domain.AttachedResourceGenerationHeadV1{}) || read.Status == ports.AttachedWorkerExecutionApplied || f.sql.writes != 0 {
				t.Fatalf("lost original authority disclosed readback: %+v err=%v", read, readErr)
			}
		})
	}
}

func TestAttachedResourceGenerationStoreLateCommitAndNoImplicitFallback(t *testing.T) {
	f := newGenerationStoreFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	f.sql.afterCommit = cancel
	result, err := f.store.PublishAttachedResourceGenerationV1(ctx, ports.AttachedResourceGenerationPublicationV1{Authorization: f.auth, Target: f.head(1, false)})
	if !errors.Is(err, ErrAttachedResourceGenerationUnavailable) || !reflect.DeepEqual(result, ports.AttachedResourceGenerationResultV1{}) || f.sql.writes != 1 || f.sql.begins != 1 {
		t.Fatalf("late Commit invented ACK: %+v err=%v", result, err)
	}
	f.sql.afterCommit = nil
	read, err := f.store.ReadAttachedResourceGenerationV1(t.Context(), f.auth)
	if err != nil || read.Head != f.head(1, false) || f.sql.writes != 1 {
		t.Fatalf("late Commit exact readback failed: %+v %v", read, err)
	}
	f.store.attachedResourceGenerations = nil
	before := f.sql.begins
	if result := f.publish(t, 1, 2, false); result.Status != ports.AttachedWorkerExecutionDenied || f.sql.begins != before {
		t.Fatalf("missing trusted registry fell back: %+v", result)
	}
	if _, found, err := f.store.LoadAttachedResourceGenerationHeadV1(t.Context(), f.auth.TenantID, f.auth.OwnerUserID, f.auth.SubscriptionConnectionID); err == nil || found {
		t.Fatal("missing trusted registry returned owner head")
	}
}

func TestAttachedResourceGenerationStoreSourceFailureRollsBackWithoutHead(t *testing.T) {
	for _, failure := range []string{"query", "write", "invalid DB clock"} {
		t.Run(failure, func(t *testing.T) {
			f := newGenerationStoreFixture(t)
			switch failure {
			case "query":
				f.sql.failQuery = generationSQLKey(attachedGenerationActorSQL)
			case "write":
				f.sql.mode = "write-fail"
			case "invalid DB clock":
				f.sql.rows["SELECT CurrentUtcTimestamp()"] = []driver.Value{time.Time{}}
			}
			result, err := f.store.PublishAttachedResourceGenerationV1(t.Context(), ports.AttachedResourceGenerationPublicationV1{Authorization: f.auth, Target: f.head(1, false)})
			if !errors.Is(err, ErrAttachedResourceGenerationUnavailable) || !reflect.DeepEqual(result, ports.AttachedResourceGenerationResultV1{}) || f.sql.begins != 1 || f.sql.commits != 0 || f.sql.rollbacks != 1 || f.sql.writes != 0 {
				t.Fatalf("source failure disclosed result/retried or escaped rollback: %+v err=%v begin/commit/rollback/write=%d/%d/%d/%d", result, err, f.sql.begins, f.sql.commits, f.sql.rollbacks, f.sql.writes)
			}
		})
	}
}

func TestAttachedResourceGenerationOwnerPreflightRechecksCurrentProjection(t *testing.T) {
	f := newGenerationStoreFixture(t)
	if got := f.publish(t, 0, 1, false); got.Status != ports.AttachedWorkerExecutionApplied {
		t.Fatal(got.Status)
	}
	f.sql.rows[generationSQLKey(attachedGenerationActorSQL)] = []driver.Value{"new-foreign-owner"}
	head, found, err := f.store.LoadAttachedResourceGenerationHeadV1(t.Context(), f.auth.TenantID, f.auth.OwnerUserID, f.auth.SubscriptionConnectionID)
	if !errors.Is(err, domain.ErrMembershipDenied) || found || head != (domain.AttachedResourceGenerationHeadV1{}) || f.sql.writes != 1 {
		t.Fatalf("owner preflight disclosed head after canonical ownership changed: found=%t head=%+v err=%v", found, head, err)
	}
}

func TestAttachedResourceGenerationPortBoundsAndDigestRedaction(t *testing.T) {
	f := newGenerationStoreFixture(t)
	request := ports.AttachedResourceGenerationPublicationV1{Authorization: f.auth, Target: f.head(1, false)}
	raw, err := json.Marshal(request)
	if err != nil || len(raw) > 4096 || bytes.Contains(raw, []byte(f.auth.PresentedSecretDigest)) || bytes.Contains(raw, []byte("secret")) {
		t.Fatalf("port metadata is unbounded or exposes authentication digest: bytes=%d err=%v", len(raw), err)
	}
	request.Authorization.TenantID = domain.TenantID(strings.Repeat("x", 161))
	if err := request.Validate(); err == nil {
		t.Fatal("oversized metadata identity accepted")
	}
	before := f.sql.begins
	if result, err := f.store.PublishAttachedResourceGenerationV1(t.Context(), request); err != nil || result.Status != ports.AttachedWorkerExecutionDenied || f.sql.begins != before {
		t.Fatalf("invalid metadata reached transaction: %+v err=%v", result, err)
	}
}
