package ydbstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	"image/png"
	"io"
	"reflect"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

func newPiInputSourceFixture(t *testing.T) *piUseSourceFixture {
	t.Helper()
	g := newPiUseSourceFixture(t)
	g.loaded.Job.ContextWindow = &domain.SessionContextWindow{ThroughSequence: 2}
	g.loaded.Job.TriggerEventID = "event-2"
	g.loaded.Run.TriggerEventID = g.loaded.Job.TriggerEventID
	digest, err := domain.AttachedWorkerJobContextDigestV1(g.loaded.Job, g.loaded.InputManifest)
	if err != nil {
		t.Fatal(err)
	}
	g.head.ContextDigest, g.auth.ContextDigest = digest, digest
	g.syncOriginalRows(t)
	piInputMembership(t, g)
	return g
}

func piInputMembership(t *testing.T, g *piUseSourceFixture) {
	t.Helper()
	bucket, err := webBucket(string(g.auth.OwnerUserID))
	if err != nil {
		t.Fatal(err)
	}
	g.setRow(t, piTerminalMembershipSQL, []driver.Value{int64(bucket), string(g.auth.OwnerUserID), string(g.auth.TenantID)},
		g.sql.rows[piUseSQL(computeChoiceMembershipSQL)][0])
}

func piInputApplied(t *testing.T, g *piUseSourceFixture) {
	t.Helper()
	before, writes := g.sql.transactions, g.sql.writes
	got, err := g.store.AuthorizeAttachedWorkerSealedInput(g.ctx, g.auth)
	if err != nil || got.Status != ports.AttachedWorkerExecutionApplied || got.Attempt != g.head || got.AttemptRevision != g.head.Revision {
		t.Fatalf("original API input source not applied: status=%s err=%v", got.Status, err)
	}
	if g.sql.transactions != before+1 || g.sql.active || g.sql.writes != writes || g.sql.writeAttempts != 0 {
		t.Fatal("input authority must be one read-only serializable callback")
	}
	for _, query := range []string{piUseRunSQL, piUseJobSQL, piUseAttemptSQL, piUseQuotaSQL, piUseManifestSQL,
		piUseLeaseHeadSQL, piUseLeaseSQL, computeChoiceSessionSQL, computeChoiceParticipantSQL,
		piUseDeletionSQL, providerCredentialReadSQL, piPolicyReadSQL} {
		found := false
		for _, actual := range g.sql.transactionQueries {
			found = found || actual == piUseSQL(query)
		}
		if !found {
			t.Fatalf("missing actual input source read %s", query)
		}
	}
}

func TestPiAPIInputSourceCurrentAuthorityAndRestoration(t *testing.T) {
	for _, mode := range []string{"nil_catalog", "revoked", "replaced", "api_generation", "membership_version", "session_deletion", "expired_at_database_time"} {
		t.Run(mode, func(t *testing.T) {
			g := newPiInputSourceFixture(t)
			piInputApplied(t, g)
			rows, records, catalog, key := piInputCloneRows(g.sql.rows), piInputCloneRows(g.sql.records), g.store.attachedPiAPIPolicyCatalog, g.sql.decisionKey
			piNewWorkChangeAuthority(t, g, mode)
			piInputMembership(t, g)
			writes := g.sql.writes
			got, err := g.store.AuthorizeAttachedWorkerSealedInput(g.ctx, g.auth)
			if err != nil || got != (ports.AttachedWorkerSealedInputAuthorizationResult{Status: ports.AttachedWorkerExecutionDenied}) ||
				g.sql.writes != writes || g.sql.writeAttempts != 0 || g.sql.active {
				t.Fatalf("stale source released input authority: status=%s err=%v", got.Status, err)
			}
			// Restoring the synthetic authoritative snapshot is a causal control,
			// not a production operation that revives an immutable revoked M.
			g.sql.rows, g.sql.records, g.store.attachedPiAPIPolicyCatalog = rows, records, catalog
			g.sql.decisionKey = key
			piInputApplied(t, g)
		})
	}
}

func TestPiAPIInputSourceOriginalExecutionFenceAndAggregate(t *testing.T) {
	for _, mode := range []string{"reconnect", "cancel", "foreign_context", "missing_mapping", "lease_worker"} {
		t.Run(mode, func(t *testing.T) {
			g := newPiInputSourceFixture(t)
			piInputApplied(t, g)
			switch mode {
			case "reconnect":
				g.worker.ConnectionGeneration++
				g.worker.Revision++
				g.connection.ID = "synthetic-successor-input-connection"
				g.connection.ConnectionGeneration = g.worker.ConnectionGeneration
				g.connection.Revision++
				g.head.ConnectionID, g.head.ConnectionGeneration = g.connection.ID, g.connection.ConnectionGeneration
				g.head.Revision++
				g.auth.ConnectionID, g.auth.ConnectionGeneration, g.auth.ExpectedAttemptRevision = g.connection.ID, g.connection.ConnectionGeneration, g.head.Revision
				if g.worker.Validate() != nil || g.connection.Validate() != nil || g.head.Validate() != nil ||
					!attachedWorkerSealedInputAuthorized(g.auth, g.at, g.worker, g.connection, g.head) ||
					!piAPIOriginalJobMatches(g.loaded, g.lease, g.head, g.auth, g.binding, g.decision.MappingDigest, g.at) {
					t.Fatal("reconnect must remain coherent under broad current predicates")
				}
			case "cancel":
				g.loaded.Run.CancellationRequestedAt = &g.at
			case "foreign_context":
				g.loaded.Job.TriggerEventID = "synthetic-substituted-trigger"
				g.loaded.Run.TriggerEventID = g.loaded.Job.TriggerEventID
			case "missing_mapping":
				g.loaded.Job.HarnessBinding.AttachedPiAPIInvocationPolicyDigest = ""
			case "lease_worker":
				g.loaded.Attempt.WorkerID = "synthetic-foreign-worker"
			}
			g.syncOriginalRows(t)
			got, err := g.store.AuthorizeAttachedWorkerSealedInput(g.ctx, g.auth)
			if got.Status == ports.AttachedWorkerExecutionApplied || got.AttemptRevision != 0 || got.Attempt != (domain.AttachedWorkerAttemptV1{}) || g.sql.writeAttempts != 0 {
				t.Fatalf("foreign original state released input authority: status=%s err=%v", got.Status, err)
			}
		})
	}
}

func TestPiAPIInputSourceUnknownCommitClearsSuccess(t *testing.T) {
	g := newPiInputSourceFixture(t)
	piInputApplied(t, g)
	g.sql.unknownCommit = true
	got, err := g.store.AuthorizeAttachedWorkerSealedInput(g.ctx, g.auth)
	if err == nil || got != (ports.AttachedWorkerSealedInputAuthorizationResult{}) || g.sql.writeAttempts != 0 {
		t.Fatal("unknown read-only commit retained speculative input authorization")
	}
	g.sql.unknownCommit = false
	piInputApplied(t, g)
}

func piInputCloneRows(in map[string][]driver.Value) map[string][]driver.Value {
	out := make(map[string][]driver.Value, len(in))
	for k, v := range in {
		out[k] = append([]driver.Value(nil), v...)
	}
	return out
}

const piInputEventsSQL = `SELECT record FROM session_events WHERE tenant_id = $1 AND session_id = $2 AND sequence > $3 AND sequence <= $4 ORDER BY sequence ASC LIMIT $5`

// This test-only bridge exists because sealedinput -> receipt -> ydbstore
// prevents importing Service in internal ydbstore tests. It exports synthetic
// DATA and the actual Store, never an alternative production authorizer.
type PiInputSourceFixtureForTest struct {
	Store         *Store
	Context       context.Context
	Authorization ports.AttachedWorkerSealedInputAuthorization
	Head          domain.AttachedWorkerAttemptV1
	Bodies        map[string][]byte
	Secret        []byte
	Change        func(string)
	Counts        func() (transactions, writes int)
}

func NewPiInputSourceFixtureForTest(t *testing.T, inputLimit uint64) PiInputSourceFixtureForTest {
	t.Helper()
	g := newPiInputSourceFixture(t)
	j := &g.loaded.Job
	j.Limits.MaxInputBytes = inputLimit
	j.ContextWindow, j.TriggerEventID = &domain.SessionContextWindow{ThroughSequence: 2}, "event-2"
	g.loaded.Run.TriggerEventID = j.TriggerEventID
	j.WorkspaceSnapshot, j.SkillBundle, j.AllowedMCPServers = nil, nil, nil
	// The only bearer bytes used by the joined test are this synthetic value.
	secret := bytes.Repeat([]byte{0x71}, 32)
	g.auth.PresentedSecretDigest = domain.DigestAttachedWorkerConnectionSecret(secret)
	g.connection.SecretDigest = g.auth.PresentedSecretDigest
	bodies := map[string][]byte{}
	events := make([]domain.SessionEvent, 0, 2)
	for seq := uint64(1); seq <= 2; seq++ {
		id := domain.SessionEventID(fmt.Sprintf("event-%d", seq))
		dimensions := image.Rect(0, 0, 3000, 1000)
		if seq == 2 {
			dimensions = image.Rect(0, 0, 64, 32)
		}
		var original bytes.Buffer
		encoder := png.Encoder{CompressionLevel: png.BestSpeed}
		if err := encoder.Encode(&original, image.NewGray(dimensions)); err != nil {
			t.Fatal(err)
		}
		imageBytes := original.Bytes()
		if len(imageBytes) > 1<<20 {
			t.Fatal("deterministic PNG exceeds the original image byte bound")
		}
		imageRef := piInputBlob(j.TenantID, domain.SessionEventObjectPrefix(j.TenantID, j.SessionID, id)+"image.png", imageBytes)
		payload, err := json.Marshal(struct {
			Version     uint32                         `json:"version"`
			Text        string                         `json:"text"`
			Attachments []sessioncontext.AttachmentRef `json:"attachments"`
		}{1, fmt.Sprintf("turn %d", seq), []sessioncontext.AttachmentRef{{Name: "image.png", MediaType: "image/png", Blob: imageRef}}})
		if err != nil {
			t.Fatal(err)
		}
		ref := piInputBlob(j.TenantID, domain.SessionEventObjectPrefix(j.TenantID, j.SessionID, id)+"payload.json", payload)
		owner := j.CredentialOwnerUserID
		events = append(events, domain.SessionEvent{ID: id, TenantID: j.TenantID, SessionID: j.SessionID, Sequence: seq, Kind: domain.SessionEventUserMessage,
			AuthorUserID: &owner, IdempotencyKey: domain.IdempotencyKey(id), Payload: ref, CreatedAt: g.at})
		bodies[ref.Key], bodies[imageRef.Key] = payload, imageBytes
	}
	if len(g.loaded.InputManifest.Artifacts) != 0 {
		t.Fatal("joined fixture requires explicit bodies for any added original manifest artifacts")
	}
	digest, err := domain.AttachedWorkerJobContextDigestV1(*j, g.loaded.InputManifest)
	if err != nil {
		t.Fatal(err)
	}
	g.head.ContextDigest, g.auth.ContextDigest = digest, digest
	g.syncOriginalRows(t)
	piInputMembership(t, g)
	var session domain.Session
	if err := json.Unmarshal([]byte(g.sql.rows[piUseSQL(computeChoiceSessionSQL)][0].(string)), &session); err != nil {
		t.Fatal(err)
	}
	session.LastEventSequence = 2
	g.setRow(t, computeChoiceSessionSQL, []driver.Value{string(j.TenantID), string(j.SessionID)}, choiceFixtureJSON(t, session), string(session.CreatedBy), string(session.Status), int64(2), nil)
	// Register the public LoadWorkerJob point selectors, still checked exactly.
	for _, pair := range [][2]string{
		{piUseJobSQL, `SELECT payload FROM worker_jobs WHERE tenant_id = $1 AND run_id = $2`},
		{piUseQuotaSQL, `SELECT payload FROM quota_reservations WHERE tenant_id = $1 AND quota_reservation_id = $2`},
		{piUseManifestSQL, `SELECT payload FROM artifact_manifests WHERE tenant_id = $1 AND artifact_manifest_id = $2`},
	} {
		g.setRow(t, pair[1], g.sql.args[piUseSQL(pair[0])], g.sql.rows[piUseSQL(pair[0])]...)
	}
	g.setRow(t, `SELECT payload FROM checkpoints WHERE tenant_id = $1 AND attempt_id = $2 ORDER BY sequence DESC LIMIT 1`,
		[]driver.Value{string(j.TenantID), string(j.AttemptID)})
	f := &piInputSQLSource{piUseSQLSource: g.sql, events: events,
		eventArgs: []driver.Value{string(j.TenantID), string(j.SessionID), int64(0), int64(2), int64(j.Limits.EffectiveMaxContextEvents() + 1)}}
	db := sql.OpenDB(f)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	g.store.db = db
	piInputApplied(t, g)
	return PiInputSourceFixtureForTest{Store: g.store, Context: g.ctx, Authorization: g.auth, Head: g.head, Bodies: bodies, Secret: secret,
		Change: func(mode string) {
			if mode == "unknown_commit" {
				g.sql.unknownCommit = true
				return
			}
			piNewWorkChangeAuthority(t, g, mode)
			piInputMembership(t, g)
		},
		Counts: func() (int, int) { return g.sql.transactions, g.sql.writeAttempts }}
}

func piInputBlob(tenant domain.TenantID, key string, body []byte) domain.BlobRef {
	digest := sha256.Sum256(body)
	return domain.BlobRef{TenantID: tenant, Key: key, Size: int64(len(body)), SHA256: hex.EncodeToString(digest[:])}
}

type piInputSQLSource struct {
	*piUseSQLSource
	events    []domain.SessionEvent
	eventArgs []driver.Value
}

func (f *piInputSQLSource) Connect(context.Context) (driver.Conn, error) { return f, nil }
func (f *piInputSQLSource) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	if piUseSQL(q) != piUseSQL(piInputEventsSQL) {
		return f.piUseSQLSource.QueryContext(ctx, q, args)
	}
	if err := f.bounded(ctx); err != nil {
		return nil, err
	}
	actual := make([]driver.Value, len(args))
	for i := range args {
		actual[i] = args[i].Value
	}
	if f.active || !reflect.DeepEqual(actual, f.eventArgs) {
		return nil, errors.New("unexpected canonical input range selector")
	}
	rows := &piInputEventRows{}
	for _, event := range f.events {
		encoded, err := json.Marshal(event)
		if err != nil {
			return nil, err
		}
		rows.rows = append(rows.rows, string(encoded))
	}
	return rows, nil
}

type piInputEventRows struct{ rows []string }

func (*piInputEventRows) Columns() []string { return []string{"record"} }
func (*piInputEventRows) Close() error      { return nil }
func (r *piInputEventRows) Next(out []driver.Value) error {
	if len(r.rows) == 0 {
		return io.EOF
	}
	out[0], r.rows = r.rows[0], r.rows[1:]
	return nil
}
