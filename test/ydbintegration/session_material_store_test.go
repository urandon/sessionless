//go:build ydbintegration

package ydbintegration

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
	"gitcode.com/urandon/sessionless/internal/sessionmaterial"
)

func readNativeMaterialPrefix(t *testing.T, ctx context.Context, db *sql.DB, tenant domain.TenantID, session domain.SessionID, sequence *uint64) sessioncontext.SessionMaterialPrefixV1 {
	t.Helper()
	statement := `SELECT SUBSTRING(CAST(record AS String),0,8193) FROM session_material_current_v1 WHERE tenant_id=$1 AND session_id=$2`
	args := []any{tenant, session}
	if sequence != nil {
		statement = `SELECT SUBSTRING(CAST(record AS String),0,8193) FROM session_material_boundaries_v1 WHERE tenant_id=$1 AND session_id=$2 AND sequence=$3`
		args = append(args, *sequence)
	}
	var record string
	if err := db.QueryRowContext(ctx, statement, args...).Scan(&record); err != nil {
		t.Fatalf("native material row: %v", err)
	}
	prefix, err := sessioncontext.DecodeSessionMaterialPrefixV1([]byte(record))
	if err != nil {
		t.Fatalf("native material decode: %v", err)
	}
	return prefix
}

// Real Serializable writers and immutable anchors, separate from SQL fault
// connectors. Running/compiling this test is not a serving-generation rollout.
func TestSessionMaterialYDBConcurrentVerifiedAppendReplayUnknownAndParity(t *testing.T) {
	store, client := openStore(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	var at time.Time
	if err := client.DB.QueryRowContext(ctx, `SELECT CurrentUtcTimestamp()`).Scan(&at); err != nil {
		t.Fatal(err)
	}
	// A package-local counter alone repeats across separate go test processes.
	// Isolate this fixture from retained rows when reusing an owned test cluster.
	nonce := rand.Text()
	tenant, user := domain.TenantID(uniqueID("tenant-material-"+nonce)), domain.UserID(uniqueID("user-material-"+nonce))
	session, owner := canonicalSessionFixture(tenant, user, domain.SessionID(uniqueID("session-material-"+nonce)), at)
	if err := store.CreateSession(ctx, session, owner); err != nil {
		t.Fatal(err)
	}
	genesis, err := sessioncontext.NewSessionMaterialGenesisV1(tenant, session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got := readNativeMaterialPrefix(t, ctx, client.DB, tenant, session.ID, nil); !sessioncontext.EqualSessionMaterialPrefixV1(got, genesis) {
		t.Fatal("native new-empty genesis differs")
	}
	const writers = 4
	events := make([]domain.SessionEvent, writers)
	prepared := make(map[domain.SessionEventID]*sessionmaterial.PreparedV1, writers)
	for i := range writers {
		body := []byte(fmt.Sprintf(`{"version":1,"text":"native source %d"}`, i))
		digest := sha256.Sum256(body)
		id := domain.SessionEventID(uniqueID("event-material"))
		events[i] = domain.SessionEvent{ID: id, TenantID: tenant, SessionID: session.ID, Kind: domain.SessionEventUserMessage, AuthorUserID: &user, IdempotencyKey: domain.IdempotencyKey(uniqueID("idem-material")), CreatedAt: at,
			Payload: domain.BlobRef{TenantID: tenant, Key: domain.SessionEventObjectPrefix(tenant, session.ID, id) + "uploads/test/message.json", Size: int64(len(body)), SHA256: hex.EncodeToString(digest[:])}}
		p, err := sessionmaterial.PrepareV1(events[i], body, nil)
		clear(body)
		if err != nil {
			t.Fatal(err)
		}
		prepared[id] = p
		t.Cleanup(p.Release)
	}
	start := make(chan struct{})
	errors := make(chan error, writers)
	var group sync.WaitGroup
	for _, event := range events {
		group.Go(func() {
			<-start
			created, err := store.AppendPreparedSessionEventV1(ctx, event, prepared[event.ID])
			if err != nil {
				errors <- fmt.Errorf("append %s: %w", event.ID, err)
			} else if !created {
				errors <- fmt.Errorf("append %s unexpectedly replayed", event.ID)
			}
		})
	}
	close(start)
	group.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}
	if t.Failed() {
		return
	}
	history, err := store.ListSessionHistory(ctx, tenant, session.ID, 0, 10)
	if err != nil || len(history) != writers {
		t.Fatalf("native history count=%d want%d err=%v", len(history), writers, err)
	}
	expected := genesis
	for index, event := range history {
		if event.Sequence != uint64(index+1) {
			t.Fatalf("gap at index%d sequence%d", index, event.Sequence)
		}
		measure, err := sessioncontext.MeasurePreparedRecordV1(event, prepared[event.ID])
		if err != nil {
			t.Fatalf("actual final native measurement: %v", err)
		}
		expected, err = sessioncontext.FoldSessionMaterialV1(expected, event, &measure)
		if err != nil {
			t.Fatal(err)
		}
		if boundary := readNativeMaterialPrefix(t, ctx, client.DB, tenant, session.ID, &event.Sequence); !sessioncontext.EqualSessionMaterialPrefixV1(boundary, expected) {
			t.Fatalf("native immutable boundary %d differs from actual ordered source fold", event.Sequence)
		}
	}
	if current := readNativeMaterialPrefix(t, ctx, client.DB, tenant, session.ID, nil); !sessioncontext.EqualSessionMaterialPrefixV1(current, expected) || current.Coverage != sessioncontext.SessionMaterialCompleteV1 {
		t.Fatal("native current is not exact complete prefix")
	}
	if created, err := store.AppendPreparedSessionEventV1(ctx, events[0], prepared[events[0].ID]); err != nil || created {
		t.Fatalf("exact native retry created=%v err=%v", created, err)
	}
	if current := readNativeMaterialPrefix(t, ctx, client.DB, tenant, session.ID, nil); !sessioncontext.EqualSessionMaterialPrefixV1(current, expected) {
		t.Fatal("exact retry refolded prefix")
	}
	unknown := events[0]
	unknown.ID = domain.SessionEventID(uniqueID("event-unknown"))
	unknown.IdempotencyKey = domain.IdempotencyKey(uniqueID("idem-unknown"))
	unknown.Payload.Key = domain.SessionEventObjectPrefix(tenant, session.ID, unknown.ID) + "uploads/test/message.json"
	if created, err := store.AppendSessionEvent(ctx, unknown); err != nil || !created {
		t.Fatalf("metadata-only native append created=%v err=%v", created, err)
	}
	current := readNativeMaterialPrefix(t, ctx, client.DB, tenant, session.ID, nil)
	if current.ThroughSequence != writers+1 || current.UnknownRecords != 1 || current.Coverage != sessioncontext.SessionMaterialUnknownV1 {
		t.Fatal("metadata-only append acquired complete coverage")
	}
	// A preparation obtained later cannot repair the already committed row.
	laterBody := []byte(`{"version":1,"text":"native source 0"}`)
	later, err := sessionmaterial.PrepareV1(unknown, laterBody, nil)
	clear(laterBody)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(later.Release)
	if created, err := store.AppendPreparedSessionEventV1(ctx, unknown, later); err != nil || created {
		t.Fatalf("unknown exact retry created=%v err=%v", created, err)
	}
	if after := readNativeMaterialPrefix(t, ctx, client.DB, tenant, session.ID, nil); !sessioncontext.EqualSessionMaterialPrefixV1(after, current) {
		t.Fatal("exact replay upgraded prior unknown facts")
	}
	// Change a valid non-key measure while retaining sequence/digest. The real
	// next writer must reject the entire current/immutable common-shape mismatch.
	corrupt := current.Clone()
	corrupt.VerifiedPayloadBytes++
	encoded, err := sessioncontext.EncodeSessionMaterialPrefixV1(corrupt)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.DB.ExecContext(ctx, `UPDATE session_material_boundaries_v1 SET record=CAST($4 AS String) WHERE tenant_id=$1 AND session_id=$2 AND sequence=$3`, tenant, session.ID, current.ThroughSequence, string(encoded)); err != nil {
		t.Fatal(err)
	}
	conflict := unknown
	conflict.ID = domain.SessionEventID(uniqueID("event-conflict"))
	conflict.IdempotencyKey = domain.IdempotencyKey(uniqueID("idem-conflict"))
	conflict.Payload.Key = domain.SessionEventObjectPrefix(tenant, session.ID, conflict.ID) + "uploads/test/message.json"
	if created, err := store.AppendSessionEvent(ctx, conflict); err == nil || created {
		t.Fatalf("native mismatch silently repaired: created=%v err=%v", created, err)
	}
	if history, err := store.ListSessionHistory(ctx, tenant, session.ID, 0, 10); err != nil || len(history) != writers+1 {
		t.Fatalf("failed native fold changed canonical history: count=%d err=%v", len(history), err)
	}
}

func TestSessionMaterialYDBExactLargeAttachmentIntegerSurvivesNextAppend(t *testing.T) {
	store, client := openStore(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	var at time.Time
	if err := client.DB.QueryRowContext(ctx, `SELECT CurrentUtcTimestamp()`).Scan(&at); err != nil {
		t.Fatal(err)
	}
	nonce := rand.Text()
	tenant, user := domain.TenantID("tenant-integer-"+nonce), domain.UserID("user-integer-"+nonce)
	session, owner := canonicalSessionFixture(tenant, user, domain.SessionID("session-integer-"+nonce), at)
	if err := store.CreateSession(ctx, session, owner); err != nil {
		t.Fatal(err)
	}
	id := domain.SessionEventID("event-large-reference")
	attachment := domain.Artifact{Name: "large.bin", MediaType: "application/octet-stream", Blob: domain.BlobRef{
		TenantID: tenant, Key: domain.SessionEventObjectPrefix(tenant, session.ID, id) + "uploads/source/large.bin", Size: math.MaxInt64, SHA256: strings.Repeat("a", 64),
	}}
	body, err := json.Marshal(struct {
		Version     uint32            `json:"version"`
		Text        string            `json:"text"`
		Attachments []domain.Artifact `json:"attachments"`
	}{Version: 1, Text: "large reference, not attachment body evidence", Attachments: []domain.Artifact{attachment}})
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(body)
	event := domain.SessionEvent{ID: id, TenantID: tenant, SessionID: session.ID, Kind: domain.SessionEventUserMessage, AuthorUserID: &user,
		IdempotencyKey: "large-reference", CreatedAt: at, Payload: domain.BlobRef{TenantID: tenant,
			Key: domain.SessionEventObjectPrefix(tenant, session.ID, id) + "uploads/source/message.json", Size: int64(len(body)), SHA256: hex.EncodeToString(digest[:])}}
	authorized := attachment
	authorized.Name = "attachment-01-large.bin"
	prepared, err := sessionmaterial.PrepareV1(event, body, []domain.Artifact{authorized})
	clear(body)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(prepared.Release)
	if created, err := store.AppendPreparedSessionEventV1(ctx, event, prepared); err != nil || !created {
		t.Fatalf("native large-reference append created=%v err=%v", created, err)
	}
	prefix := readNativeMaterialPrefix(t, ctx, client.DB, tenant, session.ID, nil)
	if prefix.ReferencedAttachmentBytes != math.MaxInt64 || prefix.AttachmentCount != 1 || prefix.Coverage != sessioncontext.SessionMaterialCompleteV1 {
		t.Fatal("native storage changed an exact large attachment reference measure")
	}
	next := event
	next.ID, next.IdempotencyKey = "event-after-large-reference", "after-large-reference"
	next.Payload.Key = domain.SessionEventObjectPrefix(tenant, session.ID, next.ID) + "uploads/source/message.json"
	if created, err := store.AppendSessionEvent(ctx, next); err != nil || !created {
		t.Fatalf("large persisted integer blocked next canonical append: created=%v err=%v", created, err)
	}
	after := readNativeMaterialPrefix(t, ctx, client.DB, tenant, session.ID, nil)
	if after.ReferencedAttachmentBytes != math.MaxInt64 || after.ThroughSequence != 2 || after.UnknownRecords != 1 {
		t.Fatal("next append lost the exact prior integer or its unknown contribution")
	}
	// Independent storage fidelity for all three new record columns. This is
	// not a snapshot producer or authorization proof; no fake boundary is used
	// by the real writer above. Exact source bytes include both uint64 extremes.
	literal := `{"n":9007199254740993,"m":9223372036854775807,"u":18446744073709551615}`
	storageSession := domain.SessionID("storage-probe-" + nonce)
	for _, test := range []struct{ put, get string }{
		{`UPSERT INTO session_material_current_v1 (tenant_id,session_id,record) VALUES ($1,$2,CAST($3 AS String))`, `SELECT record FROM session_material_current_v1 WHERE tenant_id=$1 AND session_id=$2`},
		{`UPSERT INTO session_material_boundaries_v1 (tenant_id,session_id,sequence,record) VALUES ($1,$2,18446744073709551615ul,CAST($3 AS String))`, `SELECT record FROM session_material_boundaries_v1 WHERE tenant_id=$1 AND session_id=$2 AND sequence=18446744073709551615ul`},
		{`UPSERT INTO session_snapshot_material_v1 (tenant_id,session_id,snapshot_version,record) VALUES ($1,$2,1ul,CAST($3 AS String))`, `SELECT record FROM session_snapshot_material_v1 WHERE tenant_id=$1 AND session_id=$2 AND snapshot_version=1ul`},
	} {
		if _, err := client.DB.ExecContext(ctx, test.put, tenant, storageSession, literal); err != nil {
			t.Fatal(err)
		}
		var got string
		if err := client.DB.QueryRowContext(ctx, test.get, tenant, storageSession).Scan(&got); err != nil {
			t.Fatal(err)
		}
		if got != literal {
			t.Fatalf("native integer storage changed exact bytes: %s", got)
		}
	}
}
