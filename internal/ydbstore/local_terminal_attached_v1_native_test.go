package ydbstore_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
	"gitcode.com/urandon/sessionless/test/support/aw07native"
)

// Immutable bounded cooperative storage; this is NOT Object Storage or actual
// harness-process cleanup proof. Native YDB, the original signed AW07 setup,
// receipt/materializer, canonical commit and classifier are real production code.
type attachedHistoryNativeBlobs struct {
	mu            sync.Mutex
	objects       map[string][]byte
	puts, deletes uint64
}

func (b *attachedHistoryNativeBlobs) Put(ctx context.Context, tenant domain.TenantID, key string, r io.Reader) (domain.BlobRef, error) {
	if ctx.Err() != nil {
		return domain.BlobRef{}, ctx.Err()
	}
	body, err := io.ReadAll(io.LimitReader(r, (1<<20)+1))
	if err != nil || len(body) == 0 || len(body) > 1<<20 {
		return domain.BlobRef{}, errors.New("native test blob size bound")
	}
	defer clear(body)
	sum := sha256.Sum256(body)
	b.mu.Lock()
	defer b.mu.Unlock()
	b.puts++
	if previous, exists := b.objects[key]; exists && !bytes.Equal(previous, body) {
		return domain.BlobRef{}, errors.New("native test immutable key conflict")
	}
	b.objects[key] = bytes.Clone(body)
	return domain.BlobRef{TenantID: tenant, Key: key, Size: int64(len(body)), SHA256: hex.EncodeToString(sum[:])}, nil
}

func (b *attachedHistoryNativeBlobs) Open(ctx context.Context, tenant domain.TenantID, ref domain.BlobRef) (io.ReadCloser, error) {
	if ctx.Err() != nil || tenant != ref.TenantID {
		return nil, errors.New("native test blob scope/context")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	body, exists := b.objects[ref.Key]
	if !exists {
		return nil, errors.New("native test immutable body missing")
	}
	return io.NopCloser(bytes.NewReader(bytes.Clone(body))), nil
}

func (b *attachedHistoryNativeBlobs) Delete(ctx context.Context, _ domain.TenantID, ref domain.BlobRef) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.deletes++
	delete(b.objects, ref.Key)
	return nil
}

func (b *attachedHistoryNativeBlobs) writes() [2]uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return [2]uint64{b.puts, b.deletes}
}

// Existing CI's no-build-tag retained-compaction gate selects this prefix.
// This is attached V1 producer/finalizer→historical classification, not a new
// LocalV1 Source or B.RunModelStep/fresh-Run operational reconstruction proof.
func TestLocalRetainedCompactionYDBAttachedV1OriginalFinalizerHistory(t *testing.T) {
	store, db, ctx, tenant := ydbstore.OpenTerminalHistoryNativeV1(t)
	ready := aw07native.Ready(t, store, db, aw07native.ReadyOptions{Suffix: "attached-history", TenantID: tenant, Features: []attachedworkerprotocol.ProtocolFeatureV1{attachedworkerprotocol.FeatureOutputReceipt}})
	claimed := aw07native.Claim(t, store, db, ready.Worker, ready.Connection, ready.SecretDigest, aw07native.OfferOptions{Suffix: aw07native.UniqueSuffix(t, "attached-history")})
	auth := claimed.Request
	blobs := &attachedHistoryNativeBlobs{objects: make(map[string][]byte)}
	t.Cleanup(func() {
		blobs.mu.Lock()
		defer blobs.mu.Unlock()
		for _, body := range blobs.objects {
			clear(body)
		}
	})
	created, err := store.CreateAttachedWorkerOutputReceipt(ctx, blobs, ydbstore.AttachedWorkerOutputReceiptRequest{
		Authorization: auth, Nonce: "original-native-history-receipt",
		Candidate:   attachedworkeroutput.Candidate{Status: domain.AttachedWorkerTerminalSucceeded, Summary: " \noriginal attached native answer\t "},
		Observation: attachedworkeroutput.ProcessObservationV1{Version: 1, DescendantsReaped: true, BoundaryReleased: true, CleanupSucceeded: true},
	})
	if err != nil || created.Status != ports.AttachedWorkerExecutionApplied || !created.Receipt.Ready || created.Receipt.Materialization.Completion == nil {
		t.Fatalf("original receipt/materializer: status=%s ready=%t err=%v", created.Status, created.Receipt.Ready, err)
	}
	completion := created.Receipt.Materialization.Completion
	if len(completion.Events) != 1 || completion.Events[0].Kind != domain.SessionEventAssistantMessage || completion.SubstrateEvidence != nil || completion.Manifest.ID == canonicaloutput.ManifestIDV1(auth.RunID) {
		t.Fatal("attached receipt must own its distinct actual manifest and nil-evidence V1 output")
	}
	evidenceDigest, err := hex.DecodeString(string(created.Receipt.CanonicalDigest))
	if err != nil {
		t.Fatal(err)
	}
	terminal := attachedworkerprotocol.FrameV1{
		Version:   claimed.Accepted.Version,
		MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, claimed.NextWorkerSequence),
		WorkerID:  string(auth.WorkerID), EnrollmentGeneration: auth.EnrollmentGeneration,
		ConnectionGeneration: auth.ConnectionGeneration, Sequence: claimed.NextWorkerSequence,
		Ack: claimed.Accepted.Sequence, Kind: attachedworkerprotocol.MessageTerminal,
		Terminal: &attachedworkerprotocol.TerminalV1{Binding: claimed.Binding, AttemptSequence: 2, TerminalSequence: 1,
			Status: attachedworkerprotocol.TerminalSucceeded, Result: attachedworkerprotocol.TerminalResultCompleted, EvidenceDigest: evidenceDigest},
	}
	pending, err := store.ExchangeAttachedWorkerAttempt(ctx, ports.AttachedWorkerAttemptExchange{TenantID: auth.TenantID, OwnerUserID: auth.OwnerUserID, WorkerID: auth.WorkerID, ConnectionID: auth.ConnectionID, AttemptID: auth.AttemptID, LeaseGeneration: auth.LeaseGeneration, PresentedSecretDigest: auth.PresentedSecretDigest, InboundFrame: terminal})
	if err != nil || pending.Status != ports.AttachedWorkerExecutionApplied || pending.Attempt.State != domain.AttachedWorkerAttemptTerminalPending {
		t.Fatalf("actual TerminalPending exchange: status=%s state=%s err=%v", pending.Status, pending.Attempt.State, err)
	}
	commit := ports.AttachedWorkerTerminalCommit{TenantID: auth.TenantID, OwnerUserID: auth.OwnerUserID, WorkerID: auth.WorkerID, AttemptID: auth.AttemptID, LeaseGeneration: auth.LeaseGeneration, Materialization: ports.AttachedWorkerTerminalMaterialization{EvidenceDigest: created.Receipt.CanonicalDigest}}
	committed, err := store.CommitAttachedWorkerTerminal(ctx, commit)
	if err != nil || committed.Status != ports.AttachedWorkerExecutionApplied || committed.Outbound == nil {
		t.Fatalf("public original canonical commit: status=%s err=%v", committed.Status, err)
	}
	beforeReplay, err := readNativeAttachedCanonicalRows(ctx, db, tenant, completion.RunID, completion.Manifest.ID, completion.Events[0].ID, auth.AttemptID, auth.OwnerUserID, auth.WorkerID)
	if err != nil {
		t.Fatal(err)
	}
	beforeWrites := blobs.writes()
	replay, err := store.CommitAttachedWorkerTerminal(ctx, commit)
	if err != nil || replay.Status != ports.AttachedWorkerExecutionReplayed || replay.Outbound == nil || !bytes.Equal(replay.Outbound.Payload, committed.Outbound.Payload) {
		t.Fatalf("actual exact TerminalACK replay: status=%s err=%v", replay.Status, err)
	}
	afterReplay, err := readNativeAttachedCanonicalRows(ctx, db, tenant, completion.RunID, completion.Manifest.ID, completion.Events[0].ID, auth.AttemptID, auth.OwnerUserID, auth.WorkerID)
	if err != nil || !reflect.DeepEqual(afterReplay, beforeReplay) || blobs.writes() != beforeWrites {
		t.Fatal("ACK replay changed canonical rows/messages/objects", err)
	}
	var run domain.Run
	if json.Unmarshal([]byte(beforeReplay.Run), &run) != nil || run.Validate() != nil || run.Status != domain.RunSucceeded || run.TerminalCustody != nil || run.DerivedContext != nil {
		t.Fatal("actual persisted legacy attached Run")
	}
	read := func() (ydbstore.TerminalHistoryNativeResultV1, error) {
		return ydbstore.ReadTerminalHistoryNativeV1(ctx, store, tenant, run.SessionID, run.ID, blobs)
	}
	check := func(t *testing.T) {
		t.Helper()
		v, err := read()
		if err != nil {
			t.Fatalf("original persisted attached V1 classification: %v", err)
		}
		if !reflect.DeepEqual(v.Run, run) {
			t.Fatal("historical Run differs from the actual persisted canonical Run")
		}
		if !reflect.DeepEqual(v.Manifest, completion.Manifest) {
			t.Fatalf("historical manifest differs from original materialization: idEqual=%t createdEqual=%t artifactsEqual=%t", v.Manifest.ID == completion.Manifest.ID, v.Manifest.CreatedAt.Equal(completion.Manifest.CreatedAt), reflect.DeepEqual(v.Manifest.Artifacts, completion.Manifest.Artifacts))
		}
		if len(v.Events) != 1 {
			t.Fatalf("historical canonical event count=%d, want 1", len(v.Events))
		}
		if v.Events[0].ID != completion.Events[0].ID {
			t.Fatal("historical event identity differs from original materialization")
		}
		// AW07 deliberately signs the SAME canonical V1 commitment. The receipt
		// is not a separate-domain digest; both original writers use the same
		// status/manifest/ordered-event identity encoding.
		if v.Marker != beforeReplay.Marker || v.NilEvidenceSeal != v.Marker || v.Marker != string(created.Receipt.CanonicalDigest) {
			t.Fatalf("original attached receipt/stored marker/recomputed V1 seal differ: storedEqual=%t nilSealEqual=%t receiptEqual=%t markerLen=%d", v.Marker == beforeReplay.Marker, v.NilEvidenceSeal == v.Marker, v.Marker == string(created.Receipt.CanonicalDigest), len(v.Marker))
		}
		if v.UsesEvidence {
			t.Fatal("attached nil-evidence V1 classifier used managed evidence")
		}
		if blobs.writes() != beforeWrites {
			t.Fatal("historical classification replayed object effects")
		}
	}
	check(t)
	// Erase only this completed execution's TTL rows, AFTER ACK replay. The
	// historical reader receives no old private owner or fallback DTO.
	for _, deletion := range []struct {
		sql  string
		args []any
	}{
		{`DELETE FROM worker_jobs WHERE tenant_id=$1 AND run_id=$2`, []any{tenant, run.ID}},
		{`DELETE FROM attempts WHERE tenant_id=$1 AND attempt_id=$2`, []any{tenant, auth.AttemptID}},
		{`DELETE FROM leases WHERE tenant_id=$1 AND lease_id=$2`, []any{tenant, auth.LeaseID}},
		{`DELETE FROM lease_heads WHERE tenant_id=$1 AND run_id=$2`, []any{tenant, run.ID}},
		{`DELETE FROM quota_reservations WHERE tenant_id=$1 AND quota_reservation_id=$2`, []any{tenant, completion.ReservationID}},
		{`DELETE FROM attached_worker_attempt_heads WHERE tenant_id=$1 AND owner_user_id=$2 AND worker_id=$3`, []any{tenant, auth.OwnerUserID, auth.WorkerID}},
		{`DELETE FROM attached_worker_connections WHERE tenant_id=$1 AND owner_user_id=$2 AND worker_id=$3`, []any{tenant, auth.OwnerUserID, auth.WorkerID}},
	} {
		if _, err := db.ExecContext(ctx, deletion.sql, deletion.args...); err != nil {
			t.Fatal("remove exact old TTL execution row", err)
		}
		var remaining uint64
		countSQL := strings.Replace(deletion.sql, "DELETE FROM", "SELECT COUNT(*) FROM", 1)
		if err := db.QueryRowContext(ctx, countSQL, deletion.args...).Scan(&remaining); err != nil || remaining != 0 {
			t.Fatalf("old execution row remains available after committed removal: count=%d err=%v", remaining, err)
		}
	}
	check(t)
	var head string
	var headRevision uint64
	if err := db.QueryRowContext(ctx, `SELECT revision,CAST(record AS String) FROM run_explanation_heads_v1 WHERE tenant_id=$1 AND run_id=$2`, tenant, run.ID).Scan(&headRevision, &head); err != nil {
		t.Fatal("original attached admission head", err)
	}
	// Attached nil-evidence seals do not depend on a managed locator. A corrupt
	// or missing head remains positive; adding a dependency would be incorrect.
	for _, mode := range []string{"corrupt_head", "missing_head"} {
		t.Run(mode, func(t *testing.T) {
			t.Cleanup(func() {
				cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if _, err := db.ExecContext(cleanup, `UPSERT INTO run_explanation_heads_v1 (tenant_id,run_id,revision,record) VALUES ($1,$2,$3,CAST($4 AS JsonDocument))`, tenant, run.ID, headRevision, head); err != nil {
					t.Error("restore original head", err)
				}
				check(t)
			})
			var err error
			if mode == "corrupt_head" {
				_, err = db.ExecContext(ctx, `UPDATE run_explanation_heads_v1 SET record=CAST($3 AS JsonDocument) WHERE tenant_id=$1 AND run_id=$2`, tenant, run.ID, "{}")
			} else {
				_, err = db.ExecContext(ctx, `DELETE FROM run_explanation_heads_v1 WHERE tenant_id=$1 AND run_id=$2`, tenant, run.ID)
			}
			if err != nil {
				t.Fatal(err)
			}
			check(t)
		})
	}
	for _, fault := range []string{"manifest", "marker", "canonical_body"} {
		t.Run(fault, func(t *testing.T) {
			var restore func(context.Context) error
			switch fault {
			case "manifest":
				var manifest domain.ArtifactManifest
				if json.Unmarshal([]byte(beforeReplay.Manifest), &manifest) != nil {
					t.Fatal("actual stored manifest")
				}
				manifest.CreatedAt = manifest.CreatedAt.Add(-time.Microsecond)
				if manifest.ValidateForRun(run) != nil {
					t.Fatal("manifest corruption must remain valid DATA")
				}
				raw, err := json.Marshal(manifest)
				if err != nil {
					t.Fatal(err)
				}
				restore = func(c context.Context) error {
					_, err := db.ExecContext(c, `UPDATE artifact_manifests SET payload=CAST($3 AS JsonDocument) WHERE tenant_id=$1 AND artifact_manifest_id=$2`, tenant, completion.Manifest.ID, beforeReplay.Manifest)
					return err
				}
				t.Cleanup(func() {
					cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					if err := restore(cleanup); err != nil {
						t.Error(err)
					}
					check(t)
				})
				if _, err := db.ExecContext(ctx, `UPDATE artifact_manifests SET payload=CAST($3 AS JsonDocument) WHERE tenant_id=$1 AND artifact_manifest_id=$2`, tenant, completion.Manifest.ID, string(raw)); err != nil {
					t.Fatal(err)
				}
			case "marker":
				changed := "0" + beforeReplay.Marker[1:]
				if changed == beforeReplay.Marker {
					changed = "1" + beforeReplay.Marker[1:]
				}
				restore = func(c context.Context) error {
					_, err := db.ExecContext(c, `UPDATE run_finalizations SET content_digest=$3 WHERE tenant_id=$1 AND run_id=$2`, tenant, run.ID, beforeReplay.Marker)
					return err
				}
				t.Cleanup(func() {
					cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
					defer cancel()
					if err := restore(cleanup); err != nil {
						t.Error(err)
					}
					check(t)
				})
				if _, err := db.ExecContext(ctx, `UPDATE run_finalizations SET content_digest=$3 WHERE tenant_id=$1 AND run_id=$2`, tenant, run.ID, changed); err != nil {
					t.Fatal(err)
				}
			case "canonical_body":
				key := completion.Events[0].Payload.Key
				blobs.mu.Lock()
				original := bytes.Clone(blobs.objects[key])
				blobs.mu.Unlock()
				t.Cleanup(func() { blobs.mu.Lock(); blobs.objects[key] = original; blobs.mu.Unlock(); check(t) })
				blobs.mu.Lock()
				blobs.objects[key] = []byte(`{"schema":"sessionless.assistant-message.v1","summary":"corrupt"}`)
				blobs.mu.Unlock()
			}
			if _, err := read(); err == nil {
				t.Fatal("corrupt retained canonical material admitted", fault)
			}
			if blobs.writes() != beforeWrites {
				t.Fatal("failed classifier repaired/replayed objects")
			}
		})
	}
	afterFaults, err := readNativeAttachedCanonicalRows(ctx, db, tenant, run.ID, completion.Manifest.ID, completion.Events[0].ID, auth.AttemptID, auth.OwnerUserID, auth.WorkerID)
	if err != nil || !reflect.DeepEqual(afterFaults, beforeReplay) {
		t.Fatal("history checks changed/restored different canonical postimages", err)
	}
	check(t)
}

type attachedNativeCanonicalRows struct {
	Run, Manifest, Marker, Status, Event    string
	EventCount, FinalizationCount, ACKCount uint64
}

func readNativeAttachedCanonicalRows(ctx context.Context, db *sql.DB, tenant domain.TenantID, run domain.RunID, manifest domain.ArtifactManifestID, event domain.SessionEventID, attempt domain.AttemptID, owner domain.UserID, worker domain.AttachedWorkerID) (attachedNativeCanonicalRows, error) {
	var v attachedNativeCanonicalRows
	if err := db.QueryRowContext(ctx, `SELECT CAST(payload AS String) FROM runs WHERE tenant_id=$1 AND run_id=$2`, tenant, run).Scan(&v.Run); err != nil {
		return v, err
	}
	if err := db.QueryRowContext(ctx, `SELECT CAST(payload AS String) FROM artifact_manifests WHERE tenant_id=$1 AND artifact_manifest_id=$2`, tenant, manifest).Scan(&v.Manifest); err != nil {
		return v, err
	}
	if err := db.QueryRowContext(ctx, `SELECT terminal_status,content_digest FROM run_finalizations WHERE tenant_id=$1 AND run_id=$2`, tenant, run).Scan(&v.Status, &v.Marker); err != nil {
		return v, err
	}
	var r domain.Run
	if json.Unmarshal([]byte(v.Run), &r) != nil || r.ID != run || r.TenantID != tenant {
		return v, errors.New("stored canonical Run scope")
	}
	if err := db.QueryRowContext(ctx, `SELECT CAST(record AS String) FROM session_events WHERE tenant_id=$1 AND session_id=$2 AND sequence=$3`, tenant, r.SessionID, uint64(1)).Scan(&v.Event); err != nil {
		return v, err
	}
	var e domain.SessionEvent
	if json.Unmarshal([]byte(v.Event), &e) != nil || e.ID != event {
		return v, errors.New("stored canonical event identity")
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM session_events WHERE tenant_id=$1 AND session_id=$2`, tenant, r.SessionID).Scan(&v.EventCount); err != nil {
		return v, err
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM run_finalizations WHERE tenant_id=$1 AND run_id=$2`, tenant, run).Scan(&v.FinalizationCount); err != nil {
		return v, err
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM attached_worker_attempt_messages WHERE tenant_id=$1 AND owner_user_id=$2 AND worker_id=$3 AND attempt_id=$4 AND direction=$5 AND kind=$6`, tenant, owner, worker, attempt, string(domain.AttachedWorkerAttemptPlatformToWorker), string(domain.AttachedWorkerAttemptMessageTerminalCommitted)).Scan(&v.ACKCount); err != nil {
		return v, err
	}
	if v.EventCount != 1 || v.FinalizationCount != 1 || v.ACKCount != 1 || v.Status != string(domain.RunSucceeded) || len(v.Marker) != 64 || strings.Contains(v.Marker, ":") {
		return v, errors.New("canonical commit/ACK cardinality")
	}
	return v, nil
}
