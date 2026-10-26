package ydbstore_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
	"gitcode.com/urandon/sessionless/test/support/aw07native"
)

// Native YDB transactions and signed Ready/Offer/Claim are real. The declared
// output and immutable blob peer are synthetic: this is not a native producer,
// HTTP/authentication, Object Storage, deletion or real-profile qualification.
type inlineNativeFixtureV2 struct {
	store   *ydbstore.Store
	db      *sql.DB
	ctx     context.Context
	cancel  context.CancelFunc
	prepare attachedworkeroutput.AttachedWorkerInlinePrepareV2
	blobs   *inlineNativeBlobsV2
	body    []byte
}

func newInlineNativeFixtureV2(t *testing.T) *inlineNativeFixtureV2 {
	t.Helper()
	store, db, ctx, tenant := ydbstore.OpenTerminalHistoryNativeV1(t)
	ready := aw07native.Ready(t, store, db, aw07native.ReadyOptions{Suffix: "inline-v2", TenantID: tenant,
		Features: []attachedworkerprotocol.ProtocolFeatureV1{attachedworkerprotocol.FeatureOutputReceipt, attachedworkerprotocol.FeatureInlineOutputV2}})
	claimed := aw07native.Claim(t, store, db, ready.Worker, ready.Connection, ready.SecretDigest,
		aw07native.OfferOptions{Suffix: aw07native.UniqueSuffix(t, "inline-v2")})
	loaded, found, err := store.LoadWorkerJob(ctx, tenant, claimed.Request.RunID)
	if err != nil || !found {
		t.Fatalf("load original claimed worker job: found=%t err=%v", found, err)
	}
	harness, err := loaded.Job.HarnessBinding.Digest()
	if err != nil {
		t.Fatal(err)
	}
	// Only the authoritative DB clock creates the export authority window.
	// The 12s caller failure bound is shorter than this resource's 15s window;
	// none of these tests uses expiry to decide correctness.
	var at time.Time
	if err := db.QueryRowContext(ctx, `SELECT CurrentUtcTimestamp()`).Scan(&at); err != nil {
		t.Fatal(err)
	}
	call, cancel := context.WithTimeout(ctx, 12*time.Second)
	t.Cleanup(cancel)
	body := []byte(" {\"integer\":9007199254740993123456789,\"exponent\":1e999} \n")
	answer := "original bounded native SQL answer"
	sum, answerSum := sha256.Sum256(body), sha256.Sum256([]byte(answer))
	f := &inlineNativeFixtureV2{store: store, db: db, ctx: call, cancel: cancel, body: body,
		prepare: attachedworkeroutput.AttachedWorkerInlinePrepareV2{Version: 2, Authorization: claimed.Request, Nonce: "inline-native-original",
			Candidate: attachedworkeroutput.InlineCandidateV2{Summary: answer, Artifacts: []attachedworkeroutput.InlineArtifactV2{{Name: "report.json", MediaType: "application/json", Size: uint64(len(body)), SHA256: hex.EncodeToString(sum[:])}}},
			Producer: attachedworkeroutput.InlineProducerBindingV2{SessionID: loaded.Job.SessionID, HarnessBindingDigest: harness, ReservationID: loaded.Job.ReservationID,
				AcceptedAt: claimed.Attempt.CreatedAt, SchemaDigest: strings.Repeat("a", 64), FinalDigest: strings.Repeat("b", 64), AnswerDigest: hex.EncodeToString(answerSum[:])},
			Observation: attachedworkeroutput.ProcessObservationV1{Version: 1, DescendantsReaped: true, BoundaryReleased: true, CleanupSucceeded: true},
			ExpiresAt:   at.UTC().Truncate(time.Microsecond).Add(15 * time.Second)}}
	f.blobs = &inlineNativeBlobsV2{attachedHistoryNativeBlobs: &attachedHistoryNativeBlobs{objects: make(map[string][]byte)}, db: db, auth: claimed.Request, plan: f.prepare}
	t.Cleanup(func() {
		f.blobs.mu.Lock()
		defer f.blobs.mu.Unlock()
		for _, body := range f.blobs.objects {
			clear(body)
		}
		clear(f.body)
	})
	return f
}

func readInlineNativeReceiptV2(ctx context.Context, db *sql.DB, auth ports.AttachedWorkerSealedInputAuthorization) (ydbstore.AttachedWorkerOutputReceiptV2, error) {
	var raw, columnDigest, fingerprint string
	err := db.QueryRowContext(ctx, `SELECT CAST(payload AS String),canonical_digest,candidate_fingerprint FROM attached_worker_output_receipts
 WHERE tenant_id=$1 AND run_id=$2 AND owner_user_id=$3 AND worker_id=$4 AND attempt_id=$5 AND lease_generation=$6`,
		auth.TenantID, auth.RunID, auth.OwnerUserID, auth.WorkerID, auth.AttemptID, auth.LeaseGeneration).Scan(&raw, &columnDigest, &fingerprint)
	var record ydbstore.AttachedWorkerOutputReceiptV2
	if err != nil {
		return record, err
	}
	// Test-only observation of the actual JsonDocument frame. Serving reads
	// independently use the closed production codec and actual query-key joins.
	var stored struct {
		Encoding string `json:"persistence_encoding"`
		ydbstore.AttachedWorkerOutputReceiptV2
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.DisallowUnknownFields()
	if len(raw) > (512<<10)+42 || decoder.Decode(&stored) != nil || stored.Encoding != "single-answer-v1" ||
		strings.Contains(raw, `"display_text"`) {
		return record, errors.New("native inline receipt lost its single-answer private frame")
	}
	record = stored.AttachedWorkerOutputReceiptV2
	if record.Stage == attachedworkeroutput.InlineCanonicalCopyingV2 || record.Stage == attachedworkeroutput.InlineReadyV2 {
		if record.Materialization.Completion == nil || len(record.Materialization.Completion.Events) != 1 {
			return record, errors.New("native compact receipt lost its original canonical event")
		}
		record.Materialization.Completion.Events[0].DisplayText = strings.TrimSpace(record.Candidate.Summary)
	}
	expected := auth
	expected.PresentedSecretDigest, expected.ExpectedAttemptRevision = "", 0
	if record.Version != 2 ||
		string(record.CanonicalDigest) != columnDigest || record.CandidateFingerprint != fingerprint ||
		record.Binding != expected {
		return record, errors.New("native inline receipt payload/columns/original scope disagree")
	}
	return record, nil
}

func (f *inlineNativeFixtureV2) prepared(t *testing.T) attachedworkeroutput.AttachedWorkerInlineBodyV2 {
	t.Helper()
	fingerprint, err := attachedworkeroutput.InlineOutputRequestFingerprintV2(f.prepare)
	if err != nil {
		t.Fatal(err)
	}
	f.blobs.fingerprint = fingerprint
	result, err := f.store.PrepareAttachedWorkerInlineOutput(f.ctx, f.blobs, f.prepare)
	if err != nil || result.Stage != attachedworkeroutput.InlinePreparedV2 || len(result.Capability) != 64 || result.CanonicalDigest != "" || !result.ExpiresAt.Equal(f.prepare.ExpiresAt) || result.Fingerprint != fingerprint || result.Nonce != f.prepare.Nonce {
		t.Fatalf("original native prepare: stage=%s capability-bytes=%d err=%v", result.Stage, len(result.Capability), err)
	}
	record, err := readInlineNativeReceiptV2(f.ctx, f.db, f.prepare.Authorization)
	if err != nil || record.Stage != attachedworkeroutput.InlinePreparedV2 || record.BodyClaimID != "" || record.CopyOwnerID != "" || record.Materialization.Completion != nil ||
		record.Producer != f.prepare.Producer || !reflect.DeepEqual(record.Candidate, f.prepare.Candidate) || f.blobs.writes() != [2]uint64{} {
		t.Fatalf("prepared native record acquired early write authority: stage=%s err=%v", record.Stage, err)
	}
	return attachedworkeroutput.AttachedWorkerInlineBodyV2{Version: 2, Authorization: f.prepare.Authorization,
		Nonce: result.Nonce, Fingerprint: result.Fingerprint, Capability: result.Capability, ExpiresAt: result.ExpiresAt}
}

type inlineNativeBlobsV2 struct {
	*attachedHistoryNativeBlobs
	db             *sql.DB
	auth           ports.AttachedWorkerSealedInputAuthorization
	plan           attachedworkeroutput.AttachedWorkerInlinePrepareV2
	fingerprint    string
	copyOwner      string
	copyDigest     domain.AttachedWorkerTerminalEvidenceDigest
	checkedPuts    int
	loseFirstReply bool
	beforePut      func() error
	borrowedBodies []*bytes.Reader
}

var errInlineNativeLostPutV2 = errors.New("controlled lost native blob Put acknowledgement")

func (b *inlineNativeBlobsV2) Put(ctx context.Context, tenant domain.TenantID, key string, body io.Reader) (domain.BlobRef, error) {
	if b.beforePut != nil {
		if err := b.beforePut(); err != nil {
			return domain.BlobRef{}, err
		}
	}
	// A separate DB read must see the COMMITTED canonical plan before even
	// consuming this first physical peer Put's bytes. No fake SQL callback.
	record, err := readInlineNativeReceiptV2(ctx, b.db, b.auth)
	if err != nil || record.Stage != attachedworkeroutput.InlineCanonicalCopyingV2 || len(record.BodyClaimID) != 64 || len(record.CopyOwnerID) != 64 ||
		record.CanonicalDigest == "" || record.Materialization.Completion == nil || tenant != b.auth.TenantID ||
		record.Nonce != b.plan.Nonce || record.CandidateFingerprint != b.fingerprint || record.Producer != b.plan.Producer ||
		!reflect.DeepEqual(record.Candidate, b.plan.Candidate) || !record.ExpiresAt.Equal(b.plan.ExpiresAt) {
		return domain.BlobRef{}, fmt.Errorf("physical Put preceded native canonical custody: stage=%s err=%v", record.Stage, err)
	}
	completion := record.Materialization.Completion
	if completion.TenantID != tenant || completion.RunID != b.auth.RunID || completion.AttemptID != b.auth.AttemptID ||
		completion.ReservationID != b.plan.Producer.ReservationID || completion.LeaseID != b.auth.LeaseID || completion.Fence != b.auth.LeaseGeneration ||
		completion.Manifest.TenantID != tenant || completion.Manifest.RunID != b.auth.RunID || len(completion.Manifest.Artifacts) != 1 || len(completion.Events) != 1 {
		return domain.BlobRef{}, errors.New("native canonical plan escaped original completion scope")
	}
	refs := make(map[string]domain.BlobRef)
	for _, artifact := range completion.Manifest.Artifacts {
		refs[artifact.Blob.Key] = artifact.Blob
	}
	for _, event := range completion.Events {
		refs[event.Payload.Key] = event.Payload
	}
	planned, exists := refs[key]
	if !exists || planned.TenantID != tenant || len(refs) != 2 || record.Materialization.EvidenceDigest != record.CanonicalDigest {
		return domain.BlobRef{}, errors.New("physical Put key not owned by complete original native plan")
	}
	b.mu.Lock()
	if b.checkedPuts != 0 && (b.copyOwner != record.CopyOwnerID || b.copyDigest != record.CanonicalDigest) {
		b.mu.Unlock()
		return domain.BlobRef{}, errors.New("native copy owner changed between physical writes")
	}
	b.copyOwner, b.copyDigest = record.CopyOwnerID, record.CanonicalDigest
	b.checkedPuts++
	if borrowed, ok := body.(*bytes.Reader); ok {
		b.borrowedBodies = append(b.borrowedBodies, borrowed)
	}
	lose := b.loseFirstReply && b.checkedPuts == 1
	b.mu.Unlock()
	ref, err := b.attachedHistoryNativeBlobs.Put(ctx, tenant, key, body)
	if err != nil || ref != planned {
		return domain.BlobRef{}, errors.Join(err, errors.New("original canonical Put differs from native committed ref"))
	}
	if lose {
		return domain.BlobRef{}, errInlineNativeLostPutV2 // bytes exist, caller does not know
	}
	return ref, nil
}

type inlineNativeClaimReaderV2 struct {
	ctx      context.Context
	db       *sql.DB
	auth     ports.AttachedWorkerSealedInputAuthorization
	nonce    domain.IdempotencyKey
	fp       string
	body     *bytes.Reader
	entered  chan struct{}
	observed chan error
	resume   chan struct{}
	start    sync.Once
	release  sync.Once
	claimErr error
	borrowed [][]byte
}

func (r *inlineNativeClaimReaderV2) Read(p []byte) (int, error) {
	r.start.Do(func() {
		record, err := readInlineNativeReceiptV2(r.ctx, r.db, r.auth)
		if err != nil || record.Stage != attachedworkeroutput.InlineBodyClaimedV2 || len(record.BodyClaimID) != 64 || record.CopyOwnerID != "" || record.CanonicalDigest != "" || record.Materialization.Completion != nil || record.Nonce != r.nonce || record.CandidateFingerprint != r.fp {
			r.claimErr = fmt.Errorf("first body Read preceded native one-shot claim: stage=%s err=%v", record.Stage, err)
		}
		r.observed <- r.claimErr
		close(r.entered)
		select {
		case <-r.resume:
		case <-r.ctx.Done():
			r.claimErr = errors.Join(r.claimErr, r.ctx.Err())
		}
	})
	if r.claimErr != nil {
		return 0, r.claimErr
	}
	n, err := r.body.Read(p)
	if n > 0 {
		r.borrowed = append(r.borrowed, p[:n])
	}
	return n, err
}

type inlineNativeDeniedReaderV2 struct{ reads int }

func (r *inlineNativeDeniedReaderV2) Read([]byte) (int, error) {
	r.reads++
	return 0, errors.New("duplicate native body must never be read")
}

func TestAttachedInlineOutputYDBClaimAndWriteCustody(t *testing.T) {
	f := newInlineNativeFixtureV2(t)
	request := f.prepared(t)
	if _, err := f.store.PrepareAttachedWorkerInlineOutput(f.ctx, f.blobs, f.prepare); !errors.Is(err, ydbstore.ErrAttachedWorkerAttemptConflict) {
		t.Fatalf("duplicate native Prepare minted another capability: %v", err)
	}
	reader := &inlineNativeClaimReaderV2{ctx: f.ctx, db: f.db, auth: request.Authorization, nonce: request.Nonce, fp: request.Fingerprint, body: bytes.NewReader(f.body), entered: make(chan struct{}), observed: make(chan error, 1), resume: make(chan struct{})}
	f.blobs.beforePut = func() error {
		// Executed by the SAME handler after body reading/materialization, not
		// a polling goroutine. Borrowed probe references deliberately keep its
		// read buffer reachable: this proves erasure, not heap/GC reclamation.
		total := 0
		for _, raw := range reader.borrowed {
			total += len(raw)
			if cap(raw) > len(f.body) || !bytes.Equal(raw, make([]byte, len(raw))) {
				return errors.New("raw native request copy survived until physical Put or exceeded exact capacity")
			}
		}
		if total != len(f.body) {
			return errors.New("raw native request erasure probe missed original bytes")
		}
		return nil
	}
	type outcome struct {
		result attachedworkeroutput.AttachedWorkerInlineResultV2
		err    error
	}
	done, joined := make(chan outcome, 1), make(chan struct{})
	go func() {
		defer close(joined)
		result, err := f.store.SubmitAttachedWorkerInlineOutput(f.ctx, f.blobs, request, int64(len(f.body)), reader)
		done <- outcome{result, err}
	}()
	t.Cleanup(func() {
		f.cancel()
		reader.release.Do(func() { close(reader.resume) })
		bound, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		select {
		case <-joined:
		case <-bound.Done():
			select {
			case <-joined:
			default:
				t.Error("original native Submit did not join fixture cleanup")
			}
		}
	})
	select {
	case <-reader.entered:
	case early := <-done:
		t.Fatalf("native Submit failed before first body Read: %v", early.err)
	case <-f.ctx.Done():
		t.Fatal("native body claim barrier timed out", f.ctx.Err())
	}
	if err := <-reader.observed; err != nil {
		t.Fatal("native committed claim observation failed before body consumption", err)
	}
	// The original handler remains blocked, but its claim has committed.
	// This tests a late duplicate, NOT two prepared-snapshot transactions
	// competing to win the initial claim; that native race remains separate.
	denied := new(inlineNativeDeniedReaderV2)
	if _, err := f.store.SubmitAttachedWorkerInlineOutput(f.ctx, f.blobs, request, int64(len(f.body)), denied); !errors.Is(err, ydbstore.ErrAttachedWorkerAttemptConflict) || denied.reads != 0 || f.blobs.writes() != [2]uint64{} {
		t.Fatalf("late duplicate native submission acquired body/write authority: reads=%d writes=%v err=%v", denied.reads, f.blobs.writes(), err)
	}
	reader.release.Do(func() { close(reader.resume) })
	var completed outcome
	select {
	case completed = <-done:
	case <-f.ctx.Done():
		t.Fatal("original native inline Submit timed out", f.ctx.Err())
	}
	<-joined
	if completed.err != nil || completed.result.Stage != attachedworkeroutput.InlineReadyV2 || completed.result.Capability != "" || completed.result.CanonicalDigest == "" || f.blobs.writes() != [2]uint64{2, 0} {
		t.Fatalf("native canonical promotion: stage=%s writes=%v err=%v", completed.result.Stage, f.blobs.writes(), completed.err)
	}
	f.blobs.mu.Lock()
	if len(f.blobs.borrowedBodies) != 2 {
		t.Error("successful native copy did not observe both original planned readers")
	}
	for _, retained := range f.blobs.borrowedBodies {
		if _, err := retained.Seek(0, io.SeekStart); err != nil {
			t.Error(err)
			continue
		}
		raw, err := io.ReadAll(retained)
		if err != nil || len(raw) == 0 || !bytes.Equal(raw, make([]byte, len(raw))) {
			t.Error("server planner copy remained after successful writes/rereads", err)
		}
	}
	f.blobs.mu.Unlock()
	record, err := readInlineNativeReceiptV2(f.ctx, f.db, request.Authorization)
	if err != nil || record.Stage != attachedworkeroutput.InlineReadyV2 || record.CanonicalDigest != completed.result.CanonicalDigest || record.CopyOwnerID != f.blobs.copyOwner {
		t.Fatalf("native READY did not retain original committed copy identity: stage=%s err=%v", record.Stage, err)
	}
	reconcile, err := f.store.ReconcileAttachedWorkerInlineOutput(f.ctx, attachedworkeroutput.AttachedWorkerInlineReconcileV2{Version: 2, Authorization: request.Authorization, Nonce: request.Nonce, Fingerprint: request.Fingerprint, BodySendBurned: true})
	if err != nil || reconcile.Stage != completed.result.Stage || reconcile.Version != completed.result.Version ||
		reconcile.CanonicalDigest != completed.result.CanonicalDigest || reconcile.Nonce != request.Nonce || reconcile.Fingerprint != request.Fingerprint ||
		reconcile.Capability != "" || !reconcile.ExpiresAt.Equal(request.ExpiresAt) || f.blobs.writes() != [2]uint64{2, 0} {
		t.Fatalf("native READY reconciliation repeated writes or changed identity: stage=%s writes=%v err=%v", reconcile.Stage, f.blobs.writes(), err)
	}
	if _, err := f.store.SubmitAttachedWorkerInlineOutput(f.ctx, f.blobs, request, int64(len(f.body)), denied); !errors.Is(err, ydbstore.ErrAttachedWorkerAttemptConflict) || denied.reads != 0 || f.blobs.writes() != [2]uint64{2, 0} {
		t.Fatalf("native READY reopened body/write authority: reads=%d writes=%v err=%v", denied.reads, f.blobs.writes(), err)
	}
	artifact := record.Materialization.Completion.Manifest.Artifacts[0]
	f.blobs.mu.Lock()
	defer f.blobs.mu.Unlock()
	if !bytes.Equal(f.blobs.objects[artifact.Blob.Key], f.body) {
		t.Fatal("native receipt path changed original JSON semantic bytes")
	}
	if artifact.Name != "report.json" || artifact.MediaType != "application/json" || artifact.Blob.SHA256 != f.prepare.Candidate.Artifacts[0].SHA256 ||
		artifact.Blob.Key != domain.SessionRunObjectPrefix(request.Authorization.TenantID, f.prepare.Producer.SessionID, request.Authorization.RunID)+"artifacts/sha256/"+artifact.Blob.SHA256 {
		t.Fatal("native canonical artifact name/media/hash/key differs from original declaration")
	}
	var answer struct {
		Schema   string                    `json:"schema"`
		Summary  string                    `json:"summary"`
		Manifest domain.ArtifactManifestID `json:"artifact_manifest_id"`
	}
	event := record.Materialization.Completion.Events[0]
	if event.Kind != domain.SessionEventAssistantMessage || event.DisplayText != strings.TrimSpace(f.prepare.Candidate.Summary) || json.Unmarshal(f.blobs.objects[event.Payload.Key], &answer) != nil ||
		answer.Schema != "sessionless.assistant-message.v1" || answer.Summary != f.prepare.Candidate.Summary || answer.Manifest != record.Materialization.Completion.Manifest.ID {
		t.Fatal("native canonical answer payload lost original summary or output manifest")
	}
}

func TestAttachedInlineOutputYDBAmbiguousPutRetainsCustody(t *testing.T) {
	f := newInlineNativeFixtureV2(t)
	request := f.prepared(t)
	f.blobs.loseFirstReply = true
	_, err := f.store.SubmitAttachedWorkerInlineOutput(f.ctx, f.blobs, request, int64(len(f.body)), bytes.NewReader(f.body))
	if !errors.Is(err, errInlineNativeLostPutV2) || f.blobs.writes() != [2]uint64{1, 0} {
		t.Fatalf("native ambiguous Put was hidden or repeated: writes=%v err=%v", f.blobs.writes(), err)
	}
	f.blobs.mu.Lock()
	if len(f.blobs.borrowedBodies) != 1 {
		t.Error("ambiguous Put did not retain exactly its original reader")
	} else {
		retained := f.blobs.borrowedBodies[0]
		if _, seekErr := retained.Seek(0, io.SeekStart); seekErr != nil {
			t.Error(seekErr)
		} else if raw, readErr := io.ReadAll(retained); readErr != nil || !bytes.Equal(raw, f.body) {
			t.Error("ambiguous Put borrowed reader was erased without quiescence", readErr)
		}
	}
	f.blobs.mu.Unlock()
	before, err := readInlineNativeReceiptV2(f.ctx, f.db, request.Authorization)
	if err != nil || before.Stage != attachedworkeroutput.InlineCanonicalCopyingV2 || before.CopyOwnerID != f.blobs.copyOwner || before.CanonicalDigest != f.blobs.copyDigest {
		t.Fatalf("native ambiguous Put lost durable writer custody: stage=%s err=%v", before.Stage, err)
	}
	result, err := f.store.ReconcileAttachedWorkerInlineOutput(f.ctx, attachedworkeroutput.AttachedWorkerInlineReconcileV2{Version: 2, Authorization: request.Authorization, Nonce: request.Nonce, Fingerprint: request.Fingerprint, BodySendBurned: true})
	if err != nil || result.Stage != attachedworkeroutput.InlineCanonicalCopyingV2 || result.Capability != "" || f.blobs.writes() != [2]uint64{1, 0} {
		t.Fatalf("native ambiguous reconciliation claimed READY or repeated Put: stage=%s writes=%v err=%v", result.Stage, f.blobs.writes(), err)
	}
	denied := new(inlineNativeDeniedReaderV2)
	if _, err := f.store.SubmitAttachedWorkerInlineOutput(f.ctx, f.blobs, request, int64(len(f.body)), denied); !errors.Is(err, ydbstore.ErrAttachedWorkerAttemptConflict) || denied.reads != 0 {
		t.Fatalf("ambiguous native Put granted another body claim: reads=%d err=%v", denied.reads, err)
	}
	after, err := readInlineNativeReceiptV2(f.ctx, f.db, request.Authorization)
	if err != nil || !reflect.DeepEqual(before, after) || f.blobs.writes() != [2]uint64{1, 0} {
		t.Fatalf("native replay/reconcile retired or replaced ambiguous custody: writes=%v err=%v", f.blobs.writes(), err)
	}
}
