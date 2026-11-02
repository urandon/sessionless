//go:build sessionless_mcp_offline

package ydbstore_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/providerloopsession"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// STORAGE only. The original physical holder owns its inventory, verification,
// fencing and erasure receipt. This peer actually removes exact in-memory
// objects and checks absence; it never provides a namespace-clean boolean.
type canonicalTerminalBlobs struct {
	*sessionCompactionBlobs
	owner                                     *testing.T
	seed                                      *ydbstore.LocalCanonicalTerminalSeed
	plan                                      domain.TerminalCustodyPlanV1
	drafts                                    []domain.SessionEventDraft
	capsulePuts, eventPuts, deletes, absences int
	capsuleClosed                             bool
}

func (b *canonicalTerminalBlobs) Put(ctx context.Context, tenant domain.TenantID, key string, r io.Reader) (domain.BlobRef, error) {
	if b.plan.Capsule.Key != "" {
		if key == b.plan.Capsule.Key {
			writes, commits, reads := b.seed.TerminalPublicationCounts()
			if writes != 1 || commits != 1 || reads < 1 {
				return domain.BlobRef{}, errors.New("capsule before confirmed original custody/readback")
			}
			b.capsulePuts++
		} else {
			for _, draft := range b.drafts {
				if key == draft.Payload.Key {
					if !b.capsuleClosed {
						return domain.BlobRef{}, errors.New("canonical event before original capsule verification close")
					}
					b.eventPuts++
					break
				}
			}
		}
	}
	return b.sessionCompactionBlobs.Put(ctx, tenant, key, r)
}
func (b *canonicalTerminalBlobs) Open(ctx context.Context, tenant domain.TenantID, ref domain.BlobRef) (io.ReadCloser, error) {
	r, e := b.sessionCompactionBlobs.Open(ctx, tenant, ref)
	if e != nil || b.plan.Capsule.Key == "" || ref.Key != b.plan.Capsule.Key {
		return r, e
	}
	return &terminalSessionReader{ReadCloser: r, closed: func() { b.capsuleClosed = true }}, nil
}
func (b *canonicalTerminalBlobs) Delete(ctx context.Context, tenant domain.TenantID, ref domain.BlobRef) error {
	if ctx.Err() != nil || ref.TenantID != tenant || ref.Validate() != nil || !strings.HasPrefix(ref.Key, domain.SessionRunObjectPrefix(b.seed.Event.TenantID, b.seed.Event.SessionID, *b.seed.Event.RunID)+"attempt-operations-v1/") {
		return errors.New("terminal exact ordinary namespace")
	}
	if _, ok := ctx.Deadline(); !ok {
		return errors.New("terminal delete without original bound")
	}
	a, c := b.seed.TerminalAllocation(b.test())
	if a.State != domain.RegisteredPreparationActiveV1 || c.State != domain.RegisteredPreparationRetiredV1 || c.ValidateTransitionFrom(a) != nil {
		return errors.New("delete before original allocation retirement")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	key := string(tenant) + "\x00" + ref.Key
	if _, ok := b.objects[key]; !ok {
		return errors.New("delete was not an existing original body")
	}
	delete(b.objects, key)
	b.deletes++
	return nil
}

// Set by the owning synchronous test, never supplied to production as a gate.
func (b *canonicalTerminalBlobs) test() *testing.T { return b.owner }
func (b *canonicalTerminalBlobs) RequireBlobAbsent(ctx context.Context, tenant domain.TenantID, ref domain.BlobRef) error {
	if ctx.Err() != nil || ref.TenantID != tenant || ref.Validate() != nil {
		return errors.New("invalid terminal absence target")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.objects[string(tenant)+"\x00"+ref.Key]; ok {
		return errors.New("ordinary body remains")
	}
	b.absences++
	return nil
}

// Same genuine canonical tool lineage, actual terminal composer/publisher,
// keeper+bridge stop/join, retirement, holder erasure and local finalizer.
// SQL/storage/DNS/RemoteAddr are controlled DATA; no native/cloud/Manager/Web.
func TestLocalSessionCredentialedMCPOfflineCanonicalTerminalComplete(t *testing.T) {
	for _, fault := range []string{"none", "rollback", "ambiguous"} {
		t.Run(fault, func(t *testing.T) { runCanonicalTerminalComplete(t, fault) })
	}
}
func runCanonicalTerminalComplete(t *testing.T, fault string) {
	peer := newOfflineModelChainPeer(t)
	seed := ydbstore.NewLocalCanonicalTerminalSeed(t, peer.endpoint())
	profile, err := seed.Source.Configuration()
	if err != nil {
		t.Fatal(err)
	}
	peer.configure(t, profile.ModelID)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancel)
	blobs := &canonicalTerminalBlobs{sessionCompactionBlobs: newSessionCompactionBlobs(), seed: seed, owner: t}
	ref, err := blobs.Put(ctx, seed.Payload.TenantID, seed.Payload.Key, bytes.NewReader(seed.Body))
	if err != nil || ref != seed.Payload {
		t.Fatal("original canonical blob", err)
	}
	session, err := providerloopsession.NewLocalV1(ctx, providerloopsession.LocalConfigV1{Source: seed.Source, DB: seed.DB, StoreOptions: seed.Options, Prepare: seed.Prepare, Operate: seed.Operate, Backend: seed.Backend, Substrate: seed.Substrate, Blobs: blobs})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if e := session.Close(); e != nil {
			t.Error(e)
		}
	})
	mcp := newOfflineMCPPeer(t, seed.Grant.Descriptor, 4, nil, false)
	if err := session.ConfigureMCPOfflineNetwork(mcp.lookup, mcp.dial, mcp.roots); err != nil {
		t.Fatal(err)
	}
	state, err := session.WorkerState()
	if err != nil {
		t.Fatal(err)
	}
	if err := session.PreflightWorkerJob(ctx, seed.Loaded); err != nil {
		t.Fatal(err)
	}
	claim := seed.Claim
	claim.Now = seed.Now()
	claim.ExpiresAt = claim.Now.Add(30 * time.Second)
	lease, err := state.ClaimWorkerLease(ctx, claim)
	if err != nil {
		t.Fatal(err)
	}
	if err := state.StartWorkerJob(ctx, seed.Loaded, lease, seed.Now()); err != nil {
		t.Fatal(err)
	}
	fresh, err := state.(ports.AttemptEffectStoreV1).ReserveAttemptEffect(ctx, ports.ReserveAttemptEffectRequestV1{TenantID: claim.TenantID, RunID: claim.RunID, AttemptID: claim.AttemptID, LeaseID: lease.ID, FenceToken: lease.FenceToken, PhysicalInvocationClaimID: "physical-canonical-terminal"})
	if err != nil || fresh.Status != ports.AttemptEffectOwnedV1 {
		t.Fatal(err)
	}
	if _, err := session.EnterWorkerEffect(ctx, fresh); err != nil {
		t.Fatal(err)
	}
	result, err := session.RunWorkerLoop(ctx)
	if err != nil || result.Tool == nil || !result.Model.SendAttempted || !result.Tool.SendAttempted || !seed.ExactThreeEffects() {
		t.Fatalf("same worker tool loop: %v trace=%s", err, seed.TerminalTrace())
	}
	mcp.wait(t)
	wires := peer.wait(t, false)
	reservations, observed := seed.OperationReadback(t)
	if len(reservations) != 3 || len(observed) != 3 || len(wires) != 2 {
		t.Fatal("original three-operation frontier missing")
	}
	h, err := session.PrepareTerminal(ctx)
	if err != nil {
		t.Fatal("original terminal freeze", err, seed.TerminalTrace())
	}
	output, err := h.PrepareOutput(ctx)
	if err != nil {
		t.Fatal("original terminal output", err, seed.TerminalTrace())
	}
	blobs.plan, err = output.Plan()
	if err != nil {
		t.Fatal(err)
	}
	blobs.drafts, err = output.Drafts()
	if err != nil {
		t.Fatal(err)
	}
	if err := h.PublishOutput(ctx); err != nil {
		t.Fatal("original terminal publication", err, seed.TerminalTrace())
	}
	before := seed.TerminalRun(t)
	if before.TerminalCustody == nil || before.TerminalCustody.Plan != blobs.plan || before.TerminalCustody.LocalEvidence != nil || blobs.capsulePuts != 1 || blobs.eventPuts != 3 || !blobs.capsuleClosed {
		t.Fatal("not original complete publisher custody before cleanup")
	}
	blobs.mu.Lock()
	capsule := bytes.Clone(blobs.objects[string(seed.Payload.TenantID)+"\x00"+blobs.plan.Capsule.Key])
	retained := make(map[string][]byte)
	for key, body := range blobs.objects {
		if !strings.Contains(key, "/attempt-operations-v1/") {
			retained[key] = bytes.Clone(body)
		}
	}
	blobs.mu.Unlock()
	var sealed struct {
		Schema     string                                      `json:"schema"`
		Accounting ports.OwnedOperationAccountingDispositionV3 `json:"accounting_disposition"`
		Events     []struct {
			ID      domain.SessionEventID   `json:"id"`
			Kind    domain.SessionEventKind `json:"kind"`
			Payload domain.BlobRef          `json:"payload"`
		} `json:"events"`
	}
	if json.Unmarshal(capsule, &sealed) != nil || sealed.Schema != "sessionless.terminal-custody.v2" || len(sealed.Events) != 3 || len(sealed.Accounting.Operations) != 3 || sealed.Accounting.AggregateDebit != reservations[2].AggregateDebit {
		t.Fatal("actual capsule lost original tool frontier/accounting")
	}
	clear(capsule)
	cleanup, err := h.FinishCleanup(ctx)
	if err != nil || cleanup == nil {
		t.Fatal("original stop/join/retire/physical erase", err, seed.TerminalTrace())
	}
	if !reflect.DeepEqual(before, seed.TerminalRun(t)) || blobs.deletes != 6 || blobs.absences != 6 {
		t.Fatal("cleanup changed canonical outcome or missed actual ordinary bodies")
	}
	blobs.mu.Lock()
	for key, body := range retained {
		if !bytes.Equal(body, blobs.objects[key]) {
			t.Error("retained original/canonical output changed", key)
		}
	}
	for key := range blobs.objects {
		if strings.Contains(key, "/attempt-operations-v1/") {
			t.Error("ordinary body remains", key)
		}
	}
	blobs.mu.Unlock()
	if fault != "none" {
		seed.LoseNextTerminalCommit(t, fault)
	}
	if err := cleanup.Complete(ctx); err != nil {
		t.Fatal("same original local finalizer", err, seed.TerminalTrace())
	}
	got := seed.TerminalSnapshot(t)
	if got.Run.Status != domain.RunSucceeded || got.Attempt.Status != domain.AttemptSucceeded || got.Reservation.Status != domain.ReservationCommitted || got.Finalizations != 1 || len(got.Events) != 3 || got.Session.LastEventSequence != seed.Event.Sequence+3 || !seed.TerminalStorageReleased() {
		t.Fatalf("canonical terminal/capacity effects missing: run=%s quota=%s events=%d trace=%s", got.Run.Status, got.Reservation.Status, len(got.Events), seed.TerminalTrace())
	}
	if got.Run.TerminalCustody == nil || got.Run.TerminalCustody.LocalEvidence == nil || got.Run.TerminalCustody.Correspondence == nil || !reflect.DeepEqual(got.Reservation.LocalCapacityAssociation, seed.Loaded.Reservation.LocalCapacityAssociation) || got.Reservation.SubscriptionConnectionID != "" {
		t.Fatal("local canonical evidence/association not original")
	}
	if got.Commits != 1 || (fault == "none" && got.LostACKs != 0) || (fault != "none" && got.LostACKs != 1) {
		t.Fatal("fault did not target one actual canonical commit", got.Commits, got.LostACKs)
	}
	accountingDigest, e := sealed.Accounting.Digest()
	if e != nil || got.Run.TerminalCustody.Correspondence.AccountingDispositionDigest != accountingDigest || got.Run.TerminalCustody.LocalEvidence.AccountingDispositionDigest != accountingDigest {
		t.Fatal("actual capsule accounting did not bind canonical evidence/correspondence", e)
	}
	for i, row := range sealed.Accounting.Operations {
		rd, _ := reservations[i].Digest()
		od, _ := observed[i].Digest()
		if row.Sequence != uint64(i+1) || row.Kind != reservations[i].Operation.Kind || row.Reserved != reservations[i].Operation.Debit || row.ReservationDigest != rd || row.ObservationDigest != od || !reflect.DeepEqual(row.Usage, observed[i].Usage) {
			t.Fatal("retained accounting changed original operation")
		}
	}
	if got.Reservation.CapacityUnits != seed.Loaded.Reservation.CapacityUnits || !got.Reservation.HeldAt.Equal(seed.Loaded.Reservation.HeldAt) || !got.Reservation.ExpiresAt.Equal(seed.Loaded.Reservation.ExpiresAt) {
		t.Fatal("terminal quota changed original held shape")
	}
	var evidence domain.ExecutionEvidenceV2
	if json.Unmarshal([]byte(got.EvidenceRecord), &evidence) != nil || evidence.Kind != domain.ExecutionEvidenceLocalV1 || evidence.ManagedV1 != nil || evidence.LocalV1 == nil || evidence.LocalV1.Validate() != nil || !reflect.DeepEqual(evidence.LocalV1, got.Run.TerminalCustody.LocalEvidence) {
		t.Fatal("terminal evidence is not exact genuine local V2")
	}
	projectable := 0
	for _, draft := range blobs.drafts {
		if draft.ProjectionEligible() {
			projectable++
		}
	}
	projected, indexes := seed.TerminalProjectionCounts()
	if projected != projectable || indexes != [4]int{projectable, projectable, projectable, projectable} {
		t.Fatal("actual original frontend binding projection effects missing", projected, indexes)
	}
	for i, kind := range []domain.SessionEventKind{domain.SessionEventToolCall, domain.SessionEventToolResult, domain.SessionEventAssistantMessage} {
		if got.Events[i].Kind != kind || got.Events[i].ID != sealed.Events[i].ID || got.Events[i].Payload != sealed.Events[i].Payload || got.Events[i].Sequence != seed.Event.Sequence+uint64(i)+1 {
			t.Fatal("canonical output event/capsule correspondence changed")
		}
		blobs.mu.Lock()
		body := bytes.Clone(blobs.objects[string(got.Events[i].TenantID)+"\x00"+got.Events[i].Payload.Key])
		blobs.mu.Unlock()
		sum := sha256.Sum256(body)
		if int64(len(body)) != got.Events[i].Payload.Size || hex.EncodeToString(sum[:]) != got.Events[i].Payload.SHA256 {
			t.Fatal("physical canonical event bytes differ after original erasure")
		}
		if i == 2 {
			var message struct {
				Schema, Summary string
				Manifest        domain.ArtifactManifestID `json:"artifact_manifest_id"`
			}
			if json.Unmarshal(body, &message) != nil || message.Schema != "sessionless.assistant-message.v1" || message.Summary != "offline joined final answer" || message.Manifest != got.Manifest.ID {
				t.Fatal("actual final MODEL answer changed in canonical output")
			}
		}
		clear(body)
	}
	material := seed.TerminalMaterial(t)
	if material.ThroughSequence != got.Session.LastEventSequence || material.LastEventID != got.Events[2].ID {
		t.Fatal("actual canonical material frontier did not advance")
	}
	beforeStats, puts, deletes := seed.Stats(), blobs.capsulePuts+blobs.eventPuts, blobs.deletes
	if err := cleanup.Complete(ctx); err != nil {
		t.Fatal("original exact terminal replay", err, seed.TerminalTrace())
	}
	if !reflect.DeepEqual(got, seed.TerminalSnapshot(t)) || seed.Stats() != beforeStats || puts != blobs.capsulePuts+blobs.eventPuts || deletes != blobs.deletes {
		t.Fatal("terminal replay repeated effects")
	}
	if seed.MaxCallbackSQL() > 32 || seed.TerminalRawStatements() != 64 {
		t.Fatal("original authority bound or exact canonical inventory changed", seed.MaxCallbackSQL(), seed.TerminalRawStatements())
	}
	t.Logf("authority max SQL=%d; distinct canonical helper max SQL=%d; canonical mutations=%d; projections=%d; lost ACK=%d", seed.MaxCallbackSQL(), seed.TerminalRawStatements(), got.Mutations, projected, got.LostACKs)
}
