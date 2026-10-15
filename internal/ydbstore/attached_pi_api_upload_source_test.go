package ydbstore

import (
	"crypto/sha256"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func piUploadSourceRequest(t *testing.T, g *piUseSourceFixture) (AttachedWorkerOutputUploadAdmissionV1, AttachedWorkerOutputReceiptV1) {
	t.Helper()
	piNewWorkReadyProtocol(t, g, attachedworkerprotocol.FeatureOutputReceipt)
	a := g.auth
	bucket, err := webBucket(string(a.OwnerUserID))
	if err != nil {
		t.Fatal(err)
	}
	g.setRow(t, piTerminalMembershipSQL, []driver.Value{int64(bucket), string(a.OwnerUserID), string(a.TenantID)},
		g.sql.rows[piUseSQL(computeChoiceMembershipSQL)][0])
	sessionKey := []driver.Value{string(a.TenantID), string(g.loaded.Run.SessionID)}
	g.setRow(t, `SELECT record FROM sessions WHERE tenant_id = $1 AND session_id = $2`, sessionKey,
		g.sql.rows[piUseSQL(computeChoiceSessionSQL)][0])
	g.setRow(t, `SELECT record FROM session_deletions WHERE tenant_id = $1 AND session_id = $2`, sessionKey)
	observation := attachedworkeroutput.ProcessObservationV1{Version: 1, DescendantsReaped: true, BoundaryReleased: true,
		CleanupSucceeded: true, CredentialReleaseRequired: true, CredentialReleased: true}
	observationDigest, err := observation.Digest()
	if err != nil {
		t.Fatal(err)
	}
	schema := sha256.Sum256([]byte(attachedworkeroutput.StructuredFinalSchemaV1))
	metadata := uploadReceiptData(t).UploadPlan.Ordinals[0].Metadata
	r := AttachedWorkerOutputUploadAdmissionV1{Authorization: a, SessionID: g.loaded.Run.SessionID, HarnessBindingDigest: g.binding,
		ProfileDigest: g.loaded.Job.HarnessBinding.Backend.BackendProfileDigest, SchemaDigest: hex.EncodeToString(schema[:]),
		FinalDigest: strings.Repeat("b", 64), AnswerDigest: strings.Repeat("c", 64), ProducerDigest: strings.Repeat("d", 64),
		Nonce: "synthetic-source-upload", RemainingNanoseconds: uint64(10 * time.Second), Observation: observation,
		ObservationDigest: observationDigest, Artifacts: []attachedworkeroutput.UploadArtifactMetadataV1{metadata}}
	if err := r.validate(); err != nil {
		t.Fatal(err)
	}
	plan, err := r.planAt(g.at, g.at.Add(10*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	prior := AttachedWorkerOutputReceiptV1{Version: 1, Binding: receiptBindingWithoutBearer(a), Nonce: r.Nonce,
		Observation: observation, ObservationDigest: observationDigest, CredentialRequired: true,
		HarnessBindingDigest: g.binding, Status: domain.AttachedWorkerTerminalSucceeded, CreatedAt: g.at, UploadPlan: &plan}
	if err := validateUploadReceiptData(prior); err != nil {
		t.Fatal(err)
	}
	return r, prior
}

func piUploadSetSourceReceipt(t *testing.T, g *piUseSourceFixture, prior *AttachedWorkerOutputReceiptV1) {
	t.Helper()
	a := g.auth
	key := []driver.Value{string(a.TenantID), string(a.RunID), string(a.OwnerUserID), string(a.WorkerID), string(a.AttemptID), int64(a.LeaseGeneration)}
	if prior == nil {
		g.setRow(t, piPublicationReceiptSQL, key)
		return
	}
	g.setRow(t, piPublicationReceiptSQL, key, string(prior.CanonicalDigest), prior.CandidateFingerprint, prior.CreatedAt, choiceFixtureJSON(t, prior))
}

// The actual admission and ordinal-claim methods stop at their FIRST write.
// Qualified-source positives exercise the real caller/control flow, but this
// is synthetic SQL/catalog/protocol data, not a PUT capability/native proof.
func TestPiAPIUploadActualAdmissionAndClaimRequireCurrentPolicy(t *testing.T) {
	for _, operation := range []string{"admit", "claim"} {
		t.Run(operation, func(t *testing.T) {
			for _, mode := range []string{"current", "nil_catalog", "revoked", "replaced", "api_generation", "membership_version", "session_deletion"} {
				t.Run(mode, func(t *testing.T) {
					g, f := piNewWorkSource(t)
					r, prior := piUploadSourceRequest(t, g)
					if operation == "claim" {
						piUploadSetSourceReceipt(t, g, &prior)
					} else {
						piUploadSetSourceReceipt(t, g, nil)
					}
					if mode != "current" {
						piNewWorkChangeAuthority(t, g, mode)
					}
					before := g.sql.transactions
					var err error
					var status ports.AttachedWorkerExecutionStatus
					if operation == "claim" {
						got, e := g.store.ClaimAttachedWorkerOutputUpload(g.ctx, AttachedWorkerOutputUploadClaimV1{Version: 1,
							Authorization: r.Authorization, Nonce: r.Nonce, PlanDigest: prior.UploadPlan.PlanDigest,
							Ordinal: 0, RemainingNanoseconds: r.RemainingNanoseconds})
						status, err = got.Status, e
						if got.Plan.Version != 0 || got.AcceptedAttempt != (domain.AttachedWorkerAttemptV1{}) || !got.CheckedAt.IsZero() || !got.ExpiresAt.IsZero() {
							t.Fatal("claim leaked speculative plan/head/times")
						}
					} else {
						got, e := g.store.AdmitAttachedWorkerOutputUpload(g.ctx, r)
						status, err = got.Status, e
						if got.Plan.Version != 0 || got.AcceptedAttempt != (domain.AttachedWorkerAttemptV1{}) || !got.CheckedAt.IsZero() {
							t.Fatal("admission leaked speculative plan/head/times")
						}
					}
					if g.sql.transactions != before+1 || g.sql.active {
						t.Fatal("upload effect escaped original single callback")
					}
					if mode == "current" {
						if !errors.Is(err, errPiNewWorkFirstWrite) || f.firstWrites != 1 || !strings.Contains(f.writeSQL, "attached_worker_output_receipts ") {
							t.Fatalf("operation=%s current source did not reach original first custody write: writes=%d err=%v", operation, f.firstWrites, err)
						}
					} else if err != nil || status != ports.AttachedWorkerExecutionDenied || f.firstWrites != 0 {
						t.Fatalf("operation=%s mode=%s stale source reached new effect: status=%s writes=%d err=%v", operation, mode, status, f.firstWrites, err)
					}
				})
			}
		})
	}
}

func TestPiAPIUploadActualReadonlyReconcileAfterPolicyRevoke(t *testing.T) {
	g, f := piNewWorkSource(t)
	r, prior := piUploadSourceRequest(t, g)
	piUploadSetSourceReceipt(t, g, &prior)
	piNewWorkChangeAuthority(t, g, "revoked")
	g.store.attachedPiAPIPolicyCatalog = nil
	before := g.sql.transactions
	got, err := g.store.ReconcileAttachedWorkerOutputUpload(g.ctx, AttachedWorkerOutputUploadReconciliationV1{Version: 1,
		Authorization: r.Authorization, Nonce: r.Nonce, ProducerDigest: r.ProducerDigest, FinalDigest: r.FinalDigest,
		RemainingNanoseconds: r.RemainingNanoseconds})
	if err != nil || got.Status != ports.AttachedWorkerExecutionReplayed || got.Plan.PlanDigest != prior.UploadPlan.PlanDigest ||
		got.Plan.Ordinals[0].State != prior.UploadPlan.Ordinals[0].State || f.firstWrites != 0 || g.sql.transactions != before+1 || g.sql.active {
		t.Fatalf("exact pending custody must reconcile after revoke without effect: status=%s writes=%d err=%v", got.Status, f.firstWrites, err)
	}
	for _, q := range g.sql.transactionQueries {
		if q == piUseSQL(piPolicyReadSQL) || q == piUseSQL(providerCredentialReadSQL) || q == piUseSQL(computeChoiceParticipantSQL) {
			t.Fatal("historical read-only reconciliation consulted a new-use prerequisite")
		}
	}
}
