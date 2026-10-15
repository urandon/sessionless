package ydbstore

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
)

// Tests the exact binding helper used after strict row read. Not SQL/current
// Source authority, transport authentication or native deletion-race evidence.
func TestAttemptOutputUploadReconcilePreservesPendingAndIdentity(t *testing.T) {
	prior, claim, at := uploadClaimData(t)
	spent, err := spendUploadReceiptOrdinal(prior, claim, at, at.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	request := AttachedWorkerOutputUploadReconciliationV1{Version: 1, Authorization: claim.Authorization,
		Nonce: claim.Nonce, ProducerDigest: spent.UploadPlan.Identity.ProducerDigest, FinalDigest: spent.UploadPlan.Identity.FinalDigest,
		RemainingNanoseconds: uint64(time.Second)}
	if err := request.validate(); err != nil || !uploadReconciliationMatchesRecord(spent, request) {
		t.Fatalf("lost admission/claim observation failed: %v", err)
	}
	observed := spent.UploadPlan.Clone()
	if observed.Ordinals[0].State != attachedworkeroutput.UploadOrdinalWritePendingV1 ||
		observed.Ordinals[0].GrantDigest != spent.UploadPlan.Ordinals[0].GrantDigest ||
		observed.Identity.ExpiresAt != prior.UploadPlan.Identity.ExpiresAt || attachedWorkerReceiptAllowsDeletion(spent) {
		t.Fatal("metadata observation renewed/reset pending custody or allowed deletion")
	}
	observed.Ordinals[0].State = attachedworkeroutput.UploadOrdinalUnissuedV1
	if spent.UploadPlan.Ordinals[0].State != attachedworkeroutput.UploadOrdinalWritePendingV1 {
		t.Fatal("observation retained a mutable plan alias")
	}
	for name, mutate := range map[string]func(*AttachedWorkerOutputUploadReconciliationV1){
		"nonce":      func(r *AttachedWorkerOutputUploadReconciliationV1) { r.Nonce = "other-nonce" },
		"producer":   func(r *AttachedWorkerOutputUploadReconciliationV1) { r.ProducerDigest = strings.Repeat("b", 64) },
		"final":      func(r *AttachedWorkerOutputUploadReconciliationV1) { r.FinalDigest = strings.Repeat("b", 64) },
		"revision":   func(r *AttachedWorkerOutputUploadReconciliationV1) { r.Authorization.ExpectedAttemptRevision++ },
		"connection": func(r *AttachedWorkerOutputUploadReconciliationV1) { r.Authorization.ConnectionID = "other-connection" },
	} {
		t.Run(name, func(t *testing.T) {
			changed := request
			mutate(&changed)
			if uploadReconciliationMatchesRecord(spent, changed) {
				t.Fatalf("foreign original identity observed: %s", name)
			}
		})
	}
	encoded, err := json.Marshal(request)
	if err != nil || string(encoded) != `"[REDACTED]"` {
		t.Fatal("private reconciliation request not redacted")
	}
}

func TestAttemptOutputUploadReconcilePreflight(t *testing.T) {
	prior, claim, _ := uploadClaimData(t)
	request := AttachedWorkerOutputUploadReconciliationV1{Version: 1, Authorization: claim.Authorization,
		Nonce: claim.Nonce, ProducerDigest: prior.UploadPlan.Identity.ProducerDigest, FinalDigest: prior.UploadPlan.Identity.FinalDigest,
		RemainingNanoseconds: uint64(time.Second)}
	for name, mutate := range map[string]func(*AttachedWorkerOutputUploadReconciliationV1){
		"version":          func(r *AttachedWorkerOutputUploadReconciliationV1) { r.Version++ },
		"no revision":      func(r *AttachedWorkerOutputUploadReconciliationV1) { r.Authorization.ExpectedAttemptRevision = 0 },
		"invalid producer": func(r *AttachedWorkerOutputUploadReconciliationV1) { r.ProducerDigest = strings.Repeat("x", 1<<20) },
		"invalid final":    func(r *AttachedWorkerOutputUploadReconciliationV1) { r.FinalDigest = strings.Repeat("F", 64) },
		"zero remainder":   func(r *AttachedWorkerOutputUploadReconciliationV1) { r.RemainingNanoseconds = 0 },
		"new grace": func(r *AttachedWorkerOutputUploadReconciliationV1) {
			r.RemainingNanoseconds = uint64(15*time.Second) + 1
		},
	} {
		t.Run(name, func(t *testing.T) {
			changed := request
			mutate(&changed)
			if changed.validate() == nil {
				t.Fatalf("invalid reconciliation accepted: %s", name)
			}
		})
	}
}
