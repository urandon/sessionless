package ydbstore

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/domain"
)

// These are unit controls for the transition used by the actual Store method,
// NOT SQL serialization, current Source authority or private exporter proof.
func uploadClaimData(t *testing.T) (AttachedWorkerOutputReceiptV1, AttachedWorkerOutputUploadClaimV1, time.Time) {
	t.Helper()
	admission := uploadAdmissionData(t)
	at := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	plan, err := admission.planAt(at, at.Add(15*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	prior := AttachedWorkerOutputReceiptV1{Version: 1, Binding: receiptBindingWithoutBearer(admission.Authorization), Nonce: admission.Nonce,
		Observation: admission.Observation, ObservationDigest: admission.ObservationDigest, CredentialRequired: true,
		HarnessBindingDigest: admission.HarnessBindingDigest, Status: domain.AttachedWorkerTerminalSucceeded, CreatedAt: at, UploadPlan: &plan}
	request := AttachedWorkerOutputUploadClaimV1{Version: 1, Authorization: admission.Authorization, Nonce: admission.Nonce,
		PlanDigest: plan.PlanDigest, Ordinal: 0, RemainingNanoseconds: uint64(5 * time.Second)}
	return prior, request, at.Add(time.Second)
}

func TestAttemptOutputUploadClaimIrreversibleAndIsolated(t *testing.T) {
	prior, request, at := uploadClaimData(t)
	cutoff := at.Add(5 * time.Second)
	claimed, err := spendUploadReceiptOrdinal(prior, request, at, cutoff)
	if err != nil {
		t.Fatal("first bounded claim:", err)
	}
	ordinal := claimed.UploadPlan.Ordinals[0]
	if ordinal.State != attachedworkeroutput.UploadOrdinalWritePendingV1 || !uploadClaimDigest(ordinal.GrantDigest) || ordinal.Source != nil {
		t.Fatal("claim did not retain pending nonsecret grant without an observed object")
	}
	if prior.UploadPlan.Ordinals[0].State != attachedworkeroutput.UploadOrdinalUnissuedV1 || prior.UploadPlan.Ordinals[0].GrantDigest != "" ||
		claimed.UploadPlan.PlanDigest != prior.UploadPlan.PlanDigest || claimed.UploadPlan.Identity != prior.UploadPlan.Identity ||
		claimed.Ready || claimed.CopyInProgress || claimed.CanonicalDigest != "" || attachedWorkerReceiptAllowsDeletion(claimed) {
		t.Fatal("claim mutated input/identity, published READY or released deletion custody")
	}
	if _, err := spendUploadReceiptOrdinal(claimed, request, at, cutoff); err == nil {
		t.Fatal("identical repeated/lost-reply claim reissued a spent ordinal")
	}
	request.RemainingNanoseconds = uint64(time.Second)
	if _, err := spendUploadReceiptOrdinal(claimed, request, at.Add(time.Second), at.Add(2*time.Second)); err == nil {
		t.Fatal("shorter retry reopened a spent ordinal")
	}
	// Exact admission replay still observes the original spent plan; no reset.
	replay := uploadAdmissionFromRecord(claimed, request.Authorization)
	if replay.validate() != nil || !sameUploadAdmission(claimed, replay) || claimed.UploadPlan.Identity.ExpiresAt != prior.UploadPlan.Identity.ExpiresAt {
		t.Fatal("reconstructing admission metadata changed original spend/cutoff")
	}
}

func TestAttemptOutputUploadClaimRejectsSubstitutionAndRenewal(t *testing.T) {
	cases := map[string]func(*AttachedWorkerOutputReceiptV1, *AttachedWorkerOutputUploadClaimV1, *time.Time, *time.Time){
		"version": func(_ *AttachedWorkerOutputReceiptV1, r *AttachedWorkerOutputUploadClaimV1, _, _ *time.Time) {
			r.Version++
		},
		"nonce": func(_ *AttachedWorkerOutputReceiptV1, r *AttachedWorkerOutputUploadClaimV1, _, _ *time.Time) {
			r.Nonce = "another-nonce"
		},
		"plan": func(_ *AttachedWorkerOutputReceiptV1, r *AttachedWorkerOutputUploadClaimV1, _, _ *time.Time) {
			r.PlanDigest = strings.Repeat("b", 64)
		},
		"missing revision": func(_ *AttachedWorkerOutputReceiptV1, r *AttachedWorkerOutputUploadClaimV1, _, _ *time.Time) {
			r.Authorization.ExpectedAttemptRevision = 0
		},
		"revision": func(_ *AttachedWorkerOutputReceiptV1, r *AttachedWorkerOutputUploadClaimV1, _, _ *time.Time) {
			r.Authorization.ExpectedAttemptRevision++
		},
		"connection": func(_ *AttachedWorkerOutputReceiptV1, r *AttachedWorkerOutputUploadClaimV1, _, _ *time.Time) {
			r.Authorization.ConnectionID = "another-connection"
		},
		"lease generation": func(_ *AttachedWorkerOutputReceiptV1, r *AttachedWorkerOutputUploadClaimV1, _, _ *time.Time) {
			r.Authorization.LeaseGeneration++
		},
		"ordinal not admitted": func(_ *AttachedWorkerOutputReceiptV1, r *AttachedWorkerOutputUploadClaimV1, _, _ *time.Time) {
			r.Ordinal = 1
		},
		"ordinal beyond product cap": func(_ *AttachedWorkerOutputReceiptV1, r *AttachedWorkerOutputUploadClaimV1, _, _ *time.Time) {
			r.Ordinal = 2
		},
		"no remaining time": func(_ *AttachedWorkerOutputReceiptV1, r *AttachedWorkerOutputUploadClaimV1, _, _ *time.Time) {
			r.RemainingNanoseconds = 0
		},
		"fresh grace": func(_ *AttachedWorkerOutputReceiptV1, r *AttachedWorkerOutputUploadClaimV1, _, _ *time.Time) {
			r.RemainingNanoseconds = uint64(15*time.Second) + 1
		},
		"past original deadline": func(p *AttachedWorkerOutputReceiptV1, _ *AttachedWorkerOutputUploadClaimV1, at, cutoff *time.Time) {
			*at = p.UploadPlan.Identity.ExpiresAt
			*cutoff = at.Add(time.Second)
		},
		"before original admission": func(p *AttachedWorkerOutputReceiptV1, _ *AttachedWorkerOutputUploadClaimV1, at, cutoff *time.Time) {
			*at = p.CreatedAt.Add(-time.Second)
			*cutoff = p.CreatedAt
		},
		"cutoff renewal": func(p *AttachedWorkerOutputReceiptV1, r *AttachedWorkerOutputUploadClaimV1, at, cutoff *time.Time) {
			*at = p.CreatedAt.Add(14 * time.Second)
			r.RemainingNanoseconds = uint64(5 * time.Second)
			*cutoff = p.UploadPlan.Identity.ExpiresAt.Add(time.Second)
		},
		"beyond remaining": func(_ *AttachedWorkerOutputReceiptV1, _ *AttachedWorkerOutputUploadClaimV1, _ *time.Time, cutoff *time.Time) {
			*cutoff = cutoff.Add(time.Microsecond)
		},
		"no positive cutoff": func(_ *AttachedWorkerOutputReceiptV1, _ *AttachedWorkerOutputUploadClaimV1, at, cutoff *time.Time) {
			*cutoff = *at
		},
		"submicrosecond cutoff": func(_ *AttachedWorkerOutputReceiptV1, _ *AttachedWorkerOutputUploadClaimV1, _ *time.Time, cutoff *time.Time) {
			*cutoff = cutoff.Add(-time.Nanosecond)
		},
		"settled": func(p *AttachedWorkerOutputReceiptV1, _ *AttachedWorkerOutputUploadClaimV1, _, _ *time.Time) {
			p.UploadPlan.Phase = attachedworkeroutput.UploadPlanSettledAbandonedV1
		},
		"ready": func(p *AttachedWorkerOutputReceiptV1, _ *AttachedWorkerOutputUploadClaimV1, _, _ *time.Time) {
			p.Ready = true
		},
		"copy pending": func(p *AttachedWorkerOutputReceiptV1, _ *AttachedWorkerOutputUploadClaimV1, _, _ *time.Time) {
			p.CopyInProgress = true
		},
		"missing plan": func(p *AttachedWorkerOutputReceiptV1, _ *AttachedWorkerOutputUploadClaimV1, _, _ *time.Time) {
			p.UploadPlan = nil
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			prior, request, at := uploadClaimData(t)
			cutoff := at.Add(5 * time.Second)
			mutate(&prior, &request, &at, &cutoff)
			if _, err := spendUploadReceiptOrdinal(prior, request, at, cutoff); err == nil {
				t.Fatalf("invalid claim accepted: %s", name)
			}
		})
	}
}

func TestAttemptOutputUploadClaimTwoOrdinalsRetainPriorSpend(t *testing.T) {
	prior, request, at := uploadClaimData(t)
	metadata := []attachedworkeroutput.UploadArtifactMetadataV1{prior.UploadPlan.Ordinals[0].Metadata, prior.UploadPlan.Ordinals[0].Metadata}
	metadata[1].Ordinal, metadata[1].Name = 1, "second.txt"
	plan, err := attachedworkeroutput.NewAttemptOutputUploadPlanV1(prior.UploadPlan.Identity, metadata)
	if err != nil {
		t.Fatal(err)
	}
	prior.UploadPlan, request.PlanDigest = &plan, plan.PlanDigest
	first, err := spendUploadReceiptOrdinal(prior, request, at, at.Add(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	request.Ordinal = 1
	second, err := spendUploadReceiptOrdinal(first, request, at.Add(time.Second), at.Add(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if first.UploadPlan.Ordinals[1].State != attachedworkeroutput.UploadOrdinalUnissuedV1 ||
		second.UploadPlan.Ordinals[0] != first.UploadPlan.Ordinals[0] ||
		second.UploadPlan.Ordinals[1].State != attachedworkeroutput.UploadOrdinalWritePendingV1 ||
		second.UploadPlan.Ordinals[0].GrantDigest == second.UploadPlan.Ordinals[1].GrantDigest {
		t.Fatal("second ordinal reset prior spend, aliased input or reused grant identity")
	}
	for _, private := range []any{request, AttachedWorkerOutputUploadClaimResultV1{Plan: *second.UploadPlan}} {
		encoded, err := json.Marshal(private)
		if err != nil || string(encoded) != `"[REDACTED]"` {
			t.Fatalf("private claim leaked through generic JSON: err=%v", err)
		}
	}
}

func TestAttemptOutputUploadClaimRetryOnlyTightensDeadline(t *testing.T) {
	prior, request, at := uploadClaimData(t)
	first := tightenUploadClaimCutoff(time.Time{}, at, request.RemainingNanoseconds, prior.UploadPlan.Identity.ExpiresAt)
	later := tightenUploadClaimCutoff(first, at.Add(2*time.Second), request.RemainingNanoseconds, prior.UploadPlan.Identity.ExpiresAt)
	if first != at.Add(5*time.Second) || later != first {
		t.Fatal("transaction retry restarted caller remaining duration")
	}
	tighter := tightenUploadClaimCutoff(later, at.Add(3*time.Second), request.RemainingNanoseconds, at.Add(4*time.Second))
	if tighter != at.Add(4*time.Second) {
		t.Fatal("new earlier connection ceiling did not tighten deadline")
	}
	a, err := spendUploadReceiptOrdinal(prior, request, at, first)
	if err != nil {
		t.Fatal(err)
	}
	b, err := spendUploadReceiptOrdinal(prior, request, at.Add(time.Second), tighter)
	if err != nil {
		t.Fatal(err)
	}
	if a.UploadPlan.Ordinals[0].GrantDigest != b.UploadPlan.Ordinals[0].GrantDigest {
		t.Fatal("rolled-back callback retry minted a different original claim identity")
	}
}

func TestAttemptOutputUploadClaimNeverRefundsTerminalOrdinal(t *testing.T) {
	for _, state := range []attachedworkeroutput.UploadOrdinalStateV1{
		attachedworkeroutput.UploadOrdinalWritePendingV1, attachedworkeroutput.UploadOrdinalNotDispatchedV1, attachedworkeroutput.UploadOrdinalSourceVerifiedV1,
	} {
		t.Run(string(state), func(t *testing.T) {
			prior, request, at := uploadClaimData(t)
			ordinal := &prior.UploadPlan.Ordinals[0]
			ordinal.State, ordinal.GrantDigest = state, strings.Repeat("a", 64)
			if state == attachedworkeroutput.UploadOrdinalSourceVerifiedV1 {
				ordinal.Source = &domain.BlobRef{TenantID: request.Authorization.TenantID, Key: ordinal.Key, Size: ordinal.Metadata.Size, SHA256: ordinal.Metadata.SHA256}
			}
			if err := validateUploadReceiptData(prior); err != nil {
				t.Fatal("invalid positive stored-state control:", err)
			}
			if _, err := spendUploadReceiptOrdinal(prior, request, at, at.Add(5*time.Second)); err == nil {
				t.Fatalf("already consumed state %s was refunded", state)
			}
		})
	}
}
