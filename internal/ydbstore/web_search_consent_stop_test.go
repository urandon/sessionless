package ydbstore

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Reuse the existing stored-parent/consent SQL model. Its original MODEL and
// Source are explicit fixtures, not an authentic production MODEL producer.
// The real Store transaction and immutable decision/CAS are exercised here.
func TestWebSearchConsentHistoricalExpiryWithoutGrantCustody(t *testing.T) {
	for _, name := range []string{"pending-missing-body", "approved-missing-body", "pending-no-body-adapter"} {
		t.Run(name, func(t *testing.T) {
			f := newPrivateConsentViewFixture(t)
			store := f.allocation.parent.store
			f.legacyGrantOnly = true
			if name == "approved-missing-body" {
				approved, err := store.DecideWebSearchConsent(t.Context(), privateConsentDecisionInput(f))
				if err != nil || approved.Decision == nil {
					t.Fatalf("actual decision CAS prerequisite: %v", err)
				}
			}
			original := f.state.Clone()
			f.bodies = nil
			f.reads = nil
			if name == "pending-no-body-adapter" {
				store.attemptOperationBodies = nil
			}
			// DB time, not a wall-clock sleep or a newly extended deadline.
			f.allocation.parent.graph.at = original.Request.ExpiresAt
			input := ports.ReconcileWebSearchConsentRequestV1{Locator: consentLocator(original.Request), ExpectedRevision: original.Revision}
			ctx, cancel := ownedAuthorityCallContext(t.Context())
			defer cancel()
			writes := f.decisionWrites
			stopped, err := store.ReconcileWebSearchConsent(ctx, input)
			if err != nil || stopped.Status != domain.WebSearchConsentExpiredV1 || stopped.Revision != original.Revision+1 || f.decisionWrites != writes+1 {
				b := ctx.Value(ownedAuthorityCallContextKey{}).(*ownedAuthorityBudget)
				t.Logf("stop callback budget: callbacks=%d statements=%d bytes=%d failed=%t; final SQL=%v", b.callbacks, b.statements, b.bytes, b.failed, f.allocation.parent.base.statements[max(0, len(f.allocation.parent.base.statements)-4):])
				t.Fatalf("sealed request expiry must stop without grant I/O: err=%v status=%s revision=%d writes=%d/%d reads=%v", err, stopped.Status, stopped.Revision, f.decisionWrites, writes, f.reads)
			}
			if !reflect.DeepEqual(stopped.Request, original.Request) || !reflect.DeepEqual(stopped.Decision, original.Decision) || len(f.reads) != 0 {
				t.Fatal("historical stop changed original intent/receipt or read grant bytes")
			}
			b := ctx.Value(ownedAuthorityCallContextKey{}).(*ownedAuthorityBudget)
			if b.callbacks != 1 || b.statements > ownedAuthorityMaxStatements || b.bytes > ownedAuthorityMaxBytes {
				t.Fatalf("stop renewed or widened original budget: callbacks=%d statements=%d bytes=%d", b.callbacks, b.statements, b.bytes)
			}
			// A new logical call recovers the exact committed stop, no new write,
			// body keeper, approval or time renewal.
			recovered, err := store.ReconcileWebSearchConsent(t.Context(), input)
			if err != nil || !reflect.DeepEqual(recovered, stopped) || f.decisionWrites != writes+1 || len(f.reads) != 0 {
				t.Fatalf("exact stop replay changed evidence/effects: err=%v writes=%d reads=%v", err, f.decisionWrites, f.reads)
			}
		})
	}
}

func TestWebSearchConsentMissingGrantDoesNotInventStopOrApproval(t *testing.T) {
	f := newPrivateConsentViewFixture(t)
	store := f.allocation.parent.store
	f.legacyGrantOnly = true
	f.bodies = nil
	original := f.state.Clone()
	if got, err := store.DecideWebSearchConsent(t.Context(), privateConsentDecisionInput(f)); !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, domain.WebSearchConsentStateV1{}) || f.decisionWrites != 0 {
		t.Fatalf("missing current grant must not approve: err=%v writes=%d", err, f.decisionWrites)
	}
	f.reads = nil
	input := ports.ReconcileWebSearchConsentRequestV1{Locator: consentLocator(original.Request), ExpectedRevision: original.Revision}
	if got, err := store.ReconcileWebSearchConsent(t.Context(), input); !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, domain.WebSearchConsentStateV1{}) || f.decisionWrites != 0 || !reflect.DeepEqual(f.state, original) || len(f.reads) != 0 {
		t.Fatalf("healthy heads and unexpired request cannot manufacture stop: err=%v status=%s writes=%d reads=%v", err, got.Status, f.decisionWrites, f.reads)
	}
}

func TestWebSearchConsentHistoricalStopStillRequiresOriginalSource(t *testing.T) {
	f := newPrivateConsentViewFixture(t)
	f.bodies = nil
	f.allocation.parent.graph.at = f.state.Request.ExpiresAt
	f.allocation.parent.verifier.reject = true
	input := ports.ReconcileWebSearchConsentRequestV1{Locator: consentLocator(f.state.Request), ExpectedRevision: f.state.Revision}
	if got, err := f.allocation.parent.store.ReconcileWebSearchConsent(t.Context(), input); !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, domain.WebSearchConsentStateV1{}) || f.decisionWrites != 0 || len(f.reads) != 0 {
		t.Fatalf("expired request bypassed original Source authentication: err=%v writes=%d reads=%v", err, f.decisionWrites, f.reads)
	}
}

func TestWebSearchConsentStopUsesActualCanonicalFacts(t *testing.T) {
	for _, name := range []string{"cancelled-run", "lost-original-claim", "revoked-profile", "revoked-spending", "corrupt-lease", "future-head", "changed-grant", "revision-conflict", "unknown-commit", "cancel-after-commit", "exhausted-budget"} {
		t.Run(name, func(t *testing.T) {
			f := newPrivateConsentViewFixture(t)
			f.bodies = nil
			f.reads = nil
			original := f.state.Clone()
			input := ports.ReconcileWebSearchConsentRequestV1{Locator: consentLocator(original.Request), ExpectedRevision: original.Revision}
			ctx, cancel := ownedAuthorityCallContext(t.Context())
			defer cancel()
			want := domain.WebSearchConsentStatusV1("")
			switch name {
			case "cancelled-run":
				privateConsentDiscoveryRun(t, f, func(run *domain.Run) {
					if err := run.RequestCancellation(f.allocation.parent.graph.at); err != nil {
						t.Fatal(err)
					}
				})
				want = domain.WebSearchConsentCancelledV1
			case "lost-original-claim":
				f.allocation.parent.sql.leaseRow[2] = "another-current-lease"
				want = domain.WebSearchConsentReconcileOnlyV1
			case "revoked-profile":
				reader := f.allocation.parent.base.reader
				reader.profile.State = domain.OwnedAgentCurrentRevokedV1
				reader.profile.RevocationGeneration++
				reader.sync(t)
				want = domain.WebSearchConsentRevokedV1
			case "revoked-spending":
				reader := f.allocation.parent.base.reader
				reader.spending[3].State = domain.OwnedAgentCurrentRevokedV1
				reader.spending[3].RevocationGeneration++
				reader.sync(t)
				want = domain.WebSearchConsentRevokedV1
			case "corrupt-lease":
				f.allocation.parent.sql.leaseRow[16] = "{"
			case "future-head":
				f.allocation.parent.sql.leaseRow[7] = f.allocation.parent.graph.at.Add(time.Microsecond)
			case "changed-grant":
				f.state.Request.GrantDigest = ownedConsentTestHash([]byte("foreign grant"))
			case "revision-conflict":
				input.ExpectedRevision++
			case "unknown-commit", "cancel-after-commit":
				f.allocation.parent.graph.at = original.Request.ExpiresAt
				if name == "unknown-commit" {
					f.commitErrorAt = 1
				} else {
					f.afterCommit = cancel
				}
			case "exhausted-budget":
				ctx.Value(ownedAuthorityCallContextKey{}).(*ownedAuthorityBudget).callbacks = ownedAuthorityMaxCallbacks
			}
			got, err := f.allocation.parent.store.ReconcileWebSearchConsent(ctx, input)
			if want == "" {
				if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, domain.WebSearchConsentStateV1{}) {
					t.Fatalf("%s must not release speculative stop: err=%v state=%+v", name, err, got)
				}
				if name != "unknown-commit" && name != "cancel-after-commit" && f.decisionWrites != 0 {
					t.Fatalf("%s wrote despite denial", name)
				}
			} else if err != nil || got.Status != want || got.Revision != original.Revision+1 || f.decisionWrites != 1 || !reflect.DeepEqual(got.Request, original.Request) || !reflect.DeepEqual(got.Decision, original.Decision) {
				t.Fatalf("%s actual fact stop: err=%v status=%s want=%s writes=%d", name, err, got.Status, want, f.decisionWrites)
			}
			if len(f.reads) != 0 {
				t.Fatalf("%s unexpectedly materialized grant/query bodies: %v", name, f.reads)
			}
			if name == "unknown-commit" || name == "cancel-after-commit" {
				f.commitErrorAt, f.afterCommit = 0, nil
				recovered, err := f.allocation.parent.store.ReconcileWebSearchConsent(t.Context(), input)
				if err != nil || recovered.Status != domain.WebSearchConsentExpiredV1 || !reflect.DeepEqual(recovered.Request, original.Request) || f.decisionWrites != 1 {
					t.Fatalf("%s exact committed-stop recovery: err=%v writes=%d", name, err, f.decisionWrites)
				}
			}
		})
	}
}
