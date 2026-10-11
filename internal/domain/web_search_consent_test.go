package domain_test

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

type consentFixtureClock struct{ at time.Time }

func (clock *consentFixtureClock) Now() time.Time                 { return clock.at }
func (clock *consentFixtureClock) Advance(duration time.Duration) { clock.at = clock.at.Add(duration) }

type consentFixture struct {
	clock     consentFixtureClock
	state     domain.WebSearchConsentStateV1
	authority domain.WebSearchConsentAuthorityV1
	body      []byte
}

func consentHash(body string) string {
	sum := sha256.Sum256([]byte(body))
	return hex.EncodeToString(sum[:])
}

func newConsentFixture(t *testing.T) consentFixture {
	t.Helper()
	at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	base := at.Add(-time.Minute)
	deadline := at.Add(time.Minute)
	body := []byte(`{"query":"public compiler documentation","max_results":5}`)
	scope := domain.WebSearchConsentScopeV1{TenantID: "tenant-consent", SessionID: "session-consent", OwnerUserID: "resource-owner", RunID: "run-consent", AttemptID: "attempt-consent", LeaseID: "lease-consent", FenceToken: 7, PhysicalInvocationClaimID: "claim-consent", EffectReservationDigest: consentHash("effect"), InvocationAuthorityDigest: consentHash("invocation"), HarnessBindingDigest: consentHash("binding-v2"), PolicyDigest: consentHash("loop-policy")}
	request := domain.WebSearchConsentRequestV1{Version: domain.WebSearchConsentVersionV1, ID: "consent-1", Scope: scope, InitiatingUserID: "task-initiator", ModelResponseDigest: consentHash("durable-model-response"), CallID: "search-call-1", ArgumentsDigest: consentHash(string(body)), QueryDigest: consentHash("public compiler documentation"), GrantDigest: consentHash("search-grant"), ConsentRevision: 3, RevocationGeneration: 9, Arguments: domain.BlobRef{TenantID: scope.TenantID, Key: domain.SessionRunObjectPrefix(scope.TenantID, scope.SessionID, scope.RunID) + "attempts/" + string(scope.AttemptID) + "/consent/consent-1/arguments.json", Size: int64(len(body)), SHA256: consentHash(string(body))}, CreatedAt: at, ExpiresAt: deadline}
	run := domain.Run{ID: scope.RunID, TenantID: scope.TenantID, SessionID: scope.SessionID, TriggerEventID: "event-task", SubscriptionConnectionID: "subscription", Status: domain.RunRunning, IdempotencyKey: "run-key", CreatedAt: base, UpdatedAt: base}
	attempt := domain.Attempt{ID: scope.AttemptID, TenantID: scope.TenantID, RunID: scope.RunID, Number: 1, Status: domain.AttemptRunning, WorkerID: "original-worker", CreatedAt: base, UpdatedAt: base}
	lease := domain.Lease{ID: scope.LeaseID, TenantID: scope.TenantID, RunID: scope.RunID, AttemptID: scope.AttemptID, WorkerID: attempt.WorkerID, FenceToken: scope.FenceToken, AcquiredAt: base, ExpiresAt: deadline}
	web := domain.WebSession{SessionDigest: domain.SecretDigest(consentHash("fake-web-session")), CSRFTokenDigest: domain.SecretDigest(consentHash("fake-csrf")), UserID: request.InitiatingUserID, ActiveTenantID: scope.TenantID, AuthenticatedSubject: domain.ExternalSubject{Provider: domain.IdentityProviderYandex, Subject: "fixture-subject"}, MembershipSecurityVersion: 4, IssuedAt: base, LastSeenAt: base, IdleExpiresAt: at.Add(24 * time.Hour), AbsoluteExpiresAt: at.Add(24 * time.Hour)}
	authority := domain.WebSearchConsentAuthorityV1{Scope: scope, Run: run, Attempt: attempt, Lease: lease, WebSession: web, Membership: domain.TenantMembership{TenantID: scope.TenantID, UserID: request.InitiatingUserID, Role: domain.TenantMembershipMember, Status: domain.TenantMembershipActive, SecurityVersion: 4, CreatedAt: base, UpdatedAt: base}, Participant: domain.SessionParticipant{TenantID: scope.TenantID, SessionID: scope.SessionID, UserID: request.InitiatingUserID, Role: domain.SessionParticipantMember, Status: domain.SessionParticipantActive, CreatedAt: base, UpdatedAt: base}, InitiatingUserID: request.InitiatingUserID, GrantDigest: request.GrantDigest, ConsentRevision: request.ConsentRevision, RevocationGeneration: request.RevocationGeneration, DataClass: domain.ProviderDataPublicV1, ClaimState: domain.WebSearchConsentClaimActiveV1, InvocationDeadline: deadline, ActiveDeadline: deadline, GrantExpiresAt: deadline, PolicyExpiresAt: deadline, PriceExpiresAt: deadline, AllocationExpiresAt: deadline}
	f := consentFixture{clock: consentFixtureClock{at: at}, state: domain.WebSearchConsentStateV1{Request: request, Revision: 1, Status: domain.WebSearchConsentPendingV1, UpdatedAt: at}, authority: authority, body: body}
	if err := f.state.Validate(); err != nil {
		t.Fatalf("setup consent state: %v", err)
	}
	if err := authority.ValidateCurrent(request, at); err != nil {
		t.Fatalf("setup current consent authority: %v", err)
	}
	if err := authority.ValidateActor(request, at); err != nil {
		t.Fatalf("setup actor consent authority: %v", err)
	}
	return f
}

func decideConsent(f consentFixture, choice domain.WebSearchConsentChoiceV1) (domain.WebSearchConsentStateV1, error) {
	return domain.DecideWebSearchConsentV1(f.state, 1, choice, f.authority.WebSession.SessionDigest, f.authority.WebSession.CSRFTokenDigest, f.authority, f.clock.Now())
}

func approveConsent(t *testing.T, f consentFixture) (domain.WebSearchConsentStateV1, domain.WebSearchConsentReceiptBindingV1) {
	t.Helper()
	state, err := decideConsent(f, domain.WebSearchConsentApproveV1)
	if err != nil {
		t.Fatalf("approve exact consent: %v", err)
	}
	digest, err := state.Decision.Digest()
	if err != nil {
		t.Fatalf("digest approved decision: %v", err)
	}
	return state, domain.WebSearchConsentReceiptBindingV1{Request: state.Request, RequestDigest: state.Decision.RequestDigest, DecisionDigest: digest}
}

func TestWebSearchConsentExactArguments(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name, body, query string
		valid             bool
	}{
		{"exact", `{"query":"public compiler documentation","max_results":5}`, "public compiler documentation", true},
		{"bounded_arbitrary_query", `{"query":"news about public quantum compiler projects"}`, "news about public quantum compiler projects", true},
		{"unknown_field", `{"query":"public","url":"https://example.com"}`, "public", false},
		{"duplicate_query", `{"query":"private","query":"public"}`, "public", false},
		{"duplicate_limit", `{"query":"public","max_results":1,"max_results":5}`, "public", false},
		{"null_limit", `{"query":"public","max_results":null}`, "public", false},
		{"zero_limit", `{"query":"public","max_results":0}`, "public", false},
		{"excessive_limit", `{"query":"public","max_results":6}`, "public", false},
		{"partial", `{"query":"public"`, "public", false},
		{"trailing", `{"query":"public"}{}`, "public", false},
		{"no_query", `{"max_results":5}`, "public", false},
		{"empty_query", `{"query":" "}`, " ", false},
		{"null_query", `{"query":null}`, "public", false},
		{"invalid_utf8", "{\"query\":\"\xff\"}", "�", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newConsentFixture(t)
			f.state.Request.Arguments.Size = int64(len(tc.body))
			f.state.Request.Arguments.SHA256 = consentHash(tc.body)
			f.state.Request.ArgumentsDigest = consentHash(tc.body)
			f.state.Request.QueryDigest = consentHash(tc.query)
			err := f.state.Request.ValidateArguments([]byte(tc.body))
			if (err == nil) != tc.valid {
				t.Errorf("ValidateArguments(%q) error=%v, want valid=%v", tc.body, err, tc.valid)
			}
		})
	}
	f := newConsentFixture(t)
	if err := f.state.Request.ValidateArguments([]byte(`{"query":"replacement query"}`)); err == nil {
		t.Error("replacement argument bytes accepted under original digest")
	}
}

func TestWebSearchConsentRequestDigestAndBounds(t *testing.T) {
	t.Parallel()
	f := newConsentFixture(t)
	original, err := f.state.Request.Digest()
	if err != nil {
		t.Fatal(err)
	}
	changes := []struct {
		name   string
		mutate func(*domain.WebSearchConsentRequestV1)
	}{
		{"version", func(r *domain.WebSearchConsentRequestV1) { r.Version++ }}, {"id", func(r *domain.WebSearchConsentRequestV1) { r.ID = "different-request" }},
		{"tenant", func(r *domain.WebSearchConsentRequestV1) { r.Scope.TenantID = "tenant-other" }}, {"session", func(r *domain.WebSearchConsentRequestV1) { r.Scope.SessionID = "session-other" }},
		{"owner", func(r *domain.WebSearchConsentRequestV1) { r.Scope.OwnerUserID = "other-resource-owner" }}, {"run", func(r *domain.WebSearchConsentRequestV1) { r.Scope.RunID = "run-other" }},
		{"attempt", func(r *domain.WebSearchConsentRequestV1) { r.Scope.AttemptID = "attempt-other" }}, {"lease", func(r *domain.WebSearchConsentRequestV1) { r.Scope.LeaseID = "lease-other" }},
		{"fence", func(r *domain.WebSearchConsentRequestV1) { r.Scope.FenceToken++ }}, {"claim", func(r *domain.WebSearchConsentRequestV1) { r.Scope.PhysicalInvocationClaimID = "claim-replacement" }},
		{"effect", func(r *domain.WebSearchConsentRequestV1) {
			r.Scope.EffectReservationDigest = consentHash("effect-other")
		}}, {"invocation", func(r *domain.WebSearchConsentRequestV1) {
			r.Scope.InvocationAuthorityDigest = consentHash("invocation-other")
		}},
		{"binding", func(r *domain.WebSearchConsentRequestV1) { r.Scope.HarnessBindingDigest = consentHash("binding-other") }}, {"policy", func(r *domain.WebSearchConsentRequestV1) { r.Scope.PolicyDigest = consentHash("policy-other") }},
		{"initiator", func(r *domain.WebSearchConsentRequestV1) { r.InitiatingUserID = "other-initiator" }}, {"model_response", func(r *domain.WebSearchConsentRequestV1) { r.ModelResponseDigest = consentHash("model-other") }},
		{"call", func(r *domain.WebSearchConsentRequestV1) { r.CallID = "call-other" }}, {"arguments", func(r *domain.WebSearchConsentRequestV1) {
			r.ArgumentsDigest = consentHash("args-other")
			r.Arguments.SHA256 = r.ArgumentsDigest
		}},
		{"query", func(r *domain.WebSearchConsentRequestV1) { r.QueryDigest = consentHash("query-other") }}, {"grant", func(r *domain.WebSearchConsentRequestV1) { r.GrantDigest = consentHash("grant-other") }},
		{"consent_revision", func(r *domain.WebSearchConsentRequestV1) { r.ConsentRevision++ }}, {"revocation_generation", func(r *domain.WebSearchConsentRequestV1) { r.RevocationGeneration++ }},
		{"blob_key", func(r *domain.WebSearchConsentRequestV1) { r.Arguments.Key += ".other" }}, {"blob_tenant", func(r *domain.WebSearchConsentRequestV1) { r.Arguments.TenantID = "tenant-other" }},
		{"blob_size", func(r *domain.WebSearchConsentRequestV1) { r.Arguments.Size++ }}, {"blob_sha", func(r *domain.WebSearchConsentRequestV1) { r.Arguments.SHA256 = consentHash("blob-other") }},
		{"created_at", func(r *domain.WebSearchConsentRequestV1) { r.CreatedAt = r.CreatedAt.Add(time.Nanosecond) }}, {"expires_at", func(r *domain.WebSearchConsentRequestV1) { r.ExpiresAt = r.ExpiresAt.Add(time.Nanosecond) }},
	}
	for _, tc := range changes {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := f.state.Request
			tc.mutate(&r)
			got, err := r.Digest()
			if err == nil && got == original {
				t.Errorf("%s did not change/reject request digest", tc.name)
			}
		})
	}
	t.Run("utc_equivalence", func(t *testing.T) {
		r := f.state.Request
		r.CreatedAt = r.CreatedAt.In(time.FixedZone("fixture", 3*3600))
		r.ExpiresAt = r.ExpiresAt.In(time.FixedZone("fixture", 3*3600))
		got, err := r.Digest()
		if err != nil || got != original {
			t.Errorf("UTC-equivalent digest got=%s err=%v want=%s", got, err, original)
		}
	})
	for _, tc := range []struct {
		name   string
		mutate func(*domain.WebSearchConsentRequestV1)
	}{
		{"zero_expiry", func(r *domain.WebSearchConsentRequestV1) { r.ExpiresAt = time.Time{} }}, {"long_window", func(r *domain.WebSearchConsentRequestV1) { r.ExpiresAt = r.CreatedAt.Add(121 * time.Second) }},
		{"oversized_blob", func(r *domain.WebSearchConsentRequestV1) { r.Arguments.Size = 4097 }}, {"zero_blob", func(r *domain.WebSearchConsentRequestV1) { r.Arguments.Size = 0 }}, {"foreign_attempt_blob", func(r *domain.WebSearchConsentRequestV1) {
			r.Arguments.Key = domain.SessionRunObjectPrefix(r.Scope.TenantID, r.Scope.SessionID, r.Scope.RunID) + "attempts/other/args.json"
		}},
		{"zero_fence", func(r *domain.WebSearchConsentRequestV1) { r.Scope.FenceToken = 0 }}, {"zero_consent_revision", func(r *domain.WebSearchConsentRequestV1) { r.ConsentRevision = 0 }}, {"zero_revocation", func(r *domain.WebSearchConsentRequestV1) { r.RevocationGeneration = 0 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := f.state.Request
			tc.mutate(&r)
			if err := r.Validate(); err == nil {
				t.Errorf("invalid %s accepted", tc.name)
			}
		})
	}
}

func TestWebSearchConsentDecisionCASAndLostResponse(t *testing.T) {
	t.Parallel()
	for _, choice := range []domain.WebSearchConsentChoiceV1{domain.WebSearchConsentApproveV1, domain.WebSearchConsentDenyV1} {
		t.Run(string(choice), func(t *testing.T) {
			t.Parallel()
			f := newConsentFixture(t)
			f.clock.Advance(time.Second)
			decided, err := decideConsent(f, choice)
			if err != nil {
				t.Fatal(err)
			}
			original, _ := decided.Decision.Digest()
			f.state = decided
			f.clock.Advance(time.Second)
			recovered, err := decideConsent(f, choice)
			if err != nil {
				t.Fatalf("recover lost response: %v", err)
			}
			got, _ := recovered.Decision.Digest()
			if got != original || recovered.Revision != 2 || !recovered.Request.ExpiresAt.Equal(f.state.Request.ExpiresAt) {
				t.Errorf("retry changed immutable receipt/revision/deadline: got=%+v", recovered)
			}
			opposite := domain.WebSearchConsentDenyV1
			if choice == opposite {
				opposite = domain.WebSearchConsentApproveV1
			}
			if _, err := decideConsent(f, opposite); !errors.Is(err, domain.ErrWebSearchConsentConflict) {
				t.Errorf("conflicting decision error=%v want conflict", err)
			}
			if _, err := domain.DecideWebSearchConsentV1(decided, 2, choice, f.authority.WebSession.SessionDigest, f.authority.WebSession.CSRFTokenDigest, f.authority, f.clock.Now()); !errors.Is(err, domain.ErrWebSearchConsentConflict) {
				t.Errorf("stale revision error=%v want conflict", err)
			}
			recovered.Decision.Choice = opposite
			if f.state.Decision.Choice != choice {
				t.Error("recovered state aliased stored immutable receipt")
			}
		})
	}
}

func TestWebSearchConsentDecisionCannotMoveTimeOrSecurityBackwards(t *testing.T) {
	t.Parallel()
	f := newConsentFixture(t)
	f.clock.Advance(time.Second)
	state, binding := approveConsent(t, f)
	f.state = state
	f.clock.Advance(-time.Second)
	if _, err := decideConsent(f, domain.WebSearchConsentApproveV1); err == nil {
		t.Error("duplicate decision accepted time before the immutable receipt")
	}
	if err := domain.ValidateWebSearchConsentReceiptV1(state, binding, f.authority, f.clock.Now()); err == nil {
		t.Error("receipt usable before its observed approval time")
	}
	f.clock.Advance(time.Second)
	f.authority.Membership.SecurityVersion++
	f.authority.WebSession.MembershipSecurityVersion++
	if _, err := decideConsent(f, domain.WebSearchConsentApproveV1); err == nil {
		t.Error("old decision recovered under a changed membership security version")
	}
	stopped, err := domain.StopWebSearchConsentV1(state, state.Revision, f.authority, f.clock.Now())
	if err != nil || stopped.Status != domain.WebSearchConsentRevokedV1 || stopped.Decision == nil {
		t.Errorf("security-version stop got=%+v err=%v, want revoked with retained immutable receipt", stopped, err)
	}
}

func TestWebSearchConsentDecisionRejectsCurrentAuthorityChanges(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		mutate func(*consentFixture)
	}{
		{"foreign_tenant", func(f *consentFixture) { f.authority.Scope.TenantID = "tenant-other" }}, {"foreign_owner", func(f *consentFixture) { f.authority.Scope.OwnerUserID = "owner-other" }},
		{"foreign_session", func(f *consentFixture) { f.authority.Scope.SessionID = "session-other" }}, {"foreign_run", func(f *consentFixture) { f.authority.Scope.RunID = "run-other" }},
		{"foreign_attempt", func(f *consentFixture) { f.authority.Scope.AttemptID = "attempt-other" }}, {"changed_lease", func(f *consentFixture) { f.authority.Scope.LeaseID = "lease-other" }},
		{"changed_fence", func(f *consentFixture) { f.authority.Scope.FenceToken++ }}, {"changed_physical_claim", func(f *consentFixture) { f.authority.Scope.PhysicalInvocationClaimID = "replacement-claim" }},
		{"changed_effect", func(f *consentFixture) { f.authority.Scope.EffectReservationDigest = consentHash("effect-other") }}, {"changed_invocation", func(f *consentFixture) { f.authority.Scope.InvocationAuthorityDigest = consentHash("invocation-other") }},
		{"changed_binding", func(f *consentFixture) { f.authority.Scope.HarnessBindingDigest = consentHash("binding-other") }}, {"changed_policy", func(f *consentFixture) { f.authority.Scope.PolicyDigest = consentHash("policy-other") }},
		{"changed_grant", func(f *consentFixture) { f.authority.GrantDigest = consentHash("grant-other") }}, {"changed_consent_revision", func(f *consentFixture) { f.authority.ConsentRevision++ }},
		{"changed_revocation_generation", func(f *consentFixture) { f.authority.RevocationGeneration++ }}, {"changed_task_initiator", func(f *consentFixture) { f.authority.InitiatingUserID = f.authority.Scope.OwnerUserID }},
		{"resource_owner_is_not_initiator", func(f *consentFixture) { f.authority.WebSession.UserID = f.authority.Scope.OwnerUserID }},
		{"changed_membership_version", func(f *consentFixture) { f.authority.Membership.SecurityVersion++ }}, {"suspended_membership", func(f *consentFixture) { f.authority.Membership.Status = domain.TenantMembershipSuspended }},
		{"viewer_membership", func(f *consentFixture) { f.authority.Membership.Role = domain.TenantMembershipViewer }}, {"removed_participant", func(f *consentFixture) { f.authority.Participant.Status = domain.SessionParticipantRemoved }},
		{"viewer_participant", func(f *consentFixture) { f.authority.Participant.Role = domain.SessionParticipantViewer }}, {"foreign_participant", func(f *consentFixture) { f.authority.Participant.UserID = "other-user" }},
		{"revoked_web_session", func(f *consentFixture) { at := f.clock.Now(); f.authority.WebSession.RevokedAt = &at }}, {"expired_web_session", func(f *consentFixture) { f.authority.WebSession.IdleExpiresAt = f.clock.Now() }},
		{"future_web_session", func(f *consentFixture) { f.authority.WebSession.LastSeenAt = f.clock.Now().Add(time.Second) }}, {"private_task", func(f *consentFixture) { f.authority.DataClass = domain.ProviderDataPrivateV1 }},
		{"cancelled_run", func(f *consentFixture) { at := f.clock.Now(); f.authority.Run.CancellationRequestedAt = &at }}, {"worker_loss", func(f *consentFixture) { f.authority.ClaimState = domain.WebSearchConsentClaimLostV1 }},
		{"replacement_worker", func(f *consentFixture) { f.authority.Lease.WorkerID = "other-worker" }}, {"missing_claim_state", func(f *consentFixture) { f.authority.ClaimState = "" }},
		{"request_expiry", func(f *consentFixture) { f.clock.Advance(time.Minute) }}, {"before_creation", func(f *consentFixture) { f.clock.Advance(-time.Second) }},
		{"lease_expiry", func(f *consentFixture) { f.authority.Lease.ExpiresAt = f.clock.Now() }}, {"grant_expiry", func(f *consentFixture) { f.authority.GrantExpiresAt = f.clock.Now() }},
		{"price_expiry", func(f *consentFixture) { f.authority.PriceExpiresAt = f.clock.Now() }}, {"policy_expiry", func(f *consentFixture) { f.authority.PolicyExpiresAt = f.clock.Now() }},
		{"allocation_expiry", func(f *consentFixture) { f.authority.AllocationExpiresAt = f.clock.Now() }}, {"invocation_expiry", func(f *consentFixture) { f.authority.InvocationDeadline = f.clock.Now() }},
		{"active_expiry", func(f *consentFixture) { f.authority.ActiveDeadline = f.clock.Now() }}, {"missing_price_window", func(f *consentFixture) { f.authority.PriceExpiresAt = time.Time{} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newConsentFixture(t)
			tc.mutate(&f)
			if _, err := decideConsent(f, domain.WebSearchConsentApproveV1); err == nil {
				t.Errorf("approve accepted changed %s authority", tc.name)
			}
		})
	}
	t.Run("csrf_wrong_same_session", func(t *testing.T) {
		f := newConsentFixture(t)
		_, err := domain.DecideWebSearchConsentV1(f.state, 1, domain.WebSearchConsentApproveV1, f.authority.WebSession.SessionDigest, domain.SecretDigest(consentHash("foreign-csrf")), f.authority, f.clock.Now())
		if err == nil {
			t.Error("foreign CSRF accepted")
		}
	})
	t.Run("different_web_session", func(t *testing.T) {
		f := newConsentFixture(t)
		_, err := domain.DecideWebSearchConsentV1(f.state, 1, domain.WebSearchConsentApproveV1, domain.SecretDigest(consentHash("foreign-session")), f.authority.WebSession.CSRFTokenDigest, f.authority, f.clock.Now())
		if err == nil {
			t.Error("foreign Web session accepted")
		}
	})
}

func TestWebSearchConsentAppendUsesSharedBounds(t *testing.T) {
	t.Parallel()
	f := newConsentFixture(t)
	if err := domain.ValidateWebSearchConsentAppendV1(nil, f.state.Request, 2, uint64(len(f.body))); err != nil {
		t.Fatalf("exact shared capacity: %v", err)
	}
	denied, err := decideConsent(f, domain.WebSearchConsentDenyV1)
	if err != nil {
		t.Fatal(err)
	}
	second := f.state.Request
	second.ID = "consent-2"
	second.CallID = "search-call-2"
	second.ArgumentsDigest = consentHash("different-args")
	second.Arguments.SHA256 = second.ArgumentsDigest
	second.QueryDigest = consentHash("different-query")
	if err := domain.ValidateWebSearchConsentAppendV1([]domain.WebSearchConsentStateV1{denied}, second, 2, 4096); err != nil {
		t.Fatalf("second distinct request: %v", err)
	}
	for _, tc := range []struct {
		name        string
		existing    []domain.WebSearchConsentStateV1
		request     domain.WebSearchConsentRequestV1
		rows, bytes uint64
	}{
		{"one_pending", []domain.WebSearchConsentStateV1{f.state}, second, 2, 4096}, {"two_already", []domain.WebSearchConsentStateV1{denied, denied}, second, 2, 4096},
		{"insufficient_rows", nil, f.state.Request, 1, 4096}, {"insufficient_blob_bytes", nil, f.state.Request, 2, uint64(len(f.body) - 1)},
		{"same_request", []domain.WebSearchConsentStateV1{denied}, f.state.Request, 2, 4096},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := domain.ValidateWebSearchConsentAppendV1(tc.existing, tc.request, tc.rows, tc.bytes); err == nil {
				t.Errorf("append accepted %s", tc.name)
			}
		})
	}
	t.Run("same_query_new_call", func(t *testing.T) {
		r := second
		r.QueryDigest = f.state.Request.QueryDigest
		if err := domain.ValidateWebSearchConsentAppendV1([]domain.WebSearchConsentStateV1{denied}, r, 2, 4096); err == nil {
			t.Error("same query replay under new model call accepted")
		}
	})
	t.Run("foreign_owner", func(t *testing.T) {
		r := second
		r.Scope.OwnerUserID = "other-owner"
		if err := domain.ValidateWebSearchConsentAppendV1([]domain.WebSearchConsentStateV1{denied}, r, 2, 4096); err == nil {
			t.Error("foreign owner rows accepted as attempt-local consent history")
		}
	})
}

func TestWebSearchConsentReceiptExactAndNoDeadlineExtension(t *testing.T) {
	t.Parallel()
	f := newConsentFixture(t)
	state, binding := approveConsent(t, f)
	if err := domain.ValidateWebSearchConsentReceiptV1(state, binding, f.authority, f.clock.Now()); err != nil {
		t.Fatalf("exact receipt rejected: %v", err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*domain.WebSearchConsentReceiptBindingV1)
	}{
		{"absent_receipt", func(b *domain.WebSearchConsentReceiptBindingV1) { b.DecisionDigest = "" }}, {"other_receipt", func(b *domain.WebSearchConsentReceiptBindingV1) {
			b.DecisionDigest = domain.WebSearchConsentDecisionDigestV1(consentHash("other-receipt"))
		}},
		{"other_request", func(b *domain.WebSearchConsentReceiptBindingV1) {
			b.RequestDigest = domain.WebSearchConsentRequestDigestV1(consentHash("other-request"))
		}},
		{"changed_args", func(b *domain.WebSearchConsentReceiptBindingV1) {
			b.Request.ArgumentsDigest = consentHash("replacement-args")
			b.Request.Arguments.SHA256 = b.Request.ArgumentsDigest
			digest, _ := b.Request.Digest()
			b.RequestDigest = digest
		}},
		{"changed_query", func(b *domain.WebSearchConsentReceiptBindingV1) {
			b.Request.QueryDigest = consentHash("replacement-query")
			digest, _ := b.Request.Digest()
			b.RequestDigest = digest
		}},
		{"extended_deadline", func(b *domain.WebSearchConsentReceiptBindingV1) {
			b.Request.ExpiresAt = b.Request.ExpiresAt.Add(time.Second)
			digest, _ := b.Request.Digest()
			b.RequestDigest = digest
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := binding
			tc.mutate(&b)
			if err := domain.ValidateWebSearchConsentReceiptV1(state, b, f.authority, f.clock.Now()); err == nil {
				t.Errorf("receipt accepted %s", tc.name)
			}
		})
	}
	f.clock.Advance(30 * time.Second)
	f.authority.Lease.ExpiresAt = f.authority.Lease.ExpiresAt.Add(time.Hour)
	if err := domain.ValidateWebSearchConsentReceiptV1(state, binding, f.authority, f.clock.Now()); err != nil {
		t.Fatalf("wait inside original deadline: %v", err)
	}
	f.clock.Advance(30 * time.Second)
	if err := domain.ValidateWebSearchConsentReceiptV1(state, binding, f.authority, f.clock.Now()); err == nil {
		t.Error("lease renewal extended original consent expiry")
	}
	t.Run("membership_version_even_with_new_web_session", func(t *testing.T) {
		f := newConsentFixture(t)
		state, binding := approveConsent(t, f)
		f.authority.Membership.SecurityVersion++
		f.authority.WebSession.MembershipSecurityVersion++
		if err := domain.ValidateWebSearchConsentReceiptV1(state, binding, f.authority, f.clock.Now()); err == nil {
			t.Error("old receipt usable after membership security version changed")
		}
	})
	t.Run("different_session_same_initiator", func(t *testing.T) {
		f := newConsentFixture(t)
		state, binding := approveConsent(t, f)
		f.authority.WebSession.SessionDigest = domain.SecretDigest(consentHash("replacement-web-session"))
		if err := domain.ValidateWebSearchConsentReceiptV1(state, binding, f.authority, f.clock.Now()); err == nil {
			t.Error("receipt transplanted to a different Web session")
		}
	})
	t.Run("immutable_decision_digest", func(t *testing.T) {
		f := newConsentFixture(t)
		state, binding := approveConsent(t, f)
		changed := state.Clone()
		changed.Decision.ObservedAt = changed.Decision.ObservedAt.Add(time.Nanosecond)
		changed.UpdatedAt = changed.Decision.ObservedAt
		if err := domain.ValidateWebSearchConsentReceiptV1(changed, binding, f.authority, f.clock.Now()); err == nil {
			t.Error("modified immutable decision accepted")
		}
	})
}

func TestWebSearchConsentStopAndWorkerLossNeverReopen(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		mutate func(*consentFixture)
		want   domain.WebSearchConsentStatusV1
	}{
		{"expiry", func(f *consentFixture) { f.clock.Advance(time.Minute) }, domain.WebSearchConsentExpiredV1},
		{"cancellation", func(f *consentFixture) { at := f.clock.Now(); f.authority.Run.CancellationRequestedAt = &at }, domain.WebSearchConsentCancelledV1},
		{"revocation", func(f *consentFixture) { f.authority.RevocationGeneration++ }, domain.WebSearchConsentRevokedV1},
		{"worker_loss", func(f *consentFixture) { f.authority.ClaimState = domain.WebSearchConsentClaimLostV1 }, domain.WebSearchConsentReconcileOnlyV1},
		{"replacement_claim", func(f *consentFixture) { f.authority.Scope.PhysicalInvocationClaimID = "replacement-claim" }, domain.WebSearchConsentReconcileOnlyV1},
	}
	for _, tc := range cases {
		for _, approved := range []bool{false, true} {
			name := tc.name + "/pending"
			if approved {
				name = tc.name + "/approved"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				f := newConsentFixture(t)
				if approved {
					f.state, _ = approveConsent(t, f)
				}
				originalExpiry := f.state.Request.ExpiresAt
				tc.mutate(&f)
				stopped, err := domain.StopWebSearchConsentV1(f.state, f.state.Revision, f.authority, f.clock.Now())
				if err != nil {
					t.Fatalf("stop %s: %v", tc.name, err)
				}
				if stopped.Status != tc.want || !stopped.Request.ExpiresAt.Equal(originalExpiry) {
					t.Errorf("stop got status=%s expiry=%s want status=%s original=%s", stopped.Status, stopped.Request.ExpiresAt, tc.want, originalExpiry)
				}
				if !approved && stopped.Decision != nil {
					t.Error("stop forged a human receipt")
				}
				if approved && stopped.Decision == nil {
					t.Error("stop discarded immutable decision")
				}
				f.state = stopped
				restored := newConsentFixture(t)
				f.authority = restored.authority
				f.clock = restored.clock
				if _, err := decideConsent(f, domain.WebSearchConsentApproveV1); !errors.Is(err, domain.ErrWebSearchConsentStopped) {
					t.Errorf("restored authority reopened terminal request: %v", err)
				}
			})
		}
	}
	t.Run("stale_stop", func(t *testing.T) {
		f := newConsentFixture(t)
		f.state, _ = approveConsent(t, f)
		f.authority.ClaimState = domain.WebSearchConsentClaimLostV1
		if _, err := domain.StopWebSearchConsentV1(f.state, 1, f.authority, f.clock.Now()); !errors.Is(err, domain.ErrWebSearchConsentConflict) {
			t.Errorf("stale stop error=%v want conflict", err)
		}
	})
	t.Run("revision_overflow", func(t *testing.T) {
		f := newConsentFixture(t)
		f.state.Revision = math.MaxUint64
		if _, err := decideConsent(f, domain.WebSearchConsentApproveV1); err == nil {
			t.Error("overflow/stale state accepted")
		}
	})
}

// The channels order the authority mutation before each exact fixture gate.
// No network, real-time sleeps, caller validity flags or automatic retries are
// involved. These reducers are evidence for contracts, not a production store.
func TestWebSearchConsentBarriersDenyReserveAndBeforeSend(t *testing.T) {
	t.Parallel()
	for _, gate := range []string{"reserve", "before_send"} {
		for _, stop := range []string{"denial", "expiry", "revocation", "cancel", "worker_loss"} {
			t.Run(gate+"/"+stop, func(t *testing.T) {
				t.Parallel()
				f := newConsentFixture(t)
				state, binding := approveConsent(t, f)
				if stop == "denial" {
					// Denial wins while the authoritative row is still pending;
					// the proposed approval digest is not a committed receipt.
					state = f.state
				}
				reached := make(chan struct{})
				resume := make(chan struct{})
				done := make(chan error, 1)
				var mutex sync.Mutex
				sends := 0
				go func() {
					close(reached)
					<-resume
					mutex.Lock()
					defer mutex.Unlock()
					err := domain.ValidateWebSearchConsentReceiptV1(state, binding, f.authority, f.clock.Now())
					if err == nil {
						sends++
					}
					done <- err
				}()
				<-reached
				mutex.Lock()
				switch stop {
				case "denial":
					denied, err := decideConsent(f, domain.WebSearchConsentDenyV1)
					if err != nil {
						mutex.Unlock()
						close(resume)
						<-done
						t.Fatalf("fixture denial: %v", err)
					}
					state = denied
				case "expiry":
					f.clock.Advance(time.Minute)
				case "revocation":
					f.authority.RevocationGeneration++
				case "cancel":
					at := f.clock.Now()
					f.authority.Run.CancellationRequestedAt = &at
				case "worker_loss":
					f.authority.ClaimState = domain.WebSearchConsentClaimLostV1
				}
				mutex.Unlock()
				close(resume)
				err := <-done
				mutex.Lock()
				gotSends := sends
				mutex.Unlock()
				if err == nil || gotSends != 0 {
					t.Errorf("%s barrier after %s: sends=%d err=%v, want zero unauthorized sends", gate, stop, gotSends, err)
				}
			})
		}
	}
}
