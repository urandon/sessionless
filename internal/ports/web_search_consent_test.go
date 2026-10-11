package ports_test

import (
	"crypto/sha256"
	"encoding/hex"
	"reflect"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func consentPortHash(value string) string {
	hash := sha256.Sum256([]byte(value))
	return hex.EncodeToString(hash[:])
}

func consentPortFixture() (ports.WebSearchConsentLocatorV1, domain.WebSearchConsentRequestV1) {
	at := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	scope := domain.WebSearchConsentScopeV1{TenantID: "tenant-port", SessionID: "session-port", OwnerUserID: "resource-owner", RunID: "run-port", AttemptID: "attempt-port", LeaseID: "lease-port", FenceToken: 1, PhysicalInvocationClaimID: "claim-port", EffectReservationDigest: consentPortHash("effect"), InvocationAuthorityDigest: consentPortHash("invocation"), HarnessBindingDigest: consentPortHash("binding-v2"), PolicyDigest: consentPortHash("policy")}
	request := domain.WebSearchConsentRequestV1{Version: 1, ID: "request-port", Scope: scope, InitiatingUserID: "task-initiator", ModelResponseDigest: consentPortHash("model"), CallID: "call-port", ArgumentsDigest: consentPortHash("args"), QueryDigest: consentPortHash("query"), GrantDigest: consentPortHash("grant"), ConsentRevision: 1, RevocationGeneration: 1, Arguments: domain.BlobRef{TenantID: scope.TenantID, Key: domain.SessionRunObjectPrefix(scope.TenantID, scope.SessionID, scope.RunID) + "attempts/attempt-port/consent/request-port/args.json", Size: 4, SHA256: consentPortHash("args")}, CreatedAt: at, ExpiresAt: at.Add(time.Minute)}
	return ports.WebSearchConsentLocatorV1{TenantID: scope.TenantID, SessionID: scope.SessionID, RunID: scope.RunID, AttemptID: scope.AttemptID, RequestID: request.ID}, request
}

func TestWebSearchConsentPortExactLocator(t *testing.T) {
	t.Parallel()
	locator, request := consentPortFixture()
	if err := locator.ValidateForRequest(request); err != nil {
		t.Fatalf("exact request locator: %v", err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*ports.WebSearchConsentLocatorV1)
	}{
		{"tenant", func(l *ports.WebSearchConsentLocatorV1) { l.TenantID = "tenant-foreign" }}, {"session", func(l *ports.WebSearchConsentLocatorV1) { l.SessionID = "session-foreign" }},
		{"run", func(l *ports.WebSearchConsentLocatorV1) { l.RunID = "run-foreign" }}, {"attempt", func(l *ports.WebSearchConsentLocatorV1) { l.AttemptID = "attempt-foreign" }},
		{"request", func(l *ports.WebSearchConsentLocatorV1) { l.RequestID = "request-foreign" }}, {"empty", func(l *ports.WebSearchConsentLocatorV1) { l.RequestID = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			candidate := locator
			tc.mutate(&candidate)
			if err := candidate.ValidateForRequest(request); err == nil {
				t.Errorf("foreign %s locator accepted", tc.name)
			}
		})
	}
}

func TestWebSearchConsentPortDecisionBounds(t *testing.T) {
	t.Parallel()
	locator, _ := consentPortFixture()
	request := ports.DecideWebSearchConsentRequestV1{Locator: locator, WebSessionDigest: domain.SecretDigest(consentPortHash("session")), CSRFTokenDigest: domain.SecretDigest(consentPortHash("csrf")), ExpectedRevision: 1, Choice: domain.WebSearchConsentApproveV1}
	if err := request.Validate(); err != nil {
		t.Fatalf("valid decision selector: %v", err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*ports.DecideWebSearchConsentRequestV1)
	}{
		{"empty_web_session", func(r *ports.DecideWebSearchConsentRequestV1) { r.WebSessionDigest = "" }}, {"invalid_web_session", func(r *ports.DecideWebSearchConsentRequestV1) { r.WebSessionDigest = "not-a-digest" }},
		{"empty_csrf", func(r *ports.DecideWebSearchConsentRequestV1) { r.CSRFTokenDigest = "" }}, {"empty_request", func(r *ports.DecideWebSearchConsentRequestV1) { r.Locator.RequestID = "" }},
		{"zero_revision", func(r *ports.DecideWebSearchConsentRequestV1) { r.ExpectedRevision = 0 }}, {"stale_revision", func(r *ports.DecideWebSearchConsentRequestV1) { r.ExpectedRevision = 2 }},
		{"unknown_choice", func(r *ports.DecideWebSearchConsentRequestV1) { r.Choice = "renew" }}, {"empty_choice", func(r *ports.DecideWebSearchConsentRequestV1) { r.Choice = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			candidate := request
			tc.mutate(&candidate)
			if err := candidate.Validate(); err == nil {
				t.Errorf("invalid %s decision selector accepted", tc.name)
			}
		})
	}
	request.Choice = domain.WebSearchConsentDenyV1
	if err := request.Validate(); err != nil {
		t.Errorf("deny selector rejected: %v", err)
	}
}

func TestWebSearchConsentPortCannotReplaceArgumentsPrincipalOrDeadline(t *testing.T) {
	t.Parallel()
	// Freeze this deliberately small input surface. An authority projection,
	// caller-selected principal, replacement body, deadline or validity bool
	// cannot quietly be added to the later authenticated transaction bridge.
	for _, tc := range []struct {
		value  any
		fields []string
	}{
		{ports.DecideWebSearchConsentRequestV1{}, []string{"Locator", "WebSessionDigest", "CSRFTokenDigest", "ExpectedRevision", "Choice"}},
		{ports.ReadWebSearchConsentRequestV1{}, []string{"Locator", "WebSessionDigest"}},
		{ports.ReconcileWebSearchConsentRequestV1{}, []string{"Locator", "ExpectedRevision"}},
	} {
		typ := reflect.TypeOf(tc.value)
		if typ.NumField() != len(tc.fields) {
			t.Fatalf("%s field count=%d want=%d", typ.Name(), typ.NumField(), len(tc.fields))
		}
		for i, name := range tc.fields {
			if got := typ.Field(i).Name; got != name {
				t.Errorf("%s field %d=%s want=%s", typ.Name(), i, got, name)
			}
		}
	}
	store := reflect.TypeOf((*ports.WebSearchConsentStoreV1)(nil)).Elem()
	state := reflect.TypeOf(domain.WebSearchConsentStateV1{})
	errorType := reflect.TypeOf((*error)(nil)).Elem()
	if store.NumMethod() != 4 {
		t.Fatalf("consent store methods=%d want=4 bounded transactions", store.NumMethod())
	}
	for i := 0; i < store.NumMethod(); i++ {
		method := store.Method(i)
		if method.Type.NumOut() != 2 || method.Type.Out(0) != state || method.Type.Out(1) != errorType {
			t.Errorf("%s exposes something other than immutable state/error: %s", method.Name, method.Type)
		}
	}
}

func TestWebSearchConsentPortReadAndReconcileAreBounded(t *testing.T) {
	t.Parallel()
	locator, _ := consentPortFixture()
	read := ports.ReadWebSearchConsentRequestV1{Locator: locator, WebSessionDigest: domain.SecretDigest(consentPortHash("session"))}
	if err := read.Validate(); err != nil {
		t.Fatalf("read exact selector: %v", err)
	}
	read.WebSessionDigest = ""
	if err := read.Validate(); err == nil {
		t.Error("read accepted missing Web session")
	}
	for _, revision := range []uint64{0, 1, 2, 3, ^uint64(0)} {
		r := ports.ReconcileWebSearchConsentRequestV1{Locator: locator, ExpectedRevision: revision}
		err := r.Validate()
		wantValid := revision == 1 || revision == 2
		if (err == nil) != wantValid {
			t.Errorf("reconcile revision=%d err=%v want valid=%v", revision, err, wantValid)
		}
	}
}
