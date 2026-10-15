package webbff_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/webbff"
	"gitcode.com/urandon/sessionless/internal/webcontract"
)

// Transport boundary fixture only. Actual current source/immutable R/M/CAS
// behavior is exercised separately by ydbstore SQL-snapshot tests; this fake
// neither qualifies a model nor creates an authorized spending decision.
type piPolicyBFFFixture struct {
	reads, confirms, revokes int
	confirm                  ports.ConfirmAttachedPiAPIPolicyV1
	revoke                   ports.RevokeAttachedPiAPIPolicyV1
}

func (f *piPolicyBFFFixture) Read(context.Context, ports.ReadAttachedPiAPIPolicyV1) (ports.AttachedPiAPIPolicyOfferV1, error) {
	f.reads++
	return ports.AttachedPiAPIPolicyOfferV1{}, nil
}
func (f *piPolicyBFFFixture) Confirm(_ context.Context, r ports.ConfirmAttachedPiAPIPolicyV1) (ports.AttachedPiAPIPolicyDecisionV1, error) {
	f.confirms++
	f.confirm = r
	return ports.AttachedPiAPIPolicyDecisionV1{}, nil
}
func (f *piPolicyBFFFixture) Revoke(_ context.Context, r ports.RevokeAttachedPiAPIPolicyV1) error {
	f.revokes++
	f.revoke = r
	return nil
}

func newPiPolicyBFF(t *testing.T, service *piPolicyBFFFixture) (http.Handler, *http.Cookie, *http.Cookie) {
	t.Helper()
	store := newMemoryAuthStore()
	subject := domain.ExternalSubject{Provider: domain.IdentityProviderTelegram, Subject: "pi-policy-user"}
	owner := domain.UserID("pi-policy-browser-owner")
	store.identities[subject] = domain.ExternalIdentity{Subject: subject, UserID: owner, CreatedAt: bffTestTime, UpdatedAt: bffTestTime}
	store.memberships[owner] = []domain.TenantMembership{membership("pi-policy-browser-tenant", owner, domain.TenantMembershipOwner)}
	h, err := webbff.New(webbff.Config{BaseURL: "https://web.dev.sessionless.triborg.dev", RedirectURI: "https://web.dev.sessionless.triborg.dev" + webcontract.RouteOIDCCallback,
		OIDCPolicy: domain.OIDCVerificationPolicy{Issuer: "https://oauth.telegram.org", Audience: "100000", AllowedAlgorithms: []string{"RS256"}}, Provider: fakeProvider{subject: subject.Subject}, Store: store, IDs: fixedIDs{}, Clock: fixedClock{now: bffTestTime}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), AttachedPiAPIPolicies: service})
	if err != nil {
		t.Fatal(err)
	}
	session, csrf := performLogin(t, h, "/sessions")
	return h, session, csrf
}
func piPolicyBFFRequest(route, body string, session, csrf *http.Cookie) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "https://web.dev.sessionless.triborg.dev"+route, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Origin", "https://web.dev.sessionless.triborg.dev")
	r.Header.Set(webcontract.CSRFHeaderName, csrf.Value)
	r.AddCookie(session)
	r.AddCookie(csrf)
	return r
}

func TestAttachedPiAPIPolicyBFFUsesOriginalAuthenticatedMutation(t *testing.T) {
	f := &piPolicyBFFFixture{}
	h, session, csrf := newPiPolicyBFF(t, f)
	selector := `"version":1,"resource_id":"selected-api","model_vendor_id":"selected-vendor","model_id":"selected-model","input_data_class":"private"`
	body := `{` + selector + `,"terms_digest":"` + strings.Repeat("a", 64) + `","expected_generation":0,"mutation_id":"browser-confirm-exact-terms"}`
	w := httptest.NewRecorder()
	h.ServeHTTP(w, piPolicyBFFRequest(webcontract.RouteAttachedPiAPIPolicyConfirm, body, session, csrf))
	if w.Code != http.StatusOK || f.confirms != 1 {
		t.Fatalf("confirmation status=%d calls=%d body=%s", w.Code, f.confirms, w.Body.String())
	}
	r := f.confirm
	if r.Locator.TenantID != "pi-policy-browser-tenant" || r.Locator.OwnerUserID != "pi-policy-browser-owner" || r.WebSessionDigest != domain.DigestSecret(session.Value) || r.CSRFTokenDigest != domain.DigestSecret(csrf.Value) || r.TermsDigest != domain.ProviderEvidenceDigestV1(strings.Repeat("a", 64)) || r.MutationID != "browser-confirm-exact-terms" {
		t.Fatal("authenticated actor/digest or exact confirmation changed")
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, piPolicyBFFRequest(webcontract.RouteAttachedPiAPIPolicyRead, `{`+selector+`}`, session, csrf))
	if w.Code != http.StatusOK || f.reads != 1 {
		t.Fatal("authenticated offer read failed")
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, piPolicyBFFRequest(webcontract.RouteAttachedPiAPIPolicyRevoke, `{`+selector+`,"expected_generation":1,"expected_mapping":"`+strings.Repeat("b", 64)+`"}`, session, csrf))
	if w.Code != http.StatusNoContent || f.revokes != 1 || f.revoke.CSRFTokenDigest != r.CSRFTokenDigest {
		t.Fatal("authenticated revoke lost original mutation boundary")
	}
}

func TestAttachedPiAPIPolicyBFFRejectsForgedAndMalformedBeforeService(t *testing.T) {
	base := `{"version":1,"resource_id":"selected-api","model_vendor_id":"selected-vendor","model_id":"selected-model","input_data_class":"private","terms_digest":"` + strings.Repeat("a", 64) + `","expected_generation":0,"mutation_id":"exact-confirmation"}`
	for _, mode := range []string{"csrf", "origin", "actor_field", "receipt_field", "duplicate", "case_version", "case_terms_digest", "case_model_id", "missing_generation", "null_generation", "nested", "trailing", "oversize", "query", "wrong_type"} {
		t.Run(mode, func(t *testing.T) {
			f := &piPolicyBFFFixture{}
			h, session, csrf := newPiPolicyBFF(t, f)
			body := base
			switch mode {
			case "actor_field":
				body = strings.TrimSuffix(body, "}") + `,"owner_user_id":"foreign"}`
			case "receipt_field":
				body = strings.TrimSuffix(body, "}") + `,"receipt":{}}`
			case "duplicate":
				body = strings.TrimSuffix(body, "}") + `,"expected_generation":1}`
			case "case_version":
				body = strings.Replace(body, `"version":1`, `"Version":1`, 1)
			case "case_terms_digest":
				body = strings.Replace(body, `"terms_digest":`, `"Terms_Digest":`, 1)
			case "case_model_id":
				body = strings.Replace(body, `"model_id":`, `"Model_ID":`, 1)
			case "missing_generation":
				body = strings.Replace(body, `,"expected_generation":0`, "", 1)
			case "null_generation":
				body = strings.Replace(body, `"expected_generation":0`, `"expected_generation":null`, 1)
			case "nested":
				body = strings.Replace(body, `"resource_id":"selected-api"`, `"resource_id":{}`, 1)
			case "trailing":
				body += " {}"
			case "oversize":
				body = strings.Repeat("x", 4097)
			}
			r := piPolicyBFFRequest(webcontract.RouteAttachedPiAPIPolicyConfirm, body, session, csrf)
			switch mode {
			case "csrf":
				r.Header.Set(webcontract.CSRFHeaderName, "foreign")
			case "origin":
				r.Header.Set("Origin", "https://foreign.invalid")
			case "query":
				r.URL.RawQuery = "tenant=foreign"
			case "wrong_type":
				r.Header.Set("Content-Type", "text/plain")
			}
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code < 400 || f.confirms != 0 {
				t.Fatalf("mode=%s status=%d service_calls=%d", mode, w.Code, f.confirms)
			}
		})
	}
}
