package ydbstore

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"reflect"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// This prover is intentionally unusable. Pure account reads/construction must
// never probe a provider or manufacture proof observations.
type accountNoProbeV2 struct{}

func (accountNoProbeV2) ProofAuthorizationURL(context.Context, ports.IdentityProviderProofAuthorizationRequestV1) (string, error) {
	panic("unexpected provider probe")
}
func (accountNoProbeV2) ExchangeAndVerifyProof(context.Context, ports.IdentityProviderProofTokenRequestV1) (ports.IdentityProviderProofResultV1, error) {
	panic("unexpected provider exchange")
}

func accountRandomV2() []byte {
	raw := make([]byte, 1024)
	for i := range raw {
		raw[i] = byte(i/32 + 1)
	}
	return raw
}
func accountOptionsV2(config identityConfigurationV2) IdentityAccountServiceOptionsV2 {
	options := IdentityAccountServiceOptionsV2{HMACKey: bytes.Repeat([]byte{37}, 32), Random: bytes.NewReader(accountRandomV2())}
	for _, binding := range config.Providers {
		options.Providers = append(options.Providers, IdentityAccountProviderV2{Binding: binding, Prover: accountNoProbeV2{}})
	}
	return options
}
func newAccountTestV2(t *testing.T, kind domain.IdentityOperationKindV2) (*identityProofSQLV2, *Store, *identityAccountServiceV2, context.Context, identityConfigurationV2) {
	t.Helper()
	f, store, ctx, _, config := newIdentityProofStorageV2(t, kind)
	store.webIdentityV2 = &config
	options := accountOptionsV2(config)
	service, err := NewIdentityAccountServiceV2(store, options)
	if err != nil {
		t.Fatal(err)
	}
	f.digest = domain.DigestSecret("op_" + base64.RawURLEncoding.EncodeToString(accountRandomV2()[:32]))
	return f, store, service.(*identityAccountServiceV2), ctx, config
}
func accountAuthV2() ports.IdentityAccountMutationAuthV2 {
	return ports.IdentityAccountMutationAuthV2{SessionCookie: "rows-session", CSRFCookie: "rows-csrf", PresentedCSRF: "rows-csrf"}
}
func accountStartV2(s *identityAccountServiceV2, ctx context.Context) (ports.IdentityAccountStartResultV2, error) {
	return s.Start(ctx, ports.IdentityAccountStartRequestV2{Auth: accountAuthV2(), Kind: domain.IdentityOperationLinkV2, TargetProvider: domain.IdentityProviderTelegram, ExpectedRevision: 7})
}

func TestIdentityAccountV2CurrentSessionStaleContinueAndConfirm(t *testing.T) {
	for _, method := range []string{"continue", "confirm"} {
		t.Run(method, func(t *testing.T) {
			f, _, service, ctx, _ := newAccountTestV2(t, domain.IdentityOperationLinkV2)
			start, err := accountStartV2(service, ctx)
			if err != nil {
				t.Fatal("valid original start", err)
			}
			baseline, err := service.Outcome(ctx, ports.IdentityAccountOutcomeRequestV2{Auth: ports.IdentityAccountReadAuthV2{SessionCookie: "rows-session"}, OperationID: start.View.OperationID, OperationCookie: start.OperationCookie})
			if err != nil || baseline.BeforeRevision != 7 || len(baseline.AllowedActions) == 0 {
				t.Fatal("valid CURRENT original-auth baseline", err)
			}
			before, writes, queries := f.operation, f.base.writes, f.base.queries
			if method == "continue" {
				result, err := service.Continue(ctx, ports.IdentityAccountContinueRequestV2{Auth: accountAuthV2(), OperationID: start.View.OperationID, OperationCookie: start.OperationCookie, ExpectedRevision: 8})
				if err != ports.IdentityAccountStaleRevisionV2 || result.AuthorizationURL != "" {
					t.Fatal("stale continue lost typed revision", err)
				}
			} else {
				result, err := service.Confirm(ctx, ports.IdentityAccountConfirmRequestV2{Auth: accountAuthV2(), OperationID: start.View.OperationID, OperationCookie: start.OperationCookie, ExpectedRevision: 8})
				if err != ports.IdentityAccountStaleRevisionV2 || result.View.OperationID != "" || result.Cookies != nil {
					t.Fatal("stale confirm lost typed revision", err)
				}
			}
			if f.operation != before || f.base.writes != writes || f.base.queries <= queries || len(f.children) != 0 {
				t.Fatal("stale phase performed writes or failed before actual current read")
			}
		})
	}
}

func TestIdentityAccountV2RevokedCookieIsNotAuthenticatedStaleRevision(t *testing.T) {
	f, _, service, ctx, _ := newAccountTestV2(t, domain.IdentityOperationLinkV2)
	start, err := accountStartV2(service, ctx)
	if err != nil {
		t.Fatal("current original positive", err)
	}
	session, err := domain.DecodeWebSessionV2([]byte(f.base.durable.sessions[string(domain.DigestSecret("rows-session"))]))
	if err != nil {
		t.Fatal(err)
	}
	at := f.at
	session.Session.RevokedAt = &at
	f.base.durable.sessions[string(session.Session.SessionDigest)] = identityRowsV2JSON(t, session)
	before, writes := f.operation, f.base.writes
	result, err := service.Confirm(ctx, ports.IdentityAccountConfirmRequestV2{Auth: accountAuthV2(), OperationID: start.View.OperationID, OperationCookie: start.OperationCookie, ExpectedRevision: 8})
	if err != ports.IdentityAccountUnauthenticatedV2 || result.View.OperationID != "" || result.Cookies != nil || f.operation != before || f.base.writes != writes {
		t.Fatal("revoked cookie was misclassified as authenticated revision conflict", err)
	}
}

func TestIdentityAccountV2UnknownForeignAndMismatchedPendingPredecessorsAbortStart(t *testing.T) {
	for _, mode := range []string{"unknown", "pending_browser", "foreign_terminal"} {
		t.Run(mode, func(t *testing.T) {
			raw := newIdentityAccountConnectedFixtureV2(t, domain.IdentityProviderYandex, false, nil)
			service := raw.Service.(*identityAccountServiceV2)
			foreignAuth := ports.IdentityAccountMutationAuthV2{SessionCookie: "foreign-current-cookie", CSRFCookie: "foreign-current-csrf", PresentedCSRF: "foreign-current-csrf"}
			if mode == "foreign_terminal" {
				_, identity, cookie, member := identityRowsV2Data(t)
				identity.UserID, identity.Subject.Subject = "foreign-current-user", "foreign-current-subject"
				member.UserID, member.TenantID = identity.UserID, "foreign-current-tenant"
				cookie.Session.SessionDigest, cookie.Session.CSRFTokenDigest = domain.DigestSecret(foreignAuth.SessionCookie), domain.DigestSecret(foreignAuth.CSRFCookie)
				cookie.Session.UserID, cookie.Session.ActiveTenantID, cookie.Session.AuthenticatedSubject = identity.UserID, member.TenantID, identity.Subject
				if identity.Validate() != nil || member.Validate() != nil || cookie.AuthorizeCurrent(identity, member, domain.TenantPermissionRead, raw.f.at) != nil {
					t.Fatal("foreign principal must be independently coherent before operation creation")
				}
				raw.f.base.durable.forward[identityRowsV2SubjectKey(identity.Subject)] = identityRowsV2JSON(t, identity)
				raw.f.base.durable.reverse[identityRowsV2ReverseKey(identity.UserID, identity.Subject)] = identityRowsV2Projection{user: identity.UserID, subject: identity.Subject, created: identity.CreatedAt}
				raw.f.base.durable.members[identityRowsV2MemberKey(member.UserID, member.TenantID)] = identityRowsV2JSON(t, member)
				raw.f.base.durable.sessions[string(cookie.Session.SessionDigest)] = identityRowsV2JSON(t, cookie)
				view, err := service.Methods(t.Context(), ports.IdentityAccountMethodsRequestV2{Auth: ports.IdentityAccountReadAuthV2{SessionCookie: foreignAuth.SessionCookie}})
				if err != nil || view.Revision != 7 || len(view.Methods) != 2 {
					t.Fatal("coherent foreign current methods baseline", err)
				}
			}
			start, err := accountStartV2(service, t.Context())
			if err != nil {
				t.Fatal("actual owner start prerequisite", err)
			}
			request := ports.IdentityAccountStartRequestV2{Auth: accountAuthV2(), Kind: domain.IdentityOperationLinkV2, TargetProvider: domain.IdentityProviderTelegram, ExpectedRevision: 7, PreviousOperationCookie: start.OperationCookie}
			switch mode {
			case "unknown":
				request.PreviousOperationCookie = "op_" + base64.RawURLEncoding.EncodeToString(make([]byte, 32)) + "." + base64.RawURLEncoding.EncodeToString(make([]byte, 32))
			case "pending_browser":
				request.PreviousOperationCookie = start.View.OperationID + "." + base64.RawURLEncoding.EncodeToString(make([]byte, 32))
			case "foreign_terminal":
				if view, err := service.Cancel(t.Context(), ports.IdentityAccountCancelRequestV2{Auth: accountAuthV2(), OperationID: start.View.OperationID, OperationCookie: start.OperationCookie, ExpectedRevision: 7}); err != nil || view.Outcome != domain.IdentityOutcomeCancelledV2 {
					t.Fatal("actual original owner's terminal prerequisite", err)
				}
				request.Auth = foreignAuth
			}
			before := raw.Snapshot()
			result, err := service.Start(t.Context(), request)
			after := raw.Snapshot()
			if err == nil || result.OperationCookie != "" || result.View.OperationID != "" || !reflect.DeepEqual(before, after) {
				t.Fatal("untrusted predecessor published a new cookie/operation or changed canonical state", err)
			}
		})
	}
}

func TestIdentityAccountV2ConstructorDoesNotInstall(t *testing.T) {
	f, store, _, _, config := newAccountTestV2(t, domain.IdentityOperationLinkV2)
	before := f.base.writes
	store.webIdentityV2 = nil
	if got, err := NewIdentityAccountServiceV2(store, accountOptionsV2(config)); err == nil || got != nil || store.webIdentityV2 != nil {
		t.Error("constructor activated unconfigured Store")
	}
	store.webIdentityV2 = &config
	for _, mutate := range []func(*IdentityAccountServiceOptionsV2){func(o *IdentityAccountServiceOptionsV2) { o.HMACKey = nil }, func(o *IdentityAccountServiceOptionsV2) { o.Providers[0].Prover = nil }, func(o *IdentityAccountServiceOptionsV2) { o.Providers[0].Binding.ClientID = "other" }, func(o *IdentityAccountServiceOptionsV2) { o.Providers = o.Providers[:1] }} {
		options := accountOptionsV2(config)
		mutate(&options)
		if got, err := NewIdentityAccountServiceV2(store, options); err == nil || got != nil {
			t.Error("mismatched construction accepted")
		}
	}
	if f.base.writes != before {
		t.Error("constructor performed storage effects")
	}
}

func TestIdentityAccountV2LoginBindingsRequireExactPairing(t *testing.T) {
	f, _, service, _, config := newAccountTestV2(t, domain.IdentityOperationLinkV2)
	before := f.base.writes
	if err := service.ValidateLoginBindings(config.Providers); err != nil {
		t.Fatal("actual immutable baseline pairing", err)
	}
	for _, mutate := range []func([]domain.IdentityProviderBindingV2) []domain.IdentityProviderBindingV2{
		func(v []domain.IdentityProviderBindingV2) []domain.IdentityProviderBindingV2 { return v[:1] },
		func(v []domain.IdentityProviderBindingV2) []domain.IdentityProviderBindingV2 {
			v[0], v[1] = v[1], v[0]
			return v
		},
		func(v []domain.IdentityProviderBindingV2) []domain.IdentityProviderBindingV2 {
			v[0].ClientID = "other"
			return v
		},
		func(v []domain.IdentityProviderBindingV2) []domain.IdentityProviderBindingV2 {
			v[0].CallbackURI = "https://other.example.com/auth/login/callback"
			return v
		},
	} {
		bindings := append([]domain.IdentityProviderBindingV2(nil), config.Providers...)
		if service.ValidateLoginBindings(mutate(bindings)) == nil {
			t.Error("BFF/Store/prover pairing mismatch admitted")
		}
	}
	if f.base.writes != before || f.base.queries != 0 {
		t.Error("pairing validation performed I/O")
	}
}

func TestIdentityAccountV2MethodsAndSelectorsArePure(t *testing.T) {
	f, _, service, ctx, _ := newAccountTestV2(t, domain.IdentityOperationUnlinkV2)
	before := f.base.durable.clone()
	writes := f.base.writes
	view, err := service.Methods(ctx, ports.IdentityAccountMethodsRequestV2{Auth: ports.IdentityAccountReadAuthV2{SessionCookie: "rows-session"}})
	if err != nil || view.Revision != 7 || len(view.Methods) != 2 {
		t.Fatal("actual own-methods read:", view, err)
	}
	for _, method := range view.Methods {
		if !method.Linked || method.MethodHandle == "" || method.RemainingProvider == "" || len(method.AllowedActions) != 1 || method.AllowedActions[0] != ports.IdentityAccountActionUnlinkV2 {
			t.Error("methods omitted exact own/removal projection", method)
		}
	}
	if !reflect.DeepEqual(before, f.base.durable) || f.base.writes != writes {
		t.Error("methods touched session or canonical data")
	}
	identity := identityRowsV2Identity(domain.IdentityProviderYandex, 7)
	handle := service.methodHandle(identity.UserID, identity)
	identity.Revision++
	if handle == service.methodHandle(identity.UserID, identity) || handle == service.methodHandle("other-owner", identity) {
		t.Error("method handle is not owner/revision bound")
	}
	binding := service.bindings[domain.IdentityProviderYandex]
	if service.state("op-a", domain.IdentityPhaseSourceProofV2, binding) == service.state("op-a", domain.IdentityPhaseTargetProofV2, binding) || service.state("op-a", domain.IdentityPhaseSourceProofV2, binding) == service.state("op-b", domain.IdentityPhaseSourceProofV2, binding) {
		t.Error("proof states lack exact phase/operation separation")
	}
}

func TestIdentityAccountV2StartOutcomeHintsCancelAndUnknown(t *testing.T) {
	f, _, service, ctx, _ := newAccountTestV2(t, domain.IdentityOperationLinkV2)
	start, err := accountStartV2(service, ctx)
	if err != nil || start.OperationCookie == "" || start.View.OperationID == "" {
		t.Fatal("actual start:", err)
	}
	original := f.operation
	writes := f.base.writes
	state := f.base.durable.clone()
	for _, cookie := range []string{"", "wrong-cookie", start.OperationCookie} {
		view, err := service.Outcome(ctx, ports.IdentityAccountOutcomeRequestV2{Auth: ports.IdentityAccountReadAuthV2{SessionCookie: "rows-session"}, OperationID: start.View.OperationID, OperationCookie: cookie})
		if err != nil || view.Phase != domain.IdentityPhaseTargetProofV2 {
			t.Fatal("same-user GET:", err)
		}
		if (len(view.AllowedActions) > 0) != (cookie == start.OperationCookie) {
			t.Error("optional hints acquired GET authority")
		}
	}
	if f.operation != original || f.base.writes != writes || !reflect.DeepEqual(state, f.base.durable) {
		t.Error("outcome GET wrote/refreshed data")
	}
	wrong := accountAuthV2()
	wrong.PresentedCSRF = "foreign"
	if value, err := service.Cancel(ctx, ports.IdentityAccountCancelRequestV2{Auth: wrong, OperationID: start.View.OperationID, OperationCookie: start.OperationCookie, ExpectedRevision: 7}); err == nil || value.Phase != "" || f.operation != original {
		t.Error("wrong CSRF stopped operation")
	}
	cancelled, err := service.Cancel(ctx, ports.IdentityAccountCancelRequestV2{Auth: accountAuthV2(), OperationID: start.View.OperationID, OperationCookie: start.OperationCookie, ExpectedRevision: 7})
	if err != nil || cancelled.Outcome != domain.IdentityOutcomeCancelledV2 || cancelled.CompletedAt == nil || cancelled.TargetProvider != "" {
		t.Fatal("actual cancel:", cancelled, err)
	}
	if len(f.audit) != 1 {
		t.Error("cancel missing atomic audit")
	}
	view, err := service.Outcome(ctx, ports.IdentityAccountOutcomeRequestV2{Auth: ports.IdentityAccountReadAuthV2{SessionCookie: "rows-session"}, OperationID: start.View.OperationID})
	if err != nil || view.Outcome != cancelled.Outcome {
		t.Error("receipt GET required browser/CSRF", err)
	}
}

func TestIdentityAccountV2StartFailuresPublishNoCookie(t *testing.T) {
	for _, mode := range []string{"stale", "csrf", "lost-commit", "previous-unknown"} {
		t.Run(mode, func(t *testing.T) {
			f, _, service, ctx, _ := newAccountTestV2(t, domain.IdentityOperationLinkV2)
			request := ports.IdentityAccountStartRequestV2{Auth: accountAuthV2(), Kind: domain.IdentityOperationLinkV2, TargetProvider: domain.IdentityProviderTelegram, ExpectedRevision: 7}
			if mode == "stale" {
				request.ExpectedRevision++
			}
			if mode == "csrf" {
				request.Auth.PresentedCSRF = "other"
			}
			if mode == "lost-commit" {
				f.commitReplyError = errors.New("controlled durable reply loss")
			}
			if mode == "previous-unknown" {
				request.PreviousOperationCookie = "invalid"
			}
			got, err := service.Start(ctx, request)
			if err == nil || got.OperationCookie != "" || got.View.OperationID != "" {
				t.Fatal("failed/unknown start published cookie/view", err)
			}
			if mode == "lost-commit" {
				if f.operation == "" {
					t.Error("lost commit did not actually persist original operation")
				}
			} else if f.operation != "" {
				t.Error("denial persisted operation")
			}
		})
	}
}

func TestIdentityAccountV2OutcomeStorageFailureIsNotAbsence(t *testing.T) {
	f, _, service, ctx, _ := newAccountTestV2(t, domain.IdentityOperationLinkV2)
	id := "op_" + base64.RawURLEncoding.EncodeToString(accountRandomV2()[:32])
	request := ports.IdentityAccountOutcomeRequestV2{Auth: ports.IdentityAccountReadAuthV2{SessionCookie: "rows-session"}, OperationID: id}
	view, err := service.Outcome(ctx, request)
	if err != nil || view.Outcome != domain.IdentityOutcomeUnavailableV2 {
		t.Fatal("genuine authorized absence:", err)
	}
	f.base.failStatement = identityRowsV2Statement(identityOperationRowSQLV2)
	f.base.failure = errors.New("controlled storage failure")
	view, err = service.Outcome(ctx, request)
	if err != ports.IdentityAccountUnavailableV2 || view.Outcome != "" {
		t.Error("unknown storage failure became retained absence", view, err)
	}
}
