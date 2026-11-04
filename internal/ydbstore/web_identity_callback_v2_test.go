package ydbstore

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/telegramoidc"
	"gitcode.com/urandon/sessionless/internal/yandexoauth"
)

func accountPendingChildV2(t *testing.T) (*identityProofSQLV2, *Store, *identityAccountServiceV2, ports.IdentityAccountStartResultV2, domain.IdentityChallengeV2) {
	t.Helper()
	f, store, service, ctx, _ := newAccountTestV2(t, domain.IdentityOperationLinkV2)
	start, err := accountStartV2(service, ctx)
	if err != nil {
		t.Fatal("actual start", err)
	}
	phase, err := service.phase(accountAuthV2(), start.View.OperationID, start.OperationCookie, 7)
	if err != nil {
		t.Fatal(err)
	}
	f.at = f.at.Add(time.Second)
	state := service.state(start.View.OperationID, domain.IdentityPhaseSourceProofV2, service.bindings[domain.IdentityProviderYandex])
	child, err := store.createIdentityProofV2(ctx, identityProofChildV2{Phase: phase, ChallengeDigest: domain.DigestSecret(state), PKCEVerifier: strings.Repeat("v", 43)}, service.config)
	if err != nil {
		t.Fatal("actual child", err)
	}
	f.at = f.at.Add(time.Second)
	return f, store, service, start, child
}

type accountErrorProverV2 struct {
	err   error
	calls int
}

func (*accountErrorProverV2) ProofAuthorizationURL(context.Context, ports.IdentityProviderProofAuthorizationRequestV1) (string, error) {
	panic("error-classification fixture must not request another authorization URL")
}
func (p *accountErrorProverV2) ExchangeAndVerifyProof(_ context.Context, request ports.IdentityProviderProofTokenRequestV1) (ports.IdentityProviderProofResultV1, error) {
	p.calls++
	if err := request.Validate(); err != nil {
		return ports.IdentityProviderProofResultV1{}, err
	}
	return ports.IdentityProviderProofResultV1{}, p.err
}

// Starts and consumes through the actual service/Store owners; the only supplied
// error is at the already-consumed provider boundary, never a supplied proof.
func accountCallbackErrorFixtureV2(t *testing.T, provider domain.IdentityProvider, override ports.IdentityProviderProverV1) (*IdentityAccountConnectedFixtureV2, ports.IdentityAccountCallbackRequestV2, domain.SecretDigest) {
	t.Helper()
	raw := newIdentityAccountConnectedFixtureV2(t, provider, false, override)
	service := raw.Service.(*identityAccountServiceV2)
	target := domain.IdentityProviderTelegram
	if provider == target {
		target = domain.IdentityProviderYandex
	}
	start, err := service.Start(t.Context(), ports.IdentityAccountStartRequestV2{Auth: accountAuthV2(), Kind: domain.IdentityOperationLinkV2, TargetProvider: target, ExpectedRevision: 7})
	if err != nil {
		t.Fatal("actual current start before provider error", err)
	}
	phase, err := service.phase(accountAuthV2(), start.View.OperationID, start.OperationCookie, 7)
	if err != nil {
		t.Fatal(err)
	}
	raw.Advance(time.Second)
	state := service.state(start.View.OperationID, domain.IdentityPhaseSourceProofV2, service.bindings[provider])
	nonce := ""
	if provider == domain.IdentityProviderTelegram {
		nonce = "original-proof-nonce"
	}
	child, err := raw.Store.createIdentityProofV2(t.Context(), identityProofChildV2{Phase: phase, ChallengeDigest: domain.DigestSecret(state), PKCEVerifier: strings.Repeat("v", 43), Nonce: nonce}, service.config)
	if err != nil {
		t.Fatal("actual original child before provider error", err)
	}
	raw.Advance(time.Second)
	path := "/auth/login/callback"
	if provider == domain.IdentityProviderTelegram {
		path = "/auth/telegram/callback"
	}
	return raw, ports.IdentityAccountCallbackRequestV2{Auth: ports.IdentityAccountCallbackAuthV2{SessionCookie: "rows-session", OperationCookie: start.OperationCookie}, CallbackPath: path, State: state, Code: "valid-code"}, child.StateDigest
}

func TestIdentityAccountV2CallbackClassifiesOnlyKnownProviderFailures(t *testing.T) {
	for _, provider := range []domain.IdentityProvider{domain.IdentityProviderYandex, domain.IdentityProviderTelegram} {
		unavailable, denied, other := yandexoauth.ErrProviderUnavailable, yandexoauth.ErrProviderResponse, telegramoidc.ErrProviderUnavailable
		if provider == domain.IdentityProviderTelegram {
			unavailable, denied, other = telegramoidc.ErrProviderUnavailable, telegramoidc.ErrProviderResponse, yandexoauth.ErrProviderUnavailable
		}
		for _, test := range []struct {
			name    string
			err     error
			outcome domain.IdentityOperationOutcomeV2
		}{
			{"known_unavailable", unavailable, domain.IdentityOutcomeProviderUnavailableV2},
			{"known_denial", denied, domain.IdentityOutcomeProofUnavailableV2},
			{"arbitrary", errors.New("unclassified private provider failure"), ""},
			{"validation", domain.ValidationError{Field: "identity_proof.code", Reason: "invalid"}, ""},
			{"cancelled_child_parent_alive", context.Canceled, ""},
			{"other_provider_error", other, ""},
		} {
			t.Run(string(provider)+"/"+test.name, func(t *testing.T) {
				prover := &accountErrorProverV2{err: test.err}
				raw, request, digest := accountCallbackErrorFixtureV2(t, provider, prover)
				before := raw.Snapshot()
				view, err := raw.Service.Callback(t.Context(), request)
				if t.Context().Err() != nil || prover.calls != 1 {
					t.Fatal("did not reach one provider call with live parent")
				}
				stored, decodeErr := domain.DecodeIdentityChallengeV2([]byte(raw.f.children[digest][9].(string)))
				if decodeErr != nil || stored.ConsumedAt == nil || stored.Secrets != nil || raw.f.children[digest][3] != "" || raw.f.children[digest][4] != "" {
					t.Fatal("error path did not retain actual committed scrub", decodeErr)
				}
				operation, decodeErr := domain.DecodeIdentityOperationV2([]byte(raw.f.operation))
				if decodeErr != nil {
					t.Fatal(decodeErr)
				}
				if test.outcome == "" {
					if err != ports.IdentityAccountUnavailableV2 || !reflect.DeepEqual(view, ports.IdentityAccountOperationViewV2{}) || operation.Receipt != nil || operation.Pending == nil || len(raw.f.audit) != 0 || raw.f.operation != before.Operation {
						t.Fatal("unknown error invented receipt/audit or leaked output", err)
					}
				} else {
					if err != nil || view.Outcome != test.outcome || operation.Pending != nil || operation.Receipt == nil || len(raw.f.audit) != 1 {
						t.Fatal("known provider result did not terminalize exactly", view, err)
					}
					var audit domain.WebSecurityAuditEvent
					if json.Unmarshal([]byte(raw.f.audit[0][10].(string)), &audit) != nil || audit.Action != domain.WebSecurityIdentityProofFailed || audit.Provider != provider || audit.ReasonCode != string(test.outcome) {
						t.Fatal("known failure audit lost original CHILD attribution")
					}
				}
				writes := raw.f.base.writes
				if repeated, err := raw.Service.Callback(t.Context(), request); err == nil || repeated.OperationID != "" || prover.calls != 1 || raw.f.base.writes != writes {
					t.Fatal("consumed failure exchanged/terminalized again", err)
				}
				after := raw.Snapshot()
				if !reflect.DeepEqual(before.Forward, after.Forward) || !reflect.DeepEqual(before.Members, after.Members) || !reflect.DeepEqual(before.Sessions, after.Sessions) {
					t.Fatal("provider failure changed identities/member/session")
				}
			})
		}
	}
}

func TestIdentityAccountV2RealAdapterValidationDoesNotInventFailureReceipt(t *testing.T) {
	for _, provider := range []domain.IdentityProvider{domain.IdentityProviderYandex, domain.IdentityProviderTelegram} {
		t.Run(string(provider), func(t *testing.T) {
			raw, request, digest := accountCallbackErrorFixtureV2(t, provider, nil)
			request.Code = "x\n"
			before := raw.Snapshot()
			view, err := raw.Service.Callback(t.Context(), request)
			after := raw.Snapshot()
			child, decodeErr := domain.DecodeIdentityChallengeV2([]byte(after.Children[digest][9].(string)))
			if err != ports.IdentityAccountUnavailableV2 || view.OperationID != "" || decodeErr != nil || child.ConsumedAt == nil || child.Secrets != nil || after.Tokens != 0 || after.Infos != 0 || after.JWKS != 0 || after.Operation != before.Operation || len(raw.f.audit) != 0 {
				t.Fatal("real adapter pre-I/O validation fabricated durable failure", err, decodeErr)
			}
		})
	}
}

func TestIdentityAccountV2StoredPurposeAndCallbackBinding(t *testing.T) {
	f, store, service, start, child := accountPendingChildV2(t)
	state := service.state(start.View.OperationID, domain.IdentityPhaseSourceProofV2, child.Binding)
	before := append([]any(nil), f.children[child.StateDigest]...)
	writes := f.base.writes
	purpose, err := service.CallbackPurpose(t.Context(), ports.IdentityAccountCallbackPurposeRequestV2{CallbackPath: "/auth/login/callback", State: state})
	if err != nil || purpose != ports.IdentityAccountCallbackProofV2 {
		t.Fatal("stored proof purpose", purpose, err)
	}
	if purpose, err := service.CallbackPurpose(t.Context(), ports.IdentityAccountCallbackPurposeRequestV2{CallbackPath: "/auth/telegram/callback", State: state}); err == nil || purpose != 0 {
		t.Error("cross-provider callback lookup accepted")
	}
	if !reflect.DeepEqual(before, f.children[child.StateDigest]) || f.base.writes != writes {
		t.Error("purpose read consumed/touched state")
	}
	login := domain.OIDCLoginChallenge{Provider: domain.IdentityProviderYandex, Issuer: domain.YandexOAuthIssuer, Audience: child.Binding.ClientID, StateDigest: domain.DigestSecret("ordinary-state"), BrowserBindingDigest: domain.DigestSecret("ordinary-browser"), PKCEVerifier: strings.Repeat("l", 43), RedirectPath: "/", CreatedAt: f.at, ExpiresAt: f.at.Add(time.Minute)}
	if err := store.CreateLoginChallenge(t.Context(), login); err != nil {
		t.Fatal("actual V2 login child", err)
	}
	if purpose, err := service.CallbackPurpose(t.Context(), ports.IdentityAccountCallbackPurposeRequestV2{CallbackPath: "/auth/login/callback", State: "ordinary-state"}); err != nil || purpose != ports.IdentityAccountCallbackLoginV2 {
		t.Fatal("ordinary login purpose", purpose, err)
	}
	ordinary := append([]any(nil), f.children[login.StateDigest]...)
	_, err = service.Callback(t.Context(), ports.IdentityAccountCallbackRequestV2{Auth: ports.IdentityAccountCallbackAuthV2{SessionCookie: "rows-session", OperationCookie: start.OperationCookie}, CallbackPath: "/auth/login/callback", State: "ordinary-state", ProviderDenied: true})
	if err == nil || !reflect.DeepEqual(ordinary, f.children[login.StateDigest]) {
		t.Error("proof callback consumed a LOGIN challenge")
	}
}

func TestIdentityAccountV2ProviderDenialPrunesActualSourceOnly(t *testing.T) {
	f, _, service, start, child := accountPendingChildV2(t)
	view, err := service.Callback(t.Context(), ports.IdentityAccountCallbackRequestV2{Auth: ports.IdentityAccountCallbackAuthV2{SessionCookie: "rows-session", OperationCookie: start.OperationCookie}, CallbackPath: "/auth/login/callback", State: service.state(start.View.OperationID, domain.IdentityPhaseSourceProofV2, child.Binding), ProviderDenied: true})
	if err != nil || view.Outcome != domain.IdentityOutcomeProofUnavailableV2 || view.Phase != domain.IdentityPhaseTerminalV2 || view.TargetProvider != "" || view.AfterRevision != nil {
		t.Fatal("negative callback result", view, err)
	}
	consumed, err := domain.DecodeIdentityChallengeV2([]byte(f.children[child.StateDigest][9].(string)))
	if err != nil || consumed.ConsumedAt == nil || consumed.Secrets != nil {
		t.Fatal("actual child not scrubbed", err)
	}
	terminal, err := domain.DecodeIdentityOperationV2([]byte(f.operation))
	if err != nil || terminal.Pending != nil || terminal.Receipt == nil || len(f.audit) != 1 {
		t.Fatal("negative not atomically pruned/audited", err)
	}
	var audit domain.WebSecurityAuditEvent
	if json.Unmarshal([]byte(f.audit[0][10].(string)), &audit) != nil || audit.Action != domain.WebSecurityIdentityProofFailed || audit.Provider != domain.IdentityProviderYandex || audit.ReasonCode != "proof_unavailable" || audit.SubjectFingerprint != "" {
		t.Error("failure audit attributed TARGET rather than actual SOURCE child")
	}
}

func TestIdentityAccountV2UnknownConsumeCannotExchangeOrRecreate(t *testing.T) {
	f, _, service, start, child := accountPendingChildV2(t)
	f.commitReplyError = errors.New("controlled lost reply after actual scrub")
	view, err := service.Callback(t.Context(), ports.IdentityAccountCallbackRequestV2{Auth: ports.IdentityAccountCallbackAuthV2{SessionCookie: "rows-session", OperationCookie: start.OperationCookie}, CallbackPath: "/auth/login/callback", State: service.state(start.View.OperationID, domain.IdentityPhaseSourceProofV2, child.Binding), Code: "not-exchanged"})
	if err != ports.IdentityAccountUnavailableV2 || view.OperationID != "" {
		t.Fatal("unknown consume published proof permission", err)
	}
	stored, decodeErr := domain.DecodeIdentityChallengeV2([]byte(f.children[child.StateDigest][9].(string)))
	if decodeErr != nil || stored.ConsumedAt == nil || stored.Secrets != nil {
		t.Fatal("lost response did not actually scrub", decodeErr)
	}
	if operation, err := domain.DecodeIdentityOperationV2([]byte(f.operation)); err != nil || operation.Receipt != nil {
		t.Error("unknown consume invented terminal receipt", err)
	}
	writes := f.base.writes
	if result, err := service.Continue(t.Context(), ports.IdentityAccountContinueRequestV2{Auth: accountAuthV2(), OperationID: start.View.OperationID, OperationCookie: start.OperationCookie, ExpectedRevision: 7}); err == nil || result.AuthorizationURL != "" || f.base.writes != writes {
		t.Error("consumed child recreated/exchanged after unknown commit")
	}
}
