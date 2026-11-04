package ydbstore

import (
	"context"
	"errors"
	"net/url"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/telegramoidc"
	"gitcode.com/urandon/sessionless/internal/yandexoauth"
)

func identityAccountCallbackMatchesV2(path string, binding domain.IdentityProviderBindingV2) bool {
	if binding.Validate() != nil {
		return false
	}
	u, err := url.Parse(binding.CallbackURI)
	return err == nil && path == u.Path
}

func (s *identityAccountServiceV2) CallbackPurpose(ctx context.Context, request ports.IdentityAccountCallbackPurposeRequestV2) (ports.IdentityAccountCallbackPurposeV2, error) {
	ctx, cancel, err := s.requestContext(ctx)
	if err != nil {
		return 0, err
	}
	defer cancel()
	if domain.ValidateOpaqueID("identity.callback_state", request.State) != nil {
		return 0, ports.IdentityAccountAccessDeniedV2
	}
	var result ports.IdentityAccountCallbackPurposeV2
	err = s.store.identityAuthorityTxV2(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		result = 0
		if err := identityAccountCopiesV2(q); err != nil {
			return err
		}
		child, found, err := readStoredIdentityChallengeV2(ctx, q, domain.DigestSecret(request.State))
		if err != nil || !found {
			return domain.ErrMembershipDenied
		}
		if s.bindings[child.Binding.Provider] != child.Binding || !identityAccountCallbackMatchesV2(request.CallbackPath, child.Binding) || q.at.Before(child.CreatedAt) || !q.at.Before(child.ExpiresAt) {
			return domain.ErrMembershipDenied
		}
		switch child.Purpose {
		case domain.IdentityPurposeLoginV2:
			result = ports.IdentityAccountCallbackLoginV2
		case domain.IdentityPurposeProviderProofV2:
			result = ports.IdentityAccountCallbackProofV2
		default:
			return domain.ErrMembershipDenied
		}
		return nil
	})
	if err != nil {
		return 0, identityAccountErrorV2(err)
	}
	return result, nil
}

func (s *identityAccountServiceV2) Callback(ctx context.Context, request ports.IdentityAccountCallbackRequestV2) (ports.IdentityAccountOperationViewV2, error) {
	var empty ports.IdentityAccountOperationViewV2
	ctx, cancel, err := s.requestContext(ctx)
	if err != nil {
		return empty, err
	}
	defer cancel()
	if !identityAccountCookieV2(request.Auth.SessionCookie) {
		return empty, ports.IdentityAccountUnauthenticatedV2
	}
	if domain.ValidateOpaqueID("identity.callback_state", request.State) != nil || (request.Code == "") == !request.ProviderDenied {
		return empty, ports.IdentityAccountAccessDeniedV2
	}
	id, err := identityAccountCookieIDV2(request.Auth.OperationCookie)
	if err != nil {
		return empty, err
	}
	var consumed identityConsumedProofV2
	var first time.Time
	consumeStarted := time.Now()
	err = s.store.identityAuthorityTxV2(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		consumed = identityConsumedProofV2{}
		if identityProofFirstTimeV2(&first, q.at) != nil {
			return domain.ErrMembershipDenied
		}
		if err := identityAccountCopiesV2(q); err != nil {
			return err
		}
		child, found, err := readIdentityProofChildV2(ctx, q, domain.DigestSecret(request.State))
		if err != nil || !found || child.Proof == nil || child.Proof.ParentOperationDigest != domain.DigestSecret(id) || s.bindings[child.Binding.Provider] != child.Binding || !identityAccountCallbackMatchesV2(request.CallbackPath, child.Binding) {
			return domain.ErrMembershipDenied
		}
		// A callback has no Strict CSRF cookie/header. Derive the original digest
		// from this exact CURRENT login session inside the owning transaction.
		session, _, err := s.readSession(ctx, q, request.Auth.SessionCookie)
		if err != nil {
			return err
		}
		phase := identityPhaseRequestV2{OperationDigest: child.Proof.ParentOperationDigest, SessionDigest: session.Session.SessionDigest, CSRFDigest: session.Session.CSRFTokenDigest, BrowserBindingDigest: domain.DigestSecret(request.Auth.OperationCookie), ExpectedRevision: session.IdentityRevision}
		if _, err := identityProofBindingsV2(phase, s.config); err != nil {
			return err
		}
		operation, err := currentIdentityProofOperationV2(ctx, q, phase, s.config, s.bindings)
		if err != nil {
			return err
		}
		if s.state(id, operation.Phase, child.Binding) != request.State {
			return domain.ErrMembershipDenied
		}
		consumed, err = consumeIdentityProofInTxV2(ctx, q, identityProofConsumeV2{Phase: phase, ChallengeDigest: child.StateDigest, BrowserSecret: request.Auth.OperationCookie}, operation, child, s.bindings, first)
		return err
	})
	if err != nil {
		return empty, identityAccountErrorV2(err)
	}
	if request.ProviderDenied {
		consumed.PKCEVerifier = ""
		consumed.Nonce = ""
		return s.failedProof(ctx, id, consumed, domain.IdentityOutcomeProofUnavailableV2)
	}
	proofRequest := ports.IdentityProviderProofTokenRequestV1{Binding: consumed.Binding, Code: request.Code, PKCEVerifier: consumed.PKCEVerifier, ExpectedNonce: consumed.Nonce, ChallengeCreatedAt: consumed.ChallengeCreatedAt, ChallengeExpiresAt: consumed.ChallengeExpiresAt, Now: consumed.At.Add(time.Since(consumeStarted))}
	if proofRequest.Validate() != nil {
		// Concrete adapters intentionally redact invalid input as their public
		// response error. Validate the canonical request here BEFORE invoking
		// one so malformed caller input cannot become a provider observation.
		consumed.PKCEVerifier, consumed.Nonce = "", ""
		proofRequest.PKCEVerifier, proofRequest.ExpectedNonce = "", ""
		return empty, ports.IdentityAccountUnavailableV2
	}
	// These secrets never become a new durable value or a caller-supplied proof.
	verified, providerErr := s.provers[consumed.Binding.Provider].ExchangeAndVerifyProof(ctx, proofRequest)
	consumed.PKCEVerifier = ""
	consumed.Nonce = ""
	proofRequest.PKCEVerifier = ""
	proofRequest.ExpectedNonce = ""
	if ctx.Err() != nil {
		return empty, ports.IdentityAccountUnavailableV2
	}
	if providerErr != nil {
		outcome, known := identityAccountProofFailureV2(consumed.Binding.Provider, providerErr)
		if !known {
			// A scrubbed child is not evidence of a definite provider result.
			// Unknown, validation and cancellation errors retain the pending
			// operation without authorizing another exchange or a receipt.
			return empty, ports.IdentityAccountUnavailableV2
		}
		return s.failedProof(ctx, id, consumed, outcome)
	}
	operation, err := s.store.recordIdentityProofV2(ctx, identityProofCompletionV2{Phase: consumed.Phase, ChallengeDigest: consumed.StateDigest, Verified: acceptedIdentityProofV2{Subject: verified.Subject, Kind: verified.Kind, SignedAuthTime: verified.SignedAuthTime, Binding: consumed.Binding}}, s.config)
	if err != nil {
		return empty, identityAccountErrorV2(err)
	}
	view := identityAccountViewV2(id, operation, consumed.At)
	if operation.Phase == domain.IdentityPhaseConfirmV2 {
		view.AllowedActions = []ports.IdentityAccountActionV2{ports.IdentityAccountActionConfirmV2, ports.IdentityAccountActionCancelV2}
	} else {
		view.AllowedActions = []ports.IdentityAccountActionV2{ports.IdentityAccountActionContinueV2, ports.IdentityAccountActionCancelV2}
	}
	return view, nil
}

func identityAccountProofFailureV2(provider domain.IdentityProvider, err error) (domain.IdentityOperationOutcomeV2, bool) {
	switch provider {
	case domain.IdentityProviderYandex:
		if errors.Is(err, yandexoauth.ErrProviderUnavailable) {
			return domain.IdentityOutcomeProviderUnavailableV2, true
		}
		if errors.Is(err, yandexoauth.ErrProviderResponse) {
			return domain.IdentityOutcomeProofUnavailableV2, true
		}
	case domain.IdentityProviderTelegram:
		if errors.Is(err, telegramoidc.ErrProviderUnavailable) {
			return domain.IdentityOutcomeProviderUnavailableV2, true
		}
		if errors.Is(err, telegramoidc.ErrProviderResponse) {
			return domain.IdentityOutcomeProofUnavailableV2, true
		}
	}
	return "", false
}

// The only negative proof owner: actual consumed/scrubbed child, exact original
// scope and phase, fresh DB clock, and atomic terminal prune + security audit.
// SQL/auth/commit errors are NOT evidence for selecting an outcome.
func (s *identityAccountServiceV2) failedProof(ctx context.Context, id string, consumed identityConsumedProofV2, outcome domain.IdentityOperationOutcomeV2) (ports.IdentityAccountOperationViewV2, error) {
	var empty ports.IdentityAccountOperationViewV2
	if ctx.Err() != nil || (outcome != domain.IdentityOutcomeProofUnavailableV2 && outcome != domain.IdentityOutcomeProviderUnavailableV2) {
		return empty, ports.IdentityAccountUnavailableV2
	}
	var result ports.IdentityAccountOperationViewV2
	err := s.store.identityAuthorityTxV2(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		result = empty
		current, _, err := currentIdentityPhaseV2(ctx, q, consumed.Phase, s.config)
		if err != nil {
			return err
		}
		operation, found, err := readIdentityOperationV2(ctx, q, consumed.Phase.OperationDigest)
		if err != nil || !found || operation.Pending == nil || operation.Pending.Scope != current {
			return domain.ErrMembershipDenied
		}
		child, found, err := readIdentityProofChildV2(ctx, q, consumed.StateDigest)
		if err != nil || !found || child.ConsumedAt == nil || child.Secrets != nil || child.Binding != consumed.Binding || child.ValidateForOperation(operation) != nil || child.Proof.ParentPhase != operation.Phase || !q.at.Before(child.ConsumedAt.Add(identityProofCompletionWindowV2)) {
			return domain.ErrMembershipDenied
		}
		selected, action, provider := outcome, domain.WebSecurityIdentityProofFailed, child.Binding.Provider
		if !q.at.Before(operation.ExpiresAt) {
			selected = domain.IdentityOutcomeExpiredV2
			action = domain.WebSecurityIdentityOperationStopped
			provider = operation.Pending.TargetProvider
		}
		view, err := s.store.writeIdentityStopInTxV2(ctx, q, operation, selected, action, provider)
		if err != nil {
			return err
		}
		result = ports.IdentityAccountOperationViewV2{OperationID: id, Kind: view.Kind, Phase: view.Phase, Outcome: view.Outcome, ExpiresAt: view.ExpiresAt, BeforeRevision: view.BeforeRevision, AfterRevision: view.AfterRevision, CompletedAt: view.CompletedAt}
		return nil
	})
	if err != nil {
		return empty, identityAccountErrorV2(err)
	}
	return result, nil
}

func validateIdentityAccountAuthorizationV2(raw string, binding domain.IdentityProviderBindingV2, state, nonce, challenge string) error {
	if len(raw) == 0 || len(raw) > 8192 {
		return ports.IdentityAccountProviderUnavailableV2
	}
	u, err := url.Parse(raw)
	if err != nil || u.User != nil || u.Fragment != "" || u.Opaque != "" || u.RawPath != "" {
		return ports.IdentityAccountProviderUnavailableV2
	}
	expected := yandexoauth.DefaultAuthorizationEndpoint
	want := url.Values{"client_id": {binding.ClientID}, "redirect_uri": {binding.CallbackURI}, "response_type": {"code"}, "state": {state}, "code_challenge": {challenge}, "code_challenge_method": {"S256"}}
	if binding.Provider == domain.IdentityProviderYandex {
		want.Set("scope", "login:info")
		want.Set("force_confirm", "yes")
	} else if binding.Provider == domain.IdentityProviderTelegram {
		expected = telegramoidc.DefaultAuthorizationEndpoint
		want.Set("scope", "openid profile")
		want.Set("max_age", "0")
		want.Set("nonce", nonce)
	} else {
		return ports.IdentityAccountProviderUnavailableV2
	}
	values, err := url.ParseQuery(u.RawQuery)
	if err != nil || u.Scheme+"://"+u.Host+u.Path != expected || values.Encode() != want.Encode() || u.RawQuery != values.Encode() {
		return ports.IdentityAccountProviderUnavailableV2
	}
	return nil
}
