package ydbstore

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"sync"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type IdentityAccountProviderV2 struct {
	Binding domain.IdentityProviderBindingV2
	Prover  ports.IdentityProviderProverV1
}

// Trusted construction only. These options install no Store reader or writer.
type IdentityAccountServiceOptionsV2 struct {
	Providers []IdentityAccountProviderV2
	HMACKey   []byte
	Random    io.Reader
}

type identityAccountServiceV2 struct {
	store    *Store
	config   identityConfigurationV2
	bindings map[domain.IdentityProvider]domain.IdentityProviderBindingV2
	provers  map[domain.IdentityProvider]ports.IdentityProviderProverV1
	key      []byte
	random   io.Reader
	randomMu sync.Mutex
}

func NewIdentityAccountServiceV2(store *Store, options IdentityAccountServiceOptionsV2) (ports.IdentityAccountServiceV2, error) {
	if store == nil || store.db == nil || store.webIdentityV2 == nil || len(options.HMACKey) < 32 || len(options.HMACKey) > 4096 {
		return nil, ports.IdentityAccountUnavailableV2
	}
	config := *store.webIdentityV2
	config.Providers = append([]domain.IdentityProviderBindingV2(nil), config.Providers...)
	bindings, err := config.bindings()
	if err != nil || len(options.Providers) != len(bindings) {
		return nil, ports.IdentityAccountUnavailableV2
	}
	provers := make(map[domain.IdentityProvider]ports.IdentityProviderProverV1, len(bindings))
	for _, p := range options.Providers {
		if p.Prover == nil || p.Binding.Validate() != nil || bindings[p.Binding.Provider] != p.Binding {
			return nil, ports.IdentityAccountUnavailableV2
		}
		value := reflect.ValueOf(p.Prover)
		if (value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface) && value.IsNil() {
			return nil, ports.IdentityAccountUnavailableV2
		}
		if _, found := provers[p.Binding.Provider]; found {
			return nil, ports.IdentityAccountUnavailableV2
		}
		provers[p.Binding.Provider] = p.Prover
	}
	if options.Random == nil {
		options.Random = rand.Reader
	}
	service := &identityAccountServiceV2{store: store, config: config, bindings: bindings, provers: provers, key: append([]byte(nil), options.HMACKey...), random: options.Random}
	if err := service.ValidateLoginBindings(config.Providers); err != nil {
		clear(service.key)
		return nil, err
	}
	return service, nil
}

func (s *identityAccountServiceV2) ValidateLoginBindings(bindings []domain.IdentityProviderBindingV2) error {
	if s == nil || s.store == nil || s.store.webIdentityV2 == nil || !reflect.DeepEqual(*s.store.webIdentityV2, s.config) || len(bindings) < 1 || len(bindings) > 2 || len(bindings) != len(s.config.Providers) {
		return ports.IdentityAccountUnavailableV2
	}
	if len(bindings) == 2 && (bindings[0].Provider != domain.IdentityProviderYandex || bindings[1].Provider != domain.IdentityProviderTelegram) {
		return ports.IdentityAccountUnavailableV2
	}
	for i, binding := range bindings {
		if binding.Validate() != nil || binding != s.config.Providers[i] {
			return ports.IdentityAccountUnavailableV2
		}
	}
	return nil
}

func (s *identityAccountServiceV2) requestContext(ctx context.Context) (context.Context, context.CancelFunc, error) {
	if ctx == nil || ctx.Err() != nil || s == nil || s.store == nil || s.store.webIdentityV2 == nil || !reflect.DeepEqual(*s.store.webIdentityV2, s.config) {
		return nil, nil, ports.IdentityAccountUnavailableV2
	}
	child, cancel := context.WithTimeout(ctx, 15*time.Second)
	return child, cancel, nil
}

func identityAccountErrorV2(err error) error {
	if err == nil {
		return nil
	}
	var unavailable identityStoreUnavailableV2
	if errors.As(err, &unavailable) {
		return ports.IdentityAccountUnavailableV2
	}
	var revision identityRevisionMismatchV2
	if errors.As(err, &revision) {
		return ports.IdentityAccountStaleRevisionV2
	}
	var closed ports.IdentityAccountErrorV2
	if errors.As(err, &closed) {
		return closed
	}
	switch {
	case errors.Is(err, domain.ErrWebSessionRevoked):
		return ports.IdentityAccountUnauthenticatedV2
	case errors.Is(err, domain.ErrExternalIdentityConflict), errors.Is(err, domain.ErrAuthRevisionOverflowV2):
		return ports.IdentityAccountConflictV2
	case errors.Is(err, domain.ErrIdentityLastMethodV2):
		return ports.IdentityAccountLastMethodV2
	case errors.Is(err, domain.ErrIdentityProviderUnavailableV2):
		return ports.IdentityAccountProviderUnavailableV2
	case errors.Is(err, domain.ErrMembershipDenied):
		return ports.IdentityAccountAccessDeniedV2
	}
	var validation domain.ValidationError
	if errors.As(err, &validation) {
		return ports.IdentityAccountAccessDeniedV2
	}
	return ports.IdentityAccountUnavailableV2
}

func identityAccountCopiesV2(q *ownedAuthorityQuery) error {
	return q.budget.materialized(q.callback, 4*domain.IdentityAuthMaxRecordBytesV2)
}
func identityAccountCookieV2(raw string) bool { return len(raw) > 0 && len(raw) <= 512 }
func identityAccountMutationV2(auth ports.IdentityAccountMutationAuthV2) error {
	if !identityAccountCookieV2(auth.SessionCookie) {
		return ports.IdentityAccountUnauthenticatedV2
	}
	if !identityAccountCookieV2(auth.CSRFCookie) || !identityAccountCookieV2(auth.PresentedCSRF) || subtle.ConstantTimeCompare([]byte(auth.CSRFCookie), []byte(auth.PresentedCSRF)) != 1 {
		return ports.IdentityAccountAccessDeniedV2
	}
	return nil
}

func (s *identityAccountServiceV2) secret(ctx context.Context, prefix string) (string, error) {
	if ctx.Err() != nil {
		return "", ports.IdentityAccountUnavailableV2
	}
	var raw [32]byte
	s.randomMu.Lock()
	_, err := io.ReadFull(s.random, raw[:])
	s.randomMu.Unlock()
	defer clear(raw[:])
	if err != nil || ctx.Err() != nil {
		return "", ports.IdentityAccountUnavailableV2
	}
	return prefix + base64.RawURLEncoding.EncodeToString(raw[:]), nil
}

func identityAccount256V2(raw string) bool {
	if len(raw) != 43 {
		return false
	}
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	return err == nil && len(decoded) == 32 && base64.RawURLEncoding.EncodeToString(decoded) == raw
}
func identityAccountIDV2(id string) bool {
	return len(id) == 46 && strings.HasPrefix(id, "op_") && identityAccount256V2(strings.TrimPrefix(id, "op_"))
}
func identityAccountOperationCookieV2(id, cookie string) bool {
	parts := strings.SplitN(cookie, ".", 3)
	return identityAccountIDV2(id) && len(parts) == 2 && parts[0] == id && identityAccount256V2(parts[1])
}
func identityAccountCookieIDV2(cookie string) (string, error) {
	if len(cookie) != 90 {
		return "", ports.IdentityAccountAccessDeniedV2
	}
	parts := strings.SplitN(cookie, ".", 3)
	if len(parts) != 2 || !identityAccountOperationCookieV2(parts[0], cookie) {
		return "", ports.IdentityAccountAccessDeniedV2
	}
	return parts[0], nil
}
func (s *identityAccountServiceV2) mac(kind string, parts ...string) string {
	values := append([]string{"sessionless.identity-account.v2", kind}, parts...)
	raw, _ := json.Marshal(values)
	mac := hmac.New(sha256.New, s.key)
	_, _ = mac.Write(raw)
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}
func (s *identityAccountServiceV2) methodHandle(user domain.UserID, identity domain.ExternalIdentityV2) string {
	return "method_" + s.mac("method", string(user), string(identity.Subject.Provider), identity.Subject.Subject, identity.Revision.String())
}
func (s *identityAccountServiceV2) state(id string, phase domain.IdentityOperationPhaseV2, binding domain.IdentityProviderBindingV2) string {
	return "proof_" + s.mac("state", id, string(phase), string(s.config.ManifestDigest), string(binding.Provider), binding.Issuer, binding.ClientID, binding.CallbackURI)
}
func (s *identityAccountServiceV2) phase(auth ports.IdentityAccountMutationAuthV2, id, cookie string, revision domain.AuthRevisionV2) (identityPhaseRequestV2, error) {
	if err := identityAccountMutationV2(auth); err != nil {
		return identityPhaseRequestV2{}, err
	}
	if !identityAccountOperationCookieV2(id, cookie) || revision.Validate() != nil {
		return identityPhaseRequestV2{}, ports.IdentityAccountAccessDeniedV2
	}
	return identityPhaseRequestV2{OperationDigest: domain.DigestSecret(id), SessionDigest: domain.DigestSecret(auth.SessionCookie), CSRFDigest: domain.DigestSecret(auth.PresentedCSRF), BrowserBindingDigest: domain.DigestSecret(cookie), ExpectedRevision: revision}, nil
}

func (s *identityAccountServiceV2) readSession(ctx context.Context, q *ownedAuthorityQuery, raw string) (domain.WebSessionV2, domain.TenantMembership, error) {
	if !identityAccountCookieV2(raw) {
		return domain.WebSessionV2{}, domain.TenantMembership{}, ports.IdentityAccountUnauthenticatedV2
	}
	session, member, err := authorizeWebSessionV2(ctx, q, domain.DigestSecret(raw), domain.TenantPermissionRead, q.at)
	if err != nil {
		return domain.WebSessionV2{}, domain.TenantMembership{}, err
	}
	if _, enabled := s.bindings[session.Session.AuthenticatedSubject.Provider]; !enabled {
		return domain.WebSessionV2{}, domain.TenantMembership{}, ports.IdentityAccountUnauthenticatedV2
	}
	return session, member, nil
}

func (s *identityAccountServiceV2) Methods(ctx context.Context, request ports.IdentityAccountMethodsRequestV2) (ports.IdentityAccountMethodsViewV2, error) {
	var empty, result ports.IdentityAccountMethodsViewV2
	ctx, cancel, err := s.requestContext(ctx)
	if err != nil {
		return empty, err
	}
	defer cancel()
	err = s.store.identityAuthorityTxV2(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		result = empty
		if err := identityAccountCopiesV2(q); err != nil {
			return err
		}
		session, _, err := s.readSession(ctx, q, request.Auth.SessionCookie)
		if err != nil {
			return err
		}
		active, err := readActiveExternalIdentitiesV2(ctx, q, session.Session.UserID)
		if err != nil {
			return err
		}
		for _, identity := range active {
			if identity.Revision != session.IdentityRevision || identity.UpdatedAt.After(q.at) {
				return domain.ErrMembershipDenied
			}
		}
		view := ports.IdentityAccountMethodsViewV2{Revision: session.IdentityRevision, Methods: make([]ports.IdentityAccountMethodViewV2, 0, 2)}
		for _, provider := range []domain.IdentityProvider{domain.IdentityProviderYandex, domain.IdentityProviderTelegram} {
			_, enabled := s.bindings[provider]
			method := ports.IdentityAccountMethodViewV2{Provider: provider, Enabled: enabled}
			for _, identity := range active {
				if identity.Subject.Provider == provider {
					method.Linked = true
					method.Current = identity.Subject == session.Session.AuthenticatedSubject
					method.MethodHandle = s.methodHandle(session.Session.UserID, identity)
				}
			}
			if s.config.SensitiveOperationsEnabled {
				if !method.Linked && enabled {
					method.AllowedActions = []ports.IdentityAccountActionV2{ports.IdentityAccountActionLinkV2}
				}
				if method.Linked {
					for _, remaining := range active {
						if remaining.Subject.Provider != provider {
							if _, ok := s.bindings[remaining.Subject.Provider]; ok {
								method.RemainingProvider = remaining.Subject.Provider
								method.AllowedActions = []ports.IdentityAccountActionV2{ports.IdentityAccountActionUnlinkV2}
							}
						}
					}
				}
			}
			view.Methods = append(view.Methods, method)
		}
		result = view
		return nil
	})
	if err != nil {
		return empty, identityAccountErrorV2(err)
	}
	return result, nil
}

func (s *identityAccountServiceV2) Start(ctx context.Context, request ports.IdentityAccountStartRequestV2) (ports.IdentityAccountStartResultV2, error) {
	var empty, result ports.IdentityAccountStartResultV2
	ctx, cancel, err := s.requestContext(ctx)
	if err != nil {
		return empty, err
	}
	defer cancel()
	if err := identityAccountMutationV2(request.Auth); err != nil {
		return empty, err
	}
	if !s.config.SensitiveOperationsEnabled || request.ExpectedRevision.Validate() != nil {
		return empty, ports.IdentityAccountAccessDeniedV2
	}
	if request.Kind == domain.IdentityOperationLinkV2 {
		if request.TargetProvider.Validate() != nil || request.MethodHandle != "" {
			return empty, ports.IdentityAccountAccessDeniedV2
		}
	} else if request.Kind == domain.IdentityOperationUnlinkV2 {
		if request.TargetProvider != "" || !strings.HasPrefix(request.MethodHandle, "method_") || !identityAccount256V2(strings.TrimPrefix(request.MethodHandle, "method_")) {
			return empty, ports.IdentityAccountAccessDeniedV2
		}
	} else {
		return empty, ports.IdentityAccountAccessDeniedV2
	}
	id, err := s.secret(ctx, "op_")
	if err != nil {
		return empty, err
	}
	browser, err := s.secret(ctx, "")
	if err != nil {
		return empty, err
	}
	cookie := id + "." + browser
	if request.PreviousOperationCookie != "" {
		previous, err := identityAccountCookieIDV2(request.PreviousOperationCookie)
		if err != nil {
			return empty, err
		}
		phase, err := s.phase(request.Auth, previous, request.PreviousOperationCookie, request.ExpectedRevision)
		if err != nil {
			return empty, err
		}
		if _, err := s.store.cancelIdentityOperationV2(ctx, phase, s.config); err != nil {
			return empty, identityAccountErrorV2(err)
		}
	}
	phase, err := s.phase(request.Auth, id, cookie, request.ExpectedRevision)
	if err != nil {
		return empty, err
	}
	var first time.Time
	err = s.store.identityAuthorityTxV2(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		result = empty
		if identityProofFirstTimeV2(&first, q.at) != nil {
			return domain.ErrMembershipDenied
		}
		if err := identityAccountCopiesV2(q); err != nil {
			return err
		}
		scope, active, err := currentIdentityPhaseV2(ctx, q, phase, s.config)
		if err != nil {
			return err
		}
		target := request.TargetProvider
		if request.Kind == domain.IdentityOperationUnlinkV2 {
			for _, identity := range active {
				handle := s.methodHandle(scope.UserID, identity)
				if subtle.ConstantTimeCompare([]byte(handle), []byte(request.MethodHandle)) == 1 {
					target = identity.Subject.Provider
				}
			}
			if target == "" {
				return domain.ErrMembershipDenied
			}
		}
		source := scope.AuthenticatedSubject.Provider
		if request.Kind == domain.IdentityOperationUnlinkV2 {
			source = ""
			for _, identity := range active {
				if identity.Subject.Provider != target {
					if _, ok := s.bindings[identity.Subject.Provider]; ok {
						source = identity.Subject.Provider
					}
				}
			}
			if source == "" {
				return domain.ErrIdentityLastMethodV2
			}
		}
		start := identityOperationStartV2{Phase: phase, Kind: request.Kind, TargetProvider: target}
		if request.Kind == domain.IdentityOperationLinkV2 {
			binding, ok := s.bindings[target]
			if !ok {
				return domain.ErrIdentityProviderUnavailableV2
			}
			start.TargetChallengeDigest = domain.DigestSecret(s.state(id, domain.IdentityPhaseTargetProofV2, binding))
		} else {
			start.SourceChallengeDigest = domain.DigestSecret(s.state(id, domain.IdentityPhaseSourceProofV2, s.bindings[source]))
		}
		operation, err := buildIdentityOperationV2(start, scope, active, s.bindings, first)
		if err != nil {
			return err
		}
		if err := persistNewIdentityOperationV2(ctx, q, operation); err != nil {
			return err
		}
		view := identityAccountViewV2(id, operation, q.at)
		view.AllowedActions = []ports.IdentityAccountActionV2{ports.IdentityAccountActionContinueV2, ports.IdentityAccountActionCancelV2}
		result = ports.IdentityAccountStartResultV2{View: view, OperationCookie: cookie}
		return nil
	})
	if err != nil {
		return empty, identityAccountErrorV2(err)
	}
	return result, nil
}

func (s *identityAccountServiceV2) Continue(ctx context.Context, request ports.IdentityAccountContinueRequestV2) (ports.IdentityAccountContinueResultV2, error) {
	var empty ports.IdentityAccountContinueResultV2
	ctx, cancel, err := s.requestContext(ctx)
	if err != nil {
		return empty, err
	}
	defer cancel()
	phase, err := s.phase(request.Auth, request.OperationID, request.OperationCookie, request.ExpectedRevision)
	if err != nil {
		return empty, err
	}
	if _, err := identityProofBindingsV2(phase, s.config); err != nil {
		return empty, identityAccountErrorV2(err)
	}
	verifier, err := s.secret(ctx, "")
	if err != nil {
		return empty, err
	}
	nonce, err := s.secret(ctx, "nonce_")
	if err != nil {
		return empty, err
	}
	var child domain.IdentityChallengeV2
	var first, at time.Time
	var state string
	preparedAt := time.Now()
	err = s.store.identityAuthorityTxV2(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		child = domain.IdentityChallengeV2{}
		state = ""
		at = time.Time{}
		if identityProofFirstTimeV2(&first, q.at) != nil {
			return domain.ErrMembershipDenied
		}
		if err := identityAccountCopiesV2(q); err != nil {
			return err
		}
		operation, err := currentIdentityProofOperationV2(ctx, q, phase, s.config, s.bindings)
		if err != nil {
			return err
		}
		if operation.Phase != domain.IdentityPhaseSourceProofV2 && operation.Phase != domain.IdentityPhaseTargetProofV2 {
			return ports.IdentityAccountProofRequiredV2
		}
		provider := operation.Pending.SourceSubject.Provider
		if operation.Phase == domain.IdentityPhaseTargetProofV2 {
			provider = operation.Pending.TargetProvider
		}
		state = s.state(request.OperationID, operation.Phase, s.bindings[provider])
		selectedNonce := nonce
		if provider == domain.IdentityProviderYandex {
			selectedNonce = ""
		}
		child, err = createIdentityProofInTxV2(ctx, q, operation, identityProofChildV2{Phase: phase, ChallengeDigest: domain.DigestSecret(state), PKCEVerifier: verifier, Nonce: selectedNonce}, s.bindings, first)
		at = q.at
		return err
	})
	if err != nil {
		return empty, identityAccountErrorV2(err)
	}
	digest := sha256.Sum256([]byte(child.Secrets.PKCEVerifier))
	// Include the consume/read transaction's elapsed time conservatively; a
	// delayed response must not turn its earlier SQL sample into renewed time.
	url, err := s.provers[child.Binding.Provider].ProofAuthorizationURL(ctx, ports.IdentityProviderProofAuthorizationRequestV1{Binding: child.Binding, State: state, Nonce: child.Secrets.Nonce, CodeChallenge: base64.RawURLEncoding.EncodeToString(digest[:]), ChallengeCreatedAt: child.CreatedAt, ChallengeExpiresAt: child.ExpiresAt, Now: at.Add(time.Since(preparedAt))})
	if err != nil {
		return empty, ports.IdentityAccountProviderUnavailableV2
	}
	if err := validateIdentityAccountAuthorizationV2(url, child.Binding, state, child.Secrets.Nonce, base64.RawURLEncoding.EncodeToString(digest[:])); err != nil {
		return empty, err
	}
	return ports.IdentityAccountContinueResultV2{AuthorizationURL: url}, nil
}

func identityAccountViewV2(id string, operation domain.IdentityOperationV2, at time.Time) ports.IdentityAccountOperationViewV2 {
	view := identityOperationOutcomeView(operation, at)
	result := ports.IdentityAccountOperationViewV2{OperationID: id, Kind: view.Kind, Phase: view.Phase, Outcome: view.Outcome, ExpiresAt: view.ExpiresAt, BeforeRevision: view.BeforeRevision, AfterRevision: view.AfterRevision, CompletedAt: view.CompletedAt}
	if operation.Pending != nil {
		result.TargetProvider = operation.Pending.TargetProvider
	}
	return result
}

func (s *identityAccountServiceV2) Confirm(ctx context.Context, request ports.IdentityAccountConfirmRequestV2) (ports.IdentityAccountConfirmResultV2, error) {
	var empty ports.IdentityAccountConfirmResultV2
	ctx, cancel, err := s.requestContext(ctx)
	if err != nil {
		return empty, err
	}
	defer cancel()
	phase, err := s.phase(request.Auth, request.OperationID, request.OperationCookie, request.ExpectedRevision)
	if err != nil {
		return empty, err
	}
	rawSession, err := s.secret(ctx, "session_")
	if err != nil {
		return empty, err
	}
	rawCSRF, err := s.secret(ctx, "csrf_")
	if err != nil {
		return empty, err
	}
	confirmed, err := s.store.confirmIdentityOperationV2(ctx, identityConfirmationV2{OperationDigest: phase.OperationDigest, SessionDigest: phase.SessionDigest, CSRFDigest: phase.CSRFDigest, BrowserBindingDigest: phase.BrowserBindingDigest, ExpectedRevision: phase.ExpectedRevision, NextSessionDigest: domain.DigestSecret(rawSession), NextCSRFDigest: domain.DigestSecret(rawCSRF)}, s.config)
	if err != nil {
		return empty, identityAccountErrorV2(err)
	}
	view := identityAccountViewV2(request.OperationID, confirmed.Operation, confirmed.Receipt.CompletedAt)
	result := ports.IdentityAccountConfirmResultV2{View: view}
	if confirmed.Session.Version != 0 {
		if confirmed.Session.Validate() != nil || confirmed.Session.Session.SessionDigest != domain.DigestSecret(rawSession) || confirmed.Session.Session.CSRFTokenDigest != domain.DigestSecret(rawCSRF) {
			return empty, ports.IdentityAccountUnavailableV2
		}
		result.Cookies = &ports.IdentityAccountSessionCookiesV2{SessionCookie: rawSession, CSRFCookie: rawCSRF}
	}
	return result, nil
}

func (s *identityAccountServiceV2) Outcome(ctx context.Context, request ports.IdentityAccountOutcomeRequestV2) (ports.IdentityAccountOperationViewV2, error) {
	var empty ports.IdentityAccountOperationViewV2
	ctx, cancel, err := s.requestContext(ctx)
	if err != nil {
		return empty, err
	}
	defer cancel()
	if !identityAccountIDV2(request.OperationID) {
		return empty, ports.IdentityAccountAccessDeniedV2
	}
	if !identityAccountCookieV2(request.Auth.SessionCookie) {
		return empty, ports.IdentityAccountUnauthenticatedV2
	}
	view, err := s.store.readIdentityOperationOutcomeV2(ctx, identityOutcomeReadV2{OperationDigest: domain.DigestSecret(request.OperationID), SessionDigest: domain.DigestSecret(request.Auth.SessionCookie), OperationCookie: request.OperationCookie, Service: s}, s.config)
	if err != nil {
		return empty, identityAccountErrorV2(err)
	}
	return ports.IdentityAccountOperationViewV2{OperationID: request.OperationID, Kind: view.Kind, Phase: view.Phase, TargetProvider: view.TargetProvider, Outcome: view.Outcome, ExpiresAt: view.ExpiresAt, BeforeRevision: view.BeforeRevision, AfterRevision: view.AfterRevision, CompletedAt: view.CompletedAt, AllowedActions: view.AllowedActions}, nil
}

func (s *identityAccountServiceV2) Cancel(ctx context.Context, request ports.IdentityAccountCancelRequestV2) (ports.IdentityAccountOperationViewV2, error) {
	var empty ports.IdentityAccountOperationViewV2
	ctx, cancel, err := s.requestContext(ctx)
	if err != nil {
		return empty, err
	}
	defer cancel()
	phase, err := s.phase(request.Auth, request.OperationID, request.OperationCookie, request.ExpectedRevision)
	if err != nil {
		return empty, err
	}
	view, err := s.store.cancelIdentityOperationV2(ctx, phase, s.config)
	if err != nil {
		return empty, identityAccountErrorV2(err)
	}
	return ports.IdentityAccountOperationViewV2{OperationID: request.OperationID, Kind: view.Kind, Phase: view.Phase, Outcome: view.Outcome, ExpiresAt: view.ExpiresAt, BeforeRevision: view.BeforeRevision, AfterRevision: view.AfterRevision, CompletedAt: view.CompletedAt}, nil
}

var _ ports.IdentityAccountServiceV2 = (*identityAccountServiceV2)(nil)

func (s *identityAccountServiceV2) outcomeActions(ctx context.Context, q *ownedAuthorityQuery, cookie string, operation domain.IdentityOperationV2, session domain.WebSessionV2, member domain.TenantMembership) ([]ports.IdentityAccountActionV2, error) {
	if operation.Pending == nil || !s.config.SensitiveOperationsEnabled || !q.at.Before(operation.ExpiresAt) {
		return nil, nil
	}
	id, err := identityAccountCookieIDV2(cookie)
	if err != nil || domain.DigestSecret(id) != operation.OperationDigest {
		return nil, nil
	}
	current := domain.IdentityOperationScopeV2{UserID: session.Session.UserID, SessionDigest: session.Session.SessionDigest, AuthenticatedSubject: session.Session.AuthenticatedSubject, TenantID: member.TenantID, MembershipSecurityVersion: member.SecurityVersion, BrowserBindingDigest: domain.DigestSecret(cookie), ConfigurationDigest: s.config.ManifestDigest, ExpectedRevision: session.IdentityRevision}
	if operation.ValidateCurrentScope(current, q.at) != nil {
		return nil, nil
	}
	for _, proof := range []*domain.IdentityProviderProofV2{operation.Pending.SourceProof, operation.Pending.TargetProof} {
		if proof != nil && (proof.ValidateFresh(q.at) != nil || s.bindings[proof.Subject.Provider] != proof.Binding) {
			return nil, nil
		}
	}
	actions := []ports.IdentityAccountActionV2{ports.IdentityAccountActionCancelV2}
	if operation.Phase == domain.IdentityPhaseConfirmV2 {
		if operation.ValidateConfirm(q.at) == nil {
			actions = append(actions, ports.IdentityAccountActionConfirmV2)
		}
		return actions, nil
	}
	provider, digest := operation.Pending.SourceSubject.Provider, operation.Pending.SourceChallengeDigest
	if operation.Phase == domain.IdentityPhaseTargetProofV2 {
		provider, digest = operation.Pending.TargetProvider, operation.Pending.TargetChallengeDigest
	} else if operation.Phase != domain.IdentityPhaseSourceProofV2 {
		return nil, nil
	}
	binding, enabled := s.bindings[provider]
	if !enabled {
		return nil, nil
	}
	if domain.DigestSecret(s.state(id, operation.Phase, binding)) != digest {
		return nil, nil
	}
	child, found, err := readIdentityProofChildV2(ctx, q, digest)
	if err != nil {
		return nil, err
	}
	if !found {
		actions = append(actions, ports.IdentityAccountActionContinueV2)
	} else if child.ConsumedAt == nil && child.Secrets != nil && child.ValidateForOperation(operation) == nil && child.Binding == binding && q.at.Before(child.ExpiresAt) {
		actions = append(actions, ports.IdentityAccountActionContinueV2)
	}
	return actions, nil
}
