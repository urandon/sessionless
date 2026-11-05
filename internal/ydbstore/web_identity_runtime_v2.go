package ydbstore

import (
	"database/sql"
	"encoding/json"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// WebIdentityRuntimeOptionsV2 is trusted process configuration, not request
// data or evidence that an operator has retired old writers. Providers use the
// same adapter instances and fixed bindings as the ordinary login service.
type WebIdentityRuntimeOptionsV2 struct {
	Providers                  []IdentityAccountProviderV2
	HMACKey                    []byte
	SensitiveOperationsEnabled bool
}

// WebIdentityManifestDigestV2 derives the maintenance/serving configuration pin
// from the closed, ordered provider bindings and accepted proof policy. It is
// independent of credentials, enablement, time and source commit. It proves no
// external freeze, schema readiness, client compatibility or writer retirement.
func WebIdentityManifestDigestV2(bindings []domain.IdentityProviderBindingV2) (domain.SecretDigest, error) {
	if len(bindings) < 1 || len(bindings) > 2 {
		return "", ports.IdentityAccountUnavailableV2
	}
	for i, binding := range bindings {
		if binding.Validate() != nil || (i == 1 && (bindings[0].Provider != domain.IdentityProviderYandex || binding.Provider != domain.IdentityProviderTelegram)) {
			return "", ports.IdentityAccountUnavailableV2
		}
	}
	// This version changes if the accepted proof semantics change. These are
	// the policies implemented by the concrete adapters, not caller choices.
	raw, err := json.Marshal(struct {
		Version   string                             `json:"version"`
		Yandex    domain.IdentityProviderProofKindV2 `json:"yandex_proof"`
		Telegram  domain.IdentityProviderProofKindV2 `json:"telegram_proof"`
		Providers []domain.IdentityProviderBindingV2 `json:"providers"`
	}{"web-identity-runtime.v2", domain.IdentityProofYandexAuthorizationV2, domain.IdentityProofTelegramAuthTimeV2, bindings})
	if err != nil {
		return "", ports.IdentityAccountUnavailableV2
	}
	return domain.DigestSecret(string(raw)), nil
}

// NewWebIdentityRuntimeV2 publishes one fully paired Store/service. The private
// reader/writer version is installed exactly once before either pointer escapes;
// there is no setter or V1 decoding fallback. This performs no SQL, migration,
// repair, enrollment, provider probe or cutover. The operator must separately
// establish the accepted all-writer/schema/data transition before serving it.
// V2 Enroll/bootstrap/Telegram-chat writers remain explicitly retired: ordinary
// login and linking never grant membership or rewrite actors/history.
func NewWebIdentityRuntimeV2(db *sql.DB, options Options, identityOptions WebIdentityRuntimeOptionsV2) (*Store, ports.IdentityAccountServiceV2, error) {
	if len(identityOptions.Providers) < 1 || len(identityOptions.Providers) > 2 {
		return nil, nil, ports.IdentityAccountUnavailableV2
	}
	bindings := make([]domain.IdentityProviderBindingV2, len(identityOptions.Providers))
	for i, provider := range identityOptions.Providers {
		bindings[i] = provider.Binding
	}
	manifest, err := WebIdentityManifestDigestV2(bindings)
	if err != nil {
		return nil, nil, err
	}
	store, err := New(db, options)
	if err != nil {
		return nil, nil, ports.IdentityAccountUnavailableV2
	}
	store.webIdentityV2 = &identityConfigurationV2{ManifestDigest: manifest, Providers: bindings, SensitiveOperationsEnabled: identityOptions.SensitiveOperationsEnabled}
	service, err := NewIdentityAccountServiceV2(store, IdentityAccountServiceOptionsV2{Providers: identityOptions.Providers, HMACKey: identityOptions.HMACKey})
	if err != nil {
		return nil, nil, err
	}
	return store, service, nil
}
