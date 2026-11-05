package ydbstore

import (
	"database/sql"
	"encoding/json"
	"reflect"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// WebIdentityReadModeV2 is trusted, nonsecret constructor DATA. It selects the
// strict V2 cookie/current-identity language, not a proof service or a cutover
// receipt. Nil in library options deliberately preserves pre-cutover V1.
type WebIdentityReadModeV2 struct {
	Providers []domain.IdentityProviderBindingV2
}

func (mode *WebIdentityReadModeV2) Clone() *WebIdentityReadModeV2 {
	if mode == nil {
		return nil
	}
	return &WebIdentityReadModeV2{Providers: append([]domain.IdentityProviderBindingV2(nil), mode.Providers...)}
}

func (mode *WebIdentityReadModeV2) Validate() error {
	if mode == nil {
		return ports.IdentityAccountUnavailableV2
	}
	_, err := WebIdentityManifestDigestV2(mode.Providers)
	return err
}

func (mode *WebIdentityReadModeV2) configuration() (*identityConfigurationV2, error) {
	if mode == nil {
		return nil, nil
	}
	owned := mode.Clone()
	digest, err := WebIdentityManifestDigestV2(owned.Providers)
	if err != nil {
		return nil, err
	}
	return &identityConfigurationV2{ManifestDigest: digest, Providers: owned.Providers}, nil
}

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
	_, err := WebIdentityManifestDigestV2(bindings)
	if err != nil {
		return nil, nil, err
	}
	if options.WebIdentityReadsV2 != nil && (options.WebIdentityReadsV2.Validate() != nil || !reflect.DeepEqual(options.WebIdentityReadsV2.Providers, bindings)) {
		return nil, nil, ports.IdentityAccountUnavailableV2
	}
	options.WebIdentityReadsV2 = &WebIdentityReadModeV2{Providers: bindings}
	store, err := New(db, options)
	if err != nil {
		return nil, nil, ports.IdentityAccountUnavailableV2
	}
	// Still inside construction: no pointer has escaped and there is no setter.
	store.webIdentityV2.SensitiveOperationsEnabled = identityOptions.SensitiveOperationsEnabled
	service, err := NewIdentityAccountServiceV2(store, IdentityAccountServiceOptionsV2{Providers: identityOptions.Providers, HMACKey: identityOptions.HMACKey})
	if err != nil {
		return nil, nil, err
	}
	return store, service, nil
}
