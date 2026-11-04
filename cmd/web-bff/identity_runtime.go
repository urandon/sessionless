package main

import (
	"database/sql"
	"errors"

	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/webauthconfig"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

var errWebIdentityConfiguration = errors.New("WEB_IDENTITY_VERSION must explicitly be 1 or 2; V2 requires WEB_IDENTITY_SENSITIVE_OPERATIONS=true or false and the existing valid identity HMAC key")

type webIdentityConfig struct {
	version   string
	sensitive bool
	providers []ydbstore.IdentityAccountProviderV2
}

// Validate the explicit deployment choice before opening dependencies. Neither
// a version nor a manifest asserts that other revisions/writers were retired.
// That separately authorized cutover must precede actual V2 enablement.
func webIdentityFromEnvironment(lookup func(string) (string, bool), login webLoginConfig, baseURL string, identityKey []byte) (webIdentityConfig, error) {
	runtime, err := webauthconfig.RuntimeFromEnvironment(lookup)
	if err != nil {
		return webIdentityConfig{}, errWebIdentityConfiguration
	}
	if runtime.Version == "1" {
		return webIdentityConfig{version: runtime.Version}, nil
	}
	if len(identityKey) < 32 || len(identityKey) > 4096 {
		return webIdentityConfig{}, errWebIdentityConfiguration
	}
	providers, err := login.identityProvidersV2(baseURL)
	if err != nil {
		return webIdentityConfig{}, errWebIdentityConfiguration
	}
	return webIdentityConfig{version: runtime.Version, sensitive: runtime.SensitiveOperationsEnabled, providers: providers}, nil
}

func (config webIdentityConfig) newStore(db *sql.DB, options ydbstore.Options, identityKey []byte) (*ydbstore.Store, ports.IdentityAccountServiceV2, error) {
	switch config.version {
	case "1":
		if config.sensitive || len(config.providers) != 0 {
			return nil, nil, errWebIdentityConfiguration
		}
		store, err := ydbstore.New(db, options)
		return store, nil, err
	case "2":
		// Even sensitive OFF uses the SAME V2 Store and mounted service: login,
		// revision checks and authenticated retained outcome reads remain V2.
		return ydbstore.NewWebIdentityRuntimeV2(db, options, ydbstore.WebIdentityRuntimeOptionsV2{Providers: config.providers, HMACKey: identityKey, SensitiveOperationsEnabled: config.sensitive})
	default:
		return nil, nil, errWebIdentityConfiguration
	}
}
