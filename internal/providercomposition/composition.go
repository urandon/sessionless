// Package providercomposition assembles the exact native provider adapters
// below the Sessionless-owned harness registry. It does not discover profiles,
// select a default backend, or enable provider execution.
package providercomposition

import (
	"errors"
	"time"

	"gitcode.com/urandon/sessionless/internal/codexopenrouter"
	"gitcode.com/urandon/sessionless/internal/directopenrouter"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/opencodeopenrouter"
	"gitcode.com/urandon/sessionless/internal/piopenrouter"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
)

var ErrContract = errors.New("provider composition contract is invalid")

// DependenciesV1 contains only explicitly constructed, already pinned native
// drivers. Profile discovery, environment lookup and production boundary
// construction belong to reviewed outer composition layers.
type DependenciesV1 struct {
	Codex    *codexopenrouter.Driver
	OpenCode *opencodeopenrouter.Driver
	Pi       *piopenrouter.Driver
	Direct   *directopenrouter.Driver
}

// NewDisabledRegistryV1 constructs the closed four-backend registry. Every
// registration remains disabled; an exact cancellation can still route to its
// matching driver so teardown is possible without authorizing new execution.
func NewDisabledRegistryV1(now func() time.Time, dependencies DependenciesV1) (*sessionlessharness.Registry, error) {
	if now == nil {
		return nil, ErrContract
	}
	registrations, err := registrationsV1(dependencies)
	if err != nil {
		return nil, err
	}
	registry, err := sessionlessharness.NewRegistry(now, registrations...)
	if err != nil {
		return nil, ErrContract
	}
	return registry, nil
}

func registrationsV1(dependencies DependenciesV1) ([]sessionlessharness.Registration, error) {
	if dependencies.Codex == nil || dependencies.OpenCode == nil || dependencies.Pi == nil || dependencies.Direct == nil {
		return nil, ErrContract
	}
	builders := []struct {
		kind  domain.HarnessBackendKindV1
		build func() (sessionlessharness.Registration, error)
	}{
		{kind: domain.HarnessBackendCodexOpenRouterV1, build: func() (sessionlessharness.Registration, error) {
			return codexopenrouter.DisabledRegistrationV1(dependencies.Codex)
		}},
		{kind: domain.HarnessBackendOpenCodeV1, build: func() (sessionlessharness.Registration, error) {
			return opencodeopenrouter.DisabledRegistrationV1(dependencies.OpenCode)
		}},
		{kind: domain.HarnessBackendPiV1, build: func() (sessionlessharness.Registration, error) {
			return piopenrouter.DisabledRegistrationV1(dependencies.Pi)
		}},
		{kind: domain.HarnessBackendDirectOpenRouterV1, build: func() (sessionlessharness.Registration, error) {
			return directopenrouter.DisabledRegistrationV1(dependencies.Direct)
		}},
	}
	registrations := make([]sessionlessharness.Registration, 0, len(builders))
	for _, builder := range builders {
		registration, err := builder.build()
		if err != nil || registration.Enabled || registration.Descriptor.BackendKind != builder.kind {
			return nil, ErrContract
		}
		registrations = append(registrations, registration)
	}
	return registrations, nil
}
