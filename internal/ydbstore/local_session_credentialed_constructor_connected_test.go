package ydbstore_test

import (
	"errors"
	"testing"

	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/providercredential"
	"gitcode.com/urandon/sessionless/internal/providerloopsession"
)

func TestLocalSessionCredentialedRejectsAbsentLifecycleWithValidLocalOwners(t *testing.T) {
	_, seed, blobs, credentials, _, control := newCredentialedConnectedSession(t)
	if control == nil {
		t.Fatal("coherent original Bearer owners did not construct positive control")
	}
	local := providerloopsession.LocalConfigV1{Source: seed.Source, DB: seed.DB, StoreOptions: seed.Options, Prepare: seed.Prepare,
		Operate: seed.Operate, Backend: seed.Backend, Substrate: seed.Substrate, Blobs: blobs}
	before, reads := seed.Stats(), seed.ProviderCredentialReadCount()
	var typedNil *providercredential.InvocationService
	for _, lifecycle := range []ports.ProviderResourceCredentialLifecycleV1{nil, typedNil} {
		v, err := providerloopsession.NewCredentialedV1(t.Context(), providerloopsession.CredentialedConfigV1{Local: local, Credentials: lifecycle})
		if v != nil {
			t.Cleanup(func() {
				if err := v.Close(); err != nil {
					t.Error("close unexpectedly constructed invalid-lifecycle owner")
				}
			})
		}
		if v != nil || !errors.Is(err, providerloopsession.ErrSession) {
			t.Fatal("otherwise valid original Local owners masked an absent/typed-nil lifecycle")
		}
	}
	if seed.Stats() != before || seed.ProviderCredentialReadCount() != reads || credentials.issues != 0 || credentials.materializations != 0 || credentials.releases != 0 {
		t.Fatal("constructor validation created SQL effects or credential calls")
	}
}
