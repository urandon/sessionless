package localcontrolplane

import (
	"context"
	"errors"
	"io"
	"reflect"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localoperator"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

func TestControlPlaneOptionsKeepOriginalSearchGrantCustody(t *testing.T) {
	// Constructor wiring only: this deliberately unentered Source marker
	// proves no installation, grant permission or original writer ownership.
	source := new(localoperator.Source)
	installation := &Installation{source: source}
	options := installation.StoreOptions()
	if options.OwnedProfilePublicationVerifier != source || options.AttemptSpendingPublicationVerifier != source || options.OwnedAttemptEffectSourceVerifier != source {
		t.Fatal("existing verifiers must keep the same reopened Source")
	}
	if options.OwnedWebSearchResourceGrantCustody != source {
		t.Fatal("private V2 consent reads require that same Source's grant custody")
	}
}

// These are construction checks, not a substitute for the original worker's
// durable MODEL producer or authenticated Store/consent integration.
func TestControlPlaneWebOptionsRemainDisabledWithoutInstallation(t *testing.T) {
	var installation *Installation
	blobs := new(consentWiringBlobs)
	options, err := installation.WebStoreOptions(blobs)
	if err != nil || !reflect.DeepEqual(options, ydbstore.Options{}) || blobs.calls != 0 {
		t.Fatal("disabled Web construction installed a reader or touched storage", err)
	}
}

func TestControlPlaneWebOptionsRequireStorageAndKeepSource(t *testing.T) {
	source := new(localoperator.Source)
	installation := &Installation{source: source}
	var typedNil *consentWiringBlobs
	for _, blobs := range []ports.BlobStore{nil, typedNil} {
		options, err := installation.WebStoreOptions(blobs)
		if !errors.Is(err, ErrUnavailable) || !reflect.DeepEqual(options, ydbstore.Options{}) {
			t.Fatal("enabled Web construction accepted absent storage", err)
		}
	}
	blobs := new(consentWiringBlobs)
	options, err := installation.WebStoreOptions(blobs)
	if err != nil || options.AttemptOperationBodies == nil || blobs.calls != 0 {
		t.Fatal("enabled Web construction failed or performed I/O", err)
	}
	if options.OwnedProfilePublicationVerifier != source || options.AttemptSpendingPublicationVerifier != source || options.OwnedAttemptEffectSourceVerifier != source || options.OwnedWebSearchResourceGrantCustody != source {
		t.Fatal("Web construction replaced the original Source authorities")
	}
	if options.OwnedOperationResidualCustody != nil {
		t.Fatal("read-only BFF borrowed the worker's cleanup custody")
	}
	if body, err := options.AttemptOperationBodies.ReadAttemptOperationBody(t.Context(), domain.AttemptOperationScopeV1{}, "arbitrary/private.json", 128); err == nil || body != nil || blobs.calls != 0 {
		t.Fatal("constructed reader bypassed the closed consent selector", err)
	}
}

type consentWiringBlobs struct{ calls int }

func (b *consentWiringBlobs) Put(context.Context, domain.TenantID, string, io.Reader) (domain.BlobRef, error) {
	b.calls++
	return domain.BlobRef{}, errors.New("unexpected construction-time write")
}
func (b *consentWiringBlobs) Open(context.Context, domain.TenantID, domain.BlobRef) (io.ReadCloser, error) {
	b.calls++
	return nil, errors.New("unexpected construction-time read")
}
func (b *consentWiringBlobs) Delete(context.Context, domain.TenantID, domain.BlobRef) error {
	b.calls++
	return errors.New("unexpected construction-time delete")
}
