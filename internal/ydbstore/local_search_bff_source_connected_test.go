package ydbstore_test

import (
	"context"
	"errors"
	"io"
	"reflect"
	"sync"
	"testing"
	"testing/synctest"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localcontrolplane"
	"gitcode.com/urandon/sessionless/internal/operationbody"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// This peer makes any BFF write/delete a test failure. Only the genuine worker
// argument owner, before BFF construction, may write the stored argument body.
type searchBFFOpenOnlyBlobs struct {
	inner     ports.BlobStore
	opens     int
	afterRead func()
}

func (*searchBFFOpenOnlyBlobs) Put(context.Context, domain.TenantID, string, io.Reader) (domain.BlobRef, error) {
	panic("BFF tried to acquire argument write custody")
}

func (*searchBFFOpenOnlyBlobs) Delete(context.Context, domain.TenantID, domain.BlobRef) error {
	panic("BFF tried to acquire worker cleanup custody")
}

func (b *searchBFFOpenOnlyBlobs) Open(ctx context.Context, tenant domain.TenantID, ref domain.BlobRef) (io.ReadCloser, error) {
	b.opens++
	r, err := b.inner.Open(ctx, tenant, ref)
	if err != nil {
		return nil, err
	}
	return &searchBFFReadCloser{ReadCloser: r, afterRead: b.afterRead}, nil
}

type searchBFFReadCloser struct {
	io.ReadCloser
	afterRead func()
	once      sync.Once
}

func (r *searchBFFReadCloser) Close() error {
	err := r.ReadCloser.Close()
	r.once.Do(func() {
		if r.afterRead != nil {
			r.afterRead()
		}
	})
	return err
}

func newSearchBFFStore(t *testing.T, seed *ydbstore.LocalSearchBFFSeed) (*ydbstore.Store, *localcontrolplane.Installation, *searchBFFOpenOnlyBlobs) {
	t.Helper()
	installation, err := localcontrolplane.Open(t.Context(), localcontrolplane.Config{Enabled: true, Installation: seed.Installation.Root, Executable: seed.Installation.Executable, Receipt: seed.ReceiptPath})
	if err != nil || installation == nil {
		t.Fatal("independent BFF Source reopen", err)
	}
	t.Cleanup(func() {
		if err := installation.Close(); err != nil {
			t.Error(err)
		}
	})
	blobs := &searchBFFOpenOnlyBlobs{inner: seed.Blobs}
	options, err := installation.WebStoreOptions(blobs)
	if err != nil {
		t.Fatal("actual BFF WebStoreOptions", err)
	}
	if _, ok := options.AttemptOperationBodies.(*operationbody.ConsentArgumentReaderV1); !ok || options.OwnedWebSearchResourceGrantCustody == nil || options.OwnedOperationResidualCustody != nil {
		t.Fatal("BFF factory did not install the separate Open-only reader/Source grant custody")
	}
	store, err := ydbstore.New(seed.NewBFFDatabase(t), options)
	if err != nil {
		t.Fatal("construct independent BFF Store", err)
	}
	return store, installation, blobs
}

// Source, signed funding, original worker preparation and immutable argument
// writer are genuine. Pending/canonical SQL rows are structural fixture DATA,
// not a normal MODEL producer, real YDB, HTTP/WebUI or external search proof.
func TestLocalSourceSearchBFFSeparateStoreExactPrivateArguments(t *testing.T) {
	three := uint8(3)
	for _, tc := range []struct {
		name    string
		maximum *uint8
	}{{"omitted", nil}, {"three", &three}} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				seed := ydbstore.NewLocalSearchBFFSeed(t, tc.maximum)
				store, _, blobs := newSearchBFFStore(t, seed)
				writes := seed.CanonicalWriteCount()
				got, err := store.ReadPrivateWebSearchConsent(t.Context(), seed.Input)
				if err != nil || got.Query != seed.Query || !reflect.DeepEqual(got.MaxResults, tc.maximum) || got.State.Status != domain.WebSearchConsentPendingV1 {
					t.Fatalf("separate BFF private read: query=%q max=%v err=%v", got.Query, got.MaxResults, err)
				}
				if blobs.opens != 1 || seed.CanonicalWriteCount() != writes {
					t.Fatalf("private view changed writes or body I/O: opens=%d writes=%d/%d", blobs.opens, seed.CanonicalWriteCount(), writes)
				}
				// Closing the worker's independently owned Source must not be
				// confused with closing the BFF's separately reopened Source.
				if err := seed.Installation.Source.Close(); err != nil {
					t.Fatal(err)
				}
				again, err := store.ReadPrivateWebSearchConsent(t.Context(), seed.Input)
				if err != nil || again.Query != seed.Query || blobs.opens != 2 {
					t.Fatalf("BFF borrowed worker Source lifetime: err=%v opens=%d", err, blobs.opens)
				}
			})
		})
	}
}

func TestLocalSourceSearchBFFCurrentAuthorityAndIntegrity(t *testing.T) {
	for _, fault := range []string{"membership-before-read", "source-before-read", "membership-after-read", "source-after-read", "corrupt-object"} {
		t.Run(fault, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				seed := ydbstore.NewLocalSearchBFFSeed(t, nil)
				store, installation, blobs := newSearchBFFStore(t, seed)
				// First prove this exact graph can disclose the genuine stored
				// arguments; a broken seed cannot satisfy a negative control.
				baseline, err := store.ReadPrivateWebSearchConsent(t.Context(), seed.Input)
				if err != nil || baseline.Query != seed.Query || blobs.opens != 1 {
					t.Fatal("negative control lacks passing prerequisite", err)
				}
				writes, opens, expectedOpens := seed.CanonicalWriteCount(), blobs.opens, 1
				switch fault {
				case "membership-before-read":
					seed.RevokeCurrentMembership(t)
					expectedOpens = 0
				case "source-before-read":
					if err := installation.Close(); err != nil {
						t.Fatal(err)
					}
					expectedOpens = 0
				case "membership-after-read":
					blobs.afterRead = func() { seed.RevokeCurrentMembership(t) }
				case "source-after-read":
					blobs.afterRead = func() {
						if err := installation.Close(); err != nil {
							t.Error(err)
						}
					}
				case "corrupt-object":
					seed.CorruptStoredArguments()
				}
				got, err := store.ReadPrivateWebSearchConsent(t.Context(), seed.Input)
				if !errors.Is(err, domain.ErrMembershipDenied) || !reflect.DeepEqual(got, ports.WebSearchConsentPrivateViewV1{}) {
					t.Fatalf("%s disclosed private DATA: query=%q err=%v", fault, got.Query, err)
				}
				if blobs.opens-opens != expectedOpens || seed.CanonicalWriteCount() != writes {
					t.Fatalf("%s changed custody or authorization order: opens=%d want=%d writes=%d/%d", fault, blobs.opens-opens, expectedOpens, seed.CanonicalWriteCount(), writes)
				}
			})
		})
	}
}
