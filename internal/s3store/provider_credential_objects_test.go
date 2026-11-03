package s3store

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/providercredential"
)

type custodyCloseError struct{ io.Reader }

func (custodyCloseError) Close() error { return errors.New("SYNTHETIC-PRIVATE-CLOSE") }

func TestProviderCustodyIAMAbsenceRequiresCleanClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	r := &http.Response{StatusCode: 404, Body: custodyCloseError{strings.NewReader(`<Error><Code>NoSuchKey</Code></Error>`)}}
	if providerIAMMissing(ctx, r) {
		t.Fatal("failed response Close became exact absence")
	}
	for _, raw := range []string{`<Error><Code>NoSuchKey</Code><Code>NoSuchBucket</Code></Error>`, `<Error><Code>NoSuchKey</Code></Error>trailing`} {
		if providerIAMMissing(ctx, &http.Response{StatusCode: 404, Body: io.NopCloser(strings.NewReader(raw))}) {
			t.Fatal("noncanonical service error became absence")
		}
	}
}

func TestProviderCustodyAmbiguityNoReplayAndRedaction(t *testing.T) {
	for _, iam := range []bool{false, true} {
		name := "sdk"
		if iam {
			name = "iam"
		}
		t.Run(name, func(t *testing.T) {
			s, peer, b, ctx, scope := custodyFixture(t, iam)
			peer.unknownPut = true
			if c, err := s.PutProviderCredentialCandidate(ctx, scope, []byte("SYNTHETIC-UNKNOWN-SECRET")); err == nil || !c.Reference.IsZero() || strings.Contains(err.Error(), "SYNTHETIC") {
				t.Fatalf("unknown Put redaction/candidate err=%v", err)
			}
			peer.mu.Lock()
			puts := 0
			for _, r := range peer.requests {
				if r.method == http.MethodPut {
					puts++
				}
			}
			peer.mu.Unlock()
			if puts != 1 {
				t.Fatalf("unknown PUT replayed %d times", puts)
			}
			page, err := s.ListAbandonedProviderCredentialCandidates(ctx, time.Unix(101, 0).UTC(), ports.ProviderCredentialCandidateCursorV1{}, 16)
			if err != nil || len(page.Items) != 1 {
				t.Fatalf("ambiguous object vanished items=%d err=%v", len(page.Items), err)
			}
			service, err := providercredential.New(providercredential.Config{Clock: s.config.Clock, IDs: &custodyIDs{}}, custodyAuthorizer{}, custodyRevoker{}, b, s)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := service.DrainAbandonedCandidates(ctx, time.Unix(101, 0).UTC(), ports.ProviderCredentialCandidateCursorV1{}); err != nil {
				t.Fatal(err)
			}
			peer.mu.Lock()
			remaining := len(peer.objects)
			peer.mu.Unlock()
			if remaining != 0 {
				t.Fatal("unknown object was not eventually reconciled")
			}
			peer.redirect = true
			if _, err := s.PutProviderCredentialCandidate(ctx, scope, []byte("SYNTHETIC-REDIRECT-SECRET")); err == nil || strings.Contains(err.Error(), "SYNTHETIC") {
				t.Fatal("redirect admitted or raw error exposed")
			}
			peer.redirect = false
		})
	}
}

func TestProviderCustodyDeleteMetadataOnlyAndFailureRetention(t *testing.T) {
	for _, iam := range []bool{false, true} {
		name := "sdk"
		if iam {
			name = "iam"
		}
		t.Run(name, func(t *testing.T) {
			s, peer, b, ctx, scope := custodyFixture(t, iam)
			c, err := s.PutProviderCredentialCandidate(ctx, scope, []byte("SYNTHETIC-METADATA-ONLY"))
			if err != nil {
				t.Fatal(err)
			}
			key, _ := providerObjectKey(scope.Locator, c.Reference)
			// Authenticated ciphertext with a deliberately invalid GCM tag proves that
			// metadata deletion does not call the plaintext-opening path.
			peer.mu.Lock()
			wire := append([]byte(nil), peer.objects[key]...)
			peer.mu.Unlock()
			ml := int(binary.BigEndian.Uint32(wire[8:12]))
			wire[28+ml] ^= 1
			remacProviderWire(t, s.keyState.root, c.Reference, wire)
			peer.mu.Lock()
			peer.objects[key] = wire
			peer.mu.Unlock()
			if _, err := s.ReadProviderCredentialSecret(ctx, custodyBinding(c), 8192); err == nil {
				t.Fatal("invalid AEAD admitted for plaintext read")
			}
			cleanup := ports.ProviderCredentialCleanupV1{Locator: scope.Locator, CredentialGeneration: 1, Reference: c.Reference}
			b.cleanups = append(b.cleanups, cleanup)
			service, err := providercredential.New(providercredential.Config{Clock: s.config.Clock, IDs: &custodyIDs{}}, custodyAuthorizer{}, custodyRevoker{}, b, s)
			if err != nil {
				t.Fatal(err)
			}
			peer.failDelete = true
			if _, err := service.DrainDueCleanups(ctx, 0, time.Unix(101, 0).UTC(), ports.ProviderCredentialCleanupCursorV1{}); err == nil {
				t.Fatal("unknown deletion acknowledged")
			}
			if b.acks != 0 {
				t.Fatal("failed delete lost cleanup obligation")
			}
			peer.failDelete = false
			if _, err := service.DrainDueCleanups(ctx, 0, time.Unix(101, 0).UTC(), ports.ProviderCredentialCleanupCursorV1{}); err != nil {
				t.Fatal(err)
			}
			if b.acks != 1 {
				t.Fatal("metadata-only cleanup not acknowledged")
			}
		})
	}
}

func TestProviderCustodyBadReadbackRetainsInventory(t *testing.T) {
	s, peer, _, ctx, scope := custodyFixture(t, false)
	peer.badReadback = true
	if _, err := s.PutProviderCredentialCandidate(ctx, scope, []byte("SYNTHETIC-BAD-READBACK")); err == nil {
		t.Fatal("bad readback admitted")
	}
	peer.badReadback = false
	p, err := s.ListAbandonedProviderCredentialCandidates(ctx, time.Unix(101, 0).UTC(), ports.ProviderCredentialCandidateCursorV1{}, 1)
	if err != nil || len(p.Items) != 1 {
		t.Fatalf("bad readback object disappeared items=%d err=%v", len(p.Items), err)
	}
	if _, _, err := s.list(ctx, providerObjectRoot, "", 66); err == nil {
		t.Fatal("oversized list admitted")
	}
	if _, err := s.PutProviderCredentialCandidate(ctx, scope, bytes.Repeat([]byte{1}, 8193)); err == nil {
		t.Fatal("oversized put admitted")
	}
	canceled, cancel := context.WithTimeout(context.Background(), time.Hour)
	cancel()
	if _, _, err := s.get(canceled, "not-a-secret"); err == nil {
		t.Fatal("canceled object read admitted")
	}
}

func TestProviderCustodyExactAbsenceAndCleanupACK(t *testing.T) {
	for _, iam := range []bool{false, true} {
		backend := "sdk"
		if iam {
			backend = "iam"
		}
		t.Run(backend, func(t *testing.T) {
			for _, tc := range []struct {
				name, body string
				missing    bool
			}{
				{"object", `<Error><Code>NoSuchKey</Code></Error>`, true},
				{"bucket", `<Error><Code>NoSuchBucket</Code></Error>`, false},
				{"auth", `<Error><Code>AccessDenied</Code></Error>`, false},
				{"proxy", `<html>SYNTHETIC-PRIVATE-PROXY</html>`, false},
				{"forged-code-wrong-root", `<html><Code>NoSuchKey</Code></html>`, false},
				{"conflicting-codes", `<Error><Code>NoSuchBucket</Code><Code>NoSuchKey</Code></Error>`, false},
				{"trailing-code", `<Error><Code>NoSuchKey</Code></Error>garbage`, false},
				{"close-error", `<Error><Code>NoSuchKey</Code></Error>`, false},
				{"malformed", `<Error><Code>NoSuchKey`, false},
				{"oversized", `<Error><Code>NoSuchKey</Code><Message>` + strings.Repeat("x", providerEnvelopeMax) + `</Message></Error>`, false},
			} {
				t.Run(tc.name, func(t *testing.T) {
					s, peer, b, ctx, scope := custodyFixture(t, iam)
					c, err := s.PutProviderCredentialCandidate(ctx, scope, []byte("SYNTHETIC-ABSENCE-SECRET"))
					if err != nil {
						t.Fatal(err)
					}
					peer.overrideMethod = http.MethodGet
					peer.overrideStatus = 404
					peer.overrideBody = tc.body
					peer.overrideCloseError = tc.name == "close-error"
					key, _ := providerObjectKey(scope.Locator, c.Reference)
					_, found, err := s.get(ctx, key)
					if tc.missing {
						if err != nil || found {
							t.Fatalf("exact missing found=%v err=%v", found, err)
						}
					} else if err == nil {
						t.Error("non-object 404 became exact absence")
					}
					if !tc.missing {
						cleanup := ports.ProviderCredentialCleanupV1{Locator: scope.Locator, CredentialGeneration: 1, Reference: c.Reference}
						b.cleanups = []ports.ProviderCredentialCleanupV1{cleanup}
						service, err := providercredential.New(providercredential.Config{Clock: s.config.Clock, IDs: &custodyIDs{}}, custodyAuthorizer{}, custodyRevoker{}, b, s)
						if err != nil {
							t.Fatal(err)
						}
						if _, err := service.DrainDueCleanups(ctx, 0, time.Unix(101, 0).UTC(), ports.ProviderCredentialCleanupCursorV1{}); err == nil || b.acks != 0 {
							t.Fatal("bucket/proxy/auth failure acknowledged cleanup")
						}
					}
				})
			}
		})
	}
}

func TestProviderCustodyCASUnknownRecoversWithoutNewWrite(t *testing.T) {
	for _, iam := range []bool{false, true} {
		name := "sdk"
		if iam {
			name = "iam"
		}
		t.Run(name, func(t *testing.T) {
			s, peer, b, ctx, scope := custodyFixture(t, iam)
			service, err := providercredential.New(providercredential.Config{Clock: s.config.Clock, IDs: &custodyIDs{}}, custodyAuthorizer{}, custodyRevoker{}, b, s)
			if err != nil {
				t.Fatal(err)
			}
			request := ports.ProviderCredentialIngestRequestV1{Principal: ports.ProviderCredentialPrincipalV1{TenantID: scope.Locator.TenantID, OwnerUserID: scope.Locator.OwnerUserID, Channel: ports.ProviderCredentialChannelLocalOperatorV1}, ResourceKind: scope.Locator.ResourceKind, ResourceID: scope.Locator.ResourceID}
			b.casUnknown = true
			secret := []byte("SYNTHETIC-CAS-UNKNOWN")
			if _, err := service.Ingest(ctx, request, secret); err == nil {
				t.Fatal("ambiguous CAS reported success")
			}
			receipt, err := service.Ingest(ctx, request, secret)
			if err != nil || receipt.Status != ports.ProviderCredentialReplayedV1 {
				t.Fatalf("recover replay status=%s err=%v", receipt.Status, err)
			}
			peer.mu.Lock()
			defer peer.mu.Unlock()
			puts := 0
			for _, r := range peer.requests {
				if r.method == http.MethodPut {
					puts++
				}
			}
			if puts != 1 || len(peer.objects) != 1 {
				t.Fatalf("ambiguous CAS caused new write: PUTs=%d objects=%d", puts, len(peer.objects))
			}
		})
	}
}

func TestProviderCustodySweepWrapFindsLateVisibility(t *testing.T) {
	s, peer, _, ctx, scope := custodyFixture(t, false)
	for i := 0; i < 3; i++ {
		copyScope := scope
		copyScope.MutationID += string(rune('a' + i))
		if _, err := s.PutProviderCredentialCandidate(ctx, copyScope, []byte("SYNTHETIC-LATE-VISIBLE")); err != nil {
			t.Fatal(err)
		}
	}
	peer.mu.Lock()
	var keys []string
	for k := range peer.objects {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	lateKey := keys[0]
	lateWire := peer.objects[lateKey]
	delete(peer.objects, lateKey)
	peer.mu.Unlock()
	var cursor ports.ProviderCredentialCandidateCursorV1
	for i := 0; i < 3; i++ {
		page, err := s.ListAbandonedProviderCredentialCandidates(ctx, time.Unix(101, 0).UTC(), cursor, 1)
		if err != nil {
			t.Fatal(err)
		}
		cursor = page.NextCursor
		if !page.HasMore {
			break
		}
	}
	peer.mu.Lock()
	peer.objects[lateKey] = lateWire
	peer.mu.Unlock()
	after, err := s.ListAbandonedProviderCredentialCandidates(ctx, time.Unix(101, 0).UTC(), cursor, 1)
	if err != nil || len(after.Items) != 0 {
		t.Fatalf("late object was not actually behind cursor count=%d err=%v", len(after.Items), err)
	}
	wrapped, err := s.ListAbandonedProviderCredentialCandidates(ctx, time.Unix(101, 0).UTC(), ports.ProviderCredentialCandidateCursorV1{}, 1)
	if err != nil || len(wrapped.Items) != 1 {
		t.Fatal("new full sweep failed to discover late object")
	}
	observed, _ := providerObjectKey(wrapped.Items[0].Scope.Locator, wrapped.Items[0].Reference)
	if observed != lateKey {
		t.Fatal("wrapped sweep did not begin with late object")
	}
}

func TestProviderCustodyConditionalCreateDoesNotOverwrite(t *testing.T) {
	for _, iam := range []bool{false, true} {
		name := "sdk"
		if iam {
			name = "iam"
		}
		t.Run(name, func(t *testing.T) {
			s, peer, _, ctx, scope := custodyFixture(t, iam)
			s.keyState.random = bytes.NewReader(bytes.Repeat([]byte{7}, 88))
			c, err := s.PutProviderCredentialCandidate(ctx, scope, []byte("SYNTHETIC-IMMUTABLE-FIRST"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := s.PutProviderCredentialCandidate(ctx, scope, []byte("SYNTHETIC-IMMUTABLE-OTHER")); err == nil {
				t.Fatal("identity collision overwrote immutable object")
			}
			v, err := s.ReadProviderCredentialSecret(ctx, custodyBinding(c), 8192)
			if err != nil || !bytes.Equal(v, []byte("SYNTHETIC-IMMUTABLE-FIRST")) {
				t.Fatal("conditional create failed to preserve original")
			}
			clear(v)
			peer.mu.Lock()
			defer peer.mu.Unlock()
			if len(peer.objects) != 1 {
				t.Fatal("identity collision forked object")
			}
		})
	}
}

func TestProviderCustodyMalformedListIsNotCompletion(t *testing.T) {
	for _, iam := range []bool{false, true} {
		name := "sdk"
		if iam {
			name = "iam"
		}
		t.Run(name, func(t *testing.T) {
			for _, raw := range []string{`<html/>`, `<ListBucketResult/>`, `<ListBucketResult><IsTruncated>false</IsTruncated><IsTruncated>true</IsTruncated></ListBucketResult>`, `<ListBucketResult><IsTruncated>1</IsTruncated></ListBucketResult>`, `<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>garbage`, `<ListBucketResult><IsTruncated>true</IsTruncated></ListBucketResult>`, `<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>outside-prefix</Key></Contents></ListBucketResult>`} {
				s, peer, _, ctx, scope := custodyFixture(t, iam)
				peer.overrideMethod = http.MethodGet
				peer.overrideStatus = 200
				peer.overrideBody = raw
				if _, err := s.ListUncommittedProviderCredentialCandidates(ctx, scope.Locator, 1); err == nil {
					t.Errorf("malformed list admitted: %s", raw)
				}
			}
		})
	}
}
