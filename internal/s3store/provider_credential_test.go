package s3store

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func TestProviderCustodyInventoryPhysicalCursorAndScope(t *testing.T) {
	for _, iam := range []bool{false, true} {
		name := "sdk"
		if iam {
			name = "iam"
		}
		t.Run(name, func(t *testing.T) {
			s, peer, b, ctx, scope := custodyFixture(t, iam)
			secret := []byte("SYNTHETIC-INVENTORY-SECRET")
			var candidates []ports.ProviderCredentialSecretCandidateV1
			for i := 0; i < 4; i++ {
				copyScope := scope
				copyScope.MutationID += string(rune('a' + i))
				c, err := s.PutProviderCredentialCandidate(ctx, copyScope, secret)
				if err != nil {
					t.Fatal(err)
				}
				candidates = append(candidates, c)
			}
			b.values[scope.Locator] = custodyBinding(candidates[0])
			list, err := s.ListUncommittedProviderCredentialCandidates(ctx, scope.Locator, 2)
			if err != nil || len(list) != 2 {
				t.Fatalf("scoped list count=%d err=%v", len(list), err)
			}
			for _, c := range list {
				if c == candidates[0] {
					t.Fatal("active binding included as uncommitted")
				}
			}
			var cursor ports.ProviderCredentialCandidateCursorV1
			seen := map[domain.CredentialSecretRef]bool{}
			for page := 0; page < 5; page++ {
				p, err := s.ListAbandonedProviderCredentialCandidates(ctx, time.Unix(101, 0).UTC(), cursor, 1)
				if err != nil {
					t.Fatal(err)
				}
				if p.HasMore && !p.NextCursor.Present {
					t.Fatal("no physical progress")
				}
				for _, c := range p.Items {
					if seen[c.Reference] || c == candidates[0] {
						t.Fatal("duplicate or authoritative inventory item")
					}
					seen[c.Reference] = true
				}
				cursor = p.NextCursor
				if !p.HasMore {
					break
				}
			}
			if len(seen) != 3 {
				t.Fatalf("global traversal found %d want3", len(seen))
			}
			p, err := s.ListAbandonedProviderCredentialCandidates(ctx, time.Unix(99, 0).UTC(), ports.ProviderCredentialCandidateCursorV1{}, 1)
			if err != nil || len(p.Items) != 0 || !p.HasMore || !p.NextCursor.Present {
				t.Fatalf("filtered physical progress items=%d more=%v cursor=%v err=%v", len(p.Items), p.HasMore, p.NextCursor.Present, err)
			}
			b.loadErr = true
			if _, err := s.ListUncommittedProviderCredentialCandidates(ctx, scope.Locator, 1); err == nil {
				t.Fatal("binding error became absence")
			}
			b.loadErr = false
			b.loadForeign = true
			if _, err := s.ListUncommittedProviderCredentialCandidates(ctx, scope.Locator, 1); err == nil {
				t.Fatal("foreign binding admitted")
			}
			b.loadForeign = false
			peer.mu.Lock()
			for _, r := range peer.requests {
				if r.method == http.MethodGet && r.key == "" && r.query.Get("max-keys") == "" {
					t.Error("unbounded inventory request")
				}
			}
			peer.mu.Unlock()
		})
	}
}

func TestProviderCustodyLogicalCommitCannotResurrect(t *testing.T) {
	s, peer, b, ctx, scope := custodyFixture(t, false)
	c, err := s.PutProviderCredentialCandidate(ctx, scope, []byte("SYNTHETIC-DELETED-SECRET"))
	if err != nil {
		t.Fatal(err)
	}
	binding := custodyBinding(c)
	b.values[scope.Locator] = binding
	if err := s.CommitProviderCredentialCandidate(ctx, c); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteProviderCredentialSecret(ctx, ports.ProviderCredentialCleanupV1{Locator: scope.Locator, CredentialGeneration: 1, Reference: c.Reference}); err != nil {
		t.Fatal(err)
	}
	peer.mu.Lock()
	writes := 0
	for _, r := range peer.requests {
		if r.method == http.MethodPut {
			writes++
		}
	}
	peer.mu.Unlock()
	if err := s.CommitProviderCredentialCandidate(ctx, c); err == nil {
		t.Fatal("missing object committed")
	}
	if ok, err := s.RecoverProviderCredentialCandidate(ctx, binding); err != nil || ok {
		t.Fatalf("recover absent ok=%v err=%v", ok, err)
	}
	peer.mu.Lock()
	defer peer.mu.Unlock()
	actual := 0
	for _, r := range peer.requests {
		if r.method == http.MethodPut {
			actual++
		}
	}
	if actual != writes || len(peer.objects) != 0 {
		t.Fatal("post-cleanup verification recreated ciphertext")
	}
}

func TestProviderCustodyCloseJoinsAndRetainsOnTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s, peer, b, ctx, scope := custodyFixture(t, false)
		c, err := s.PutProviderCredentialCandidate(ctx, scope, []byte("SYNTHETIC-CLOSE-SECRET"))
		if err != nil {
			t.Fatal(err)
		}
		binding := custodyBinding(c)
		b.values[scope.Locator] = binding
		peer.blockGet = make(chan struct{})
		peer.entered = make(chan struct{})
		release := peer.blockGet
		entered := peer.entered
		done := make(chan error, 1)
		go func() { v, err := s.ReadProviderCredentialSecret(ctx, binding, 8192); clear(v); done <- err }()
		<-entered
		closing, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := s.Close(closing); err == nil {
			t.Fatal("Close completed while an active read owned the key")
		}
		s.keyState.mu.Lock()
		retained := len(s.keyState.root) == 32
		s.keyState.mu.Unlock()
		if !retained {
			t.Fatal("key cleared under active read")
		}
		if _, err := s.ReadProviderCredentialSecret(ctx, binding, 8192); err == nil {
			t.Fatal("closing store admitted new reader")
		}
		close(release)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if err := s.Close(ctx); err != nil {
			t.Fatal(err)
		}
		s.keyState.mu.Lock()
		zeroed := len(s.keyState.root) == 0
		s.keyState.mu.Unlock()
		if !zeroed {
			t.Fatal("joined Close retained root key")
		}
	})
}

func TestProviderCustodyConstructorAndArgumentBounds(t *testing.T) {
	s, _, b, ctx, scope := custodyFixture(t, false)
	key := bytes.Repeat([]byte{3}, 32)
	other, err := NewProviderCredentialSecretStore(s.objects, s.config, b, key)
	if err != nil {
		t.Fatal(err)
	}
	clear(key)
	if bytes.Equal(other.keyState.root, key) {
		t.Fatal("constructor borrowed caller key")
	}
	if err := other.Close(ctx); err != nil {
		t.Fatal(err)
	}
	for _, format := range []string{"%v", "%+v", "%#v", "%x", "%d"} {
		text := fmt.Sprintf(format, s)
		if text != "[redacted provider credential object store]" {
			t.Errorf("formatter %s exposed non-redacted output %q", format, text)
		}
	}
	if _, err := s.PutProviderCredentialCandidate(context.Background(), scope, []byte("x")); err == nil {
		t.Fatal("nonfinite operation admitted")
	}
	if _, err := s.PutProviderCredentialCandidate(ctx, scope, bytes.Repeat([]byte{1}, 8193)); err == nil {
		t.Fatal("configured limit bypassed")
	}
	for _, epoch := range []string{"", "../epoch", "EPOCH"} {
		config := s.config
		config.KeyEpoch = epoch
		if _, err := NewProviderCredentialSecretStore(s.objects, config, b, bytes.Repeat([]byte{3}, 32)); err == nil {
			t.Fatal("invalid epoch admitted")
		}
	}
	for _, kind := range []domain.ProviderResourceKindV1{domain.ProviderResourceAPIAccountV1, domain.ProviderResourceRouterAccountV1, domain.ProviderResourceLocalEndpointV1} {
		local := scope
		local.Locator.ResourceKind = kind
		c, err := s.PutProviderCredentialCandidate(ctx, local, []byte("SYNTHETIC-RESOURCE-KIND"))
		if err != nil {
			t.Fatal(err)
		}
		if err := s.DeleteProviderCredentialCandidate(ctx, c); err != nil {
			t.Fatal(err)
		}
	}
}

func TestProviderCustodyForeignEpochDeniedBeforeIO(t *testing.T) {
	s, peer, b, ctx, scope := custodyFixture(t, false)
	c, err := s.PutProviderCredentialCandidate(ctx, scope, []byte("SYNTHETIC-EPOCH-SECRET"))
	if err != nil {
		t.Fatal(err)
	}
	c.Reference, err = domain.NewCredentialSecretRef(strings.Replace(c.Reference.StorageValue(), "pcs1.epoch-1.", "pcs1.epoch-2.", 1))
	if err != nil {
		t.Fatal(err)
	}
	binding := custodyBinding(c)
	b.values[scope.Locator] = binding
	peer.mu.Lock()
	before := len(peer.requests)
	peer.mu.Unlock()
	if err := s.CommitProviderCredentialCandidate(ctx, c); err == nil {
		t.Error("foreign epoch committed")
	}
	if _, err := s.RecoverProviderCredentialCandidate(ctx, binding); err == nil {
		t.Error("foreign epoch recovered")
	}
	if _, err := s.ReadProviderCredentialSecret(ctx, binding, 8192); err == nil {
		t.Error("foreign epoch read")
	}
	if err := s.DeleteProviderCredentialCandidate(ctx, c); err == nil {
		t.Error("foreign epoch candidate deleted")
	}
	if err := s.DeleteProviderCredentialSecret(ctx, ports.ProviderCredentialCleanupV1{Locator: scope.Locator, CredentialGeneration: 1, Reference: c.Reference}); err == nil {
		t.Error("foreign epoch cleanup acknowledged")
	}
	if _, err := s.ListAbandonedProviderCredentialCandidates(ctx, time.Unix(101, 0).UTC(), providerCursor(c), 1); err == nil {
		t.Error("foreign epoch cursor admitted")
	}
	peer.mu.Lock()
	defer peer.mu.Unlock()
	if len(peer.requests) != before {
		t.Fatalf("foreign epoch caused %d object requests", len(peer.requests)-before)
	}
}

func TestProviderCustodyCopiedStoreNeverFormatsRootKey(t *testing.T) {
	s, _, _, _, _ := custodyFixture(t, false)
	value := *s
	for _, v := range []any{value, []ProviderCredentialObjectStore{value}, map[string]ProviderCredentialObjectStore{"store": value}} {
		for _, format := range []string{"%v", "%+v", "%#v", "%d", "%x", "%q"} {
			text := fmt.Sprintf(format, v)
			for _, marker := range []string{"66 66 66 66", "0x42, 0x42, 0x42, 0x42", strings.Repeat("42", 16), strings.Repeat("B", 8)} {
				if strings.Contains(text, marker) {
					t.Errorf("copied store/container %T formatter %s disclosed root-key marker", v, format)
				}
			}
		}
	}
}
