//go:build linux || darwin

package ownerlocalcredential

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/credentiallifecycle"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/testkit"
)

func TestOwnerLocalOriginalReleaseDrainsRepeatedRefresh(t *testing.T) {
	s, config, _, _ := initializedStore(t)
	scratch, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	life, err := NewLifecycle(s, credentiallifecycle.Config{ScratchRoot: scratch, MaxAuthBytes: 128, Clock: testkit.NewFakeClock(fixtureTime), IDs: testkit.NewSequenceIDGenerator("synthetic-drain-")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := life.Close(); err != nil {
			t.Errorf("close drained lifecycle: %v", err)
		}
	})
	for i := uint64(1); i <= 7; i++ {
		req := lifecycleRequest()
		req.ProviderResource.CredentialGeneration = i
		h, err := life.Issue(t.Context(), req)
		if err != nil {
			t.Fatal("refresh blocked before normal cleanup bound", i, err)
		}
		m, err := life.Materialize(t.Context(), h)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(m.AuthFile, []byte(fmt.Sprintf("synthetic-refresh-%d", i)), 0o600); err != nil {
			t.Fatal(err)
		}
		if wb, err := life.WriteBack(t.Context(), h, m); err != nil || !wb.Changed || wb.Generation != i+1 {
			t.Fatal("normal changed refresh", i, wb, err)
		}
		if len(s.state.Cleanups) != 1 || s.state.Cleanups[0].Released {
			t.Fatal("cleanup became ready before original Release")
		}
		if err := s.Reconcile(t.Context()); err != nil {
			t.Fatal(err)
		}
		if len(s.state.Cleanups) != 1 {
			t.Fatal("Reconcile inferred auth-file cleanup from generation")
		}
		if err := life.Release(t.Context(), h); err != nil {
			t.Fatal("original release/drain", i, err)
		}
		if len(s.state.Cleanups) != 0 {
			t.Fatal("successful original Release did not drain exact superseded reference", i)
		}
	}
	entries, err := os.ReadDir(config.Root)
	if err != nil || len(entries) != 3 {
		t.Fatal("superseded encrypted bodies accumulated", len(entries), err)
	}
}

func TestOwnerLocalHeaderProspectiveSizeMatchesStandardJSON(t *testing.T) {
	s, _, _, _ := initializedStore(t)
	for _, value := range []string{"plain", `"\\`, "<>&\n\r\t\b\f\x00", "世界\u2028\u2029", strings.Repeat("&", 512), strings.Repeat("&", 800)} {
		h := s.header(candidateRecord{Reference: strings.Repeat("a", 64), Fingerprint: domain.CredentialFingerprint(strings.Repeat("b", 64)), Expected: 123, Size: maxAuthBytes})
		h.Root = value
		want, err := json.Marshal(h)
		if err != nil {
			t.Fatal(err)
		}
		got, countErr := headerEncodedSize(h)
		if len(want) > maxHeaderBytes {
			if countErr != ErrDenied {
				t.Fatal("oversize header not prospectively denied")
			}
			continue
		}
		if countErr != nil || got != len(want) {
			t.Fatalf("exact prospective header count got=%d want=%d err=%v", got, len(want), countErr)
		}
	}
}

func TestOwnerLocalEmptyInitialTempCrashDoesNotAdoptCredential(t *testing.T) {
	s, config, key := storeFixture(t)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(config.Root, metadataName), filepath.Join(config.Root, metadataTemporary)); err != nil {
		t.Fatal(err)
	}
	reopened, err := New(t.Context(), config, key)
	if err != nil {
		t.Fatal("authenticated zero-use initial temp recovery", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if _, found, err := reopened.LoadCredentialBinding(t.Context(), config.Scope.TenantID, config.Scope.SubscriptionConnectionID); err != nil || found || reopened.state.KeyUses != 0 {
		t.Fatal("empty initialization created credential authority", found, err)
	}
}

func TestOwnerLocalAuthenticatedPrivateShapeStillClosed(t *testing.T) {
	s, _, _, _ := initializedStore(t)
	for _, change := range []struct{ old, new []byte }{
		{[]byte(`"version":1`), []byte(`"version":1,"version":1`)},
		{[]byte(`"key_uses":1`), []byte(`"KEY_USES":1`)},
		{[]byte(`"key_uses":1`), []byte(`"key_uses":1,"unknown":true`)},
	} {
		payload := bytes.Replace(s.image[8:len(s.image)-32], change.old, change.new, 1)
		encoded := append([]byte("OLCS\x00\x00\x00\x01"), payload...)
		mac := hmac.New(sha256.New, s.macKey)
		_, _ = mac.Write(encoded)
		encoded = append(encoded, mac.Sum(nil)...)
		if _, err := s.decodeSnapshot(encoded); err != ErrDenied {
			t.Fatal("authenticated unknown/alias/duplicate private fields admitted", err)
		}
	}
}
