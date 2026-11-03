package s3store

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func remacProviderWire(t *testing.T, root []byte, ref domain.CredentialSecretRef, wire []byte) {
	t.Helper()
	r, err := parseProviderRef(ref)
	if err != nil {
		t.Fatal(err)
	}
	a, m, err := providerKeys(root, r)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(a)
	defer clear(m)
	mac := hmac.New(sha256.New, m)
	_, _ = mac.Write(wire[:len(wire)-32])
	copy(wire[len(wire)-32:], mac.Sum(nil))
}

func custodyCodecFixture(t *testing.T) ([]byte, ports.ProviderCredentialSecretCandidateV1, string, []byte, []byte) {
	t.Helper()
	root := bytes.Repeat([]byte{0x42}, 32)
	scope := ports.ProviderCredentialCandidateScopeV1{Locator: ports.ProviderCredentialLocatorV1{TenantID: "codec-tenant", OwnerUserID: "codec-owner", ResourceKind: domain.ProviderResourceAPIAccountV1, ResourceID: "codec-resource"}, ResourceRevision: 2, CredentialGeneration: 3, MutationID: "codec-mutation"}
	at := time.Unix(100, 123456000).UTC()
	id := hex.EncodeToString(bytes.Repeat([]byte{7}, 32))
	ref, err := domain.NewCredentialSecretRef("pcs1.epoch-1.00000000000100123456." + providerDigest(scope) + "." + id)
	if err != nil {
		t.Fatal(err)
	}
	secret := []byte("SYNTHETIC-CODEC-SECRET")
	c := ports.ProviderCredentialSecretCandidateV1{Scope: scope, Reference: ref, Fingerprint: domain.FingerprintCredential(secret), CreatedAt: at}
	key, err := providerObjectKey(scope.Locator, ref)
	if err != nil {
		t.Fatal(err)
	}
	wire, err := sealProviderObject(root, c, "epoch-1", secret, bytes.NewReader(bytes.Repeat([]byte{9}, 12)))
	if err != nil {
		t.Fatal(err)
	}
	return root, c, key, wire, secret
}

func TestProviderCustodyCodecFixedWireAndTamper(t *testing.T) {
	root, c, key, wire, secret := custodyCodecFixture(t)
	if string(wire[:8]) != "SLPCS001" || len(wire) > providerEnvelopeMax || bytes.Contains(wire, secret) {
		t.Fatal("invalid fixed framing or plaintext exposure")
	}
	ml := int(binary.BigEndian.Uint32(wire[8:12]))
	cl := int(binary.BigEndian.Uint32(wire[12:16]))
	if cl != len(secret)+16 || len(wire) != 16+12+ml+cl+32 {
		t.Fatal("nonce/tag/length framing differs")
	}
	actual, plain, err := openProviderObject(root, "epoch-1", key, wire)
	if err != nil || actual != c || !bytes.Equal(plain, secret) {
		t.Fatalf("roundtrip candidate=%v secret-match=%v err=%v", actual == c, bytes.Equal(plain, secret), err)
	}
	clear(plain)
	for i := range wire {
		mutated := append([]byte(nil), wire...)
		mutated[i] ^= 1
		_, a, _, err := authenticateProviderObject(root, "epoch-1", key, mutated)
		clear(a)
		if err == nil {
			t.Fatalf("tamper byte %d admitted", i)
		}
	}
	for _, raw := range [][]byte{nil, wire[:len(wire)-1], append(append([]byte(nil), wire...), 0)} {
		if _, a, _, err := authenticateProviderObject(root, "epoch-1", key, raw); err == nil {
			clear(a)
			t.Fatal("malformed frame admitted")
		}
	}
	for _, wrong := range []struct {
		root       []byte
		epoch, key string
	}{{bytes.Repeat([]byte{2}, 32), "epoch-1", key}, {root, "epoch-2", key}, {root, "epoch-1", key + "x"}} {
		_, a, _, err := authenticateProviderObject(wrong.root, wrong.epoch, wrong.key, wire)
		clear(a)
		if err == nil {
			t.Fatal("foreign key/epoch/path admitted")
		}
	}
}

func TestProviderCustodyCodecMaximumAndClosedMetadata(t *testing.T) {
	root, c, key, wire, _ := custodyCodecFixture(t)
	secret := bytes.Repeat([]byte{0x6d}, 64<<10)
	c.Fingerprint = domain.FingerprintCredential(secret)
	maxWire, err := sealProviderObject(root, c, "epoch-1", secret, bytes.NewReader(bytes.Repeat([]byte{5}, 12)))
	if err != nil || len(maxWire) > providerEnvelopeMax {
		t.Fatalf("maximum valid envelope: len=%d err=%v", len(maxWire), err)
	}
	_, plain, err := openProviderObject(root, "epoch-1", key, maxWire)
	if err != nil || !bytes.Equal(plain, secret) {
		t.Fatal("maximum valid secret rejected")
	}
	clear(plain)
	if _, err := sealProviderObject(root, c, "epoch-1", append(secret, 1), bytes.NewReader(make([]byte, 12))); err == nil {
		t.Fatal("oversized secret admitted")
	}
	for _, length := range []uint32{0, providerMetadataMax + 1, ^uint32(0)} {
		raw := append([]byte(nil), wire...)
		binary.BigEndian.PutUint32(raw[8:12], length)
		_, a, _, err := authenticateProviderObject(root, "epoch-1", key, raw)
		clear(a)
		if err == nil {
			t.Fatal("oversized/empty metadata admitted")
		}
	}
	for _, change := range []func(*providerSecretMetadata){func(m *providerSecretMetadata) { m.Version = 2 }, func(m *providerSecretMetadata) { m.TenantID = "other-tenant" }, func(m *providerSecretMetadata) { m.OwnerUserID = "other-owner" }, func(m *providerSecretMetadata) { m.ResourceRevision++ }, func(m *providerSecretMetadata) { m.CredentialGeneration++ }, func(m *providerSecretMetadata) { m.MutationID = "other-mutation" }, func(m *providerSecretMetadata) { m.CreatedAt = "1970-01-01T00:01:40Z" }} {
		m := metadataFor(c, "epoch-1")
		change(&m)
		if _, err := m.candidate(); err == nil {
			t.Fatal("changed/noncanonical metadata scope admitted")
		}
	}
	for _, ref := range []string{"pcs1.epoch-1.1." + strings.Repeat("a", 64) + "." + strings.Repeat("b", 64), "pcs1.epoch-1.-0000000000000000001." + strings.Repeat("a", 64) + "." + strings.Repeat("b", 64), "pcs1.epoch-1.00000000000000000001." + strings.Repeat("A", 64) + "." + strings.Repeat("b", 64)} {
		r, err := domain.NewCredentialSecretRef(ref)
		if err == nil {
			if _, err := parseProviderRef(r); err == nil {
				t.Fatal("noncanonical opaque reference admitted")
			}
		}
	}
}

func FuzzProviderCustodyEnvelope(f *testing.F) {
	f.Add([]byte("SLPCS001"))
	f.Add(make([]byte, 128))
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > providerEnvelopeMax+1 {
			return
		}
		_, a, _, _ := authenticateProviderObject(bytes.Repeat([]byte{1}, 32), "epoch-1", "invalid-key", raw)
		clear(a)
	})
}
