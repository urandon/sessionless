package pisdktransport

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func TestFramingSourceHasStableDigestAndIndependentCopies(t *testing.T) {
	t.Parallel()
	checkModuleSource(t, "framing.mjs", FramingSource)
}

func TestResponseSourceHasStableDigestAndIndependentCopies(t *testing.T) {
	t.Parallel()
	checkModuleSource(t, "response.mjs", ResponseSource)
}

func TestPolicySourceHasStableDigestAndIndependentCopies(t *testing.T) {
	t.Parallel()
	checkModuleSource(t, "policy.mjs", PolicySource)
}

func checkModuleSource(t *testing.T, path string, source func() ([]byte, string)) {
	t.Helper()
	wantBytes, err := modules.ReadFile(path)
	if err != nil {
		t.Fatalf("read expected embedded module %q: %v", path, err)
	}
	first, digest := source()
	second, secondDigest := source()
	if !bytes.Equal(first, wantBytes) {
		t.Fatalf("module accessor returned bytes other than its expected asset %q", path)
	}
	if len(first) == 0 || !bytes.Equal(first, second) || digest != secondDigest {
		t.Fatalf("embedded module %q must have nonempty stable bytes and digest", path)
	}
	sum := sha256.Sum256(first)
	if want := hex.EncodeToString(sum[:]); digest != want {
		t.Fatalf("module %q digest got %q want %q", path, digest, want)
	}
	first[0] ^= 1
	third, thirdDigest := source()
	if !bytes.Equal(second, third) || thirdDigest != digest {
		t.Fatalf("mutating caller-owned module %q bytes changed embedded asset", path)
	}
}
