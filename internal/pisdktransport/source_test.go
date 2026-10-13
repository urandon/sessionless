package pisdktransport

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func TestFramingSourceHasStableDigestAndIndependentCopies(t *testing.T) {
	t.Parallel()
	checkModuleSource(t, FramingSource)
}

func TestResponseSourceHasStableDigestAndIndependentCopies(t *testing.T) {
	t.Parallel()
	checkModuleSource(t, ResponseSource)
}

func checkModuleSource(t *testing.T, source func() ([]byte, string)) {
	t.Helper()
	first, digest := source()
	second, secondDigest := source()
	if len(first) == 0 || !bytes.Equal(first, second) || digest != secondDigest {
		t.Fatal("embedded module must have nonempty stable bytes and digest")
	}
	sum := sha256.Sum256(first)
	if want := hex.EncodeToString(sum[:]); digest != want {
		t.Fatalf("module digest got %q want %q", digest, want)
	}
	first[0] ^= 1
	third, thirdDigest := source()
	if !bytes.Equal(second, third) || thirdDigest != digest {
		t.Fatal("mutating caller-owned module bytes changed embedded asset")
	}
}
