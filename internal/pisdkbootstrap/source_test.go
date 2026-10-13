package pisdkbootstrap

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func TestResourcesSourceHasStableDigestAndIndependentCopies(t *testing.T) {
	t.Parallel()
	first, digest := ResourcesSource()
	second, secondDigest := ResourcesSource()
	if len(first) == 0 || !bytes.Equal(first, second) || digest != secondDigest {
		t.Fatal("embedded helper must have nonempty stable bytes and digest")
	}
	sum := sha256.Sum256(first)
	if want := hex.EncodeToString(sum[:]); digest != want {
		t.Fatalf("helper digest got %q want %q", digest, want)
	}
	first[0] ^= 1
	third, thirdDigest := ResourcesSource()
	if !bytes.Equal(second, third) || thirdDigest != digest {
		t.Fatal("mutating caller-owned helper bytes changed embedded source")
	}
}
