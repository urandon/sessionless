//go:build pi_sdk_functional

package pisdkmedia

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"testing"
)

// Test harnesses are not embedded in production runner assets. Select one
// exact test-owned fake sink; no wildcard directory or configurable loader.
func functionalNativeAssets(t *testing.T, name string) []nativeAsset {
	t.Helper()
	if name != "sdk.test.mjs" && name != "entry_fake_transport.mjs" {
		t.Fatal("unknown functional test asset")
	}
	body, err := os.ReadFile("native/" + name)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(body)
	return append(fixedNativeAssets(), nativeAsset{path: "internal/pisdkmedia/native/" + name, body: body, digest: hex.EncodeToString(digest[:])})
}
