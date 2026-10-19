package domain_test

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestWebSearchResourceGrantNilProfilePredecessorGolden(t *testing.T) {
	p, _ := ownedCurrentProfileFixture(t)
	raw, err := domain.EncodeOwnedAgentProfileCurrentV1(p)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := p.Digest()
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(raw)
	if len(raw) != 7319 || hex.EncodeToString(sum[:]) != "e265cb93cd271207384b2a5a8180c9244faf3db9f19929d62d565a221989d28b" || digest != "0465440f0caeece6d3e7e431b6333f92d920b2263ea7199df56910f522dd457d" {
		t.Fatalf("observed predecessor nil-arm encoding changed: bytes=%d rawSHA=%x digest=%s", len(raw), sum, digest)
	}
}
