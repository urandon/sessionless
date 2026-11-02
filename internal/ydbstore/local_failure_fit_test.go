package ydbstore

import (
	"encoding/json"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestLocalFailureFitMatchedWireExactBoundaries(t *testing.T) {
	// Wire-length DATA only. This layout test manufactures no original entry,
	// issuer/observation, Source, filesystem, object or SQL authority.
	for _, content := range []string{"ordinary", "<>&\"\\\n\u2028", "кириллица", strings.Repeat("x", 2048)} {
		normalized, err := json.Marshal(struct {
			Content string `json:"content"`
		}{content})
		if err != nil {
			t.Fatal(err)
		}
		c := localFailureCapsuleV1{Schema: "sessionless.terminal-failure-custody.v1", Version: 1, CapturedAt: localTerminalSizeTime, Stop: domain.KnownMCPServiceErrorV1, Cleanup: "joined_retained", Operations: []localFailureOperationV1{{Normalized: json.RawMessage(`null`)}}}
		n, err := localFailureProspectiveCapsuleBytes(c, uint64(len(normalized)), domain.MaxTerminalCapsuleBytesV1)
		if err != nil {
			t.Fatal(err)
		}
		actual := c
		actual.Operations = append([]localFailureOperationV1(nil), c.Operations...)
		actual.Operations[0].Normalized = normalized
		body, err := encodeLocalFailureCapsule(actual, domain.MaxTerminalCapsuleBytesV1)
		if err != nil || uint64(len(body)) != n {
			t.Fatalf("matched serializer got=%d count=%d err=%v", len(body), n, err)
		}
		clear(body)
		if got, err := localFailureProspectiveCapsuleBytes(c, uint64(len(normalized)), n); err != nil || got != n {
			t.Fatal("exact fit denied", got, n, err)
		}
		if _, err := localFailureProspectiveCapsuleBytes(c, uint64(len(normalized)), n-1); err == nil {
			t.Fatal("one byte over admitted")
		}
		c.Operations[0].Normalized = json.RawMessage(`{}`)
		if _, err := localFailureProspectiveCapsuleBytes(c, uint64(len(normalized)), n); err == nil {
			t.Fatal("future body accepted instead of private count-only scalar")
		}
	}
}
