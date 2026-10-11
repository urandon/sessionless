package sessionmaterial

import (
	"errors"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestValidateStoredPayloadExactResponseAndContentFreeFailure(t *testing.T) {
	// An unrecognized envelope is still verified against its Put response.
	body := []byte("opaque private bytes, not JSON")
	event := preparedEvent(t, domain.SessionEventUserMessage, body)
	if err := ValidateStoredPayloadV1(event.TenantID, event.Payload.Key, event.Payload, body); err != nil {
		t.Fatalf("exact response rejected: %v", err)
	}
	cases := []struct {
		name   string
		mutate func(*domain.BlobRef)
	}{
		{"tenant", func(r *domain.BlobRef) { r.TenantID = "different-tenant" }},
		{"key", func(r *domain.BlobRef) { r.Key += "-different" }},
		{"size", func(r *domain.BlobRef) { r.Size++ }},
		{"digest", func(r *domain.BlobRef) { r.SHA256 = strings.Repeat("f", 64) }},
		{"invalid reference", func(r *domain.BlobRef) { r.SHA256 = "" }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			ref := event.Payload
			test.mutate(&ref)
			err := ValidateStoredPayloadV1(event.TenantID, event.Payload.Key, ref, body)
			if !errors.Is(err, ErrStoredPayloadMismatch) || err.Error() != "canonical stored payload does not match uploaded material" {
				t.Fatalf("mismatch not rejected with content-free error: %v", err)
			}
		})
	}
	if err := ValidateStoredPayloadV1(event.TenantID, event.Payload.Key, event.Payload, append(body, '!')); !errors.Is(err, ErrStoredPayloadMismatch) {
		t.Fatalf("changed body not rejected: %v", err)
	}
}
