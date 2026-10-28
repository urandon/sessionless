package attachedworkertransport

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/ports"
)

func TestResourceGenerationClientBearerValidationKeepsOriginalScope(t *testing.T) {
	t.Parallel()
	_, store, bearer, publication, _ := resourceGenerationServiceFixture(t)
	defer clear(bearer)
	original := bytes.Clone(bearer)
	defer clear(original)
	if err := ValidateResourceGenerationBearerV1(bearer, publication.Authorization); err != nil {
		t.Fatalf("original scoped bearer denied: %v", err)
	}
	if !bytes.Equal(bearer, original) || store.calls != 0 || store.reads != 0 || publication.Authorization.PresentedSecretDigest != "" {
		t.Fatal("validation mutated input or performed a store operation")
	}
	for _, tc := range []struct {
		name   string
		mutate func(*ports.AttachedResourceGenerationAuthorizationV1)
	}{
		{"tenant", func(v *ports.AttachedResourceGenerationAuthorizationV1) { v.TenantID = "foreign" }},
		{"owner", func(v *ports.AttachedResourceGenerationAuthorizationV1) { v.OwnerUserID = "foreign" }},
		{"worker", func(v *ports.AttachedResourceGenerationAuthorizationV1) { v.WorkerID = "foreign" }},
		{"connection", func(v *ports.AttachedResourceGenerationAuthorizationV1) { v.ConnectionID = "foreign" }},
		{"enrollment missing", func(v *ports.AttachedResourceGenerationAuthorizationV1) { v.EnrollmentGeneration = 0 }},
		{"baseline missing", func(v *ports.AttachedResourceGenerationAuthorizationV1) { v.BaselineRevision = 0 }},
		{"connection generation missing", func(v *ports.AttachedResourceGenerationAuthorizationV1) { v.ConnectionGeneration = 0 }},
		{"supplied digest", func(v *ports.AttachedResourceGenerationAuthorizationV1) {
			v.PresentedSecretDigest = "private-digest-marker"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			auth := publication.Authorization
			tc.mutate(&auth)
			err := ValidateResourceGenerationBearerV1(bearer, auth)
			if !errors.Is(err, ErrTransportUnauthorized) || strings.Contains(err.Error(), "private-digest-marker") || !bytes.Equal(bearer, original) {
				t.Fatalf("foreign/invalid metadata not safely denied: %v", err)
			}
		})
	}
	for _, raw := range [][]byte{nil, []byte("synthetic-malformed-secret-marker"), bytes.Repeat([]byte{'x'}, ConnectionBearerMaxBytes+1)} {
		err := ValidateResourceGenerationBearerV1(raw, publication.Authorization)
		if !errors.Is(err, ErrTransportUnauthorized) || strings.Contains(err.Error(), "synthetic-malformed-secret-marker") {
			t.Fatalf("invalid bearer not safely denied: %v", err)
		}
	}
}
