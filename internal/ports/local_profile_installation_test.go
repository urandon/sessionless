package ports

import (
	"encoding/json"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestLocalPublicationGrantRejectsSerializedApproval(t *testing.T) {
	t.Parallel()
	g := OwnedProfilePublicationGrantV1{LocalInstallation: &OwnedLocalInstallationIntentV1{}}
	if _, err := json.Marshal(g); err == nil {
		t.Error("process-local local grant serialized")
	}
	if err := json.Unmarshal([]byte(`{"LocalInstallation":{"approved":true}}`), &g); err == nil {
		t.Error("caller bytes minted local publication grant")
	}
	if err := g.Validate(); err == nil {
		t.Error("local tag alone approved invalid profile")
	}
	if err := g.LocalInstallation.ValidateForProfile(domain.OwnedAgentProfileCurrentV1{}); err == nil {
		t.Error("empty local intent approved empty successor")
	}
}
