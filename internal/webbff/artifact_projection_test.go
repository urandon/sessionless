package webbff

import (
	"encoding/json"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessionapi"
)

func TestProjectAssistantEventExposesOnlyArtifactSelectors(t *testing.T) {
	runID := domain.RunID("run-worker-output")
	payload, err := json.Marshal(map[string]string{
		"schema":               "sessionless.assistant-message.v1",
		"summary":              "finished",
		"artifact_manifest_id": "manifest-worker-output",
	})
	if err != nil {
		t.Fatal(err)
	}
	projected := projectEvent(sessionapi.Event{
		Event: domain.SessionEvent{
			ID: "event-assistant", TenantID: "tenant-a", SessionID: "session-a",
			Sequence: 2, Kind: domain.SessionEventAssistantMessage, RunID: &runID,
			IdempotencyKey: "assistant-event", Payload: domain.BlobRef{
				TenantID: "tenant-a", Key: "tenants/tenant-a/sessions/session-a/events/event-assistant/payload.json",
				Size: int64(len(payload)), SHA256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			},
			CreatedAt: time.Date(2026, 8, 18, 12, 0, 0, 0, time.UTC),
		},
		Payload: payload,
	})
	if projected.Content.Text != "finished" || projected.Content.Data != nil ||
		projected.Content.ArtifactManifest == nil ||
		projected.Content.ArtifactManifest.RunID != runID ||
		projected.Content.ArtifactManifest.ManifestID != "manifest-worker-output" {
		t.Fatalf("assistant projection = %+v", projected)
	}
}
