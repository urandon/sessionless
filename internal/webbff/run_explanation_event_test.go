package webbff

import (
	"encoding/json"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessionapi"
)

func TestEventExplanationCorrelationCopiesOnlyCanonicalRun(t *testing.T) {
	id := domain.RunID("canonical-run")
	item := sessionapi.Event{Event: domain.SessionEvent{ID: "event", Kind: domain.SessionEventAssistantMessage, RunID: &id}, Payload: []byte(`{"summary":"loaded reply"}`)}
	projected := projectEvent(item)
	if projected.RunID == nil || *projected.RunID != id || projected.RunID == item.Event.RunID {
		t.Fatal("canonical correlation missing or aliased")
	}
	id = "mutated-source"
	if *projected.RunID != "canonical-run" {
		t.Fatal("projected correlation retained source pointer")
	}
	item.Event.RunID = nil
	item.Event.Kind = domain.SessionEventSystemNotice
	item.Payload = []byte(`{"schema":"notice.v1","code":"harness_failed","cancelled":true}`)
	projected = projectEvent(item)
	if projected.RunID != nil {
		t.Fatal("generic notice fabricated run correlation")
	}
	encoded, err := json.Marshal(projected)
	if err != nil || strings.Contains(string(encoded), `"run_id"`) || strings.Contains(string(encoded), `"reason_code"`) {
		t.Fatal("generic payload reclassified as canonical evidence")
	}
	invalid := domain.RunID("bad id")
	item.Event.RunID = &invalid
	if projectEvent(item).RunID != nil {
		t.Fatal("invalid source ID emitted")
	}
}
