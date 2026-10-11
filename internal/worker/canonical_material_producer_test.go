package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
	"gitcode.com/urandon/sessionless/internal/sessionmaterial"
)

// This fixture never opens/deletes objects or executes a worker. Retaining the
// producer's bytes.Reader permits checking its temporary envelope cleanup, not
// inspecting private preparations or claiming remote object deletion.
type materialProducerPut struct {
	tenant domain.TenantID
	key    string
	body   []byte
	reader *bytes.Reader
}

type materialProducerBlobs struct {
	puts       []materialProducerPut
	failAt     int
	failure    error
	mutate     func(domain.BlobRef) domain.BlobRef
	otherCalls int
}

func (store *materialProducerBlobs) Put(_ context.Context, tenant domain.TenantID, key string, body io.Reader) (domain.BlobRef, error) {
	data, err := io.ReadAll(io.LimitReader(body, (3<<20)+1))
	if err != nil || len(data) > 3<<20 {
		return domain.BlobRef{}, errors.New("test upload exceeds bounded fixture")
	}
	reader, ok := body.(*bytes.Reader)
	if !ok {
		return domain.BlobRef{}, errors.New("test upload has no inspectable temporary buffer")
	}
	store.puts = append(store.puts, materialProducerPut{tenant: tenant, key: key, body: data, reader: reader})
	if store.failAt == len(store.puts) {
		return domain.BlobRef{}, store.failure
	}
	digest := sha256.Sum256(data)
	ref := domain.BlobRef{TenantID: tenant, Key: key, Size: int64(len(data)), SHA256: hex.EncodeToString(digest[:])}
	if store.mutate != nil {
		ref = store.mutate(ref)
	}
	return ref, nil
}

func (store *materialProducerBlobs) Open(context.Context, domain.TenantID, domain.BlobRef) (io.ReadCloser, error) {
	store.otherCalls++
	return nil, errors.New("producer must not open blobs")
}

func (store *materialProducerBlobs) Delete(context.Context, domain.TenantID, domain.BlobRef) error {
	store.otherCalls++
	return errors.New("producer must not delete blobs")
}

func materialProducerFixture(t *testing.T) (ports.WorkerJobState, time.Time) {
	t.Helper()
	at := time.Date(2026, 10, 11, 20, 17, 31, 123456789, time.UTC)
	loaded := ports.WorkerJobState{
		Run: domain.Run{
			ID: "run-material-producer", TenantID: "tenant-material-producer", SessionID: "session-material-producer",
			TriggerEventID: "event-trigger", SubscriptionConnectionID: "subscription-material-producer",
			Status: domain.RunQueued, IdempotencyKey: "run-material-key", CreatedAt: at, UpdatedAt: at,
		},
		Job: domain.WorkerJob{Limits: domain.ProductLimits{MaxToolEvents: 4, MaxToolEventBytes: 1 << 20}},
	}
	if err := loaded.Run.Validate(); err != nil {
		t.Fatalf("fixture run invalid: %v", err)
	}
	return loaded, at.Add(time.Second)
}

func materialProducerFinal(loaded ports.WorkerJobState, draft domain.SessionEventDraft, sequence uint64) domain.SessionEvent {
	runID := loaded.Run.ID
	return domain.SessionEvent{
		ID: draft.ID, TenantID: loaded.Run.TenantID, SessionID: loaded.Run.SessionID,
		Sequence: sequence, Kind: draft.Kind, RunID: &runID, IdempotencyKey: draft.IdempotencyKey,
		Payload: draft.Payload, CreatedAt: draft.CreatedAt,
	}
}

func materialProducerAssertCleared(t *testing.T, store *materialProducerBlobs) {
	t.Helper()
	for i, put := range store.puts {
		buffer := make([]byte, len(put.body))
		n, err := put.reader.ReadAt(buffer, 0)
		if err != nil || n != len(buffer) || !bytes.Equal(buffer, make([]byte, len(buffer))) {
			t.Errorf("upload %d temporary envelope not cleared: bytes=%d/%d error=%v", i+1, n, len(buffer), err)
		}
		clear(buffer)
	}
	if store.otherCalls != 0 {
		t.Errorf("producer made %d forbidden Open/Delete calls", store.otherCalls)
	}
}

func materialProducerAssertMeasure(t *testing.T, event domain.SessionEvent, prepared *sessionmaterial.PreparedV1, wantBody []byte, wantTool bool) sessioncontext.FinalMaterialMeasureV1 {
	t.Helper()
	payloadDigest := sha256.Sum256(wantBody)
	wantRef := domain.BlobRef{
		TenantID: event.TenantID,
		Key: domain.SessionEventObjectPrefix(event.TenantID, event.SessionID, event.ID) +
			"payloads/sha256/" + hex.EncodeToString(payloadDigest[:]) + ".json",
		Size: int64(len(wantBody)), SHA256: hex.EncodeToString(payloadDigest[:]),
	}
	if event.Payload != wantRef {
		t.Fatalf("event %s immutable uploaded source ref=%+v want=%+v", event.ID, event.Payload, wantRef)
	}
	body, source, err := prepared.CopyForFinalV1(event)
	if err != nil {
		t.Fatalf("event %s/%s final sequence %d preparation unavailable: %v", event.ID, event.Kind, event.Sequence, err)
	}
	defer clear(body)
	if !bytes.Equal(body, wantBody) || source.Kind != event.Kind || source.PayloadBytes != uint64(len(wantBody)) {
		t.Fatalf("event %s source differs from uploaded envelope: kind=%s bytes=%d want=%d equal=%t", event.ID, source.Kind, source.PayloadBytes, len(wantBody), bytes.Equal(body, wantBody))
	}
	wantToolBytes := uint64(0)
	if wantTool {
		wantToolBytes = uint64(len(wantBody))
	}
	if source.ToolEnvelopeBytes != wantToolBytes || source.AttachmentCount != 0 || source.ReferencedAttachmentBytes != 0 {
		t.Errorf("event %s source counters=%+v, want tool=%d and no attachments", event.ID, source, wantToolBytes)
	}
	measure, err := sessioncontext.MeasurePreparedRecordV1(event, prepared)
	if err != nil || !measure.Matches(event) {
		t.Fatalf("event %s actual final measure: matches=%t error=%v", event.ID, measure.Matches(event), err)
	}
	encoded, err := sessioncontext.EncodeRecord(event, wantBody)
	if err != nil {
		t.Fatalf("event %s independent actual codec: %v", event.ID, err)
	}
	defer clear(encoded)
	sum := sha256.Sum256(encoded)
	record, verified := measure.Record()
	gotDigest, digestVerified := record.Digest()
	gotBytes, bytesVerified := record.EncodedBytes()
	if !verified || !digestVerified || !bytesVerified || gotDigest != hex.EncodeToString(sum[:]) || gotBytes != int64(len(encoded)) || encoded[len(encoded)-1] != '\n' {
		t.Errorf("event %s final codec measurement differs: verified=%t/%t/%t bytes=%d want=%d digest=%s want=%x", event.ID, verified, digestVerified, bytesVerified, gotBytes, len(encoded), gotDigest, sum)
	}
	return measure
}

func TestCanonicalMaterialProducerCompletionExactSourcesAndRelease(t *testing.T) {
	loaded, at := materialProducerFixture(t)
	store := &materialProducerBlobs{}
	manager := &Manager{blobs: store}
	result := ports.ExecutionResult{Summary: "  private final answer  ", ToolEvents: []ports.ExecutionToolEvent{
		{Kind: domain.SessionEventToolCall, CallID: "call-private", ToolName: "lookup", Payload: []byte(`{"argument":"private input"}`)},
		{Kind: domain.SessionEventToolResult, CallID: "call-private", ToolName: "lookup", Payload: []byte(`{"result":"private output"}`)},
	}}
	manifest := domain.ArtifactManifest{ID: "manifest-material-producer"}
	events, materials, err := manager.canonicalCompletionEvents(t.Context(), loaded, result, manifest, at)
	if err != nil || len(events) != 3 || materials == nil || len(store.puts) != 3 {
		t.Fatalf("completion events=%d material=%t uploads=%d error=%v", len(events), materials != nil, len(store.puts), err)
	}
	t.Cleanup(materials.Release)
	materialProducerAssertCleared(t, store)
	wantBodies := []string{
		`{"schema":"sessionless.tool-event.v1","call_id":"call-private","tool_name":"lookup","content":{"argument":"private input"}}`,
		`{"schema":"sessionless.tool-event.v1","call_id":"call-private","tool_name":"lookup","content":{"result":"private output"}}`,
		`{"schema":"sessionless.assistant-message.v1","summary":"private final answer","artifact_manifest_id":"manifest-material-producer"}`,
	}
	aliases := make([]*sessionmaterial.PreparedV1, len(events))
	for i, draft := range events {
		if err := draft.ValidateForRun(loaded.Run); err != nil || !draft.CreatedAt.Equal(at) {
			t.Fatalf("draft %d final metadata error=%v at=%v want=%v", i, err, draft.CreatedAt, at)
		}
		if string(store.puts[i].body) != wantBodies[i] {
			t.Errorf("draft %d envelope got=%q want=%q", i, store.puts[i].body, wantBodies[i])
		}
		aliases[i] = materials.ForEvent(draft.ID)
		event := materialProducerFinal(loaded, draft, uint64(41+i))
		measure := materialProducerAssertMeasure(t, event, aliases[i], store.puts[i].body, i < 2)
		source, verified := measure.SourceMeasures()
		if !verified || source.CodexCompatible != (i == 2) || (source.RenderedFrameBytes > 0) != (i == 2) {
			t.Errorf("draft %d renderer source=%+v verified=%t", i, source, verified)
		}
	}
	assistant := events[2]
	if assistant.DisplayText != "private final answer" {
		t.Errorf("assistant display=%q", assistant.DisplayText)
	}
	// Display text is neither the payload nor an input to the final source
	// identity. Changing it cannot substitute different canonical bytes.
	assistant.DisplayText = "forged display shortcut"
	final := materialProducerFinal(loaded, assistant, 43)
	clone, err := aliases[2].Clone()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(clone.Release)
	measure := materialProducerAssertMeasure(t, final, aliases[2], store.puts[2].body, false)
	for _, mutation := range []struct {
		name   string
		change func(*domain.SessionEvent)
	}{
		{"event", func(event *domain.SessionEvent) { event.ID = "event-other-source" }},
		{"tenant", func(event *domain.SessionEvent) { event.TenantID = "tenant-other-source" }},
		{"session", func(event *domain.SessionEvent) { event.SessionID = "session-other-source" }},
		{"kind", func(event *domain.SessionEvent) { event.Kind = domain.SessionEventSystemNotice }},
		{"run", func(event *domain.SessionEvent) { runID := domain.RunID("run-other-source"); event.RunID = &runID }},
		{"author", func(event *domain.SessionEvent) {
			author := domain.UserID("user-other-source")
			event.AuthorUserID = &author
		}},
		{"idempotency", func(event *domain.SessionEvent) { event.IdempotencyKey = "key-other-source" }},
		{"blob_key", func(event *domain.SessionEvent) { event.Payload.Key += ".other" }},
		{"blob_hash", func(event *domain.SessionEvent) { event.Payload.SHA256 = strings.Repeat("0", 64) }},
		{"blob_size", func(event *domain.SessionEvent) { event.Payload.Size++ }},
	} {
		changed := final
		mutation.change(&changed)
		if _, err := sessioncontext.MeasurePreparedRecordV1(changed, aliases[2]); !errors.Is(err, sessioncontext.ErrRecordNotMeasurable) {
			t.Errorf("changed final %s source accepted: %v", mutation.name, err)
		}
	}
	// Sequence and timestamp belong to the final transaction, not provisional
	// preparation. A fresh measure binds both without reusing the old digest.
	postClamp := final
	postClamp.Sequence = 81
	postClamp.CreatedAt = final.CreatedAt.Add(time.Second)
	postMeasure := materialProducerAssertMeasure(t, postClamp, aliases[2], store.puts[2].body, false)
	if measure.Matches(postClamp) || postMeasure.Matches(final) {
		t.Error("final sequence/timestamp changed without invalidating the earlier measure")
	}
	setAlias := *materials
	materials.Release()
	materials.Release()
	for i, prepared := range aliases {
		if _, live := prepared.OwnedBytesV1(); live || setAlias.ForEvent(events[i].ID) != nil {
			t.Errorf("released event %d retains ownership", i)
		}
		if _, _, err := prepared.CopyForFinalV1(materialProducerFinal(loaded, events[i], uint64(41+i))); !errors.Is(err, sessionmaterial.ErrNotPrepared) {
			t.Errorf("released event %d still copies: %v", i, err)
		}
	}
	materialProducerAssertMeasure(t, final, clone, store.puts[2].body, false)
	if !measure.Matches(final) {
		t.Error("already sealed payload-free final measure lost identity on source release")
	}
}

func TestCanonicalMaterialProducerFailureNoticeExactSources(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancelled_%t", cancelled), func(t *testing.T) {
			loaded, at := materialProducerFixture(t)
			store := &materialProducerBlobs{}
			manager := &Manager{blobs: store}
			events, materials, err := manager.canonicalFailureEvents(t.Context(), loaded, cancelled, "bounded_failure", at)
			if err != nil || len(events) != 1 || materials == nil || len(store.puts) != 1 {
				t.Fatalf("notice events=%d materials=%t uploads=%d error=%v", len(events), materials != nil, len(store.puts), err)
			}
			t.Cleanup(materials.Release)
			materialProducerAssertCleared(t, store)
			want := fmt.Sprintf(`{"schema":"sessionless.run-terminal-notice.v1","code":"bounded_failure","cancelled":%t}`, cancelled)
			if string(store.puts[0].body) != want || events[0].Kind != domain.SessionEventSystemNotice || events[0].DisplayText != "" {
				t.Fatalf("notice kind=%s display=%q payload=%q want=%q", events[0].Kind, events[0].DisplayText, store.puts[0].body, want)
			}
			prepared := materials.ForEvent(events[0].ID)
			final := materialProducerFinal(loaded, events[0], 57)
			measure := materialProducerAssertMeasure(t, final, prepared, store.puts[0].body, false)
			source, _ := measure.SourceMeasures()
			if source.CodexCompatible || source.RenderedFrameBytes != 0 {
				t.Errorf("notice gained unsupported renderer compatibility: %+v", source)
			}
			materials.Release()
			if _, live := prepared.OwnedBytesV1(); live {
				t.Error("notice preparation remains live after release")
			}
		})
	}
}

func TestCanonicalMaterialProducerStoredResponseMismatch(t *testing.T) {
	mutations := []struct {
		name   string
		mutate func(domain.BlobRef) domain.BlobRef
	}{
		{"tenant", func(ref domain.BlobRef) domain.BlobRef { ref.TenantID = "tenant-foreign"; return ref }},
		{"key", func(ref domain.BlobRef) domain.BlobRef { ref.Key += ".other"; return ref }},
		{"hash", func(ref domain.BlobRef) domain.BlobRef { ref.SHA256 = strings.Repeat("0", 64); return ref }},
		{"size", func(ref domain.BlobRef) domain.BlobRef { ref.Size++; return ref }},
	}
	for _, test := range mutations {
		t.Run(test.name, func(t *testing.T) {
			loaded, at := materialProducerFixture(t)
			store := &materialProducerBlobs{mutate: test.mutate}
			manager := &Manager{blobs: store}
			payload := []byte(`{"schema":"sessionless.run-terminal-notice.v1","code":"failed","cancelled":false}`)
			draft, prepared, err := manager.putCanonicalEventDraft(t.Context(), loaded, "event-put-mismatch", domain.SessionEventSystemNotice, payload, at)
			if !errors.Is(err, sessionmaterial.ErrStoredPayloadMismatch) || draft != (domain.SessionEventDraft{}) || prepared != nil || len(store.puts) != 1 {
				t.Errorf("mismatched Put response draft=%+v prepared=%t uploads=%d error=%v", draft, prepared != nil, len(store.puts), err)
			}
		})
	}
}

func TestCanonicalMaterialProducerPutCopiesCallerAndRetainsUnknown(t *testing.T) {
	loaded, at := materialProducerFixture(t)
	for _, test := range []struct {
		name  string
		body  string
		known bool
	}{
		{"known", `{"schema":"sessionless.assistant-message.v1","summary":"private","artifact_manifest_id":"manifest"}`, true},
		{"unrecognized_v2", `{"schema":"sessionless.assistant-message.v2","summary":"private"}`, false},
		{"unrecognized_fields", `{"schema":"sessionless.assistant-message.v1","summary":"private","extra":"not certified"}`, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := &materialProducerBlobs{}
			manager := &Manager{blobs: store}
			body := []byte(test.body)
			draft, prepared, err := manager.putCanonicalEventDraft(t.Context(), loaded, "event-direct-put", domain.SessionEventAssistantMessage, body, at)
			if err != nil || draft.ValidateForRun(loaded.Run) != nil || len(store.puts) != 1 {
				t.Fatalf("canonical otherwise valid source forbidden: draft=%+v uploads=%d error=%v", draft, len(store.puts), err)
			}
			t.Cleanup(prepared.Release)
			if string(body) != test.body {
				t.Error("direct producer unexpectedly cleared caller-owned payload")
			}
			clear(body)
			if test.known {
				materialProducerAssertMeasure(t, materialProducerFinal(loaded, draft, 99), prepared, store.puts[0].body, false)
			} else if prepared != nil {
				t.Error("unrecognized envelope acquired a verified preparation")
			}
		})
	}
}

func TestCanonicalMaterialProducerPartialBatchErrorsDoNotEscape(t *testing.T) {
	for _, failAt := range []int{1, 2, 3} {
		t.Run(fmt.Sprintf("upload_%d", failAt), func(t *testing.T) {
			loaded, at := materialProducerFixture(t)
			failure := errors.New("injected bounded upload failure")
			store := &materialProducerBlobs{failAt: failAt, failure: failure}
			manager := &Manager{blobs: store}
			result := ports.ExecutionResult{Summary: "answer", ToolEvents: []ports.ExecutionToolEvent{
				{Kind: domain.SessionEventToolCall, CallID: "call", ToolName: "tool", Payload: []byte(`{"x":1}`)},
				{Kind: domain.SessionEventToolResult, CallID: "call", ToolName: "tool", Payload: []byte(`{"x":2}`)},
			}}
			events, materials, err := manager.canonicalCompletionEvents(t.Context(), loaded, result, domain.ArtifactManifest{ID: "manifest"}, at)
			if !errors.Is(err, failure) || events != nil || materials != nil || len(store.puts) != failAt {
				t.Errorf("partial completion escaped: drafts=%d materials=%t uploads=%d want=%d error=%v", len(events), materials != nil, len(store.puts), failAt, err)
			}
			// Earlier prepared allocations are private and no alias escapes this
			// error API; this checks observable batch/envelope cleanup only.
			materialProducerAssertCleared(t, store)
			if string(result.ToolEvents[0].Payload) != `{"x":1}` || string(result.ToolEvents[1].Payload) != `{"x":2}` {
				t.Error("error cleanup cleared harness-owned source payloads")
			}
		})
	}
	t.Run("notice", func(t *testing.T) {
		loaded, at := materialProducerFixture(t)
		failure := errors.New("injected notice upload failure")
		store := &materialProducerBlobs{failAt: 1, failure: failure}
		events, materials, err := (&Manager{blobs: store}).canonicalFailureEvents(t.Context(), loaded, false, "failed", at)
		if !errors.Is(err, failure) || events != nil || materials != nil || len(store.puts) != 1 {
			t.Fatalf("notice failure returned partial ownership: events=%d materials=%t error=%v", len(events), materials != nil, err)
		}
		materialProducerAssertCleared(t, store)
	})
}

func TestCanonicalMaterialProducerAdmittedLimitsBeforeUpload(t *testing.T) {
	tool := ports.ExecutionToolEvent{Kind: domain.SessionEventToolCall, CallID: "c", ToolName: "t", Payload: []byte(`{"x":1}`)}
	// Independent literal envelope, not the production marshaler's size.
	const encoded = `{"schema":"sessionless.tool-event.v1","call_id":"c","tool_name":"t","content":{"x":1}}`
	for _, test := range []struct {
		name     string
		events   []ports.ExecutionToolEvent
		limits   domain.ProductLimits
		accepted bool
	}{
		{"count_exact", []ports.ExecutionToolEvent{tool}, domain.ProductLimits{MaxToolEvents: 1, MaxToolEventBytes: uint64(len(encoded))}, true},
		{"count_over", []ports.ExecutionToolEvent{tool, tool}, domain.ProductLimits{MaxToolEvents: 1, MaxToolEventBytes: 4096}, false},
		{"raw_over", []ports.ExecutionToolEvent{tool}, domain.ProductLimits{MaxToolEvents: 1, MaxToolEventBytes: uint64(len(tool.Payload) - 1)}, false},
		{"encoded_over", []ports.ExecutionToolEvent{tool}, domain.ProductLimits{MaxToolEvents: 1, MaxToolEventBytes: uint64(len(encoded) - 1)}, false},
		{"aggregate_exact", []ports.ExecutionToolEvent{tool, tool}, domain.ProductLimits{MaxToolEvents: 2, MaxToolEventBytes: 2 * uint64(len(encoded))}, true},
		{"aggregate_over", []ports.ExecutionToolEvent{tool, tool}, domain.ProductLimits{MaxToolEvents: 2, MaxToolEventBytes: 2*uint64(len(encoded)) - 1}, false},
		{"legacy_finite_exact", []ports.ExecutionToolEvent{tool, tool}, domain.ProductLimits{MaxTurns: 1, MaxContextBytes: 2 * uint64(len(encoded))}, true},
		{"legacy_count_over", []ports.ExecutionToolEvent{tool, tool, tool}, domain.ProductLimits{MaxTurns: 1, MaxContextBytes: 4096}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			loaded, at := materialProducerFixture(t)
			loaded.Job.Limits = test.limits
			store := &materialProducerBlobs{}
			events, materials, err := (&Manager{blobs: store}).canonicalCompletionEvents(t.Context(), loaded, ports.ExecutionResult{ToolEvents: test.events}, domain.ArtifactManifest{ID: "manifest"}, at)
			if test.accepted {
				if err != nil || len(events) != len(test.events)+1 || materials == nil || len(store.puts) != len(events) {
					t.Fatalf("admitted boundary refused: events=%d materials=%t uploads=%d error=%v", len(events), materials != nil, len(store.puts), err)
				}
				t.Cleanup(materials.Release)
				if string(store.puts[0].body) != encoded {
					t.Errorf("fixture envelope differs: got=%q want=%q", store.puts[0].body, encoded)
				}
				materialProducerAssertCleared(t, store)
			} else {
				var validation domain.ValidationError
				if !errors.As(err, &validation) || events != nil || materials != nil || len(store.puts) != 0 {
					t.Errorf("over-budget source was uploaded: events=%d materials=%t uploads=%d error=%v", len(events), materials != nil, len(store.puts), err)
				}
			}
		})
	}
}

func TestCanonicalMaterialProducerValidatesWholeToolBatchBeforeUpload(t *testing.T) {
	valid := ports.ExecutionToolEvent{Kind: domain.SessionEventToolCall, CallID: "call", ToolName: "tool", Payload: []byte(`{"ok":true}`)}
	for _, test := range []struct {
		name   string
		change func(*ports.ExecutionToolEvent)
	}{
		{"kind", func(tool *ports.ExecutionToolEvent) { tool.Kind = domain.SessionEventAssistantMessage }},
		{"call", func(tool *ports.ExecutionToolEvent) { tool.CallID = " " }},
		{"tool", func(tool *ports.ExecutionToolEvent) { tool.ToolName = " " }},
		{"json", func(tool *ports.ExecutionToolEvent) { tool.Payload = []byte(`{"broken":`) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			loaded, at := materialProducerFixture(t)
			invalid := valid
			test.change(&invalid)
			store := &materialProducerBlobs{}
			events, materials, err := (&Manager{blobs: store}).canonicalCompletionEvents(t.Context(), loaded, ports.ExecutionResult{ToolEvents: []ports.ExecutionToolEvent{valid, invalid}}, domain.ArtifactManifest{ID: "manifest"}, at)
			if err == nil || events != nil || materials != nil || len(store.puts) != 0 {
				t.Errorf("invalid late tool caused partial upload: events=%d materials=%t uploads=%d error=%v", len(events), materials != nil, len(store.puts), err)
			}
		})
	}
}

func TestCanonicalMaterialProducerInspectionBoundsDoNotBanCanonicalEvents(t *testing.T) {
	t.Run("preparation_count", func(t *testing.T) {
		loaded, at := materialProducerFixture(t)
		loaded.Job.Limits.MaxToolEvents = ports.MaxCanonicalPreparedEventsV1 + 1
		tools := make([]ports.ExecutionToolEvent, loaded.Job.Limits.MaxToolEvents)
		for i := range tools {
			tools[i] = ports.ExecutionToolEvent{Kind: domain.SessionEventToolCall, CallID: fmt.Sprintf("call-%d", i), ToolName: "tool", Payload: []byte(`{}`)}
		}
		store := &materialProducerBlobs{}
		events, materials, err := (&Manager{blobs: store}).canonicalCompletionEvents(t.Context(), loaded, ports.ExecutionResult{ToolEvents: tools}, domain.ArtifactManifest{ID: "manifest"}, at)
		if err != nil || len(events) != len(tools)+1 || materials == nil {
			t.Fatalf("inspection cap became write prohibition: events=%d materials=%t error=%v", len(events), materials != nil, err)
		}
		t.Cleanup(materials.Release)
		for i, event := range events {
			if (materials.ForEvent(event.ID) != nil) != (i < ports.MaxCanonicalPreparedEventsV1) {
				t.Errorf("event %d preparation presence differs from finite inspection cap", i)
			}
		}
		materialProducerAssertCleared(t, store)
	})
	t.Run("preparation_aggregate_bytes", func(t *testing.T) {
		loaded, at := materialProducerFixture(t)
		loaded.Job.Limits.MaxToolEvents = 6
		loaded.Job.Limits.MaxToolEventBytes = 8 << 20
		payload, err := json.Marshal(strings.Repeat("x", 700<<10))
		if err != nil {
			t.Fatal(err)
		}
		tools := make([]ports.ExecutionToolEvent, 6)
		for i := range tools {
			tools[i] = ports.ExecutionToolEvent{Kind: domain.SessionEventToolResult, CallID: fmt.Sprintf("call-%d", i), ToolName: "tool", Payload: payload}
		}
		store := &materialProducerBlobs{}
		events, materials, err := (&Manager{blobs: store}).canonicalCompletionEvents(t.Context(), loaded, ports.ExecutionResult{ToolEvents: tools}, domain.ArtifactManifest{ID: "manifest"}, at)
		if err != nil || len(events) != 7 || materials == nil || len(store.puts) != 7 {
			t.Fatalf("inspection byte cap became canonical write prohibition: events=%d materials=%t uploads=%d error=%v", len(events), materials != nil, len(store.puts), err)
		}
		t.Cleanup(materials.Release)
		var owned uint64
		var unknown int
		for i, draft := range events {
			size := uint64(len(store.puts[i].body))
			wantPresent := size <= ports.MaxCanonicalPreparedBytesV1-owned
			prepared := materials.ForEvent(draft.ID)
			if (prepared != nil) != wantPresent {
				t.Errorf("event %d preparation presence=%t want=%t prior owned=%d uploaded bytes=%d", i, prepared != nil, wantPresent, owned, size)
			}
			if wantPresent {
				owned += size
			} else {
				unknown++
			}
		}
		if unknown != 1 || materials.ForEvent(events[5].ID) != nil || materials.ForEvent(events[6].ID) == nil {
			t.Errorf("aggregate inspection limit: unknown=%d want one oversized aggregate tool and still-inspectable small assistant", unknown)
		}
		materialProducerAssertCleared(t, store)
	})
	t.Run("preparation_payload", func(t *testing.T) {
		loaded, at := materialProducerFixture(t)
		loaded.Job.Limits.MaxToolEventBytes = 2 << 20
		payload, err := json.Marshal(strings.Repeat("x", sessionmaterial.MaxPayloadBytesV1))
		if err != nil {
			t.Fatal(err)
		}
		store := &materialProducerBlobs{}
		events, materials, err := (&Manager{blobs: store}).canonicalCompletionEvents(t.Context(), loaded, ports.ExecutionResult{ToolEvents: []ports.ExecutionToolEvent{
			{Kind: domain.SessionEventToolResult, CallID: "call", ToolName: "tool", Payload: payload},
		}}, domain.ArtifactManifest{ID: "manifest"}, at)
		if err != nil || len(events) != 2 || materials == nil || materials.ForEvent(events[0].ID) != nil || materials.ForEvent(events[1].ID) == nil {
			t.Fatalf("uninspectable admitted tool is not canonical unknown: events=%d materials=%t error=%v", len(events), materials != nil, err)
		}
		t.Cleanup(materials.Release)
		var envelope struct {
			Content json.RawMessage `json:"content"`
		}
		if err := json.Unmarshal(store.puts[0].body, &envelope); err != nil || events[0].ValidateForRun(loaded.Run) != nil || !bytes.Equal(payload, envelope.Content) {
			t.Errorf("admitted oversized source payload changed: envelope error=%v", err)
		}
		materialProducerAssertCleared(t, store)
	})
}
