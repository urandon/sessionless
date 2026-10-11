package sessionmaterial

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func preparedEvent(t *testing.T, kind domain.SessionEventKind, body []byte) domain.SessionEvent {
	t.Helper()
	user, run := domain.UserID("private-author"), domain.RunID("private-run")
	hash := sha256.Sum256(body)
	event := domain.SessionEvent{
		ID: "private-event", TenantID: "private-tenant", SessionID: "private-session",
		Kind: kind, AuthorUserID: &user, RunID: &run, IdempotencyKey: "private-idempotency",
		Payload: domain.BlobRef{TenantID: "private-tenant", Key: "tenants/private-tenant/sessions/private-session/events/private-event/uploads/source/message.json", Size: int64(len(body)), SHA256: hex.EncodeToString(hash[:])},
	}
	if err := event.ValidateMaterialIdentity(); err != nil {
		t.Fatalf("source fixture: %v", err)
	}
	return event
}

func preparedFinal(event domain.SessionEvent) domain.SessionEvent {
	event.Sequence = 37
	event.CreatedAt = time.Date(2026, 10, 11, 22, 1, 2, 123456789, time.UTC)
	return event
}

func requirePrepared(t *testing.T, event domain.SessionEvent, body []byte, refs []domain.Artifact) *PreparedV1 {
	t.Helper()
	prepared, err := PrepareV1(event, body, refs)
	if err != nil || prepared == nil {
		t.Fatalf("PrepareV1: prepared=%v err=%v", prepared, err)
	}
	t.Cleanup(prepared.Release)
	return prepared
}

func requireNotPrepared(t *testing.T, event domain.SessionEvent, body []byte, refs []domain.Artifact) {
	t.Helper()
	got, err := PrepareV1(event, body, refs)
	if got != nil || !errors.Is(err, ErrNotPrepared) || err.Error() != "canonical material not prepared" {
		t.Fatalf("invalid source produced preparation=%v err=%v", got, err)
	}
}

func TestPreparedStableIdentityEveryFieldAndFinalAllocation(t *testing.T) {
	body := []byte(`{"version":1,"text":"private transcript"}`)
	event := preparedEvent(t, domain.SessionEventUserMessage, body)
	// Preparation is before allocation; neither provisional sequence nor time
	// becomes an authority over the canonical writer's final values.
	event.Sequence = math.MaxUint64
	event.CreatedAt = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	prepared := requirePrepared(t, event, body, nil)
	final := preparedFinal(event)
	copy, measures, err := prepared.CopyForFinalV1(final)
	if err != nil || !bytes.Equal(copy, body) || measures.PayloadBytes != uint64(len(body)) || !measures.CodexCompatible || measures.RenderedFrameBytes != uint64(len("[user]\nprivate transcript\n")) {
		t.Fatalf("final sequence/time not independently allocated or wrong literal measures: measures=%+v err=%v", measures, err)
	}
	clear(copy)
	cases := []struct {
		name   string
		change func(*domain.SessionEvent)
	}{
		{"event", func(e *domain.SessionEvent) { e.ID = "other-event" }},
		{"tenant", func(e *domain.SessionEvent) { e.TenantID = "other-tenant" }},
		{"session", func(e *domain.SessionEvent) { e.SessionID = "other-session" }},
		{"kind", func(e *domain.SessionEvent) { e.Kind = domain.SessionEventAssistantMessage }},
		{"author", func(e *domain.SessionEvent) { v := domain.UserID("other-author"); e.AuthorUserID = &v }},
		{"author absent", func(e *domain.SessionEvent) { e.AuthorUserID = nil }},
		{"run", func(e *domain.SessionEvent) { v := domain.RunID("other-run"); e.RunID = &v }},
		{"run absent", func(e *domain.SessionEvent) { e.RunID = nil }},
		{"idempotency", func(e *domain.SessionEvent) { e.IdempotencyKey = "other-idempotency" }},
		{"blob tenant", func(e *domain.SessionEvent) { e.Payload.TenantID = "other-tenant" }},
		{"blob key", func(e *domain.SessionEvent) { e.Payload.Key += ".other" }},
		{"blob size", func(e *domain.SessionEvent) { e.Payload.Size++ }},
		{"blob digest", func(e *domain.SessionEvent) { e.Payload.SHA256 = strings.Repeat("f", 64) }},
		{"unallocated sequence", func(e *domain.SessionEvent) { e.Sequence = 0 }},
		{"unallocated time", func(e *domain.SessionEvent) { e.CreatedAt = time.Time{} }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			changed := final
			test.change(&changed)
			got, m, err := prepared.CopyForFinalV1(changed)
			if got != nil || m != (MeasuresV1{}) || !errors.Is(err, ErrNotPrepared) {
				t.Fatalf("identity substitution returned bytes/measures: measures=%+v err=%v", m, err)
			}
		})
	}
}

func TestPreparedOwnsInputsCopiesAndExplicitCloneLifetime(t *testing.T) {
	body := []byte(`{"version":1,"text":"private transcript"}`)
	want := bytes.Clone(body)
	event := preparedEvent(t, domain.SessionEventUserMessage, body)
	final := preparedFinal(event)
	user, run := *event.AuthorUserID, *event.RunID
	final.AuthorUserID, final.RunID = &user, &run
	prepared := requirePrepared(t, event, body, nil)
	clone, err := prepared.Clone()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(clone.Release)
	alias := *prepared
	owned := prepared.state.payload
	clear(body)
	*event.AuthorUserID, *event.RunID = "mutated-author", "mutated-run"
	first, m, err := prepared.CopyForFinalV1(final)
	if err != nil || !bytes.Equal(first, want) {
		t.Fatalf("input bytes/pointers retained: %v", err)
	}
	clear(first)
	second, again, err := prepared.CopyForFinalV1(final)
	if err != nil || !bytes.Equal(second, want) || again != m {
		t.Fatalf("caller copy clear altered ownership: %v", err)
	}
	prepared.Release()
	prepared.Release()
	if !bytes.Equal(owned, make([]byte, len(owned))) || prepared.state.live || prepared.state.payload != nil || prepared.state.identity != (domain.SessionEvent{}) || prepared.state.measures != (MeasuresV1{}) {
		t.Fatal("release failed to clear owned bytes, identity and measures")
	}
	if !bytes.Equal(second, want) {
		t.Fatal("release changed previously returned caller-owned bytes")
	}
	clear(second)
	for _, released := range []*PreparedV1{prepared, &alias} {
		if p, err := released.Clone(); p != nil || !errors.Is(err, ErrNotPrepared) {
			t.Fatalf("released clone resurrected: %v", err)
		}
		if p, m, err := released.CopyForFinalV1(final); p != nil || m != (MeasuresV1{}) || !errors.Is(err, ErrNotPrepared) {
			t.Fatalf("released alias reused: %v", err)
		}
	}
	copy, _, err := clone.CopyForFinalV1(final)
	if err != nil || !bytes.Equal(copy, want) {
		t.Fatalf("independent clone lost lifetime: %v", err)
	}
	clear(copy)
}

func TestPreparedNilZeroPrivateFormattingAndConcurrentRelease(t *testing.T) {
	body := []byte(`{"version":1,"text":"private transcript"}`)
	final := preparedFinal(preparedEvent(t, domain.SessionEventUserMessage, body))
	var zero PreparedV1
	for _, value := range []*PreparedV1{nil, &zero} {
		value.Release()
		if copy, m, err := value.CopyForFinalV1(final); copy != nil || m != (MeasuresV1{}) || !errors.Is(err, ErrNotPrepared) {
			t.Fatalf("zero/nil evidence: %v", err)
		}
		if clone, err := value.Clone(); clone != nil || !errors.Is(err, ErrNotPrepared) {
			t.Fatalf("zero/nil clone: %v", err)
		}
	}
	prepared := requirePrepared(t, final, body, nil)
	for _, value := range []any{*prepared, prepared, zero} {
		if encoded, err := json.Marshal(value); encoded != nil || err == nil {
			t.Fatalf("private preparation serialized: %q %v", encoded, err)
		}
		for _, format := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%d"} {
			if got := fmt.Sprintf(format, value); got != "sessionmaterial.PreparedV1" {
				t.Fatalf("format=%s leaked private state: %s", format, got)
			}
		}
	}
	var group sync.WaitGroup
	start := make(chan struct{})
	for range 8 {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			for range 16 {
				copy, m, err := prepared.CopyForFinalV1(final)
				if err == nil {
					if !bytes.Equal(copy, body) || m.PayloadBytes != uint64(len(body)) {
						t.Error("concurrent copy returned partial material")
					}
					clear(copy)
				} else if !errors.Is(err, ErrNotPrepared) || copy != nil || m != (MeasuresV1{}) {
					t.Errorf("concurrent copy error=%v", err)
				}
			}
		}()
	}
	close(start)
	prepared.Release()
	group.Wait()
}

func attachmentFixture(t *testing.T, count int) (domain.SessionEvent, []byte, []domain.Artifact, []domain.Artifact) {
	t.Helper()
	descriptors := make([]domain.Artifact, count)
	refs := make([]domain.Artifact, count)
	for i := range descriptors {
		name := fmt.Sprintf("file-%02d.bin", i+1)
		descriptors[i] = domain.Artifact{Name: name, MediaType: "application/octet-stream", Blob: domain.BlobRef{TenantID: "private-tenant", Key: fmt.Sprintf("tenants/private-tenant/sessions/private-session/events/private-event/uploads/source/%02d.bin", i+1), Size: int64(i + 1), SHA256: strings.Repeat("a", 64)}}
		refs[i] = descriptors[i]
		refs[i].Name = fmt.Sprintf("attachment-%02d-%s", i+1, name)
	}
	event, body := attachmentEnvelope(t, descriptors)
	return event, body, refs, descriptors
}

func attachmentEnvelope(t *testing.T, descriptors []domain.Artifact) (domain.SessionEvent, []byte) {
	t.Helper()
	body, err := json.Marshal(struct {
		Version     uint32            `json:"version"`
		Text        string            `json:"text"`
		Attachments []domain.Artifact `json:"attachments"`
	}{Version: 1, Text: "private transcript", Attachments: descriptors})
	if err != nil {
		t.Fatal(err)
	}
	return preparedEvent(t, domain.SessionEventUserMessage, body), body
}

func TestPreparedExactOrderedIngressAttachmentsNoBodyClaim(t *testing.T) {
	event, body, refs, _ := attachmentFixture(t, 2)
	prepared := requirePrepared(t, event, body, refs)
	refs[0].Name, refs[0].Blob.Size = "caller-mutated", 999
	copy, m, err := prepared.CopyForFinalV1(preparedFinal(event))
	if err != nil || !bytes.Equal(copy, body) || m.AttachmentCount != 2 || m.ReferencedAttachmentBytes != 3 || m.CodexCompatible || m.RenderedFrameBytes != 0 {
		t.Fatalf("ordered referenced sizes invented body/render compatibility: %+v %v", m, err)
	}
	clear(copy)
	for _, test := range []struct {
		name   string
		change func(*domain.SessionEvent, *[]domain.Artifact)
	}{
		{"reordered", func(_ *domain.SessionEvent, r *[]domain.Artifact) { (*r)[0], (*r)[1] = (*r)[1], (*r)[0] }},
		{"unprefixed descriptor", func(_ *domain.SessionEvent, r *[]domain.Artifact) { (*r)[0].Name = "file-01.bin" }},
		{"wrong position", func(_ *domain.SessionEvent, r *[]domain.Artifact) { (*r)[0].Name = "attachment-02-file-01.bin" }},
		{"wrong name", func(_ *domain.SessionEvent, r *[]domain.Artifact) { (*r)[0].Name = "attachment-01-other.bin" }},
		{"media", func(_ *domain.SessionEvent, r *[]domain.Artifact) { (*r)[0].MediaType = "image/png" }},
		{"blob tenant", func(_ *domain.SessionEvent, r *[]domain.Artifact) { (*r)[0].Blob.TenantID = "other-tenant" }},
		{"blob key", func(_ *domain.SessionEvent, r *[]domain.Artifact) { (*r)[0].Blob.Key += ".other" }},
		{"blob size", func(_ *domain.SessionEvent, r *[]domain.Artifact) { (*r)[0].Blob.Size++ }},
		{"blob digest", func(_ *domain.SessionEvent, r *[]domain.Artifact) { (*r)[0].Blob.SHA256 = strings.Repeat("b", 64) }},
		{"missing", func(_ *domain.SessionEvent, r *[]domain.Artifact) { *r = (*r)[:1] }},
		{"message.json included", func(e *domain.SessionEvent, r *[]domain.Artifact) {
			*r = append([]domain.Artifact{{Name: "message.json", MediaType: "application/json", Blob: e.Payload}}, (*r)...)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			e, body, r, _ := attachmentFixture(t, 2)
			test.change(&e, &r)
			requireNotPrepared(t, e, body, r)
		})
	}
	// A zero referenced size is a valid scalar observation, not proof that an
	// attachment body was read, is harmless, or belongs to any media class.
	_, _, refs, descriptors := attachmentFixture(t, 1)
	refs[0].Blob.Size, descriptors[0].Blob.Size = 0, 0
	event, body = attachmentEnvelope(t, descriptors)
	prepared = requirePrepared(t, event, body, refs)
	copy, m, err = prepared.CopyForFinalV1(preparedFinal(event))
	clear(copy)
	if err != nil || m.AttachmentCount != 1 || m.ReferencedAttachmentBytes != 0 || m.CodexCompatible {
		t.Fatalf("zero reference conflated with no attachment: %+v %v", m, err)
	}
}

func TestPreparedAttachmentBoundsScopeAndOverflow(t *testing.T) {
	for _, count := range []int{63, 64, 65} {
		t.Run(fmt.Sprintf("count-%d", count), func(t *testing.T) {
			e, body, refs, _ := attachmentFixture(t, count)
			if count <= MaxAttachmentRefsV1 {
				requirePrepared(t, e, body, refs)
			} else {
				requireNotPrepared(t, e, body, refs)
			}
		})
	}
	for _, test := range []struct {
		name   string
		mutate func(*domain.Artifact)
	}{
		{"negative size", func(a *domain.Artifact) { a.Blob.Size = -1 }},
		{"foreign tenant", func(a *domain.Artifact) { a.Blob.TenantID = "foreign-tenant" }},
		{"foreign event", func(a *domain.Artifact) {
			a.Blob.Key = "tenants/private-tenant/sessions/private-session/events/other-event/body.bin"
		}},
		{"foreign session", func(a *domain.Artifact) {
			a.Blob.Key = "tenants/private-tenant/sessions/other-session/events/private-event/body.bin"
		}},
		{"unnormalized key", func(a *domain.Artifact) { a.Blob.Key += "/../other" }},
		{"empty media", func(a *domain.Artifact) { a.MediaType = "" }},
		{"path name", func(a *domain.Artifact) { a.Name = "../private.bin" }},
		{"nul name", func(a *domain.Artifact) { a.Name = "private\x00.bin" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, _, refs, descriptors := attachmentFixture(t, 1)
			test.mutate(&descriptors[0])
			refs[0] = descriptors[0]
			refs[0].Name = "attachment-01-" + descriptors[0].Name
			e, body := attachmentEnvelope(t, descriptors)
			requireNotPrepared(t, e, body, refs)
		})
	}
	for _, extra := range []int64{0, 1} {
		_, _, refs, descriptors := attachmentFixture(t, 2)
		refs[0].Blob.Size, descriptors[0].Blob.Size = math.MaxInt64-1, math.MaxInt64-1
		refs[1].Blob.Size, descriptors[1].Blob.Size = 1+extra, 1+extra
		e, body := attachmentEnvelope(t, descriptors)
		if extra == 0 {
			p := requirePrepared(t, e, body, refs)
			copy, m, err := p.CopyForFinalV1(preparedFinal(e))
			clear(copy)
			if err != nil || m.ReferencedAttachmentBytes != math.MaxInt64 {
				t.Fatalf("exact MaxInt64 referenced sum: %+v %v", m, err)
			}
		} else {
			requireNotPrepared(t, e, body, refs)
		}
	}
	for _, delta := range []int{-1, 0, 1} {
		_, _, refs, descriptors := attachmentFixture(t, 16)
		remaining := MaxAttachmentMetadataBytesV1 + delta
		for _, ref := range refs {
			remaining -= len(ref.Name) + len(ref.MediaType) + len(ref.Blob.TenantID) + len(ref.Blob.Key) + len(ref.Blob.SHA256)
		}
		for i := range refs {
			add := min(remaining, 4096-len(refs[i].MediaType))
			refs[i].MediaType += strings.Repeat("x", add)
			descriptors[i].MediaType = refs[i].MediaType
			remaining -= add
		}
		if remaining != 0 {
			t.Fatalf("metadata boundary fixture remaining=%d", remaining)
		}
		e, body := attachmentEnvelope(t, descriptors)
		if delta <= 0 {
			requirePrepared(t, e, body, refs)
		} else {
			requireNotPrepared(t, e, body, refs)
		}
	}
}

func TestPreparedIdentityPayloadAndSchemaBounds(t *testing.T) {
	for _, delta := range []int{-1, 0, 1} {
		base := `{"schema":"sessionless.assistant-message.v1","summary":"` + `"}`
		body := []byte(`{"schema":"sessionless.assistant-message.v1","summary":"` + strings.Repeat("x", MaxPayloadBytesV1+delta-len(base)) + `"}`)
		e := preparedEvent(t, domain.SessionEventAssistantMessage, body)
		if delta <= 0 {
			requirePrepared(t, e, body, nil)
		} else {
			requireNotPrepared(t, e, body, nil)
		}
	}
	body := []byte(`{"version":1,"text":"private transcript"}`)
	event := preparedEvent(t, domain.SessionEventUserMessage, body)
	for _, delta := range []int{-1, 0, 1} {
		e := event
		n := len(e.ID) + len(e.TenantID) + len(e.SessionID) + len(e.Kind) + len(e.IdempotencyKey) + len(e.Payload.TenantID) + len(e.Payload.Key) + len(e.Payload.SHA256) + len(*e.AuthorUserID) + len(*e.RunID)
		e.Payload.Key += strings.Repeat("x", MaxIdentityMetadataBytesV1+delta-n)
		if delta <= 0 {
			requirePrepared(t, e, body, nil)
		} else {
			requireNotPrepared(t, e, body, nil)
		}
	}
	for _, change := range []func(*domain.SessionEvent){
		func(e *domain.SessionEvent) { e.Payload.Size-- },
		func(e *domain.SessionEvent) { e.Payload.SHA256 = strings.Repeat("b", 64) },
		func(e *domain.SessionEvent) { e.Payload.Key = "tenants/private-tenant/runs/private-run/body.json" },
		func(e *domain.SessionEvent) { e.AuthorUserID = nil },
		func(e *domain.SessionEvent) { e.IdempotencyKey = "" },
	} {
		e := event
		change(&e)
		requireNotPrepared(t, e, body, nil)
	}
	for _, body := range [][]byte{nil, {}, []byte(`{`), []byte(`{"version":2,"text":"x"}`), []byte(`{"version":1,"text":"x","approved":true}`), []byte(`{"version":1,"text":"x"} {}`)} {
		e := preparedEvent(t, domain.SessionEventUserMessage, body)
		requireNotPrepared(t, e, body, nil)
	}
	for _, test := range []struct {
		name       string
		kind       domain.SessionEventKind
		body       string
		compatible bool
		frame      string
	}{
		{"user", domain.SessionEventUserMessage, `{"version":1,"text":"hi"}`, true, "[user]\nhi\n"},
		{"assistant", domain.SessionEventAssistantMessage, `{"schema":"sessionless.assistant-message.v1","summary":"done","artifact_manifest_id":"manifest"}`, true, "[assistant]\ndone\n"},
		{"call", domain.SessionEventToolCall, `{"schema":"sessionless.tool-event.v1","call_id":"call","tool_name":"tool","content":{"private":"value"}}`, false, ""},
		{"result", domain.SessionEventToolResult, `{"schema":"sessionless.tool-event.v1","call_id":"call","tool_name":"tool","content":null}`, false, ""},
		{"notice", domain.SessionEventSystemNotice, `{"schema":"sessionless.run-terminal-notice.v1","code":"failed","cancelled":false}`, false, ""},
		{"empty user rendering", domain.SessionEventUserMessage, `{"version":1,"text":""}`, false, ""},
		{"nul user rendering", domain.SessionEventUserMessage, `{"version":1,"text":"private\u0000text"}`, false, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			body := []byte(test.body)
			e := preparedEvent(t, test.kind, body)
			p := requirePrepared(t, e, body, nil)
			copy, m, err := p.CopyForFinalV1(preparedFinal(e))
			clear(copy)
			wantTool := uint64(0)
			if test.kind == domain.SessionEventToolCall || test.kind == domain.SessionEventToolResult {
				wantTool = uint64(len(body))
			}
			want := MeasuresV1{Kind: test.kind, PayloadBytes: uint64(len(body)), ToolEnvelopeBytes: wantTool, CodexCompatible: test.compatible, RenderedFrameBytes: uint64(len(test.frame))}
			if err != nil || !reflect.DeepEqual(m, want) {
				t.Fatalf("source schema != renderer compatibility: got=%+v want=%+v err=%v", m, want, err)
			}
			if test.kind != domain.SessionEventUserMessage {
				_, _, refs, _ := attachmentFixture(t, 1)
				requireNotPrepared(t, e, body, refs)
			}
		})
	}
}

func TestPreparedRecognizedSchemaFailuresAndDecoderParity(t *testing.T) {
	for _, test := range []struct {
		name string
		kind domain.SessionEventKind
		body string
	}{
		{"assistant schema", domain.SessionEventAssistantMessage, `{"schema":"sessionless.assistant-message.v2","summary":"done"}`},
		{"assistant unknown", domain.SessionEventAssistantMessage, `{"schema":"sessionless.assistant-message.v1","summary":"done","approved":true}`},
		{"tool schema", domain.SessionEventToolCall, `{"schema":"sessionless.tool-event.v2","call_id":"call","tool_name":"tool","content":{}}`},
		{"tool no call", domain.SessionEventToolCall, `{"schema":"sessionless.tool-event.v1","call_id":" ","tool_name":"tool","content":{}}`},
		{"tool no name", domain.SessionEventToolResult, `{"schema":"sessionless.tool-event.v1","call_id":"call","tool_name":" ","content":{}}`},
		{"tool no content", domain.SessionEventToolResult, `{"schema":"sessionless.tool-event.v1","call_id":"call","tool_name":"tool"}`},
		{"notice schema", domain.SessionEventSystemNotice, `{"schema":"sessionless.run-terminal-notice.v2","code":"failed"}`},
		{"notice no code", domain.SessionEventSystemNotice, `{"schema":"sessionless.run-terminal-notice.v1","code":" "}`},
		{"notice unknown", domain.SessionEventSystemNotice, `{"schema":"sessionless.run-terminal-notice.v1","code":"failed","approved":true}`},
		{"too many decoded attachments", domain.SessionEventUserMessage, `{"version":1,"text":"x","attachments":[` + strings.Repeat(`{},`, 64) + `{}]}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			body := []byte(test.body)
			e := preparedEvent(t, test.kind, body)
			requireNotPrepared(t, e, body, nil)
		})
	}
	// These are deliberate current encoding/json semantics, not compatibility
	// aliases. The constructor must not silently use a stricter renderer parser.
	for _, body := range []string{
		`{"VERSION":1,"TEXT":"hi"}`,
		`{"version":1,"text":"wrong","text":"hi"}`,
		`{"version":1,"text":"hi","attachments":null}`,
	} {
		payload := []byte(body)
		e := preparedEvent(t, domain.SessionEventUserMessage, payload)
		p := requirePrepared(t, e, payload, nil)
		copy, m, err := p.CopyForFinalV1(preparedFinal(e))
		clear(copy)
		if err != nil || !m.CodexCompatible || m.RenderedFrameBytes != uint64(len("[user]\nhi\n")) {
			t.Fatalf("canonical decoder semantics drifted for %s: %+v %v", body, m, err)
		}
	}
	for _, kind := range []domain.SessionEventKind{domain.SessionEventAssistantMessage, domain.SessionEventToolCall, domain.SessionEventToolResult} {
		body := []byte(`{"schema":"sessionless.assistant-message.v1","summary":"done"}`)
		e := preparedEvent(t, kind, body)
		e.RunID = nil
		requireNotPrepared(t, e, body, nil)
	}
}

func TestPreparedAttachmentIndividualStringCeilings(t *testing.T) {
	for _, axis := range []string{"descriptor name", "media", "key"} {
		for _, delta := range []int{-1, 0, 1} {
			t.Run(fmt.Sprintf("%s/%+d", axis, delta), func(t *testing.T) {
				_, _, refs, descriptors := attachmentFixture(t, 1)
				switch axis {
				case "descriptor name":
					descriptors[0].Name = strings.Repeat("a", 4072+delta)
				case "media":
					descriptors[0].MediaType = strings.Repeat("a", 4096+delta)
				case "key":
					descriptors[0].Blob.Key += strings.Repeat("a", 4096+delta-len(descriptors[0].Blob.Key))
				}
				refs[0] = descriptors[0]
				refs[0].Name = "attachment-01-" + descriptors[0].Name
				e, body := attachmentEnvelope(t, descriptors)
				if delta <= 0 {
					requirePrepared(t, e, body, refs)
				} else {
					requireNotPrepared(t, e, body, refs)
				}
			})
		}
	}
}
