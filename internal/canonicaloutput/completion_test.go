package canonicaloutput_test

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

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func outputResult() ports.ExecutionResult {
	return ports.ExecutionResult{Summary: "  visible <answer> & finish\n", ToolEvents: []ports.ExecutionToolEvent{
		{Kind: domain.SessionEventToolCall, CallID: "c1", ToolName: "lookup", Payload: []byte(`{"q":"<x>"}`)},
		{Kind: domain.SessionEventToolResult, CallID: "c1", ToolName: "lookup", Payload: []byte(`{"ok":true}`)},
	}}
}

func TestCompletionEncodingOriginalWireAndOwnedRelease(t *testing.T) {
	result := outputResult()
	plan, err := canonicaloutput.EncodeCompletionV1("run-output", domain.ProductLimits{MaxToolEvents: 2, MaxToolEventBytes: 4096}, result, "manifest-output")
	if err != nil || plan == nil || len(plan.Events()) != 3 {
		t.Fatalf("complete pure plan: events=%v err=%v", plan, err)
	}
	t.Cleanup(plan.Release)
	wantIDs := []domain.SessionEventID{"evt_830ab4fb440fe1897c22cfe2ba01ed2b", "evt_b459c6df131d14cc9fcda449f283ea21", "evt_62cac09f06e774106b66eb14fcab7650"}
	wantBodies := []string{
		`{"schema":"sessionless.tool-event.v1","call_id":"c1","tool_name":"lookup","content":{"q":"\u003cx\u003e"}}`,
		`{"schema":"sessionless.tool-event.v1","call_id":"c1","tool_name":"lookup","content":{"ok":true}}`,
		`{"schema":"sessionless.assistant-message.v1","summary":"visible \u003canswer\u003e \u0026 finish","artifact_manifest_id":"manifest-output"}`,
	}
	wantKinds := []domain.SessionEventKind{domain.SessionEventToolCall, domain.SessionEventToolResult, domain.SessionEventAssistantMessage}
	aliases := make([][]byte, 0, 3)
	for i, event := range plan.Events() {
		if event.ID != wantIDs[i] || event.Kind != wantKinds[i] || string(event.Payload) != wantBodies[i] {
			t.Errorf("event %d original wire/identity changed: id=%s kind=%s payload=%s", i, event.ID, event.Kind, event.Payload)
		}
		if (i == 2 && event.DisplayText != "visible <answer> & finish") || (i < 2 && event.DisplayText != "") {
			t.Errorf("event %d display changed", i)
		}
		aliases = append(aliases, event.Payload)
		if strings.Contains(fmt.Sprintf("%+v %#v", event, event), "visible") {
			t.Error("event formatting exposed answer content")
		}
	}
	if _, err := json.Marshal(plan); err == nil {
		t.Error("transient encoding plan serialized")
	}
	plan.Release()
	plan.Release()
	if len(plan.Events()) != 0 {
		t.Error("released plan retained event entries")
	}
	for i, payload := range aliases {
		if !bytes.Equal(payload, make([]byte, len(payload))) {
			t.Errorf("owned payload %d not cleared", i)
		}
	}
	if string(result.ToolEvents[0].Payload) != `{"q":"<x>"}` || result.Summary != "  visible <answer> & finish\n" {
		t.Error("plan release cleared caller-owned source")
	}
}

func TestCompletionEncodingWholeBatchAndExactLimits(t *testing.T) {
	const envelope = `{"schema":"sessionless.tool-event.v1","call_id":"c","tool_name":"t","content":{"x":1}}`
	tool := ports.ExecutionToolEvent{Kind: domain.SessionEventToolCall, CallID: "c", ToolName: "t", Payload: []byte(`{"x":1}`)}
	for _, tc := range []struct {
		name   string
		tools  []ports.ExecutionToolEvent
		limits domain.ProductLimits
		valid  bool
	}{
		{"exact", []ports.ExecutionToolEvent{tool}, domain.ProductLimits{MaxToolEvents: 1, MaxToolEventBytes: uint64(len(envelope))}, true},
		{"first_encoded_over", []ports.ExecutionToolEvent{tool}, domain.ProductLimits{MaxToolEvents: 1, MaxToolEventBytes: uint64(len(envelope) - 1)}, false},
		{"raw_over", []ports.ExecutionToolEvent{tool}, domain.ProductLimits{MaxToolEvents: 1, MaxToolEventBytes: uint64(len(tool.Payload) - 1)}, false},
		{"count_over", []ports.ExecutionToolEvent{tool, tool}, domain.ProductLimits{MaxToolEvents: 1, MaxToolEventBytes: 4096}, false},
		{"aggregate_exact", []ports.ExecutionToolEvent{tool, tool}, domain.ProductLimits{MaxToolEvents: 2, MaxToolEventBytes: 2 * uint64(len(envelope))}, true},
		{"aggregate_over", []ports.ExecutionToolEvent{tool, tool}, domain.ProductLimits{MaxToolEvents: 2, MaxToolEventBytes: 2*uint64(len(envelope)) - 1}, false},
		{"late_bad_json", []ports.ExecutionToolEvent{tool, {Kind: domain.SessionEventToolResult, CallID: "c", ToolName: "t", Payload: []byte(`{"broken":`)}}, domain.ProductLimits{MaxToolEvents: 2, MaxToolEventBytes: 4096}, false},
		{"metadata_cannot_fit", []ports.ExecutionToolEvent{{Kind: domain.SessionEventToolCall, CallID: strings.Repeat("c", 128), ToolName: "t", Payload: []byte(`{}`)}}, domain.ProductLimits{MaxToolEvents: 1, MaxToolEventBytes: 64}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := canonicaloutput.EncodeCompletionV1("run-output", tc.limits, ports.ExecutionResult{ToolEvents: tc.tools}, "manifest-output")
			if !tc.valid {
				if err == nil || plan != nil {
					t.Fatalf("invalid whole batch escaped: plan=%v err=%v", plan, err)
				}
				return
			}
			if err != nil || plan == nil || len(plan.Events()) != len(tc.tools)+1 {
				t.Fatalf("exact admitted plan denied: %v", err)
			}
			t.Cleanup(plan.Release)
			if string(plan.Events()[0].Payload) != envelope || plan.Events()[len(plan.Events())-1].DisplayText != "Run completed." {
				t.Error("original envelope/default summary changed")
			}
		})
	}
}

func TestCompletionPlanBorrowerCannotRedirectCleanup(t *testing.T) {
	plan, err := canonicaloutput.EncodeCompletionV1("run-output", domain.ProductLimits{}, ports.ExecutionResult{Summary: "answer"}, "manifest-output")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(plan.Release)
	entries := plan.Events()
	encoded := entries[0].Payload
	caller := []byte("caller-owned-data")
	entries[0].Payload = caller
	plan.Release()
	if string(caller) != "caller-owned-data" {
		t.Error("borrower redirected cleanup onto caller-owned bytes")
	}
	if !bytes.Equal(encoded, make([]byte, len(encoded))) {
		t.Error("borrower detached the originally owned envelope from cleanup")
	}
}

func TestFailureEncodingOriginalNoticeIdentity(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		event, err := canonicaloutput.EncodeFailureV1("run-output", cancelled, "bounded_failure")
		if err != nil {
			t.Fatal(err)
		}
		want := fmt.Sprintf(`{"schema":"sessionless.run-terminal-notice.v1","code":"bounded_failure","cancelled":%t}`, cancelled)
		if event.ID != "evt_588ee355134dd75e655729c11b6fa803" || event.Kind != domain.SessionEventSystemNotice || string(event.Payload) != want {
			t.Errorf("notice wire changed: id=%s kind=%s payload=%s", event.ID, event.Kind, event.Payload)
		}
		clear(event.Payload)
	}
}

type outputBlobs struct {
	puts, other int
	key         string
	body        []byte
	failure     error
	corrupt     bool
}

func (b *outputBlobs) Put(_ context.Context, tenant domain.TenantID, key string, source io.Reader) (domain.BlobRef, error) {
	b.puts++
	body, err := io.ReadAll(io.LimitReader(source, 4097))
	if err != nil || len(body) > 4096 {
		return domain.BlobRef{}, errors.New("test payload exceeds finite bound")
	}
	b.key, b.body = key, body
	if b.failure != nil {
		return domain.BlobRef{}, b.failure
	}
	sum := sha256.Sum256(body)
	ref := domain.BlobRef{TenantID: tenant, Key: key, Size: int64(len(body)), SHA256: hex.EncodeToString(sum[:])}
	if b.corrupt {
		ref.Size++
	}
	return ref, nil
}

func (b *outputBlobs) Open(context.Context, domain.TenantID, domain.BlobRef) (io.ReadCloser, error) {
	b.other++
	return nil, errors.New("unrequested custody read")
}
func (b *outputBlobs) Delete(context.Context, domain.TenantID, domain.BlobRef) error {
	b.other++
	return errors.New("unrequested body erasure")
}

func TestOriginalUploaderIdentityAndFailures(t *testing.T) {
	at := time.Date(2026, 10, 18, 1, 2, 3, 0, time.UTC)
	run := domain.Run{ID: "run-output", TenantID: "tenant-output", SessionID: "session-output", TriggerEventID: "trigger-output", SubscriptionConnectionID: "subscription-output", Status: domain.RunRunning, IdempotencyKey: "output-key", CreatedAt: at, UpdatedAt: at}
	if err := run.Validate(); err != nil {
		t.Fatal(err)
	}
	failure := errors.New("owned PUT failure")
	for _, tc := range []struct {
		name    string
		failure error
		corrupt bool
	}{
		{"success", nil, false}, {"put_failure", failure, false}, {"wrong_ref", nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plan, err := canonicaloutput.EncodeCompletionV1(run.ID, domain.ProductLimits{}, ports.ExecutionResult{Summary: "answer"}, "manifest-output")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(plan.Release)
			event := plan.Events()[0]
			predicted, err := canonicaloutput.PlanEventDraftV1(run, event.ID, event.Kind, event.Payload, at)
			if err != nil {
				t.Fatal("pure original draft planning", err)
			}
			blobs := &outputBlobs{failure: tc.failure, corrupt: tc.corrupt}
			draft, material, err := canonicaloutput.PutEventDraftV1(t.Context(), blobs, run, event.ID, event.Kind, event.Payload, at)
			if material != nil {
				t.Cleanup(material.Release)
			}
			if blobs.puts != 1 || blobs.other != 0 {
				t.Fatalf("uploader made extra effects: PUT=%d other=%d", blobs.puts, blobs.other)
			}
			if tc.failure != nil || tc.corrupt {
				if err == nil || material != nil || draft.ID != "" || (tc.failure != nil && !errors.Is(err, failure)) {
					t.Fatalf("unverified upload escaped: draft=%s material=%t err=%v", draft.ID, material != nil, err)
				}
				return
			}
			sum := sha256.Sum256(event.Payload)
			wantKey := domain.SessionEventObjectPrefix(run.TenantID, run.SessionID, event.ID) + "payloads/sha256/" + hex.EncodeToString(sum[:]) + ".json"
			if err != nil || material == nil || draft.ID != event.ID || draft.Kind != event.Kind || draft.IdempotencyKey != "evtkey_81f542e1977540d42b22f1ef84331523" || draft.Payload.Key != wantKey || blobs.key != wantKey || !bytes.Equal(blobs.body, event.Payload) {
				t.Fatalf("original publisher contract changed: draft=%s material=%t err=%v", draft.ID, material != nil, err)
			}
			if draft != predicted {
				t.Fatal("uploader drifted from its original pure plan")
			}
		})
	}
}
