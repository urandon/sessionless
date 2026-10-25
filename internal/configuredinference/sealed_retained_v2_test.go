package configuredinference

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func TestSealedRetainedV2ActualCodecHistoricalOnly(t *testing.T) {
	for _, kind := range []string{"tool", "answer", "compaction"} {
		t.Run(kind, func(t *testing.T) {
			opKind := domain.AttemptOperationModelV1
			wire := responseWire("model-local", "stop", "retained answer", nil)
			if kind == "tool" {
				wire = toolWire("retained-call", "lookup", `{"query":"docs"}`)
			} else if kind == "compaction" {
				opKind = domain.AttemptOperationCompactionV1
			}
			p, r, o, body := retainedNormalizedFixture(t, opKind, wire)
			pd, _ := p.Digest()
			out, err := DecodeSealedRetainedResponseV2(pd, r, o, body)
			if err != nil {
				t.Fatal(err)
			}
			if kind == "tool" {
				if out.Stop != StopToolV1 || out.Text != "" || out.Call == nil || out.Call.ID != "retained-call" || out.Call.Name != "lookup" || string(out.Call.Arguments) != `{"query":"docs"}` {
					t.Fatal("historical typed call changed")
				}
				if _, err := DecodeSealedRetainedTextV1(pd, r, o, body); err == nil {
					t.Fatal("V1 text decoder silently accepted a tool")
				}
				clear(body)
				if string(out.Call.Arguments) != `{"query":"docs"}` {
					t.Fatal("returned call aliases normalized bytes")
				}
				clear(out.Call.Arguments)
			} else if out.Stop != StopFinalV1 || out.Call != nil || out.Text != "retained answer" {
				t.Fatal("historical final text changed")
			}
			if formatted := fmt.Sprintf("%+v %#v", out, out); strings.Contains(formatted, "retained answer") || strings.Contains(formatted, "docs") {
				t.Fatal("formatted historical content")
			}
		})
	}
}

func TestSealedRetainedV2DeniesTamperedToolData(t *testing.T) {
	for _, name := range []string{"profile", "request", "sequence", "bytes", "hash", "unknown_stop", "call_missing", "mixed_text", "non_object_arguments", "noncanonical_arguments", "compaction_tool", "usage"} {
		t.Run(name, func(t *testing.T) {
			p, r, o, body := retainedNormalizedFixture(t, domain.AttemptOperationModelV1, toolWire("retained-call", "lookup", `{"query":"docs"}`))
			pd, _ := p.Digest()
			var value normalizedBodyV1
			if err := json.Unmarshal(body, &value); err != nil {
				t.Fatal(err)
			}
			switch name {
			case "profile":
				pd = strings.Repeat("b", 64)
			case "request":
				r.Operation.RequestDigest = strings.Repeat("b", 64)
				rebindRetainedData(t, &r, &o, body)
			case "sequence":
				r.Operation.Sequence++ // invalid predecessor, not independently re-signed
			case "bytes":
				o.ResponseBytes++
			case "hash":
				o.ResponseDigest = strings.Repeat("b", 64)
			case "unknown_stop":
				value.Stop = "other"
			case "call_missing":
				value.Call = nil
			case "mixed_text":
				value.Text = "unprojected"
			case "non_object_arguments":
				value.Call.Arguments = []byte(`[]`)
			case "noncanonical_arguments":
				value.Call.Arguments = []byte(` { "query": "docs" } `)
			case "compaction_tool":
				r.Operation.Kind = domain.AttemptOperationCompactionV1
				r.Operation.SummaryBytes = 16 << 10
				r.Operation.Debit.CompactionCalls = 1
				value.OperationKind = domain.AttemptOperationCompactionV1
			case "usage":
				value.Usage = domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1}
			}
			if name != "profile" && name != "request" && name != "sequence" && name != "bytes" && name != "hash" {
				// Match outer hashes so the negative reaches the inner grammar,
				// never claim these DATA rows are issuer/capsule authenticated.
				var err error
				body, err = encodeNormalizedBodyV1(value, normalizedBodyLimitV1)
				if err != nil {
					t.Fatal(err)
				}
				rebindRetainedData(t, &r, &o, body)
			}
			out, err := DecodeSealedRetainedResponseV2(pd, r, o, body)
			if err == nil || !reflect.DeepEqual(out, SealedRetainedResponseV2{}) {
				t.Fatal("invalid historical tool DATA accepted", err)
			}
			clear(body)
		})
	}
}

func TestSealedRetainedV2CanonicalArgumentsPreserved(t *testing.T) {
	p, r, o, body := retainedNormalizedFixture(t, domain.AttemptOperationModelV1, toolWire("retained-call", "lookup", `{"query":"<docs>"}`))
	pd, _ := p.Digest()
	v, err := DecodeSealedRetainedResponseV2(pd, r, o, body)
	if err != nil || v.Call == nil || !bytes.Equal(v.Call.Arguments, []byte(`{"query":"\u003cdocs\u003e"}`)) {
		t.Fatal("escaped canonical arguments lost", err)
	}
	clear(v.Call.Arguments)
}
