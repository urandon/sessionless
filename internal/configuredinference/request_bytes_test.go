package configuredinference

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestConfiguredRequestBytesOnlyAfterSemanticValidation(t *testing.T) {
	for _, name := range []string{"lower_bound", "escaped_wire"} {
		t.Run(name, func(t *testing.T) {
			p := testProfile(t)
			request := testRequest()
			request.Messages[0].Text, request.Messages[1].Text = strings.Repeat("a", 256), strings.Repeat("b", 256)
			if name == "escaped_wire" {
				request.Messages[1].Text = strings.Repeat("\"\\<>&\n", 64)
			}
			full, err := FreezeRequestV1(p, p.Limits, request)
			if err != nil {
				t.Fatal("bounded semantic positive", err)
			}
			limits := p.Limits
			limits.RequestBytes = uint64(len(full.Body()))
			atLimit, err := FreezeRequestV1(p, limits, request)
			if err != nil || !bytes.Equal(atLimit.Body(), full.Body()) {
				t.Fatal("exact wire limit failed", err)
			}
			if name == "lower_bound" {
				limits.RequestBytes = uint64(len(p.ModelID) + len(request.Messages[0].Text) + len(request.Messages[1].Text) + len(request.Tools[0].Name) + len(request.Tools[0].Description) + len(request.Tools[0].Parameters) - 1)
			} else {
				limits.RequestBytes--
			}
			if f, err := FreezeRequestV1(p, limits, request); f != nil || !errors.Is(err, ErrRequestBytes) || !errors.Is(err, ErrContract) {
				t.Fatalf("validated %s overflow must retain precise contract classification: %v", name, err)
			}
			request.Tools[0].Parameters = []byte(`{"not-a-schema":true}`)
			if _, err := FreezeRequestV1(p, limits, request); !errors.Is(err, ErrContract) || errors.Is(err, ErrRequestBytes) {
				t.Fatalf("malformed catalog falsely certified as size-only failure: %v", err)
			}
			request.Tools = nil
			request.Messages[1].Role = "untrusted-role"
			if _, err := FreezeRequestV1(p, limits, request); !errors.Is(err, ErrContract) || errors.Is(err, ErrRequestBytes) {
				t.Fatalf("malformed message falsely certified as size-only failure: %v", err)
			}
		})
	}
}

func TestConfiguredRequestByteOverflowDoesNotHideEnvelopeBounds(t *testing.T) {
	for _, name := range []string{"root_depth", "root_members", "schema_nesting", "array_members", "native_call_depth"} {
		t.Run(name, func(t *testing.T) {
			p := testProfile(t)
			request := testRequest()
			limits := p.Limits
			switch name {
			case "root_depth":
				request.Tools = nil
				limits.JSONDepth = 1
			case "root_members":
				request.Tools = nil
				limits.JSONMembers = 4
			case "schema_nesting":
				limits.JSONDepth = 5 // standalone schema fits; wire nesting does not.
			case "array_members":
				request.Tools = nil
				limits.JSONMembers = 5
				for len(request.Messages) < 6 {
					request.Messages = append(request.Messages, MessageV1{Role: RoleUserV1, Text: "data"})
				}
			case "native_call_depth":
				limits.JSONDepth = 6
				request.Messages = append(request.Messages, MessageV1{Role: RoleAssistantV1, Call: &FunctionCallV1{ID: "call-1", Name: "lookup", Arguments: []byte(`{"query":"public"}`)}}, MessageV1{Role: RoleToolV1, ToolCallID: "call-1", Text: "result"})
			}
			for _, byteLimit := range []uint64{p.Limits.RequestBytes, 128} {
				limits.RequestBytes = byteLimit
				if _, err := FreezeRequestV1(p, limits, request); !errors.Is(err, ErrContract) || errors.Is(err, ErrRequestBytes) {
					t.Fatalf("invalid envelope %s at byte limit%d classified as size-only: %v", name, byteLimit, err)
				}
			}
		})
	}
}
