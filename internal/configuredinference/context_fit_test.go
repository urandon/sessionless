package configuredinference

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
)

func syntheticContextCalibration(t testing.TB) (*ContextCalibrationV1, *FrozenRequestV1, []byte) {
	t.Helper()
	p := testProfile(t)
	p.ModelWindowTokens, p.Limits.MaxInputTokens = 8192, 4096
	d := contextCalibrationDocumentV1{
		Version: 1, Algorithm: "utf8_affine.v1", InputDomain: "chat_completions_text.v1",
		TenantID: p.TenantID, OwnerUserID: p.OwnerUserID, ModelVendorID: p.ModelVendorID, ModelID: p.ModelID, ResponseModelIDs: p.ResponseModelIDs,
		ServerID: p.ServerID, ServerVersion: p.ServerVersion, Protocol: p.NativeProtocolVersion, ChatTemplateDigest: p.ChatTemplateDigest,
		EstimatorVersion: p.TokenEstimatorVersion, CapabilityEvidenceDigest: p.CapabilityEvidence.Digest, ModelWindowTokens: p.ModelWindowTokens,
		MaxWireBytes: p.Limits.RequestBytes, MaxMessages: p.Limits.Messages, MaxTools: p.Limits.Tools,
		ByteNumerator: 3, ByteDenominator: 2, FixedTokens: 11, MessageTokens: 7, ToolTokens: 13,
	}
	body, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(body)
	p.CalibrationDigest = hex.EncodeToString(sum[:])
	c, err := DecodeContextCalibrationV1(p, body)
	if err != nil {
		t.Fatalf("synthetic DATA calibration: %v", err)
	}
	f, err := FreezeRequestV1(p, p.Limits, testRequest())
	if err != nil {
		t.Fatalf("freeze synthetic request: %v", err)
	}
	return c, f, body
}

func contextCeilings() ContextFitCeilingsV1 {
	return ContextFitCeilingsV1{InputTokens: 4096, OutputTokens: 1024, TotalTokens: 8192}
}

func TestContextFitUsesExactWireAndIndependentBoundaries(t *testing.T) {
	c, f, _ := syntheticContextCalibration(t)
	// Independent oracle: complete known two-message/one-tool wire, not an
	// estimate from just message text or a round trip through bound().
	want := (uint64(len(f.Body()))*3+1)/2 + 11 + 2*7 + 13
	fit, err := c.FitRequest(f, contextCeilings())
	if err != nil || fit.InputUpperBound != want || fit.OutputReserve != 32 || fit.TotalUpperBound != want+32 || fit.RequestDigest != f.Digest() || fit.CalibrationDigest != f.profile.CalibrationDigest {
		t.Fatalf("actual frozen fit got=%+v want_input=%d err=%v", fit, want, err)
	}
	exact := ContextFitCeilingsV1{InputTokens: want, OutputTokens: 32, TotalTokens: want + 32}
	if _, err := c.FitRequest(f, exact); err != nil {
		t.Fatalf("inclusive exact boundary denied: %v", err)
	}
	for _, field := range []string{"input", "output", "total"} {
		t.Run(field, func(t *testing.T) {
			limits := exact
			switch field {
			case "input":
				limits.InputTokens--
			case "output":
				limits.OutputTokens--
			case "total":
				limits.TotalTokens--
			}
			if got, err := c.FitRequest(f, limits); !errors.Is(err, ErrContextNoFit) || got != (ContextFitV1{}) {
				t.Fatalf("one-over %s got=%+v err=%v", field, got, err)
			}
		})
	}
	for _, field := range []string{"input", "output", "total"} {
		t.Run("zero_"+field, func(t *testing.T) {
			limits := exact
			switch field {
			case "input":
				limits.InputTokens = 0
			case "output":
				limits.OutputTokens = 0
			case "total":
				limits.TotalTokens = 0
			}
			if _, err := c.FitRequest(f, limits); !errors.Is(err, ErrContract) {
				t.Fatalf("zero %s became unlimited: %v", field, err)
			}
		})
	}
}

func TestContextCalibrationStrictClosedBody(t *testing.T) {
	_, f, good := syntheticContextCalibration(t)
	for _, tc := range []struct {
		name   string
		change func([]byte) []byte
	}{
		{"unknown", func(b []byte) []byte { return append(append([]byte(nil), b[:len(b)-1]...), []byte(`,"extra":1}`)...) }},
		{"duplicate", func(b []byte) []byte { return append([]byte(`{"version":1,`), b[1:]...) }},
		{"escaped_duplicate", func(b []byte) []byte { return append([]byte(`{"vers\u0069on":1,`), b[1:]...) }},
		{"case_alias", func(b []byte) []byte { return []byte(strings.Replace(string(b), `"version":`, `"Version":`, 1)) }},
		{"missing_zero_coefficient", func(b []byte) []byte { return []byte(strings.Replace(string(b), `,"tool_tokens":13`, "", 1)) }},
		{"null_coefficient", func(b []byte) []byte {
			return []byte(strings.Replace(string(b), `"tool_tokens":13`, `"tool_tokens":null`, 1))
		}},
		{"exponent_counter", func(b []byte) []byte {
			return []byte(strings.Replace(string(b), `"tool_tokens":13`, `"tool_tokens":13e0`, 1))
		}},
		{"negative", func(b []byte) []byte {
			return []byte(strings.Replace(string(b), `"tool_tokens":13`, `"tool_tokens":-1`, 1))
		}},
		{"fraction", func(b []byte) []byte {
			return []byte(strings.Replace(string(b), `"tool_tokens":13`, `"tool_tokens":13.5`, 1))
		}},
		{"unknown_algorithm", func(b []byte) []byte {
			return []byte(strings.Replace(string(b), `utf8_affine.v1`, `bytes_div_four`, 1))
		}},
		{"unknown_domain", func(b []byte) []byte {
			return []byte(strings.Replace(string(b), `chat_completions_text.v1`, `vision`, 1))
		}},
		{"zero_denominator", func(b []byte) []byte {
			return []byte(strings.Replace(string(b), `"byte_denominator":2`, `"byte_denominator":0`, 1))
		}},
		{"subbyte_bound", func(b []byte) []byte {
			return []byte(strings.Replace(string(b), `"byte_numerator":3`, `"byte_numerator":1`, 1))
		}},
		{"zero_fixed_overhead", func(b []byte) []byte {
			return []byte(strings.Replace(string(b), `"fixed_tokens":11`, `"fixed_tokens":0`, 1))
		}},
		{"excessive_coefficient", func(b []byte) []byte {
			return []byte(strings.Replace(string(b), `"fixed_tokens":11`, `"fixed_tokens":1048577`, 1))
		}},
		{"domain_wire_overflow", func(b []byte) []byte {
			return []byte(strings.Replace(string(b), `"max_wire_bytes":1048576`, `"max_wire_bytes":1048577`, 1))
		}},
		{"domain_message_overflow", func(b []byte) []byte {
			return []byte(strings.Replace(string(b), `"max_messages":256`, `"max_messages":257`, 1))
		}},
		{"domain_tool_overflow", func(b []byte) []byte {
			return []byte(strings.Replace(string(b), `"max_tools":16`, `"max_tools":17`, 1))
		}},
		{"surrogate", func(b []byte) []byte {
			return []byte(strings.Replace(string(b), `"algorithm":"utf8_affine.v1"`, `"algorithm":"\ud800"`, 1))
		}},
		{"invalid_utf8", func(b []byte) []byte { return append(append([]byte(nil), b...), 0xff) }},
		{"trailing_json", func(b []byte) []byte { return append(append([]byte(nil), b...), []byte(`{}`)...) }},
		{"body_bound", func(b []byte) []byte {
			return append(append([]byte(nil), b...), []byte(strings.Repeat(" ", MaxContextCalibrationBytesV1))...)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := tc.change(good)
			if string(body) == string(good) {
				t.Fatal("test mutation did not change calibration")
			}
			p := f.profile.Clone()
			sum := sha256.Sum256(body)
			p.CalibrationDigest = hex.EncodeToString(sum[:]) // prevent wrong hash hiding schema defect
			if got, err := DecodeContextCalibrationV1(p, body); !errors.Is(err, ErrContract) || got != nil {
				t.Fatalf("invalid calibration accepted: %v", err)
			}
		})
	}
}

func TestContextCalibrationExactTupleAndFrozenIdentity(t *testing.T) {
	c, f, body := syntheticContextCalibration(t)
	for _, key := range []string{"tenant_id", "owner_user_id", "model_vendor_id", "model_id", "server_id", "server_version", "protocol", "chat_template_digest", "estimator_version", "capability_evidence_digest", "model_window_tokens", "response_model_ids"} {
		t.Run(key, func(t *testing.T) {
			var values map[string]any
			if err := json.Unmarshal(body, &values); err != nil {
				t.Fatal(err)
			}
			switch key {
			case "model_window_tokens":
				values[key] = 8193
			case "response_model_ids":
				values[key] = []string{"alias-local", "different-model"}
			default:
				values[key] = "different"
			}
			raw, err := json.Marshal(values)
			if err != nil {
				t.Fatal(err)
			}
			p := f.profile.Clone()
			sum := sha256.Sum256(raw)
			p.CalibrationDigest = hex.EncodeToString(sum[:])
			if _, err := DecodeContextCalibrationV1(p, raw); !errors.Is(err, ErrContract) {
				t.Fatalf("foreign %s accepted: %v", key, err)
			}
		})
	}
	p := f.profile.Clone()
	p.CalibrationDigest = strings.Repeat("f", 64)
	if _, err := DecodeContextCalibrationV1(p, body); !errors.Is(err, ErrContract) {
		t.Fatal("wrong body hash accepted")
	}
	other := *f
	other.profile = f.profile.Clone()
	other.profile.ProfileRevision++
	if _, err := c.FitRequest(&other, contextCeilings()); !errors.Is(err, ErrContract) {
		t.Fatal("different installed profile accepted")
	}
	other = *f
	other.body = append(other.Body(), ' ')
	if _, err := c.FitRequest(&other, contextCeilings()); !errors.Is(err, ErrContract) {
		t.Fatal("changed exact frozen bytes accepted")
	}
	if _, err := c.FitRequest(nil, contextCeilings()); !errors.Is(err, ErrContract) {
		t.Fatal("nil frozen request accepted")
	}
	if _, err := (*ContextCalibrationV1)(nil).FitRequest(f, contextCeilings()); !errors.Is(err, ErrContract) {
		t.Fatal("nil calibration accepted")
	}
	if _, err := new(ContextCalibrationV1).FitRequest(f, contextCeilings()); !errors.Is(err, ErrContract) {
		t.Fatal("zero calibration accepted")
	}
	clear(body) // decoder owns no caller mutable record bytes
	if _, err := c.FitRequest(f, contextCeilings()); err != nil {
		t.Fatalf("caller buffer mutation changed fit: %v", err)
	}
}

func TestContextFitCannotRelaxInstalledLimitsAndUsesActualCounts(t *testing.T) {
	c, f, _ := syntheticContextCalibration(t)
	huge := ContextFitCeilingsV1{InputTokens: math.MaxUint64, OutputTokens: math.MaxUint64, TotalTokens: math.MaxUint64}
	p := f.profile.Clone()
	request := testRequest()
	request.Messages[1].Text = strings.Repeat("x", 4000)
	tooLarge, err := FreezeRequestV1(p, p.Limits, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.FitRequest(tooLarge, huge); !errors.Is(err, ErrContextNoFit) {
		t.Fatalf("caller ceilings relaxed installed input cap: %v", err)
	}
	// Independently bind a tighter domain by decoding a matching retained body,
	// rather than changing calculator fields in the test.
	for _, tc := range []struct {
		name  string
		key   string
		value uint64
	}{
		{"wire", "max_wire_bytes", uint64(len(f.Body()) - 1)},
		{"messages", "max_messages", 1},
		{"tools", "max_tools", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, original, body := syntheticContextCalibration(t)
			var d map[string]json.RawMessage
			if err := json.Unmarshal(body, &d); err != nil {
				t.Fatal(err)
			}
			d[tc.key], err = json.Marshal(tc.value)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(d)
			if err != nil {
				t.Fatal(err)
			}
			profile := original.profile.Clone()
			sum := sha256.Sum256(raw)
			profile.CalibrationDigest = hex.EncodeToString(sum[:])
			calibration, err := DecodeContextCalibrationV1(profile, raw)
			if err != nil {
				t.Fatal(err)
			}
			frozen, err := FreezeRequestV1(profile, profile.Limits, testRequest())
			if err != nil {
				t.Fatal(err)
			}
			if _, err := calibration.FitRequest(frozen, huge); !errors.Is(err, ErrContextNoFit) {
				t.Fatalf("actual %s not counted: %v", tc.name, err)
			}
		})
	}
}

func TestContextSummaryBoundIndependentOfReportedUsage(t *testing.T) {
	c, _, _ := syntheticContextCalibration(t)
	for _, tc := range []struct {
		text string
		want uint64
	}{
		{"x", 13}, {"xx", 14}, {"é", 14}, {"雪", 16}, {strings.Repeat("x", 1358), 2048},
	} {
		got, err := c.BoundSummaryText(tc.text)
		if err != nil || got != tc.want {
			t.Fatalf("summary byte_count=%d bound=%d want=%d err=%v", len(tc.text), got, tc.want, err)
		}
	}
	for _, text := range []string{strings.Repeat("x", 1359), strings.Repeat("x", 16<<10)} {
		if _, err := c.BoundSummaryText(text); !errors.Is(err, ErrContextNoFit) {
			t.Fatalf("summary token overflow len=%d err=%v", len(text), err)
		}
	}
	for _, text := range []string{"", string([]byte{0xff}), strings.Repeat("x", (16<<10)+1)} {
		if _, err := c.BoundSummaryText(text); !errors.Is(err, ErrContract) {
			t.Fatalf("invalid summary len=%d err=%v", len(text), err)
		}
	}
}

func TestContextFitCheckedArithmetic(t *testing.T) {
	c, _, _ := syntheticContextCalibration(t)
	for _, input := range [][3]uint64{{math.MaxUint64, 0, 0}, {0, math.MaxUint64, 0}, {0, 0, math.MaxUint64}, {math.MaxUint64 / 3, math.MaxUint64 / 7, 0}} {
		if value, ok := c.bound(input[0], input[1], input[2]); ok || value != 0 {
			t.Fatalf("overflow input=%v became bound=%d", input, value)
		}
	}
}

func TestContextCalibrationDiagnosticsStayPrivate(t *testing.T) {
	c, frozen, _ := syntheticContextCalibration(t)
	for _, value := range []any{c, *c} {
		for _, format := range []string{"%v", "%+v", "%#v", "%s"} {
			text := fmt.Sprintf(format, value)
			if !strings.Contains(text, "[redacted]") || strings.Contains(text, string(frozen.profile.TenantID)) || strings.Contains(text, frozen.profile.ModelID) || strings.Contains(text, frozen.profile.CalibrationDigest) {
				t.Fatalf("format %s disclosed calibration content", format)
			}
		}
		if raw, err := json.Marshal(value); !errors.Is(err, ErrContract) || raw != nil {
			t.Fatal("calibration acquired a JSON transport representation")
		}
	}
}

func FuzzContextCalibrationStrictDecoder(f *testing.F) {
	_, frozen, body := syntheticContextCalibration(f)
	f.Add(body)
	f.Add([]byte(`{"version":1,"version":1}`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > MaxContextCalibrationBytesV1+1 {
			return
		}
		p := frozen.profile.Clone()
		sum := sha256.Sum256(raw)
		p.CalibrationDigest = hex.EncodeToString(sum[:])
		c, err := DecodeContextCalibrationV1(p, raw)
		if err == nil {
			if c == nil || c.bodyDigest != p.CalibrationDigest {
				t.Fatal("successful decode lost exact retained hash")
			}
		}
	})
}
