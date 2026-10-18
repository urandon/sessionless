package ydbstore

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"testing/synctest"

	"gitcode.com/urandon/sessionless/internal/configuredinference"
)

// The actual retained 1:1 calibration also charges fixed/message overhead.
// A byte-only necessary ceiling cannot establish that the fixed successor
// prefix fits. All limit changes below occur before fresh Source intake.
func TestLocalTerminalFitOriginalMinimumNeedsCalibratedTokens(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		control, source := terminalFitGenerationSource(t, localRuntimeActualPrebuild{}, false)
		generation, err := prepareLocalCompactionGeneration(t.Context(), source)
		if err != nil {
			t.Fatal("genuine unconstrained generation", err)
		}
		minimum, err := generation.program.MinimumSuccessorRequest()
		if err != nil {
			t.Fatal("original minimum successor DATA", err)
		}
		wire := minimum.Body()
		wireBytes := uint64(len(wire))
		var request struct {
			Messages []struct {
				Role    configuredinference.RoleV1 `json:"role"`
				Content string                     `json:"content"`
			} `json:"messages"`
		}
		if json.Unmarshal(wire, &request) != nil || len(request.Messages) != 2 {
			t.Fatal("original minimum wire must retain its real two-message shape")
		}
		clear(wire)
		var history struct {
			Summary string `json:"derived_summary"`
		}
		if json.Unmarshal([]byte(request.Messages[1].Content), &history) != nil || history.Summary == "" {
			t.Fatal("minimum must include mandatory valid summary format, not empty text")
		}
		if _, err := generation.program.ValidateSummary([]byte(history.Summary)); err != nil {
			t.Fatal("minimum DATA must fit the actual strict summary schema", err)
		}
		// Reconstruct the former empty-summary lower bound as TEST DATA only.
		// It must not become a valid-summary or paid-compaction fit witness.
		quoted, err := json.Marshal(history.Summary)
		if err != nil {
			t.Fatal(err)
		}
		field := `"derived_summary":` + string(quoted)
		if strings.Count(request.Messages[1].Content, field) != 1 {
			t.Fatal("minimum history must have one exact derived summary field")
		}
		emptyText := strings.Replace(request.Messages[1].Content, field, `"derived_summary":""`, 1)
		output := generation.program.SuccessorOutputReserve()
		empty, err := configuredinference.FreezeRequestV1(control.actual.config, control.actual.config.Limits, configuredinference.RequestV1{
			Messages: []configuredinference.MessageV1{{Role: request.Messages[0].Role, Text: request.Messages[0].Content}, {Role: request.Messages[1].Role, Text: emptyText}}, MaxOutputTokens: output,
		})
		if err != nil {
			t.Fatal("bounded empty-format DATA control", err)
		}
		emptyFit, err := control.actual.source.FitContext(empty, configuredinference.ContextFitCeilingsV1{
			InputTokens: control.actual.config.Limits.MaxInputTokens, OutputTokens: output, TotalTokens: control.actual.config.ModelWindowTokens,
		})
		if err != nil {
			t.Fatal("empty-format calibration control", err)
		}
		fit, err := control.actual.source.FitContext(minimum, configuredinference.ContextFitCeilingsV1{
			InputTokens: control.actual.config.Limits.MaxInputTokens, OutputTokens: output, TotalTokens: control.actual.config.ModelWindowTokens,
		})
		if err != nil || fit.InputUpperBound <= wireBytes+1 || fit.InputUpperBound <= emptyFit.InputUpperBound {
			t.Fatalf("control must distinguish concrete calibration from bytes+1: wire=%d fit=%+v err=%v", wireBytes, fit, err)
		}
		pending := generation.operation.Debit.InputTokens
		perCall := max(pending, fit.InputUpperBound)
		for _, tc := range []struct {
			name      string
			remaining uint64
			fitting   bool
		}{
			{name: "empty_format_is_not_finishable", remaining: emptyFit.InputUpperBound},
			{name: "byte_only_bound_omits_calibration_terms", remaining: wireBytes + 1},
			{name: "exact_valid_structural_minimum", remaining: fit.InputUpperBound, fitting: true},
		} {
			remaining := tc.remaining
			aggregate := pending + remaining
			if aggregate < perCall {
				t.Fatal("control aggregate cannot satisfy the unchanged domain per-call contract")
			}
			f, captured := terminalFitGenerationSource(t, localRuntimeActualPrebuild{inputTokens: perCall, aggregateInputTokens: aggregate}, false)
			before := f.canonical.calls
			prepared, err := prepareLocalCompactionGeneration(t.Context(), captured)
			if !tc.fitting {
				if prepared != nil || !errors.Is(err, ErrTerminalCapacity) || f.canonical.calls != before || f.entry.compactionGeneration.Load() != nil {
					t.Fatalf("%s: impossible successor performed preparation: handle=%t err=%v I/O=%d remaining=%d calibrated=%d", tc.name, prepared != nil, err, f.canonical.calls-before, remaining, fit.InputUpperBound)
				}
			} else if err != nil || prepared == nil || prepared.operation.Debit.InputTokens != pending {
				t.Fatalf("exact actual minimum calibration control failed: err=%v handle=%t pending=%d aggregate=%d", err, prepared != nil, pending, aggregate)
			}
		}
	})
}
