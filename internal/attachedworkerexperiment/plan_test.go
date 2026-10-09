package attachedworkerexperiment

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func validDraft() Manifest {
	return Manifest{
		Version: "aw03-experiment-draft-v1", SourceSHA: strings.Repeat("a", 40),
		WindowSeconds: 86400, RestartAttempts: 5,
		Cohorts:    []Cohort{{IntervalMinutes: 15, Workers: 1}, {IntervalMinutes: 30, Workers: 1}, {IntervalMinutes: 60, Workers: 1}},
		StopLimits: Limits{Requests: 1000, YDBRequestUnits: 10000, BilledSeconds: 600, EgressBytes: 10485760, RubKopecks: 10000, WallClockSeconds: 108000},
	}
}

func TestPlanKeepsScheduleWakeAndUnmeasuredCostsSeparate(t *testing.T) {
	plan, err := Prepare(validDraft())
	if err != nil {
		t.Fatalf("prepare draft: %v", err)
	}
	if plan.Status != "draft_not_authorized" || len(plan.Unknown) == 0 || len(plan.Excluded) == 0 {
		t.Fatalf("draft lost authorization or evidence limitations: %+v", plan)
	}
	want := []uint64{96, 48, 24}
	for i, cohort := range plan.Cohorts {
		if cohort.ScheduledExchangeBound != want[i] || cohort.WithWakeExchangeBound != 96 {
			t.Errorf("interval %d: schedule/wake = %d/%d, want %d/96", cohort.IntervalMinutes, cohort.ScheduledExchangeBound, cohort.WithWakeExchangeBound, want[i])
		}
	}
}

func TestPlanRejectsUnsafeOrIncompletePreparation(t *testing.T) {
	cases := []struct {
		name   string
		change func(*Manifest)
	}{
		{name: "cloud execution", change: func(m *Manifest) { m.CloudExecution = true }},
		{name: "unpinned source", change: func(m *Manifest) { m.SourceSHA = "main" }},
		{name: "short observation", change: func(m *Manifest) { m.WindowSeconds = 3600 }},
		{name: "missing cohort", change: func(m *Manifest) { m.Cohorts = m.Cohorts[:2] }},
		{name: "duplicate cohort", change: func(m *Manifest) { m.Cohorts[2] = m.Cohorts[0] }},
		{name: "short cadence", change: func(m *Manifest) { m.Cohorts[0].IntervalMinutes = 1 }},
		{name: "unbounded workers", change: func(m *Manifest) { m.Cohorts[0].Workers = 100 }},
		{name: "restart storm", change: func(m *Manifest) { m.RestartAttempts = 6 }},
		{name: "no restarts", change: func(m *Manifest) { m.RestartAttempts = 0 }},
		{name: "no RU stop", change: func(m *Manifest) { m.StopLimits.YDBRequestUnits = 0 }},
		{name: "no billing stop", change: func(m *Manifest) { m.StopLimits.RubKopecks = 0 }},
		{name: "no duration stop", change: func(m *Manifest) { m.StopLimits.BilledSeconds = 0 }},
		{name: "no egress stop", change: func(m *Manifest) { m.StopLimits.EgressBytes = 0 }},
		{name: "no setup headroom", change: func(m *Manifest) { m.StopLimits.Requests = 288 }},
		{name: "too short wall limit", change: func(m *Manifest) { m.StopLimits.WallClockSeconds = 86399 }},
		{name: "unbounded wall limit", change: func(m *Manifest) { m.StopLimits.WallClockSeconds = 172801 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			manifest := validDraft()
			tc.change(&manifest)
			if _, err := Prepare(manifest); !errors.Is(err, ErrInvalidManifest) {
				t.Fatalf("%s: got %v, want invalid draft", tc.name, err)
			}
		})
	}
}

func TestReadDraftBoundsInputAndDoesNotEchoContent(t *testing.T) {
	data, err := json.Marshal(validDraft())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ReadDraft(bytes.NewReader(data)); err != nil {
		t.Fatalf("read valid draft: %v", err)
	}
	for name, input := range map[string]string{
		"oversized": strings.Repeat(" ", maximumManifestBytes+1),
		"trailing":  string(data) + "{}",
		"unknown":   strings.TrimSuffix(string(data), "}") + `,"private_token":"secret-fixture"}`,
		"malformed": `{"private_token":"secret-fixture"`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ReadDraft(strings.NewReader(input))
			if !errors.Is(err, ErrInvalidManifest) || strings.Contains(err.Error(), "secret-fixture") {
				t.Fatalf("read %s returned unsanitized or absent failure: %v", name, err)
			}
		})
	}
}
