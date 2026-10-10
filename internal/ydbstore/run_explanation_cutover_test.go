package ydbstore

import (
	"strings"
	"testing"
	"time"
)

func TestRunExplanationCutoverReceiptRequiresExactWriterAndDrainedInventory(t *testing.T) {
	now := time.Date(2026, 10, 10, 15, 0, 0, 0, time.UTC)
	commit := strings.Repeat("a", 40)
	valid := RunExplanationCutoverReceiptV1{Version: 1, WriterVersion: 1, SchemaVersion: 1, RatedReaderVersion: 1,
		WriterCommit: commit, DrainedInventoryDigest: strings.Repeat("b", 64), CompletedAt: now}
	if err := valid.Validate(commit, now); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*RunExplanationCutoverReceiptV1){
		"version":          func(v *RunExplanationCutoverReceiptV1) { v.Version = 2 },
		"writer":           func(v *RunExplanationCutoverReceiptV1) { v.WriterVersion = 0 },
		"schema":           func(v *RunExplanationCutoverReceiptV1) { v.SchemaVersion = 2 },
		"reader":           func(v *RunExplanationCutoverReceiptV1) { v.RatedReaderVersion = 2 },
		"writer commit":    func(v *RunExplanationCutoverReceiptV1) { v.WriterCommit = strings.Repeat("c", 40) },
		"drain absent":     func(v *RunExplanationCutoverReceiptV1) { v.DrainedInventoryDigest = "" },
		"drain not hash":   func(v *RunExplanationCutoverReceiptV1) { v.DrainedInventoryDigest = strings.Repeat("g", 64) },
		"old writer live":  func(v *RunExplanationCutoverReceiptV1) { v.OldWriterCount = 1 },
		"timestamp absent": func(v *RunExplanationCutoverReceiptV1) { v.CompletedAt = time.Time{} },
		"timestamp future": func(v *RunExplanationCutoverReceiptV1) { v.CompletedAt = now.Add(time.Second) },
	} {
		t.Run(name, func(t *testing.T) {
			value := valid
			mutate(&value)
			if value.Validate(commit, now) == nil {
				t.Fatal("invalid cutover accepted")
			}
		})
	}
	for _, bad := range []string{"", "main", strings.Repeat("A", 40), strings.Repeat("a", 39)} {
		if valid.Validate(bad, now) == nil {
			t.Fatal("non-exact deployment pin accepted")
		}
	}
}
