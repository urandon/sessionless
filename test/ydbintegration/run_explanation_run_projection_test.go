//go:build ydbintegration

package ydbintegration

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Exact production point projection, exercised by the dedicated true-data YDB
// plan/semantic gate. This is deliberately not a full Run JSON read.
const explanationRunProjectionPlanSQL = `SELECT
 SUBSTRING(CAST(JSON_VALUE(payload,"strict $.id" RETURNING Utf8 ERROR ON EMPTY ERROR ON ERROR) AS String),0,161),
 SUBSTRING(CAST(JSON_VALUE(payload,"strict $.tenant_id" RETURNING Utf8 ERROR ON EMPTY ERROR ON ERROR) AS String),0,161),
 SUBSTRING(CAST(JSON_VALUE(payload,"strict $.session_id" RETURNING Utf8 ERROR ON EMPTY ERROR ON ERROR) AS String),0,161),
 SUBSTRING(CAST(JSON_VALUE(payload,"strict $.trigger_event_id" RETURNING Utf8 ERROR ON EMPTY ERROR ON ERROR) AS String),0,161),
 SUBSTRING(CAST(JSON_VALUE(payload,"strict $.subscription_connection_id" RETURNING Utf8 ERROR ON EMPTY ERROR ON ERROR) AS String),0,161),
 SUBSTRING(CAST(JSON_VALUE(payload,"strict $.status" RETURNING Utf8 ERROR ON EMPTY ERROR ON ERROR) AS String),0,33),
 SUBSTRING(CAST(JSON_VALUE(payload,"strict $.idempotency_key" RETURNING Utf8 ERROR ON EMPTY ERROR ON ERROR) AS String),0,161),
 CASE WHEN JSON_VALUE(payload,"$.cancellation_requested_at.type()" RETURNING Utf8 NULL ON EMPTY ERROR ON ERROR)="null" THEN CAST(NULL AS String) ELSE SUBSTRING(CAST(JSON_VALUE(payload,"$.cancellation_requested_at" RETURNING Utf8 NULL ON EMPTY ERROR ON ERROR) AS String),0,36) END,
 CASE WHEN JSON_VALUE(payload,"$.started_at.type()" RETURNING Utf8 NULL ON EMPTY ERROR ON ERROR)="null" THEN CAST(NULL AS String) ELSE SUBSTRING(CAST(JSON_VALUE(payload,"$.started_at" RETURNING Utf8 NULL ON EMPTY ERROR ON ERROR) AS String),0,36) END,
 CASE WHEN JSON_VALUE(payload,"$.finished_at.type()" RETURNING Utf8 NULL ON EMPTY ERROR ON ERROR)="null" THEN CAST(NULL AS String) ELSE SUBSTRING(CAST(JSON_VALUE(payload,"$.finished_at" RETURNING Utf8 NULL ON EMPTY ERROR ON ERROR) AS String),0,36) END,
 SUBSTRING(CAST(JSON_VALUE(payload,"strict $.created_at" RETURNING Utf8 ERROR ON EMPTY ERROR ON ERROR) AS String),0,36),
 SUBSTRING(CAST(JSON_VALUE(payload,"strict $.updated_at" RETURNING Utf8 ERROR ON EMPTY ERROR ON ERROR) AS String),0,36)
 FROM runs WHERE tenant_id=$1 AND run_id=$2`

func replaceExplanationRunPayload(t *testing.T, f explanationYDBFixture, value any) {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.client.DB.ExecContext(f.ctx, `UPDATE runs SET payload=CAST($1 AS JsonDocument) WHERE tenant_id=$2 AND run_id=$3`, string(body), f.auth.TenantID, f.ingress.Run.ID); err != nil {
		t.Fatal(err)
	}
}

func explanationRunBaseFields(t *testing.T, f explanationYDBFixture) map[string]any {
	t.Helper()
	body, err := json.Marshal(f.ingress.Run)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(body, &fields); err != nil {
		t.Fatal(err)
	}
	return fields
}

func TestRunExplanationYDBRunProjectionExcludesLargeMetadata(t *testing.T) {
	for _, tc := range []struct {
		name          string
		metadataBytes int
	}{{name: "small"}, {name: "10KiB", metadataBytes: 10 << 10}, {name: "28KiB", metadataBytes: 28 << 10}} {
		t.Run(tc.name, func(t *testing.T) {
			f := newExplanationYDBFixture(t)
			baseline := readExplanationYDB(t, f)
			fields := explanationRunBaseFields(t, f)
			if tc.metadataBytes > 0 {
				fields["derived_context_head"] = strings.Repeat("private-derived-source-", tc.metadataBytes/23+1)
			}
			// Omitted and explicit-null optional timestamps are both valid. This
			// also exercises the SQL CASE/JSON_VALUE null distinction in YDB.
			fields["started_at"], fields["finished_at"], fields["cancellation_requested_at"] = nil, nil, nil
			encoded, err := json.Marshal(fields)
			if err != nil {
				t.Fatal(err)
			}
			if tc.metadataBytes > 0 && (len(encoded) <= 8<<10 || len(encoded) > 32<<10) {
				t.Fatalf("enlarged serialized Run outside (8KiB,32KiB]: got=%d", len(encoded))
			}
			replaceExplanationRunPayload(t, f, fields)
			got := readExplanationYDB(t, f)
			got.ReadAt = baseline.ReadAt // each read uses its own authoritative DB instant
			if !reflect.DeepEqual(got, baseline) {
				t.Fatal("excluded Run metadata changed participant-safe explanation")
			}
			body, err := json.Marshal(got)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(body), "private-derived-source-") {
				t.Fatal("excluded metadata disclosed in response")
			}
		})
	}
}

func TestRunExplanationYDBRatedRunProjectionExcludesLargeMetadata(t *testing.T) {
	f := newRatedExplanationYDBFixture(t)
	fields := explanationRunBaseFields(t, f.explanationYDBFixture)
	fields["derived_context_head"] = strings.Repeat("private-derived-source-", (28<<10)/23+1)
	encoded, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) <= 8<<10 || len(encoded) > 32<<10 {
		t.Fatalf("enlarged rated Run outside (8KiB,32KiB]: got=%d", len(encoded))
	}
	replaceExplanationRunPayload(t, f.explanationYDBFixture, fields)
	readRatedExplanationYDB(t, f, f.store, "large-run-projection", f.ingress.Run.ID, ports.RunExplanationReadSuccessV1)
}

func TestRunExplanationYDBRunProjectionPreservesTimestampPrecision(t *testing.T) {
	f := newExplanationYDBFixture(t)
	run := f.ingress.Run
	zone := time.FixedZone("", 90*60)
	start := run.CreatedAt.Add(time.Second + 123*time.Nanosecond).In(zone)
	finish := run.CreatedAt.Add(2*time.Second + 456*time.Nanosecond).In(zone)
	cancel := run.CreatedAt.Add(1 * time.Nanosecond).In(zone)
	run.Status = domain.RunSucceeded
	run.StartedAt = &start
	run.FinishedAt = &finish
	run.CancellationRequestedAt = &cancel
	run.UpdatedAt = finish
	replaceExplanationRunPayload(t, f, run)
	got := readExplanationYDB(t, f)
	if got.Status != run.Status || got.FinishedAt == nil || !got.FinishedAt.Equal(finish) || !got.UpdatedAt.Equal(finish) || !got.CreatedAt.Equal(run.CreatedAt) {
		t.Fatal("Run projection lost valid non-null timestamp precision or offset semantics")
	}
}

func TestRunExplanationYDBRunProjectionRejectsInconsistentTimes(t *testing.T) {
	for _, name := range []string{"updated_before_created", "future_updated", "terminal_missing_finish", "finish_nonterminal", "started_before_created", "cancel_before_created", "finish_before_created", "started_after_updated", "cancel_after_updated", "finish_after_updated"} {
		t.Run(name, func(t *testing.T) {
			f := newExplanationYDBFixture(t)
			run := f.ingress.Run
			before := run.CreatedAt.Add(-time.Nanosecond)
			after := run.UpdatedAt.Add(time.Nanosecond)
			switch name {
			case "updated_before_created":
				run.UpdatedAt = before
			case "future_updated":
				run.UpdatedAt = f.now.Add(time.Hour)
			case "terminal_missing_finish":
				run.Status = domain.RunSucceeded
			case "finish_nonterminal":
				run.FinishedAt = &after
			case "started_before_created":
				run.StartedAt = &before
			case "cancel_before_created":
				run.CancellationRequestedAt = &before
			case "finish_before_created":
				run.Status = domain.RunSucceeded
				run.FinishedAt = &before
			case "started_after_updated":
				run.StartedAt = &after
			case "cancel_after_updated":
				run.CancellationRequestedAt = &after
			case "finish_after_updated":
				run.Status = domain.RunSucceeded
				run.FinishedAt = &after
			}
			replaceExplanationRunPayload(t, f, run)
			if _, err := f.store.ReadRunExplanationV1(f.ctx, f.auth, run.ID); !errors.Is(err, ports.ErrRunExplanationUnavailable) {
				t.Fatalf("inconsistent %s got=%v want=unavailable", name, err)
			}
		})
	}
}

func TestRunExplanationYDBRunProjectionRejectsMalformedBase(t *testing.T) {
	for _, tc := range []struct {
		name, field        string
		value              any
		missing, rootArray bool
		want               error
	}{
		{name: "missing_required", field: "created_at", missing: true, want: ports.ErrRunExplanationUnavailable},
		{name: "numeric_id", field: "id", value: 123, want: ports.ErrRunExplanationUnavailable},
		{name: "array_session", field: "session_id", value: []string{"x"}, want: ports.ErrRunExplanationUnavailable},
		{name: "foreign_tenant", field: "tenant_id", value: "foreign-tenant", want: ports.ErrRunExplanationNotFound},
		{name: "overlong_id", field: "id", value: strings.Repeat("r", 161), want: ports.ErrRunExplanationUnavailable},
		{name: "unicode_oversize", field: "id", value: strings.Repeat("😀", 161), want: ports.ErrRunExplanationUnavailable},
		{name: "bad_optional_time", field: "started_at", value: "not-a-time", want: ports.ErrRunExplanationUnavailable},
		{name: "wrong_optional_type", field: "started_at", value: 17, want: ports.ErrRunExplanationUnavailable},
		{name: "array_optional", field: "started_at", value: []string{"2030-01-01T00:00:00Z"}, want: ports.ErrRunExplanationUnavailable},
		{name: "null_required", field: "created_at", value: nil, want: ports.ErrRunExplanationUnavailable},
		{name: "array_root", rootArray: true, want: ports.ErrRunExplanationUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newExplanationYDBFixture(t)
			fields := explanationRunBaseFields(t, f)
			if tc.missing {
				delete(fields, tc.field)
			} else if !tc.rootArray {
				fields[tc.field] = tc.value
			}
			if tc.name == "array_optional" {
				fields[tc.field] = []string{f.ingress.Run.CreatedAt.Format(time.RFC3339Nano)}
			}
			var value any = fields
			if tc.rootArray {
				value = []any{fields}
			}
			replaceExplanationRunPayload(t, f, value)
			if _, err := f.store.ReadRunExplanationV1(f.ctx, f.auth, f.ingress.Run.ID); !errors.Is(err, tc.want) {
				t.Fatalf("malformed %s got=%v want=%v", tc.name, err, tc.want)
			}
		})
	}
}
