package ydbstore

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/runexplanation"
)

// Independent expected statement: the fixture refuses full JSON transfer,
// alternate paths, missing per-field caps or a non-point selector. It simulates
// these documented JSON_VALUE semantics; it is not a substitute for real YDB.
const explanationExpectedRunProjectionSQL = `SELECT
 SUBSTRING(JSON_VALUE(payload,"strict $.id" RETURNING Utf8 ERROR ON EMPTY ERROR ON ERROR),0,161),
 SUBSTRING(JSON_VALUE(payload,"strict $.tenant_id" RETURNING Utf8 ERROR ON EMPTY ERROR ON ERROR),0,161),
 SUBSTRING(JSON_VALUE(payload,"strict $.session_id" RETURNING Utf8 ERROR ON EMPTY ERROR ON ERROR),0,161),
 SUBSTRING(JSON_VALUE(payload,"strict $.trigger_event_id" RETURNING Utf8 ERROR ON EMPTY ERROR ON ERROR),0,161),
 SUBSTRING(JSON_VALUE(payload,"strict $.subscription_connection_id" RETURNING Utf8 ERROR ON EMPTY ERROR ON ERROR),0,161),
 SUBSTRING(JSON_VALUE(payload,"strict $.status" RETURNING Utf8 ERROR ON EMPTY ERROR ON ERROR),0,33),
 SUBSTRING(JSON_VALUE(payload,"strict $.idempotency_key" RETURNING Utf8 ERROR ON EMPTY ERROR ON ERROR),0,161),
 CASE WHEN JSON_VALUE(payload,"$.cancellation_requested_at.type()" RETURNING Utf8 NULL ON EMPTY ERROR ON ERROR)="null" THEN CAST(NULL AS Utf8) ELSE SUBSTRING(JSON_VALUE(payload,"$.cancellation_requested_at" RETURNING Utf8 NULL ON EMPTY ERROR ON ERROR),0,36) END,
 CASE WHEN JSON_VALUE(payload,"$.started_at.type()" RETURNING Utf8 NULL ON EMPTY ERROR ON ERROR)="null" THEN CAST(NULL AS Utf8) ELSE SUBSTRING(JSON_VALUE(payload,"$.started_at" RETURNING Utf8 NULL ON EMPTY ERROR ON ERROR),0,36) END,
 CASE WHEN JSON_VALUE(payload,"$.finished_at.type()" RETURNING Utf8 NULL ON EMPTY ERROR ON ERROR)="null" THEN CAST(NULL AS Utf8) ELSE SUBSTRING(JSON_VALUE(payload,"$.finished_at" RETURNING Utf8 NULL ON EMPTY ERROR ON ERROR),0,36) END,
 SUBSTRING(JSON_VALUE(payload,"strict $.created_at" RETURNING Utf8 ERROR ON EMPTY ERROR ON ERROR),0,36),
 SUBSTRING(JSON_VALUE(payload,"strict $.updated_at" RETURNING Utf8 ERROR ON EMPTY ERROR ON ERROR),0,36)
 FROM runs WHERE tenant_id=$1 AND run_id=$2`

func explanationRunProjectionRows(query string, args []driver.NamedValue, row []driver.Value) (driver.Rows, error) {
	if query != explanationExpectedRunProjectionSQL || len(args) != 2 {
		return nil, errors.New("fixture requires closed bounded Run scalar point projection")
	}
	if len(row) == 0 {
		return &explanationFixtureRows{}, nil
	}
	if len(row) != 1 {
		return nil, errors.New("fixture Run must hold one original JSON body")
	}
	raw, ok := row[0].(string)
	if !ok {
		return nil, errors.New("fixture Run JSON must be text")
	}
	var body map[string]json.RawMessage
	if json.Unmarshal([]byte(raw), &body) != nil || body == nil {
		return nil, errors.New("fixture strict Run root must be object")
	}
	keys := []string{"id", "tenant_id", "session_id", "trigger_event_id", "subscription_connection_id", "status", "idempotency_key", "cancellation_requested_at", "started_at", "finished_at", "created_at", "updated_at"}
	values := make([]driver.Value, len(keys))
	for i, key := range keys {
		value, exists := body[key]
		optional := i >= 7 && i <= 9
		if !exists || string(value) == "null" {
			if optional {
				continue
			}
			return nil, errors.New("fixture required JSON_VALUE is absent/null")
		}
		var scalar string
		if json.Unmarshal(value, &scalar) != nil {
			return nil, errors.New("fixture RETURNING Utf8 rejects non-string JSON value")
		}
		limit := 161
		if i == 5 {
			limit = 33
		} else if i >= 7 {
			limit = 36
		}
		runes := []rune(scalar)
		if len(runes) > limit {
			scalar = string(runes[:limit])
		}
		values[i] = scalar
	}
	return &explanationFixtureRows{values: values}, nil
}

func explanationRunWithExcludedMetadata(t *testing.T, raw string, bytes int) string {
	t.Helper()
	var value map[string]json.RawMessage
	if err := json.Unmarshal([]byte(raw), &value); err != nil {
		t.Fatal(err)
	}
	metadata, err := json.Marshal(strings.Repeat("private-derived-source-", bytes/23+1))
	if err != nil {
		t.Fatal(err)
	}
	value["derived_context_head"] = metadata
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if len(body) <= 8<<10 || len(body) > 32<<10 {
		t.Fatalf("test enlarged Run must occupy (8KiB,32KiB], got=%d", len(body))
	}
	return string(body)
}

// The SQL connector is scripted DATA; only the actual adapter call and its
// bounded statement/authorization behavior are exercised. The tagged YDB test
// separately verifies JSON_VALUE and point-plan semantics in the real engine.
func TestRunExplanationProjectsBaseRunBeforeTransfer(t *testing.T) {
	for _, bytes := range []int{10 << 10, 28 << 10} {
		t.Run(fmt.Sprintf("%dKiB", bytes>>10), func(t *testing.T) {
			store, f, auth, run := explanationReadFixture(t)
			f.rows["runs"] = []driver.Value{explanationRunWithExcludedMetadata(t, f.rows["runs"][0].(string), bytes)}
			got, err := store.ReadRunExplanationV1(context.Background(), auth, run.ID)
			if err != nil || got.RunID != run.ID {
				t.Fatalf("valid enlarged Run must remain readable: run=%s err=%v", got.RunID, err)
			}
			if len(f.queries) != 7 {
				t.Fatalf("projection changed point-statement budget: got=%d want=7", len(f.queries))
			}
			body, err := json.Marshal(got)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(body), "private-derived-source-") || len(body) > runexplanation.MaxResponseBytes {
				t.Fatal("excluded Run metadata entered WebUI response")
			}
		})
	}
}

func TestRatedRunExplanationProjectsBaseRunBeforeTransfer(t *testing.T) {
	store, f, auth, run := ratedReadFixture(t, false)
	f.rows["runs"] = []driver.Value{explanationRunWithExcludedMetadata(t, f.rows["runs"][0].(string), 28<<10)}
	got, err := store.ReadRatedRunExplanationV1(context.Background(), auth.WebSessionDigest, "projected-run", run.ID)
	assertRatedOutcome(t, got, err, ports.RunExplanationReadSuccessV1)
	if len(f.statements) != 12 || f.writes != 1 {
		t.Fatalf("projection changed rated point/debit budget: statements=%d writes=%d", len(f.statements), f.writes)
	}
}

func TestRunExplanationProjectionRejectsMalformedBase(t *testing.T) {
	for _, tc := range []struct {
		name, field string
		value       any
		missing     bool
		want        error
	}{
		{name: "missing_created", field: "created_at", missing: true, want: ports.ErrRunExplanationUnavailable},
		{name: "numeric_id", field: "id", value: 123, want: ports.ErrRunExplanationUnavailable},
		{name: "array_session", field: "session_id", value: []string{"session-test"}, want: ports.ErrRunExplanationUnavailable},
		{name: "foreign_tenant", field: "tenant_id", value: "other-tenant", want: ports.ErrRunExplanationNotFound},
		{name: "overlong_id", field: "id", value: strings.Repeat("r", 161), want: ports.ErrRunExplanationUnavailable},
		{name: "unicode_id", field: "id", value: strings.Repeat("😀", 161), want: ports.ErrRunExplanationUnavailable},
		{name: "malformed_optional", field: "started_at", value: "not-a-time", want: ports.ErrRunExplanationUnavailable},
		{name: "array_optional", field: "started_at", value: []string{"2030-01-01T00:00:00Z"}, want: ports.ErrRunExplanationUnavailable},
		{name: "numeric_optional", field: "started_at", value: 42, want: ports.ErrRunExplanationUnavailable},
		{name: "invalid_status", field: "status", value: "arbitrary", want: ports.ErrRunExplanationUnavailable},
		{name: "null_required", field: "updated_at", value: nil, want: ports.ErrRunExplanationUnavailable},
		{name: "optional_object", field: "finished_at", value: map[string]string{"value": "2030-01-01T00:00:00Z"}, want: ports.ErrRunExplanationUnavailable},
		{name: "overlong_time", field: "created_at", value: "2030-01-01T00:00:00Z" + strings.Repeat("x", 40), want: ports.ErrRunExplanationUnavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, f, auth, run := explanationReadFixture(t)
			var body map[string]any
			if err := json.Unmarshal([]byte(f.rows["runs"][0].(string)), &body); err != nil {
				t.Fatal(err)
			}
			if tc.missing {
				delete(body, tc.field)
			} else {
				body[tc.field] = tc.value
			}
			raw, err := json.Marshal(body)
			if err != nil {
				t.Fatal(err)
			}
			f.rows["runs"] = []driver.Value{string(raw)}
			_, err = store.ReadRunExplanationV1(context.Background(), auth, run.ID)
			if !errors.Is(err, tc.want) {
				t.Fatalf("malformed %s got=%v want=%v", tc.name, err, tc.want)
			}
		})
	}
}

func TestRunExplanationProjectionOptionalTimesAndRoot(t *testing.T) {
	t.Run("missing_and_null", func(t *testing.T) {
		store, f, auth, run := explanationReadFixture(t)
		var body map[string]any
		if err := json.Unmarshal([]byte(f.rows["runs"][0].(string)), &body); err != nil {
			t.Fatal(err)
		}
		body["started_at"], body["finished_at"], body["cancellation_requested_at"] = nil, nil, nil
		raw, err := json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
		f.rows["runs"] = []driver.Value{string(raw)}
		if _, err := store.ReadRunExplanationV1(context.Background(), auth, run.ID); err != nil {
			t.Fatalf("null optional timestamps denied: %v", err)
		}
	})
	t.Run("exact_times", func(t *testing.T) {
		store, f, auth, run := explanationReadFixture(t)
		zone := time.FixedZone("", 90*60)
		start := run.CreatedAt.Add(time.Second + 123*time.Nanosecond).In(zone)
		finish := run.CreatedAt.Add(2*time.Second + 456*time.Nanosecond).In(zone)
		cancel := run.CreatedAt.Add(1 * time.Nanosecond).In(zone)
		run.Status = domain.RunSucceeded
		run.StartedAt = &start
		run.FinishedAt = &finish
		run.CancellationRequestedAt = &cancel
		run.UpdatedAt = finish
		raw, err := json.Marshal(run)
		if err != nil {
			t.Fatal(err)
		}
		f.rows["runs"] = []driver.Value{string(raw)}
		ctx, done := context.WithTimeout(t.Context(), 3*time.Second)
		defer done()
		got, found, err := readExplanationRun(ctx, store.db, run.TenantID, run.ID)
		if err != nil || !found || got.StartedAt == nil || got.FinishedAt == nil || got.CancellationRequestedAt == nil {
			t.Fatalf("optional time projection missing: found=%t err=%v", found, err)
		}
		if !got.StartedAt.Equal(start) || !got.FinishedAt.Equal(finish) || !got.CancellationRequestedAt.Equal(cancel) || !got.UpdatedAt.Equal(finish) {
			t.Fatal("timestamp precision or offset semantics changed")
		}
		projected, err := store.ReadRunExplanationV1(context.Background(), auth, run.ID)
		if err != nil || projected.FinishedAt == nil || !projected.FinishedAt.Equal(finish) {
			t.Fatalf("actual Store timestamp projection: %v", err)
		}
	})
	t.Run("array_root", func(t *testing.T) {
		store, f, auth, run := explanationReadFixture(t)
		f.rows["runs"] = []driver.Value{"[" + f.rows["runs"][0].(string) + "]"}
		if _, err := store.ReadRunExplanationV1(context.Background(), auth, run.ID); !errors.Is(err, ports.ErrRunExplanationUnavailable) {
			t.Fatalf("array root accepted: %v", err)
		}
	})
}

func TestRunExplanationProjectionRejectsInconsistentTimes(t *testing.T) {
	for _, name := range []string{"updated_before_created", "future_updated", "terminal_missing_finish", "finish_nonterminal", "started_before_created", "cancel_before_created", "finish_before_created", "started_after_updated", "cancel_after_updated", "finish_after_updated"} {
		t.Run(name, func(t *testing.T) {
			store, f, auth, run := explanationReadFixture(t)
			before := run.CreatedAt.Add(-time.Nanosecond)
			after := run.UpdatedAt.Add(time.Nanosecond)
			switch name {
			case "updated_before_created":
				run.UpdatedAt = before
			case "future_updated":
				run.UpdatedAt = f.rows["clock"][0].(time.Time).Add(time.Second)
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
			raw, err := json.Marshal(run)
			if err != nil {
				t.Fatal(err)
			}
			f.rows["runs"] = []driver.Value{string(raw)}
			if _, err := store.ReadRunExplanationV1(context.Background(), auth, run.ID); !errors.Is(err, ports.ErrRunExplanationUnavailable) {
				t.Fatalf("inconsistent %s got=%v want=unavailable", name, err)
			}
		})
	}
}

func TestRunExplanationProjectionFixtureRejectsUnboundedOrWrongSQL(t *testing.T) {
	_, f, auth, run := explanationReadFixture(t)
	args := []driver.NamedValue{{Ordinal: 1, Value: string(auth.TenantID)}, {Ordinal: 2, Value: string(run.ID)}}
	for _, query := range []string{
		`SELECT payload FROM runs WHERE tenant_id=$1 AND run_id=$2`,
		`SELECT SUBSTRING(CAST(payload AS String),0,8193) FROM runs WHERE tenant_id=$1 AND run_id=$2`,
		strings.Replace(explanationExpectedRunProjectionSQL, ",0,161)", ",0,160)", 1),
		strings.Replace(explanationExpectedRunProjectionSQL, "strict $.id", "$.id", 1),
	} {
		if _, err := explanationRunProjectionRows(query, args, f.rows["runs"]); err == nil {
			t.Fatal("fixture accepted alternate/unbounded Run projection")
		}
	}
}
