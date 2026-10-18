package ydbstore

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/runexplanation"
)

// Read only the closed base Run scalars BEFORE transferring a row. A Run may
// contain other private metadata; neither fetching/stripping its full payload
// nor widening this route's existing 8-KiB source allowance is necessary.
// Explicit RETURNING Utf8 rejects non-string JSON scalars, unlike JSON_VALUE's
// implicit conversion. Strict required paths also reject lax array unwrapping.
// SUBSTRING requires String and measures bytes: cast only after JSON_VALUE has
// checked the JSON scalar type. Optional branches must have the same String type.
// https://ydb.tech/docs/en/yql/reference/builtins/json#json_value
// https://ydb.tech/docs/en/yql/reference/builtins/basic#substring
const explanationRunProjectionSQL = `SELECT
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

func readExplanationRun(ctx context.Context, query rowQuery, tenantID domain.TenantID, runID domain.RunID) (domain.Run, bool, error) {
	var fields [12]sql.NullString
	err := query.QueryRowContext(ctx, explanationRunProjectionSQL, tenantID, runID).Scan(
		&fields[0], &fields[1], &fields[2], &fields[3], &fields[4], &fields[5], &fields[6], &fields[7], &fields[8], &fields[9], &fields[10], &fields[11],
	)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Run{}, false, nil
	}
	if err != nil {
		return domain.Run{}, false, runexplanation.ErrCorruptProjection
	}
	// SQL caps at most 1,179 value bytes before transferring the row.
	// Check byte caps and complete grammar after Scan: a sentinel is an error,
	// never permission to accept a truncated identifier or timestamp.
	var total int
	for i, field := range fields {
		total += len(field.String)
		limit := 160
		if i == 5 {
			limit = 32
		} else if i >= 7 {
			limit = 35
		}
		if len(field.String) > limit || (!field.Valid && (i < 7 || i > 9)) {
			return domain.Run{}, false, runexplanation.ErrCorruptProjection
		}
	}
	if total > runexplanation.MaxProjectionBytes {
		return domain.Run{}, false, runexplanation.ErrCorruptProjection
	}
	created, err := parseExplanationRunTime(fields[10])
	if err != nil {
		return domain.Run{}, false, err
	}
	updated, err := parseExplanationRunTime(fields[11])
	if err != nil {
		return domain.Run{}, false, err
	}
	var optional [3]*time.Time
	for i := range optional {
		if !fields[i+7].Valid {
			continue
		}
		at, err := parseExplanationRunTime(fields[i+7])
		if err != nil {
			return domain.Run{}, false, err
		}
		if at.Before(created) {
			return domain.Run{}, false, runexplanation.ErrCorruptProjection
		}
		optional[i] = &at
	}
	// This temporary Run is a private read projection, never a write/update
	// source: omitted metadata must not be erased by reserializing it.
	run := domain.Run{ID: domain.RunID(fields[0].String), TenantID: domain.TenantID(fields[1].String), SessionID: domain.SessionID(fields[2].String), TriggerEventID: domain.SessionEventID(fields[3].String), SubscriptionConnectionID: domain.SubscriptionConnectionID(fields[4].String), Status: domain.RunStatus(fields[5].String), IdempotencyKey: domain.IdempotencyKey(fields[6].String), CancellationRequestedAt: optional[0], StartedAt: optional[1], FinishedAt: optional[2], CreatedAt: created, UpdatedAt: updated}
	if run.Validate() != nil {
		return domain.Run{}, false, runexplanation.ErrCorruptProjection
	}
	return run, true, nil
}

func parseExplanationRunTime(value sql.NullString) (time.Time, error) {
	if !value.Valid || len(value.String) == 0 || len(value.String) > 35 {
		return time.Time{}, runexplanation.ErrCorruptProjection
	}
	for _, b := range []byte(value.String) {
		if b > 127 {
			return time.Time{}, runexplanation.ErrCorruptProjection
		}
	}
	at, err := time.Parse(time.RFC3339Nano, value.String)
	if err != nil || at.IsZero() {
		return time.Time{}, runexplanation.ErrCorruptProjection
	}
	return at, nil
}
