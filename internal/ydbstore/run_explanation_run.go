package ydbstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/runexplanation"
)

// Read only the closed base Run scalars and local association BEFORE transfer.
// A Run may contain other private metadata; neither fetching its full payload
// nor widening this route's existing 8-KiB source allowance is necessary.
// Explicit RETURNING Utf8 rejects non-string JSON scalars, unlike JSON_VALUE's
// implicit conversion. Strict required paths also reject lax array unwrapping.
// SUBSTRING requires String and measures bytes: cast only after JSON_VALUE has
// checked the JSON scalar type. Optional branches must have the same String type.
// https://ydb.tech/docs/en/yql/reference/builtins/json#json_value
// JSON_QUERY returns Json? and errors on scalar results. Test presence in
// strict mode separately: a missing object key is an ERROR, not ON EMPTY.
// Present null/scalar associations therefore cannot fall back to subscription.
// https://ydb.tech/docs/en/yql/reference/builtins/json#json_query
// https://ydb.tech/docs/en/yql/reference/builtins/json#json_exists
// https://ydb.tech/docs/en/yql/reference/builtins/basic#substring
const explanationRunProjectionSQL = `SELECT
 SUBSTRING(CAST(JSON_VALUE(payload,"strict $.id" RETURNING Utf8 ERROR ON EMPTY ERROR ON ERROR) AS String),0,161),
 SUBSTRING(CAST(JSON_VALUE(payload,"strict $.tenant_id" RETURNING Utf8 ERROR ON EMPTY ERROR ON ERROR) AS String),0,161),
 SUBSTRING(CAST(JSON_VALUE(payload,"strict $.session_id" RETURNING Utf8 ERROR ON EMPTY ERROR ON ERROR) AS String),0,161),
 SUBSTRING(CAST(JSON_VALUE(payload,"strict $.trigger_event_id" RETURNING Utf8 ERROR ON EMPTY ERROR ON ERROR) AS String),0,161),
 CASE WHEN JSON_EXISTS(payload,"strict $.subscription_connection_id" FALSE ON ERROR) THEN CASE WHEN JSON_VALUE(payload,"strict $.subscription_connection_id.type()" RETURNING Utf8 ERROR ON EMPTY ERROR ON ERROR)="null" THEN CAST("" AS String) ELSE SUBSTRING(CAST(JSON_VALUE(payload,"strict $.subscription_connection_id" RETURNING Utf8 ERROR ON EMPTY ERROR ON ERROR) AS String),0,161) END ELSE CAST(NULL AS String) END,
 SUBSTRING(CAST(JSON_VALUE(payload,"strict $.status" RETURNING Utf8 ERROR ON EMPTY ERROR ON ERROR) AS String),0,33),
 SUBSTRING(CAST(JSON_VALUE(payload,"strict $.idempotency_key" RETURNING Utf8 ERROR ON EMPTY ERROR ON ERROR) AS String),0,161),
 CASE WHEN JSON_VALUE(payload,"$.cancellation_requested_at.type()" RETURNING Utf8 NULL ON EMPTY ERROR ON ERROR)="null" THEN CAST(NULL AS String) ELSE SUBSTRING(CAST(JSON_VALUE(payload,"$.cancellation_requested_at" RETURNING Utf8 NULL ON EMPTY ERROR ON ERROR) AS String),0,36) END,
 CASE WHEN JSON_VALUE(payload,"$.started_at.type()" RETURNING Utf8 NULL ON EMPTY ERROR ON ERROR)="null" THEN CAST(NULL AS String) ELSE SUBSTRING(CAST(JSON_VALUE(payload,"$.started_at" RETURNING Utf8 NULL ON EMPTY ERROR ON ERROR) AS String),0,36) END,
 CASE WHEN JSON_VALUE(payload,"$.finished_at.type()" RETURNING Utf8 NULL ON EMPTY ERROR ON ERROR)="null" THEN CAST(NULL AS String) ELSE SUBSTRING(CAST(JSON_VALUE(payload,"$.finished_at" RETURNING Utf8 NULL ON EMPTY ERROR ON ERROR) AS String),0,36) END,
 SUBSTRING(CAST(JSON_VALUE(payload,"strict $.created_at" RETURNING Utf8 ERROR ON EMPTY ERROR ON ERROR) AS String),0,36),
 SUBSTRING(CAST(JSON_VALUE(payload,"strict $.updated_at" RETURNING Utf8 ERROR ON EMPTY ERROR ON ERROR) AS String),0,36),
 CASE WHEN JSON_EXISTS(payload,"strict $.local_capacity_association" FALSE ON ERROR) THEN SUBSTRING(CAST(JSON_QUERY(payload,"strict $.local_capacity_association" WITHOUT WRAPPER ERROR ON EMPTY ERROR ON ERROR) AS String),0,1025) ELSE CAST(NULL AS String) END
 FROM runs WHERE tenant_id=$1 AND run_id=$2`

const explanationRunLocalAssociationBytes = 1024

func readExplanationRun(ctx context.Context, query rowQuery, tenantID domain.TenantID, runID domain.RunID) (domain.Run, bool, error) {
	var fields [12]sql.NullString
	var localObject sql.NullString
	err := query.QueryRowContext(ctx, explanationRunProjectionSQL, tenantID, runID).Scan(
		&fields[0], &fields[1], &fields[2], &fields[3], &fields[4], &fields[5], &fields[6], &fields[7], &fields[8], &fields[9], &fields[10], &fields[11],
		&localObject,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Run{}, false, nil
	}
	if err != nil {
		return domain.Run{}, false, runexplanation.ErrCorruptProjection
	}
	// SQL caps at most 2,204 value bytes before transferring the row. The local
	// producer's three 160-byte ASCII IDs and fixed members fit well below 1KiB.
	// This is part of the original 8KiB allowance, not another source budget.
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
		if len(field.String) > limit || (!field.Valid && i != 4 && (i < 7 || i > 9)) {
			return domain.Run{}, false, runexplanation.ErrCorruptProjection
		}
	}
	total += len(localObject.String)
	if len(localObject.String) > explanationRunLocalAssociationBytes || total > runexplanation.MaxProjectionBytes {
		return domain.Run{}, false, runexplanation.ErrCorruptProjection
	}
	var local *domain.LocalCapacityAssociationV1
	if localObject.Valid {
		// A present subscription, even empty/null in the stored document, is a
		// mixed arm. The SQL null sentinel keeps it distinct from an absent key.
		if fields[4].Valid {
			return domain.Run{}, false, runexplanation.ErrCorruptProjection
		}
		var association domain.LocalCapacityAssociationV1
		// Reuse the closed nested JsonDocument decoder. Normalized member order
		// or whitespace is not the standalone String codec's byte identity.
		if json.Unmarshal([]byte(localObject.String), &association) != nil {
			return domain.Run{}, false, runexplanation.ErrCorruptProjection
		}
		local = &association
	} else if !fields[4].Valid {
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
	run := domain.Run{ID: domain.RunID(fields[0].String), TenantID: domain.TenantID(fields[1].String), SessionID: domain.SessionID(fields[2].String), TriggerEventID: domain.SessionEventID(fields[3].String), SubscriptionConnectionID: domain.SubscriptionConnectionID(fields[4].String), LocalCapacityAssociation: local, Status: domain.RunStatus(fields[5].String), IdempotencyKey: domain.IdempotencyKey(fields[6].String), CancellationRequestedAt: optional[0], StartedAt: optional[1], FinishedAt: optional[2], CreatedAt: created, UpdatedAt: updated}
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
