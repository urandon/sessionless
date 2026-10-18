package ydbstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// Same canonical point read and 32-KiB sentinel as the owned authority path.
// The ordinary reader also bounds bytes before JSON decoding; the owned reader
// keeps its original snapshot cache, accounting and write invalidation.
const boundedRunPayloadSQL = `SELECT SUBSTRING(CAST(payload AS String),0,32769) FROM runs WHERE tenant_id = $1 AND run_id = $2`

func readRunPayload(ctx context.Context, query rowQuery, tenant domain.TenantID, id domain.RunID) (domain.Run, bool, error) {
	var raw string
	var err error
	owned, isOwned := query.(*ownedAuthorityQuery)
	if isOwned {
		raw, err = owned.readMetadata(ctx, boundedRunPayloadSQL, ownedAuthorityHeadBytes, tenant, id)
	} else {
		err = query.QueryRowContext(ctx, boundedRunPayloadSQL, tenant, id).Scan(&raw)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Run{}, false, nil
	}
	if err != nil {
		return domain.Run{}, false, err
	}
	if len(raw) > ownedAuthorityHeadBytes {
		return domain.Run{}, false, domain.ValidationError{Field: "run", Reason: "encoded payload exceeds 32 KiB"}
	}
	var run domain.Run
	err = json.Unmarshal([]byte(raw), &run)
	if err == nil && run.DerivedContext != nil {
		err = run.DerivedContext.ValidateForRun(run)
	}
	if err != nil {
		if isOwned {
			return domain.Run{}, false, owned.budget.deny()
		}
		return domain.Run{}, false, fmt.Errorf("decode run payload: %w", err)
	}
	return run, true, nil
}

// All physical Run writers must use this codec before ANY mutation, including
// explanation invalidation. This serializes the complete Run, not a projection.
func marshalRunPayload(run domain.Run) (string, error) {
	payload, err := marshal(run)
	if err != nil {
		return "", err
	}
	if len(payload) > ownedAuthorityHeadBytes {
		return "", domain.ValidationError{Field: "run", Reason: "encoded payload exceeds 32 KiB"}
	}
	return payload, nil
}
