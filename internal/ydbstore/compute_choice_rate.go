package ydbstore

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"gitcode.com/urandon/sessionless/internal/computechoice"
	"gitcode.com/urandon/sessionless/internal/runexplanationrate"
)

const computeChoiceRateReadSQL = `SELECT SUBSTRING(CAST(record AS String),0,8193), expire_at
 FROM compute_choice_rate_slots_v1 WHERE slot_id=$1`
const computeChoiceRateWriteSQL = `UPSERT INTO compute_choice_rate_slots_v1 (slot_id, record, expire_at)
 VALUES ($1, CAST($2 AS JsonDocument), $3)`

// Rate metadata belongs exclusively to this list, not Run explanation or the
// CHAT-1 quota. Current auth and cutover MUST precede this call, and candidates
// MUST be read with this same query/transaction. A decision is speculative until
// the caller's entire transaction confirms commit. Every replay reauthorizes.
func readComputeChoiceListRate(ctx context.Context, query *computeChoiceQuery, auth computeChoiceAuth, input computechoice.ListQuery, serverRequestID string) (runexplanationrate.Decision, error) {
	identity, keys, err := computechoice.ListRateIdentityKeys(auth.scope.TenantID, auth.scope.UserID)
	if err != nil {
		return runexplanationrate.Decision{}, errComputeChoiceSource
	}
	var slots [runexplanationrate.ProbeCount]*runexplanationrate.Slot
	for i, key := range keys {
		slots[i], err = readComputeChoiceRateSlot(ctx, query, key)
		if err != nil {
			return runexplanationrate.Decision{}, err
		}
	}
	decision, err := computechoice.EvaluateListRate(auth.at, auth.scope, input, slots, serverRequestID)
	if err != nil {
		return runexplanationrate.Decision{}, errComputeChoiceSource
	}
	if !decision.Debited {
		return decision, nil
	}
	chosen := false
	for _, key := range keys {
		chosen = chosen || decision.SlotID == key
	}
	if !decision.Allowed || !chosen || decision.Slot == nil || decision.Slot.IdentityDigest != identity {
		return runexplanationrate.Decision{}, errComputeChoiceSource
	}
	encoded, err := runexplanationrate.EncodeSlot(*decision.Slot)
	if err != nil {
		return runexplanationrate.Decision{}, errComputeChoiceSource
	}
	if _, err := query.ExecContext(ctx, computeChoiceRateWriteSQL, decision.SlotID, string(encoded), decision.Slot.ExpiresAt); err != nil {
		return runexplanationrate.Decision{}, err
	}
	return decision, nil
}

func readComputeChoiceRateSlot(ctx context.Context, query *computeChoiceQuery, key uint32) (*runexplanationrate.Slot, error) {
	if key >= runexplanationrate.MaxSlots {
		return nil, errComputeChoiceSource
	}
	var encoded string
	var expires time.Time
	err := query.QueryRowContext(ctx, computeChoiceRateReadSQL, key).Scan(&encoded, &expires)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, query.readError(err)
	}
	// Count the prefix, including an oversize sentinel, before any JSON decode.
	if err := query.budget.RecordMaterialized(computechoice.SourceOrdinary, int64(len(encoded))); err != nil {
		return nil, err
	}
	if err := query.budget.RecordMaterialized(computechoice.SourceOrdinary, 8); err != nil {
		return nil, err
	}
	slot, err := runexplanationrate.DecodeSlot([]byte(encoded))
	if err != nil || !computeChoiceTimestampEqual(slot.ExpiresAt, expires) {
		return nil, errComputeChoiceSource
	}
	return &slot, nil
}
