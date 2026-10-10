package ydbstore

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/runexplanation"
	"gitcode.com/urandon/sessionless/internal/runexplanationrate"
)

var _ ports.RunExplanationRatedReadStoreV1 = (*Store)(nil)

// ReadRatedRunExplanationV1 resolves current READ authority from the cookie
// digest in its only transaction. Only finite rate metadata is mutable here.
// Debit receipts make authTx's idempotent retries safe after a lost commit ACK;
// an unresolved commit never publishes a response or promises a refund.
func (store *Store) ReadRatedRunExplanationV1(ctx context.Context, sessionDigest domain.SecretDigest, serverRequestID string, runID domain.RunID) (ports.RunExplanationReadOutcomeV1, error) {
	var pending ports.RunExplanationReadOutcomeV1
	if sessionDigest.Validate("web_session.session_digest") != nil || domain.ValidateOpaqueID("request_id", serverRequestID) != nil || runID.Validate() != nil {
		return pending, ports.ErrRunExplanationUnavailable
	}
	selector, err := runexplanationrate.SelectorDigest(runID)
	if err != nil {
		return pending, ports.ErrRunExplanationUnavailable
	}
	ctx, cancel := context.WithTimeout(ctx, runexplanation.ResourceDeadline)
	defer cancel()
	budget := &explanationStatementBudget{}
	err = store.authTx(ctx, "run_explanation.rated_read_v1", func(sqlTx *sql.Tx) error {
		pending = ports.RunExplanationReadOutcomeV1{}
		query := explanationBudgetQuery{tx: sqlTx, budget: budget}
		session, at, err := readExplanationSession(ctx, query, sessionDigest)
		if err != nil {
			return err
		}
		if _, err := readExplanationMembership(ctx, query, session, at); err != nil {
			return err
		}
		identity, candidates, err := runexplanationrate.IdentityKeys(session.ActiveTenantID, session.UserID)
		if err != nil {
			return err
		}
		var slots [runexplanationrate.ProbeCount]*runexplanationrate.Slot
		for i, slotID := range candidates {
			slots[i], err = readExplanationRateSlot(ctx, query, slotID)
			if err != nil {
				return err
			}
		}
		decision, err := runexplanationrate.Evaluate(at.UTC(), identity, candidates, slots, serverRequestID, selector)
		if err != nil {
			return err
		}
		if !decision.Allowed {
			pending = ports.RunExplanationReadOutcomeV1{Kind: ports.RunExplanationReadRateLimitedV1, RetryAfter: decision.RetryAfter}
			return pending.Validate()
		}
		if decision.Debited {
			if decision.Slot == nil || decision.SlotID >= runexplanationrate.MaxSlots {
				return runexplanationrate.ErrInvalidState
			}
			chosen := false
			for _, candidate := range candidates {
				chosen = chosen || candidate == decision.SlotID
			}
			if !chosen || decision.Slot.IdentityDigest != identity {
				return runexplanationrate.ErrInvalidState
			}
			encoded, err := runexplanationrate.EncodeSlot(*decision.Slot)
			if err != nil {
				return err
			}
			if _, err := query.ExecContext(ctx, `UPSERT INTO run_explanation_rate_slots_v1 (slot_id, record, expire_at) VALUES ($1, CAST($2 AS JsonDocument), $3)`, decision.SlotID, string(encoded), decision.Slot.ExpiresAt); err != nil {
				return err
			}
		}
		// Resource authority exists only after the canonical session/member
		// resolution. This is the same SQL transaction, never store.Transact.
		tx := &stateTx{store: store, sqlTx: sqlTx, tenantID: session.ActiveTenantID, explanationQuery: query}
		value, err := readExplanationResource(ctx, tx, session.UserID, runID, at)
		if errors.Is(err, ports.ErrRunExplanationNotFound) {
			pending = ports.RunExplanationReadOutcomeV1{Kind: ports.RunExplanationReadNotFoundV1}
			return pending.Validate()
		}
		if err != nil {
			return err
		}
		pending = ports.RunExplanationReadOutcomeV1{Kind: ports.RunExplanationReadSuccessV1, Explanation: &value}
		return pending.Validate()
	})
	if err != nil {
		return ports.RunExplanationReadOutcomeV1{}, explanationReadError(err)
	}
	if err := pending.Validate(); err != nil {
		return ports.RunExplanationReadOutcomeV1{}, ports.ErrRunExplanationUnavailable
	}
	return pending, nil
}

func readExplanationRateSlot(ctx context.Context, query rowQuery, slotID uint32) (*runexplanationrate.Slot, error) {
	if slotID >= runexplanationrate.MaxSlots {
		return nil, runexplanationrate.ErrInvalidState
	}
	var encoded string
	var expires time.Time
	err := query.QueryRowContext(ctx, `SELECT SUBSTRING(CAST(record AS String),0,8193), expire_at FROM run_explanation_rate_slots_v1 WHERE slot_id=$1`, slotID).Scan(&encoded, &expires)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	slot, err := runexplanationrate.DecodeSlot([]byte(encoded))
	if err != nil {
		return nil, err
	}
	if !expires.Equal(slot.ExpiresAt) {
		return nil, runexplanationrate.ErrInvalidState
	}
	return &slot, nil
}
