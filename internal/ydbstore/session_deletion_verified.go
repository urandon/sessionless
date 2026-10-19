package ydbstore

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"time"

	"gitcode.com/urandon/sessionless/internal/compactioncontext"
	"gitcode.com/urandon/sessionless/internal/domain"
)

var errNoRetainedDeletionRuns = errors.New("session deletion contains no retained writer")

// Private trusted transaction path. Public lifecycle dispatch is deliberately
// kept unchanged until its genuine full lifecycle/causal fake proof is green.
// Neither this helper nor any public port accepts an approval/capture DTO.
func (store *Store) startRetainedSessionDeletion(ctx context.Context, tenant domain.TenantID, session domain.SessionID) (domain.SessionDeletion, error) {
	deny := func() (domain.SessionDeletion, error) { return domain.SessionDeletion{}, domain.ErrMembershipDenied }
	w := newRetainedDeletionWork(ctx)
	defer w.close()
	var original *sessionDeletionCapture
	err := w.transaction(store, tenant, func(tx *stateTx, _ time.Time) error {
		var e error
		original, e = buildRetainedDeletionCaptureWithWork(ctx, tx, session, w)
		if e != nil {
			return e
		}
		return authorizeRetainedDeletionCurrent(ctx, tx, w, original.deletion)
	})
	if err != nil {
		return domain.SessionDeletion{}, err
	}
	if original != nil && len(original.runs) == 0 && original.deletion.RetainedVerification == nil {
		return domain.SessionDeletion{}, errNoRetainedDeletionRuns
	}
	if original == nil || len(original.runs) == 0 {
		return deny()
	}
	if original.deletion.State == domain.SessionDeletionDeleting {
		if validateRetainedDeletionCapture(original) != nil {
			return deny()
		}
		return original.deletion, nil // SQL-only fresh Store restart; no reader needed.
	}
	if original.deletion.State != domain.SessionDeletionRequested || original.deletion.RetainedVerification != nil || store.sessionDeletionBlobReader == nil {
		return deny()
	}
	selected := map[domain.AttemptID]domain.RunID{}
	for _, run := range original.runs {
		if _, duplicate := selected[run.Attempt]; duplicate {
			return deny()
		}
		selected[run.Attempt] = run.ID
	}
	var previous *compactioncontext.RetainedV1
	for _, key := range original.runs {
		var writer retainedDeletionWriterData
		err = w.transaction(store, tenant, func(tx *stateTx, _ time.Time) error {
			query, e := w.forRun(key.ID)
			if e != nil {
				return e
			}
			q := &deletionCaptureQuery{query: query, budget: w.readBudget}
			run, found, e := readDeletionCaptureJSON[domain.Run](ctx, q, "runs", "payload", tenant, key.ID)
			digest, de := deletionRunDigest(run)
			anchor, owned := original.ownership.events[key.Anchor]
			if e != nil || !found || de != nil || digest != key.RunDigest || !owned || key.Sequence == 0 || anchor.Sequence != key.Sequence {
				return domain.ErrMembershipDenied
			}
			writer, e = readRetainedDeletionWriter(query.run.ctx, query.run.query, run, anchor, original.attemptRows[key.Attempt])
			return e
		})
		if err != nil {
			return domain.SessionDeletion{}, err
		}
		// The SQL callback has CLOSED. Same original Run context/budget remains;
		// blob reads cannot acquire a new deadline or be performed inside SQL.
		previous, err = verifyRetainedDeletionBodiesWithWriter(w.runs[key.ID].ctx, store.sessionDeletionBlobReader, writer.producer.terminal, original.ownership, previous, &writer)
		if err != nil {
			return domain.SessionDeletion{}, err
		}
	}
	var result domain.SessionDeletion
	err = w.transaction(store, tenant, func(tx *stateTx, at time.Time) error {
		current, e := buildRetainedDeletionCaptureWithWork(ctx, tx, session, w)
		if e != nil || !equalRetainedDeletionCapture(original, current) {
			return domain.ErrMembershipDenied
		}
		if authorizeRetainedDeletionCurrent(ctx, tx, w, current.deletion) != nil || validateRetainedDeletionOtherAdmissions(ctx, tx, w, current, selected) != nil {
			return domain.ErrMembershipDenied
		}
		d := current.deletion
		if d.State != domain.SessionDeletionRequested || d.RetainedVerification != nil || d.Start(at) != nil {
			return domain.ErrMembershipDenied
		}
		request, e := domain.SessionDeletionRequestDigestV1(d)
		inventory, ie := domain.SessionDeletionInventoryDigestV1(current.inventory)
		if e != nil || ie != nil {
			return domain.ErrMembershipDenied
		}
		d.RetainedVerification = &domain.RetainedDeletionVerificationV1{Schema: "sessionless.retained-deletion.v1", TenantID: tenant, SessionID: session, RequestDigest: request, MetadataDigest: current.metadata, InventoryDigest: inventory, Rows: current.rows, Objects: uint64(len(current.inventory.Objects)), Bytes: current.inventory.TotalBytes, VerifiedRuns: uint64(len(current.runs)), VerifiedAt: at}
		if d.Validate() != nil || d.RetainedVerification.ValidateInventory(d, current.inventory) != nil {
			return domain.ErrMembershipDenied
		}
		if e := writeSessionDeletionTx(ctx, tx, d); e != nil {
			return e
		}
		if e := appendSessionLifecycleAuditTx(ctx, tx, at, d.RequestedBy, "session.deletion.start", session, nil); e != nil {
			return e
		}
		result = d
		return nil
	})
	return result, err
}

func (store *Store) retainedSessionDeletionInventory(ctx context.Context, tenant domain.TenantID, session domain.SessionID, maxRows, maxObjects uint64) (domain.SessionDeletionInventory, error) {
	w := newRetainedDeletionWork(ctx)
	defer w.close()
	var result domain.SessionDeletionInventory
	err := w.transaction(store, tenant, func(tx *stateTx, _ time.Time) error {
		c, e := buildRetainedDeletionCaptureWithWork(ctx, tx, session, w)
		if e != nil {
			return e
		}
		if c.deletion.State == domain.SessionDeletionDeleting && validateRetainedDeletionCapture(c) != nil {
			return domain.ErrMembershipDenied
		}
		if c.rows > maxRows || uint64(len(c.inventory.Objects)) > maxObjects {
			return domain.ErrMembershipDenied
		}
		result = c.inventory
		return nil
	})
	return result, err
}

// This is a bounded DATA-only routing read, never destructive admission. Use
// the existing indexed/legacy identity union, then bounded canonical Run rows.
// Unknown/missing rows deny, rather than falling back to an incomplete Plan.
func (store *Store) sessionDeletionContainsCustody(ctx context.Context, tenant domain.TenantID, session domain.SessionID) (bool, error) {
	legacy := &sessionDeletionInventoryQuery{query: store.db}
	ids, e := legacy.listSessionRunIDs(ctx, tenant, session, maxSessionDeletionRows+1)
	if e != nil || uint64(len(ids)) > maxSessionDeletionRows {
		return false, domain.ErrMembershipDenied
	}
	q := newDeletionCaptureQuery(store.db)
	if q.budget.admitOriginalRuns(uint64(len(ids))) != nil {
		return false, domain.ErrMembershipDenied
	}
	for _, id := range ids {
		run, found, e := readDeletionCaptureJSON[domain.Run](ctx, q, "runs", "payload", tenant, id)
		if e != nil || !found || run.Validate() != nil || run.ID != id || run.TenantID != tenant || run.SessionID != session {
			return false, domain.ErrMembershipDenied
		}
		if run.TerminalCustody != nil {
			return true, nil
		}
	}
	return false, nil
}

func equalRetainedDeletionCapture(a, b *sessionDeletionCapture) bool {
	if a == nil || b == nil || a.metadata != b.metadata || a.rows != b.rows || !reflect.DeepEqual(a.runs, b.runs) || !reflect.DeepEqual(a.admissions, b.admissions) || !reflect.DeepEqual(a.attemptRows, b.attemptRows) {
		return false
	}
	ar, ae := domain.SessionDeletionRequestDigestV1(a.deletion)
	br, be := domain.SessionDeletionRequestDigestV1(b.deletion)
	ai, aie := domain.SessionDeletionInventoryDigestV1(a.inventory)
	bi, bie := domain.SessionDeletionInventoryDigestV1(b.inventory)
	return ae == nil && be == nil && aie == nil && bie == nil && ar == br && ai == bi
}

func validateRetainedDeletionCapture(v *sessionDeletionCapture) error {
	if v == nil || v.deletion.State != domain.SessionDeletionDeleting || v.deletion.RetainedVerification == nil || v.deletion.Validate() != nil || v.deletion.RetainedVerification.ValidateInventory(v.deletion, v.inventory) != nil {
		return domain.ErrMembershipDenied
	}
	c := v.deletion.RetainedVerification
	if c.MetadataDigest != v.metadata || c.Rows != v.rows || c.VerifiedRuns != uint64(len(v.runs)) || len(v.runs) == 0 {
		return domain.ErrMembershipDenied
	}
	return nil
}

func readRetainedDeletionPoint[T any](ctx context.Context, q *deletionCaptureQuery, s string, args ...any) (v T, found bool, err error) {
	var raw string
	err = q.QueryRowContext(ctx, s, args...).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return v, false, nil
	}
	if err != nil || len(raw) > ownedAuthorityHeadBytes || chargeDeletionCaptureSQL(q, uint64(len(raw))) != nil || decodeOwnedEffectCanonicalJSON(raw, &v) != nil {
		return v, false, domain.ErrMembershipDenied
	}
	return v, true, nil
}

// Current authorization is separate from the historical commitment. No Source,
// expired profile, lease or registered allocation is reopened for deletion.
func authorizeRetainedDeletionCurrent(ctx context.Context, tx *stateTx, w *retainedDeletionWork, d domain.SessionDeletion) error {
	if d.TenantID != tx.tenantID || d.Validate() != nil {
		return domain.ErrMembershipDenied
	}
	q := newDeletionCaptureQuery(tx.sqlTx)
	q.budget = w.readBudget
	bucket, e := webBucket(string(d.RequestedBy))
	if e != nil {
		return e
	}
	membership, found, e := readRetainedDeletionPoint[domain.TenantMembership](ctx, q, `SELECT SUBSTRING(CAST(record AS String),0,32769) FROM tenant_memberships WHERE user_bucket=$1 AND user_id=$2 AND tenant_id=$3`, bucket, d.RequestedBy, d.TenantID)
	if e != nil || !found || membership.Authorize(d.RequestedBy, d.TenantID, domain.TenantPermissionWrite) != nil {
		return domain.ErrMembershipDenied
	}
	p, found, e := readRetainedDeletionPoint[domain.SessionParticipant](ctx, q, `SELECT SUBSTRING(CAST(record AS String),0,32769) FROM session_participants WHERE tenant_id=$1 AND session_id=$2 AND user_id=$3`, d.TenantID, d.SessionID, d.RequestedBy)
	if e != nil || !found || p.Validate() != nil || p.Status != domain.SessionParticipantActive || p.Role != domain.SessionParticipantOwner || p.Authorize(d.TenantID, d.SessionID, d.RequestedBy, true) != nil {
		return domain.ErrMembershipDenied
	}
	hold, found, e := readRetainedDeletionPoint[domain.SessionLegalHold](ctx, q, `SELECT SUBSTRING(CAST(record AS String),0,32769) FROM session_legal_holds WHERE tenant_id=$1 AND session_id=$2`, d.TenantID, d.SessionID)
	if e != nil || found && (hold.Validate() != nil || hold.TenantID != d.TenantID || hold.SessionID != d.SessionID || hold.State == domain.SessionLegalHoldActive || hold.ReleasedAt != nil && d.RequestedAt.Before(*hold.ReleasedAt)) {
		return domain.ErrMembershipDenied
	}
	return nil
}

// Every nonselected admission must still be genuinely unprepared; the full
// capture commits ALL child ranges. A selected successful writer never lends
// its proof to another Attempt, a missing reverse owner or a prepared retry.
func validateRetainedDeletionOtherAdmissions(ctx context.Context, tx *stateTx, w *retainedDeletionWork, c *sessionDeletionCapture, selected map[domain.AttemptID]domain.RunID) error {
	for _, key := range c.admissions {
		if run, ok := selected[key.attempt]; ok {
			if run != key.run {
				return domain.ErrMembershipDenied
			}
			continue
		}
		counts := c.attemptRows[key.attempt]
		if counts.parents > 1 || counts.evidence != 0 || counts.operations != 0 || counts.calls != 0 || counts.proxies != 0 || counts.reconciliation != 0 {
			return domain.ErrMembershipDenied
		}
		query, e := w.forRun(key.run)
		if e != nil {
			return e
		}
		q := &deletionCaptureQuery{query: query, budget: w.readBudget}
		original, found, e := readRetainedDeletionPoint[domain.AttemptExecutionAdmissionV1](query.run.ctx, q, `SELECT SUBSTRING(CAST(record AS String),0,32769) FROM attempt_execution_admissions_v1 WHERE tenant_id=$1 AND attempt_id=$2`, tx.tenantID, key.attempt)
		if e != nil || !found || original.Source.Scope.TenantID != tx.tenantID || original.Source.Scope.SessionID != c.inventory.SessionID || original.Source.Scope.RunID != key.run || original.Source.Scope.AttemptID != key.attempt {
			return domain.ErrMembershipDenied
		}
		a, e := readOwnedAdmission(query.run.ctx, query.run.query, original.Source.Scope)
		if e != nil || a == nil {
			return domain.ErrMembershipDenied
		}
		if e := validateRetainedDeletionUnpreparedParent(ctx, query.run.query, *a); e != nil {
			return e
		}
	}
	return nil
}

// Read-only form of the existing unprepared-parent gate, including the newer
// residual attachment. It does not execute DELETE during eligibility checks.
func validateRetainedDeletionUnpreparedParent(ctx context.Context, q *ownedAuthorityQuery, a domain.AttemptExecutionAdmissionV1) error {
	var raw, digest string
	var legacy, recordPresent, digestPresent, children uint64
	err := q.scanMetadata(ctx, `SELECT
 COALESCE(SUBSTRING(owned_record,0,32769),CAST('' AS String)),
 COALESCE(SUBSTRING(CAST(owned_digest AS String),0,65),CAST('' AS String)),
 CASE WHEN record IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 CASE WHEN owned_record IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 CASE WHEN owned_digest IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 CASE WHEN preparation_started_at IS NOT NULL OR preparation_parent_digest IS NOT NULL
 OR allocation_original_record IS NOT NULL OR allocation_original_digest IS NOT NULL
 OR allocation_current_record IS NOT NULL OR allocation_current_digest IS NOT NULL
 OR allocation_publication_digest IS NOT NULL OR allocation_transition_parent_digest IS NOT NULL
 OR proxy_original_record IS NOT NULL OR proxy_original_digest IS NOT NULL
 OR proxy_current_record IS NOT NULL OR proxy_current_digest IS NOT NULL
 OR proxy_policy_body IS NOT NULL OR proxy_attestation_body IS NOT NULL
 OR operation_accounting_record IS NOT NULL OR operation_accounting_digest IS NOT NULL
 OR web_search_consent_record IS NOT NULL OR web_search_consent_digest IS NOT NULL
 OR operation_residual_record IS NOT NULL
 THEN CAST(1 AS Uint64) ELSE CAST(0 AS Uint64) END
 FROM attempt_effect_reservations WHERE tenant_id=$1 AND attempt_id=$2 AND kind=$3`, []any{a.Source.Scope.TenantID, a.Source.Scope.AttemptID, domain.ProviderEffectTurnV1}, &raw, &digest, &legacy, &recordPresent, &digestPresent, &children)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil || legacy != 0 || recordPresent != 1 || digestPresent != 1 || children != 0 || len(raw) > ownedAuthorityHeadBytes || len(digest) != 64 {
		return q.budget.deny()
	}
	p, e := domain.DecodeOwnedAttemptEffectParentV1([]byte(raw))
	actual, de := p.Digest()
	if e != nil || de != nil || actual != digest || p.ValidateForAdmission(a) != nil {
		return q.budget.deny()
	}
	return nil
}

// Same canonical completion transaction: reconstruct SQL-only, compare the
// ORIGINAL trusted capture, recheck current owner/hold, retire ONLY its proven
// successful scopes, then reuse canonical Session row deletion. No old guard
// is weakened and no body is reopened after physical object deletion.
func (store *Store) completeRetainedSessionDeletion(ctx context.Context, tenant domain.TenantID, session domain.SessionID, objects, bytes uint64) (domain.SessionDeletion, error) {
	w := newRetainedDeletionWork(ctx)
	defer w.close()
	var result domain.SessionDeletion
	err := w.transaction(store, tenant, func(tx *stateTx, at time.Time) error {
		c, e := buildRetainedDeletionCaptureWithWork(ctx, tx, session, w)
		if e != nil || validateRetainedDeletionCapture(c) != nil {
			return domain.ErrMembershipDenied
		}
		if objects != uint64(len(c.inventory.Objects)) || bytes != c.inventory.TotalBytes || authorizeRetainedDeletionCurrent(ctx, tx, w, c.deletion) != nil {
			return domain.ErrMembershipDenied
		}
		selected := map[domain.AttemptID]domain.RunID{}
		for _, run := range c.runs {
			if _, duplicate := selected[run.Attempt]; duplicate {
				return domain.ErrMembershipDenied
			}
			selected[run.Attempt] = run.ID
		}
		if validateRetainedDeletionOtherAdmissions(ctx, tx, w, c, selected) != nil {
			return domain.ErrMembershipDenied
		}
		for _, key := range c.runs {
			query, e := w.forRun(key.ID)
			if e != nil {
				return e
			}
			q := &deletionCaptureQuery{query: query, budget: w.readBudget}
			run, found, e := readDeletionCaptureJSON[domain.Run](ctx, q, "runs", "payload", tenant, key.ID)
			digest, de := deletionRunDigest(run)
			if e != nil || !found || de != nil || digest != key.RunDigest || run.TerminalCustody == nil || run.TerminalCustody.LocalEvidence == nil {
				return domain.ErrMembershipDenied
			}
			// Scope comes from the exact SAME capture just rejoined above. These
			// complete child ranges are not independently discoverable authority.
			for _, table := range []string{"attempt_effect_reconciliation_evidence", "attempt_operation_calls_v1", "attempt_operations_v1", "attempt_operation_proxies_v1", "substrate_execution_evidence"} {
				if _, e := tx.sqlTx.ExecContext(ctx, "DELETE FROM "+table+" WHERE tenant_id=$1 AND attempt_id=$2", tenant, key.Attempt); e != nil {
					return e
				}
			}
			if _, e := tx.sqlTx.ExecContext(ctx, `DELETE FROM attempt_effect_reservations WHERE tenant_id=$1 AND attempt_id=$2 AND kind=$3 AND owned_digest=$4`, tenant, key.Attempt, domain.ProviderEffectTurnV1, run.TerminalCustody.LocalEvidence.ParentDigest); e != nil {
				return e
			}
			if _, e := tx.sqlTx.ExecContext(ctx, `DELETE FROM attempt_execution_admissions_v1 WHERE tenant_id=$1 AND attempt_id=$2 AND run_id=$3 AND session_id=$4`, tenant, key.Attempt, key.ID, session); e != nil {
				return e
			}
			if _, e := tx.sqlTx.ExecContext(ctx, `DELETE FROM attempt_execution_admissions_by_session_v1 WHERE tenant_id=$1 AND session_id=$2 AND attempt_id=$3 AND run_id=$4`, tenant, session, key.Attempt, key.ID); e != nil {
				return e
			}
		}
		// Existing unprepared-parent guard remains unchanged for ALL remaining
		// admissions. Every destructive operation is in this single transaction.
		if e := deleteSessionRowsTx(ctx, tx, session); e != nil {
			return e
		}
		d := c.deletion
		if d.Complete(at, objects, bytes) != nil {
			return domain.ErrMembershipDenied
		}
		if e := writeSessionDeletionTx(ctx, tx, d); e != nil {
			return e
		}
		if e := appendSessionLifecycleAuditTx(ctx, tx, at, d.RequestedBy, "session.deletion.complete", session, map[string]any{"deleted_objects": objects, "deleted_bytes": bytes}); e != nil {
			return e
		}
		result = d
		return nil
	})
	return result, err
}
