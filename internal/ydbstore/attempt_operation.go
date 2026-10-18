package ydbstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

var _ ports.AttemptOperationStoreV1 = (*Store)(nil)

const ownedOperationReadSQL = `SELECT SUBSTRING(record,0,32769),SUBSTRING(CAST(digest AS String),0,65),
 SUBSTRING(CAST(operation_digest AS String),0,65),SUBSTRING(CAST(call_id AS String),0,161),
 CASE WHEN observation_record IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 CASE WHEN observation_digest IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 COALESCE(SUBSTRING(observation_record,0,32769),CAST('' AS String)),
 COALESCE(SUBSTRING(CAST(observation_digest AS String),0,65),CAST('' AS String))
 FROM attempt_operations_v1 WHERE tenant_id=$1 AND attempt_id=$2 AND sequence=$3`

const ownedOperationCallSQL = `SELECT sequence,SUBSTRING(CAST(operation_digest AS String),0,65)
 FROM attempt_operation_calls_v1 WHERE tenant_id=$1 AND attempt_id=$2 AND call_id=$3`

type ownedOperationRow struct {
	reservation domain.AttemptOperationReservationV1
	observation *domain.AttemptOperationObservationV1
}

// ownedOperationColumns is the bounded persisted representation shared by
// point reads and complete original-source inventory reads. It grants nothing.
type ownedOperationColumns struct {
	raw, digest, operationDigest, call, observedRaw, observedDigest string
	observationPresent, digestPresent                               uint64
}

func (c *ownedOperationColumns) destinations() []any {
	return []any{&c.raw, &c.digest, &c.operationDigest, &c.call, &c.observationPresent, &c.digestPresent, &c.observedRaw, &c.observedDigest}
}

func (c ownedOperationColumns) decode(q *ownedAuthorityQuery, scope domain.AttemptOperationScopeV1, sequence uint64) (*ownedOperationRow, error) {
	var r domain.AttemptOperationReservationV1
	if len(c.raw) > ownedAuthorityHeadBytes || len(c.observedRaw) > ownedAuthorityHeadBytes || json.Unmarshal([]byte(c.raw), &r) != nil || r.Validate() != nil {
		return nil, q.budget.deny()
	}
	canonical, _ := json.Marshal(r)
	actual, _ := r.Digest()
	if !bytes.Equal(canonical, []byte(c.raw)) || string(actual) != c.digest || string(r.OperationDigest) != c.operationDigest || r.Operation.Scope.TenantID != scope.TenantID || r.Operation.Scope.AttemptID != scope.AttemptID || r.Operation.Sequence != sequence || r.Operation.CallID != c.call {
		return nil, q.budget.deny()
	}
	result := &ownedOperationRow{reservation: r.Clone()}
	if c.observationPresent == 0 && c.digestPresent == 0 && c.observedRaw == "" && c.observedDigest == "" {
		return result, nil
	}
	var o domain.AttemptOperationObservationV1
	if c.observationPresent != 1 || c.digestPresent != 1 || json.Unmarshal([]byte(c.observedRaw), &o) != nil || o.ValidateForReservation(r) != nil {
		return nil, q.budget.deny()
	}
	oc, _ := json.Marshal(o)
	od, _ := o.Digest()
	if !bytes.Equal(oc, []byte(c.observedRaw)) || string(od) != c.observedDigest || o.ObservedAt.After(q.at) {
		return nil, q.budget.deny()
	}
	result.observation = &o
	return result, nil
}

func readOwnedOperation(ctx context.Context, q *ownedAuthorityQuery, op domain.AttemptOperationV1) (*ownedOperationRow, error) {
	var columns ownedOperationColumns
	err := q.scanMetadata(ctx, ownedOperationReadSQL, []any{op.Scope.TenantID, op.Scope.AttemptID, op.Sequence}, columns.destinations()...)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, q.budget.deny()
	}
	result, err := columns.decode(q, op.Scope, op.Sequence)
	if err != nil {
		return nil, err
	}
	r := result.reservation
	var indexedSequence uint64
	var indexedDigest string
	if q.scanMetadata(ctx, ownedOperationCallSQL, []any{r.Operation.Scope.TenantID, r.Operation.Scope.AttemptID, r.Operation.CallID}, &indexedSequence, &indexedDigest) != nil || indexedSequence != r.Operation.Sequence || indexedDigest != columns.operationDigest {
		return nil, q.budget.deny()
	}
	return result, nil
}

func operationParentRequest(scope domain.AttemptOperationScopeV1) ports.ReserveAttemptEffectRequestV1 {
	return ports.ReserveAttemptEffectRequestV1{TenantID: scope.TenantID, RunID: scope.RunID, AttemptID: scope.AttemptID, LeaseID: scope.LeaseID, FenceToken: scope.FenceToken, PhysicalInvocationClaimID: scope.PhysicalInvocationClaimID}
}

// The subordinate scope deliberately does not carry the outer upstream key.
// Preserve that exact original value from the canonical retained parent, not
// from an operation caller, before authenticating the complete parent join.
func (store *Store) operationParent(ctx context.Context, q *ownedAuthorityQuery, scope domain.AttemptOperationScopeV1) (ports.ReserveAttemptEffectRequestV1, domain.OwnedAttemptEffectParentV1, domain.AttemptExecutionSourceV1, time.Time, error) {
	request := operationParentRequest(scope)
	original, err := readOwnedAttemptEffectParent(ctx, q, request)
	if err != nil || original == nil {
		return request, domain.OwnedAttemptEffectParentV1{}, domain.AttemptExecutionSourceV1{}, time.Time{}, q.budget.deny()
	}
	request.UpstreamIdempotencyKeyDigest = original.Reservation.UpstreamIdempotencyKeyDigest
	parent, source, burn, err := store.readOwnedAllocationParent(ctx, q, request)
	return request, parent, source, burn, err
}

func (store *Store) operationHistory(ctx context.Context, q *ownedAuthorityQuery, r domain.AttemptOperationReservationV1) error {
	_, parent, source, burn, err := store.operationParent(ctx, q, r.Operation.Scope)
	if err != nil || r.ValidateForParent(parent.Authority, parent.Reservation) != nil || r.Operation.Scope.SessionID != parent.Scope.SessionID {
		return q.budget.deny()
	}
	allocation, err := readOwnedAllocationSnapshots(ctx, q, parent, source, burn)
	if err != nil || allocation == nil {
		return q.budget.deny()
	}
	proxy, err := readOwnedProxySnapshotAt(ctx, q, parent, source, allocation.original, r.Operation.Sequence)
	if err != nil || proxy == nil {
		return q.budget.deny()
	}
	ad, _ := allocation.original.Digest()
	pd, _ := proxy.original.Digest()
	if ad != r.RegisteredAllocationDigest || pd != r.RegisteredProxyDigest || proxy.original.OperationSequence != r.Operation.Sequence || proxy.original.OperationDigest != r.OperationDigest {
		return q.budget.deny()
	}
	return nil
}

func (store *Store) operationReady(ctx context.Context) bool {
	return store.validOwnedAllocationContext(ctx) && store.attemptOperationIssuer != nil && store.attemptOperationBodies != nil
}

func (store *Store) validateOperationBody(ctx context.Context, scope domain.AttemptOperationScopeV1, ref, digest string, size uint64) error {
	if size == 0 || size > 1<<20 {
		return domain.ErrMembershipDenied
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	body, err := store.attemptOperationBodies.ReadAttemptOperationBody(ctx, scope, ref, size)
	if err != nil || uint64(len(body)) != size || ctx.Err() != nil {
		return domain.ErrMembershipDenied
	}
	// Do not retain or clear a keeper's borrowed buffer.
	copyBody := append([]byte(nil), body...)
	defer clear(copyBody)
	sum := sha256.Sum256(copyBody)
	if hex.EncodeToString(sum[:]) != digest {
		return domain.ErrMembershipDenied
	}
	return nil
}

func (store *Store) ReserveAttemptOperation(ctx context.Context, request ports.ReserveAttemptOperationRequestV1) (ports.ReserveAttemptOperationResultV1, error) {
	ctx, cancel := ownedAuthorityCallContext(ctx)
	defer cancel()
	if !store.operationReady(ctx) || request.Validate() != nil {
		return ports.ReserveAttemptOperationResultV1{}, domain.ErrMembershipDenied
	}
	op := request.Operation.Clone()
	evidence, err := store.prepareOwnedOperationEvidence(ctx, op)
	if err != nil {
		return ports.ReserveAttemptOperationResultV1{}, domain.ErrMembershipDenied
	}
	return store.reserveOwnedOperation(ctx, op, false, "", evidence, request.ConsentReceipt)
}

func (store *Store) RecoverAttemptOperation(ctx context.Context, request ports.RecoverAttemptOperationRequestV1) (ports.ReserveAttemptOperationResultV1, error) {
	ctx, cancel := ownedAuthorityCallContext(ctx)
	defer cancel()
	if !store.operationReady(ctx) || request.Validate() != nil {
		return ports.ReserveAttemptOperationResultV1{}, domain.ErrMembershipDenied
	}
	return store.reserveOwnedOperation(ctx, request.Operation.Clone(), true, request.ExpectedReservationDigest, nil, nil)
}

func (store *Store) reserveOwnedOperation(ctx context.Context, op domain.AttemptOperationV1, recovery bool, expected domain.AttemptOperationReservationDigestV1, evidence *ownedOperationEvidence, receipt *domain.WebSearchConsentReceiptBindingV1) (ports.ReserveAttemptOperationResultV1, error) {
	var candidate domain.AttemptOperationReservationV1
	var fresh bool
	var at time.Time
	err := store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		candidate, fresh, at = domain.AttemptOperationReservationV1{}, false, time.Time{}
		q.metadataCache = make(map[string]ownedMetadataCacheEntry)
		request, parent, source, burnAt, err := store.operationParent(ctx, q, op.Scope)
		if err != nil || op.ValidateForParent(parent.Authority, parent.Reservation) != nil || op.Scope.SessionID != parent.Scope.SessionID {
			return q.budget.deny()
		}
		allocation, err := readOwnedAllocationSnapshots(ctx, q, parent, source, burnAt)
		if err != nil || allocation == nil {
			return q.budget.deny()
		}
		proxy, err := readOwnedProxySnapshotAt(ctx, q, parent, source, allocation.original, op.Sequence)
		if err != nil || proxy == nil {
			return q.budget.deny()
		}
		old, err := readOwnedOperation(ctx, q, op)
		if err != nil {
			return err
		}
		ad, _ := allocation.original.Digest()
		pd, _ := proxy.original.Digest()
		opDigest, _ := op.Digest()
		if proxy.original.OperationSequence != op.Sequence || proxy.original.OperationDigest != opDigest {
			return q.budget.deny()
		}
		if old != nil {
			rd, _ := old.reservation.Digest()
			if old.reservation.OperationDigest != opDigest || old.reservation.RegisteredAllocationDigest != ad || old.reservation.RegisteredProxyDigest != pd || old.reservation.ValidateForParent(parent.Authority, parent.Reservation) != nil || (expected != "" && expected != rd) {
				return q.budget.deny()
			}
			candidate, at = old.reservation.Clone(), q.at
			return nil // history/recovery never debits or mints send permission
		}
		if recovery {
			return q.budget.deny()
		} // no absent-row repair or debit
		if proxy.current.ServiceKind != ownedOperationProxyService(op.Kind) || store.validateOwnedOperationEvidence(ctx, q, op, evidence, parent, source) != nil {
			return q.budget.deny()
		}
		if op.Kind == domain.AttemptOperationWebSearchV1 {
			if evidence == nil || evidence.tool == nil || receipt == nil || store.validateOwnedOperationConsent(ctx, q, op, parent, source, receipt, *evidence.tool) != nil {
				return q.budget.deny()
			}
		}
		if expected != "" || allocation.current.ValidateAt(q.at) != nil || proxy.current.ValidateAt(q.at) != nil || op.Deadline.After(proxy.current.Window.ExpiresAt) || op.Deadline.After(allocation.current.Window.ExpiresAt) || store.validateCurrentOwnedAttemptParent(ctx, q, request, parent) != nil {
			return q.budget.deny()
		}
		accounting, err := readOwnedAttemptAccounting(ctx, q, parent, source)
		if err != nil || accounting == nil || !accounting.SetupReservedAt.Equal(burnAt) {
			return q.budget.deny()
		}
		next, err := debitOwnedOperation(*accounting, op, parent, source, q.at)
		if err != nil {
			return q.budget.deny()
		}
		value := domain.AttemptOperationReservationV1{Version: 1, Operation: op.Clone(), OperationDigest: opDigest, ReservedAt: q.at, AggregateDebit: next.Aggregate, RegisteredAllocationDigest: ad, RegisteredProxyDigest: pd, ComponentAmounts: next.Amounts}
		if value.ValidateForParent(parent.Authority, parent.Reservation) != nil {
			return q.budget.deny()
		}
		raw, _ := json.Marshal(value)
		digest, _ := value.Digest()
		if len(raw) > ownedAuthorityHeadBytes {
			return q.budget.deny()
		}
		if _, err := q.ExecContext(ctx, `INSERT INTO attempt_operations_v1 (tenant_id,attempt_id,sequence,record,digest,operation_digest,call_id) VALUES ($1,$2,$3,$4,$5,$6,$7)`, op.Scope.TenantID, op.Scope.AttemptID, op.Sequence, string(raw), string(digest), string(opDigest), op.CallID); err != nil {
			return err
		}
		if _, err := q.ExecContext(ctx, `INSERT INTO attempt_operation_calls_v1 (tenant_id,attempt_id,call_id,sequence,operation_digest) VALUES ($1,$2,$3,$4,$5)`, op.Scope.TenantID, op.Scope.AttemptID, op.CallID, op.Sequence, string(opDigest)); err != nil {
			return err
		}
		if err := writeOwnedAttemptAccounting(ctx, q, parent, source, next); err != nil {
			return err
		}
		candidate, fresh, at = value.Clone(), true, q.at
		return nil
	})
	if err != nil || ctx.Err() != nil || !store.operationEvidenceLifetimeCurrent(evidence) {
		return ports.ReserveAttemptOperationResultV1{}, domain.ErrMembershipDenied
	}
	return store.operationResult(candidate, at, fresh)
}

func (store *Store) operationResult(r domain.AttemptOperationReservationV1, at time.Time, fresh bool) (ports.ReserveAttemptOperationResultV1, error) {
	expires := r.Operation.Deadline.Add(r.Operation.CleanupDuration)
	if short := at.Add(r.Operation.CleanupDuration); short.Before(expires) {
		expires = short
	}
	grant, err := store.attemptOperationIssuer.MintAttemptOperationObservationGrant(r.Clone(), at, expires)
	if err != nil {
		return ports.ReserveAttemptOperationResultV1{}, domain.ErrMembershipDenied
	}
	result := ports.ReserveAttemptOperationResultV1{Status: ports.AttemptOperationReconcileOnlyV1, Reservation: r.Clone(), ObservationGrant: grant.Clone()}
	if fresh {
		capability, err := store.attemptOperationIssuer.MintAttemptOperationCapability(r.Clone(), at, r.Operation.Deadline)
		if err != nil {
			return ports.ReserveAttemptOperationResultV1{}, domain.ErrMembershipDenied
		}
		result.Status, result.Capability = ports.AttemptOperationReservedV1, &capability
	}
	if result.Validate() != nil {
		return ports.ReserveAttemptOperationResultV1{}, domain.ErrMembershipDenied
	}
	return result, nil
}

func (store *Store) AuthorizeAttemptOperationBeforeSend(ctx context.Context, capability ports.AttemptOperationCapabilityV1) error {
	ctx, cancel := ownedAuthorityCallContext(ctx)
	defer cancel()
	if !store.operationReady(ctx) || capability.Validate() != nil {
		return domain.ErrMembershipDenied
	}
	capability = capability.Clone()
	r := capability.Reservation
	op := r.Operation
	evidence, prepareErr := store.prepareOwnedOperationEvidence(ctx, op)
	if prepareErr != nil {
		return domain.ErrMembershipDenied
	}
	err := store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		q.metadataCache = make(map[string]ownedMetadataCacheEntry)
		request, parent, source, burnAt, err := store.operationParent(ctx, q, op.Scope)
		if err != nil || r.ValidateForParent(parent.Authority, parent.Reservation) != nil || op.Scope.SessionID != parent.Scope.SessionID || !q.at.Before(capability.ExpiresAt) || q.at.Before(capability.IssuedAt) || store.validateCurrentOwnedAttemptParent(ctx, q, request, parent) != nil {
			return q.budget.deny()
		}
		allocation, err := readOwnedAllocationSnapshots(ctx, q, parent, source, burnAt)
		if err != nil || allocation == nil || allocation.current.ValidateAt(q.at) != nil {
			return q.budget.deny()
		}
		proxy, err := readOwnedProxySnapshotAt(ctx, q, parent, source, allocation.original, op.Sequence)
		if err != nil || proxy == nil || proxy.current.ValidateAt(q.at) != nil {
			return q.budget.deny()
		}
		ad, _ := allocation.current.Digest()
		pd, _ := proxy.current.Digest()
		if ad != r.RegisteredAllocationDigest || pd != r.RegisteredProxyDigest || proxy.current.OperationSequence != op.Sequence || proxy.current.OperationDigest != r.OperationDigest || op.Deadline.After(proxy.current.Window.ExpiresAt) || op.Deadline.After(allocation.current.Window.ExpiresAt) {
			return q.budget.deny()
		}
		if proxy.current.ServiceKind != ownedOperationProxyService(op.Kind) || store.validateOwnedOperationEvidence(ctx, q, op, evidence, parent, source) != nil {
			return q.budget.deny()
		}
		if op.Kind == domain.AttemptOperationWebSearchV1 && (evidence == nil || evidence.tool == nil || store.validateOwnedOperationConsent(ctx, q, op, parent, source, nil, *evidence.tool) != nil) {
			return q.budget.deny()
		}
		actual, err := readOwnedOperation(ctx, q, op)
		if err != nil || actual == nil || actual.observation != nil {
			return q.budget.deny()
		}
		digest, _ := r.Digest()
		actualDigest, _ := actual.reservation.Digest()
		accounting, err := readOwnedAttemptAccounting(ctx, q, parent, source)
		if err != nil || accounting == nil || digest != actualDigest || accounting.Aggregate != r.AggregateDebit || accounting.Amounts != r.ComponentAmounts || !accounting.SetupReservedAt.Equal(burnAt) {
			return q.budget.deny()
		}
		return nil
	})
	if err != nil || ctx.Err() != nil || !store.operationEvidenceLifetimeCurrent(evidence) || store.attemptOperationIssuer.ConsumeAttemptOperationCapability(capability) != nil {
		return domain.ErrMembershipDenied
	}
	return nil
}

func (store *Store) RecordAttemptOperationObservation(ctx context.Context, request ports.RecordAttemptOperationObservationRequestV1) error {
	ctx, cancel := ownedAuthorityCallContext(ctx)
	defer cancel()
	if !store.operationReady(ctx) || request.Validate() != nil || store.attemptOperationIssuer.VerifyAttemptOperationObservationGrant(request.ObservationGrant.Clone()) != nil || store.attemptOperationIssuer.VerifyAttemptOperationObservation(request.Observation.Clone()) != nil {
		return domain.ErrMembershipDenied
	}
	r := request.Observation.Reservation.Clone()
	o := request.Observation.Observation.Clone()
	if o.Outcome == domain.AttemptOperationCompletedV1 {
		if store.localNormalizedModelSuccess(r.Operation, o) {
			if store.validateLocalNormalizedModel(ctx, r.Operation, o) != nil {
				return domain.ErrMembershipDenied
			}
		} else if store.validateOperationBody(ctx, r.Operation.Scope, o.ResponseRef, o.ResponseDigest, o.ResponseBytes) != nil {
			return domain.ErrMembershipDenied
		}
	}
	return store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		q.metadataCache = make(map[string]ownedMetadataCacheEntry)
		if !q.at.Before(request.ObservationGrant.ExpiresAt) || q.at.Before(request.ObservationGrant.IssuedAt) || store.operationHistory(ctx, q, r) != nil {
			return q.budget.deny()
		}
		actual, err := readOwnedOperation(ctx, q, r.Operation)
		if err != nil || actual == nil || o.ObservedAt.After(q.at) {
			return q.budget.deny()
		}
		digest, _ := r.Digest()
		actualDigest, _ := actual.reservation.Digest()
		if digest != actualDigest {
			return q.budget.deny()
		}
		od, _ := o.Digest()
		if actual.observation != nil {
			old, _ := actual.observation.Digest()
			if old != od {
				return q.budget.deny()
			}
			return nil
		}
		raw, _ := json.Marshal(o)
		if len(raw) > ownedAuthorityHeadBytes {
			return q.budget.deny()
		}
		_, err = q.ExecContext(ctx, `UPDATE attempt_operations_v1 SET observation_record=$1,observation_digest=$2 WHERE tenant_id=$3 AND attempt_id=$4 AND sequence=$5 AND digest=$6 AND observation_record IS NULL AND observation_digest IS NULL`, string(raw), string(od), r.Operation.Scope.TenantID, r.Operation.Scope.AttemptID, r.Operation.Sequence, string(digest))
		if err != nil {
			return err
		}
		actual, err = readOwnedOperation(ctx, q, r.Operation)
		if err != nil || actual == nil || actual.observation == nil {
			return q.budget.deny()
		}
		readback, _ := actual.observation.Digest()
		if readback != od {
			return q.budget.deny()
		}
		return nil
	})
}

func (store *Store) ReadAttemptOperationObservation(ctx context.Context, grant ports.AttemptOperationObservationGrantV1) (ports.AuthenticatedAttemptOperationObservationV1, error) {
	ctx, cancel := ownedAuthorityCallContext(ctx)
	defer cancel()
	if !store.operationReady(ctx) || store.attemptOperationIssuer.VerifyAttemptOperationObservationGrant(grant.Clone()) != nil {
		return ports.AuthenticatedAttemptOperationObservationV1{}, domain.ErrMembershipDenied
	}
	var observation domain.AttemptOperationObservationV1
	err := store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		observation = domain.AttemptOperationObservationV1{}
		q.metadataCache = make(map[string]ownedMetadataCacheEntry)
		if !q.at.Before(grant.ExpiresAt) || q.at.Before(grant.IssuedAt) || store.operationHistory(ctx, q, grant.Reservation) != nil {
			return q.budget.deny()
		}
		actual, err := readOwnedOperation(ctx, q, grant.Reservation.Operation)
		if err != nil || actual == nil || actual.observation == nil {
			return q.budget.deny()
		}
		digest, _ := grant.Reservation.Digest()
		actualDigest, _ := actual.reservation.Digest()
		if digest != actualDigest {
			return q.budget.deny()
		}
		observation = actual.observation.Clone()
		return nil
	})
	if err != nil || ctx.Err() != nil {
		return ports.AuthenticatedAttemptOperationObservationV1{}, domain.ErrMembershipDenied
	}
	if store.localNormalizedModelSuccess(grant.Reservation.Operation, observation) && store.validateLocalNormalizedModel(ctx, grant.Reservation.Operation, observation) != nil {
		return ports.AuthenticatedAttemptOperationObservationV1{}, domain.ErrMembershipDenied
	}
	if ctx.Err() != nil {
		return ports.AuthenticatedAttemptOperationObservationV1{}, domain.ErrMembershipDenied
	}
	return store.attemptOperationIssuer.AuthenticateAttemptOperationObservation(grant.Clone(), observation)
}
