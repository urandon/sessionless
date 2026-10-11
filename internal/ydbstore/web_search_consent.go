package ydbstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

const ownedConsentReadSQL = `SELECT
 CASE WHEN web_search_consent_record IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 CASE WHEN web_search_consent_digest IS NULL THEN CAST(0 AS Uint64) ELSE CAST(1 AS Uint64) END,
 COALESCE(SUBSTRING(web_search_consent_record,0,32769),CAST('' AS String)),
 COALESCE(SUBSTRING(CAST(web_search_consent_digest AS String),0,65),CAST('' AS String))
 FROM attempt_effect_reservations WHERE tenant_id=$1 AND attempt_id=$2 AND kind=$3`

// This bounded projection belongs to the existing sole parent. Request and
// authenticated decision facts are immutable across CAS transitions. A stop
// retains its original decision, never creates a human receipt or reopens it.
// This codec is structural, not current grant/model/actor authorization.
type ownedConsentStates struct {
	Version      uint32                           `json:"version"`
	ParentDigest string                           `json:"parent_digest"`
	States       []domain.WebSearchConsentStateV1 `json:"states"`
}

func ownedConsentScope(parent domain.OwnedAttemptEffectParentV1) domain.WebSearchConsentScopeV1 {
	r := parent.Reservation
	pd, _ := parent.Authority.HarnessBinding.AgentLoopPolicy.Digest()
	rd, _ := r.DigestForAuthority(parent.Authority)
	return domain.WebSearchConsentScopeV1{
		TenantID: parent.Scope.TenantID, SessionID: parent.Scope.SessionID, OwnerUserID: parent.Scope.OwnerUserID,
		RunID: r.RunID, AttemptID: r.AttemptID, LeaseID: r.LeaseID, FenceToken: r.FenceToken,
		PhysicalInvocationClaimID: r.PhysicalInvocationClaimID, EffectReservationDigest: string(rd),
		InvocationAuthorityDigest: string(r.InvocationAuthorityDigest), HarnessBindingDigest: string(r.HarnessBindingDigest), PolicyDigest: string(pd),
	}
}

func (v ownedConsentStates) validate(parent domain.OwnedAttemptEffectParentV1, at time.Time) error {
	pd, err := parent.Digest()
	if err != nil || parent.Authority.HarnessBinding.AgentLoopPolicy == nil || v.Version != 1 || v.ParentDigest != pd || len(v.States) == 0 || len(v.States) > 2 || !ownedEffectTime(at) {
		return domain.ErrMembershipDenied
	}
	scope := ownedConsentScope(parent)
	pending := 0
	for i, state := range v.States {
		if state.Validate() != nil || state.Request.Scope != scope || !ownedEffectTime(state.Request.CreatedAt) || !ownedEffectTime(state.Request.ExpiresAt) || !ownedEffectTime(state.UpdatedAt) || state.UpdatedAt.After(at) || state.Request.CreatedAt.Before(parent.Reservation.ReservedAt) || state.Request.ExpiresAt.After(parent.Authority.InvocationDeadline) {
			return domain.ErrMembershipDenied
		}
		if state.Status == domain.WebSearchConsentPendingV1 {
			pending++
		}
		for j := 0; j < i; j++ {
			previous := v.States[j].Request
			if previous.ID == state.Request.ID || previous.CallID == state.Request.CallID || previous.ArgumentsDigest == state.Request.ArgumentsDigest || previous.QueryDigest == state.Request.QueryDigest || previous.InitiatingUserID != state.Request.InitiatingUserID {
				return domain.ErrMembershipDenied
			}
		}
	}
	if pending > 1 {
		return domain.ErrMembershipDenied
	}
	return nil
}

func ownedConsentBytes(v ownedConsentStates, parent domain.OwnedAttemptEffectParentV1, at time.Time) ([]byte, string, error) {
	if v.validate(parent, at) != nil {
		return nil, "", domain.ErrMembershipDenied
	}
	raw, err := json.Marshal(v)
	if err != nil || len(raw) > ownedAuthorityHeadBytes {
		return nil, "", domain.ErrMembershipDenied
	}
	sum := sha256.Sum256(raw)
	return raw, hex.EncodeToString(sum[:]), nil
}

func decodeOwnedConsentStates(raw, digest string, parent domain.OwnedAttemptEffectParentV1, at time.Time) (ownedConsentStates, error) {
	var value ownedConsentStates
	if len(raw) == 0 || len(raw) > ownedAuthorityHeadBytes || len(digest) != 64 || json.Unmarshal([]byte(raw), &value) != nil {
		return ownedConsentStates{}, domain.ErrMembershipDenied
	}
	canonical, actual, err := ownedConsentBytes(value, parent, at)
	if err != nil || digest != actual || !bytes.Equal(canonical, []byte(raw)) {
		return ownedConsentStates{}, domain.ErrMembershipDenied
	}
	return value, nil
}

func readOwnedConsentStates(ctx context.Context, q *ownedAuthorityQuery, parent domain.OwnedAttemptEffectParentV1) (*ownedConsentStates, error) {
	r := parent.Reservation
	var recordPresent, digestPresent uint64
	var raw, digest string
	if err := q.scanMetadata(ctx, ownedConsentReadSQL, []any{r.TenantID, r.AttemptID, r.Kind}, &recordPresent, &digestPresent, &raw, &digest); err != nil {
		return nil, q.budget.deny()
	}
	if recordPresent == 0 && digestPresent == 0 && raw == "" && digest == "" {
		return nil, nil
	}
	if recordPresent != 1 || digestPresent != 1 {
		return nil, q.budget.deny()
	}
	value, err := decodeOwnedConsentStates(raw, digest, parent, q.at)
	if err != nil {
		return nil, q.budget.deny()
	}
	return &value, nil
}

// Caller has already reloaded exact authority and used the pure CAS reducer
// inside this SAME transaction. Repeating an INSERT/UPSERT is not recovery;
// read the immutable request/receipt first, then compare exact intent.
func writeOwnedConsentStates(ctx context.Context, q *ownedAuthorityQuery, parent domain.OwnedAttemptEffectParentV1, value ownedConsentStates) error {
	raw, digest, err := ownedConsentBytes(value, parent, q.at)
	if err != nil {
		return q.budget.deny()
	}
	r := parent.Reservation
	_, err = q.ExecContext(ctx, `UPDATE attempt_effect_reservations
 SET web_search_consent_record=$1,web_search_consent_digest=$2
 WHERE tenant_id=$3 AND attempt_id=$4 AND kind=$5 AND owned_digest=$6 AND record IS NULL`, string(raw), digest, r.TenantID, r.AttemptID, r.Kind, value.ParentDigest)
	return err
}

// Reserve bounded metadata for the immutable request plus eventual receipt or
// stop up front, together with the argument blob. This is intentionally a
// worst-case storage reservation, never a refund or a monetary/tool debit.
const ownedConsentMetadataReservationBytes uint64 = 2 * ownedAuthorityHeadBytes

func appendOwnedConsentState(old *ownedConsentStates, accounting ownedAttemptAccounting, parent domain.OwnedAttemptEffectParentV1, request domain.WebSearchConsentRequestV1, at time.Time) (ownedConsentStates, ownedAttemptAccounting, bool, error) {
	deny := func() (ownedConsentStates, ownedAttemptAccounting, bool, error) {
		return ownedConsentStates{}, ownedAttemptAccounting{}, false, domain.ErrMembershipDenied
	}
	pd, err := parent.Digest()
	if err != nil || parent.Authority.HarnessBinding.AgentLoopPolicy == nil || accounting.ParentDigest != pd || accounting.Aggregate.ValidateForPolicy(*parent.Authority.HarnessBinding.AgentLoopPolicy) != nil || request.Validate() != nil || request.Scope != ownedConsentScope(parent) || at.Before(request.CreatedAt) || !at.Before(request.ExpiresAt) {
		return deny()
	}
	value := ownedConsentStates{Version: 1, ParentDigest: pd}
	if old != nil {
		if old.validate(parent, at) != nil {
			return deny()
		}
		value.States = make([]domain.WebSearchConsentStateV1, len(old.States))
		for i, state := range old.States {
			value.States[i] = state.Clone()
		}
		for _, state := range value.States {
			if state.Request.ID == request.ID {
				want, _ := request.Digest()
				got, _ := state.Request.Digest()
				if want != got {
					return deny()
				}
				return value, accounting, false, nil // exact history never recharges
			}
		}
	}
	limit := parent.Authority.HarnessBinding.AgentLoopPolicy.Limits.MaxAggregateBodyBytes
	if accounting.Aggregate.BodyBytes > limit {
		return deny()
	}
	remaining := limit - accounting.Aggregate.BodyBytes
	if domain.ValidateWebSearchConsentAppendV1(value.States, request, uint64(4-2*len(value.States)), remaining) != nil || remaining < ownedConsentMetadataReservationBytes || uint64(request.Arguments.Size) > remaining-ownedConsentMetadataReservationBytes {
		return deny()
	}
	aggregate, err := accounting.Aggregate.Add(domain.AttemptOperationDebitV1{BodyBytes: ownedConsentMetadataReservationBytes + uint64(request.Arguments.Size)})
	if err != nil || aggregate.ValidateForPolicy(*parent.Authority.HarnessBinding.AgentLoopPolicy) != nil {
		return deny()
	}
	accounting.Aggregate, accounting.UpdatedAt = aggregate, at
	value.States = append(value.States, domain.WebSearchConsentStateV1{Request: request, Revision: 1, Status: domain.WebSearchConsentPendingV1, UpdatedAt: request.CreatedAt})
	if _, _, err := ownedConsentBytes(value, parent, at); err != nil {
		return deny()
	}
	return value, accounting, true, nil
}

// The authorized caller uses the existing pure decision/stop reducers. Keep
// their immutable append facts intact even if this structural helper is called
// incorrectly. No new or replacement request/receipt is accepted by CAS.
func replaceOwnedConsentState(old ownedConsentStates, next domain.WebSearchConsentStateV1, parent domain.OwnedAttemptEffectParentV1, at time.Time) (ownedConsentStates, error) {
	if old.validate(parent, at) != nil || next.Validate() != nil {
		return ownedConsentStates{}, domain.ErrMembershipDenied
	}
	value := ownedConsentStates{Version: old.Version, ParentDigest: old.ParentDigest, States: make([]domain.WebSearchConsentStateV1, len(old.States))}
	found := false
	for i, previous := range old.States {
		value.States[i] = previous.Clone()
		if previous.Request.ID != next.Request.ID {
			continue
		}
		found = true
		oldRequest, _ := previous.Request.Digest()
		newRequest, _ := next.Request.Digest()
		priorBytes, _ := json.Marshal(previous)
		nextBytes, _ := json.Marshal(next)
		if bytes.Equal(priorBytes, nextBytes) {
			continue
		}
		if oldRequest != newRequest || next.Revision != previous.Revision+1 || next.UpdatedAt.Before(previous.UpdatedAt) || (previous.Status != domain.WebSearchConsentPendingV1 && previous.Status != domain.WebSearchConsentApprovedV1) || next.Status == domain.WebSearchConsentPendingV1 {
			return ownedConsentStates{}, domain.ErrMembershipDenied
		}
		if previous.Decision != nil {
			if next.Decision == nil {
				return ownedConsentStates{}, domain.ErrMembershipDenied
			}
			oldDecision, _ := previous.Decision.Digest()
			newDecision, _ := next.Decision.Digest()
			if oldDecision != newDecision {
				return ownedConsentStates{}, domain.ErrMembershipDenied
			}
		}
		value.States[i] = next.Clone()
	}
	if !found || value.validate(parent, at) != nil {
		return ownedConsentStates{}, domain.ErrMembershipDenied
	}
	return value, nil
}
