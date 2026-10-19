// Package websearch implements bounded search request/result DATA only. It has
// no HTTP client, credential lookup, issuer, dispatch method or registration.
package websearch

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

var (
	ErrContract               = errors.New("web search bounded contract denied")
	ErrResponse               = errors.New("web search response malformed or unsupported")
	ErrOverflow               = errors.New("web search response limit exceeded")
	ErrOperationCompatibility = errors.New("web search physical wire is incompatible with current operation contract")
)

func digest(body []byte) string { s := sha256.Sum256(body); return hex.EncodeToString(s[:]) }

type BindingV1 struct {
	Scope                     domain.AttemptOperationScopeV1
	OperationDigest           domain.AttemptOperationDigestV1
	Sequence                  uint64
	CallID                    string
	ModelResponseDigest       string
	InitiatingUserID          domain.UserID
	ParentCostCeilingDigestV2 domain.AdmissionCostCeilingDigestV2
	ParentPriceRevision       string
}

func (v BindingV1) validate(g domain.WebSearchGrantV1) error {
	if v.Scope.Validate() != nil || v.InitiatingUserID.Validate() != nil || v.ParentCostCeilingDigestV2.Validate() != nil || domain.ValidateOpaqueID("web_search.parent_price_revision", v.ParentPriceRevision) != nil || domain.ValidateSHA256Digest("web_search.model_response", v.ModelResponseDigest) != nil || (v.OperationDigest != "" && v.OperationDigest.Validate() != nil) || v.Sequence < 2 || v.Sequence > 12 || domain.ValidateOpaqueID("web_search.call", v.CallID) != nil ||
		v.Scope.TenantID != g.TenantID || v.Scope.OwnerUserID != g.OwnerUserID || v.Scope.RunID != g.RunID || v.Scope.AttemptID != g.AttemptID {
		return ErrContract
	}
	return nil
}

type HTTPMetadataV1 struct {
	StatusCode                   int
	ContentType, ContentEncoding string
}

// ValidateOperation checks DATA correlation only, never approval or freshness.
// A valid 4KiB argument object can compile to >4KiB physical wire; the existing
// tool-operation cap is deliberately NOT bypassed or silently shrunk here.
func (f *FrozenRequestV1) ValidateOperation(op domain.AttemptOperationV1) error {
	if f == nil || op.Validate() != nil || op.Kind != domain.AttemptOperationWebSearchV1 || op.Scope != f.binding.Scope || op.Sequence != f.binding.Sequence || op.CallID != f.binding.CallID ||
		op.ModelResponseDigest != f.binding.ModelResponseDigest || op.ArgumentsDigest != f.argumentsDigest || op.QueryDigest != f.queryDigest || op.ToolGrantDigest != f.grantDigest || op.RequestDigest != f.wireDigest || op.RequestBytes != uint64(len(f.wire)) ||
		op.Debit.AmountMicrounits != f.grant.PerCallReservationMicrounits || op.Currency != f.grant.Currency || op.PriceRevision != f.binding.ParentPriceRevision || op.AdmissionCostCeilingDigest != f.binding.ParentCostCeilingDigestV2 || op.ExternalChargeEvidenceDigest != f.grant.PriceEvidenceDigest ||
		!op.Deadline.After(f.compiledAt) || op.Deadline.After(f.validUntil) || op.Debit.ActiveDuration > 15*time.Second {
		return ErrOperationCompatibility
	}
	d, e := op.Digest()
	if e != nil || (f.binding.OperationDigest != "" && d != f.binding.OperationDigest) {
		return ErrOperationCompatibility
	}
	return nil
}

// BindOperationV1 completes the DATA association AFTER the exact physical wire
// has been compiled and its digest sealed in the operation (no digest cycle).
// It cannot issue, reserve, approve, debit or send. Current V1's 4KiB cap remains.
func (f *FrozenRequestV1) BindOperationV1(op domain.AttemptOperationV1) (*FrozenRequestV1, error) {
	if f.ValidateOperation(op) != nil {
		return nil, ErrOperationCompatibility
	}
	copy := *f
	copy.arguments = f.Arguments()
	copy.wire = f.Body()
	copy.binding.OperationDigest, _ = op.Digest()
	copy.operationDeadline = op.Deadline
	return &copy, nil
}

// This compares the immutable pending consent request to original bytes. It
// neither approves a query nor substitutes for the durable decision/current Tx.
func (f *FrozenRequestV1) ValidateConsentRequest(r domain.WebSearchConsentRequestV1) error {
	if f == nil || r.ValidateArguments(f.arguments) != nil || r.InitiatingUserID != f.binding.InitiatingUserID || r.ModelResponseDigest != f.binding.ModelResponseDigest || r.CallID != f.binding.CallID || r.GrantDigest != f.grantDigest || r.ArgumentsDigest != f.argumentsDigest ||
		r.QueryDigest != f.queryDigest || r.ConsentRevision != f.grant.ConsentRevision || r.RevocationGeneration != f.grant.RevocationGeneration || r.ExpiresAt.After(f.validUntil) || (!f.operationDeadline.IsZero() && r.ExpiresAt.Before(f.operationDeadline)) {
		return ErrContract
	}
	s := f.binding.Scope
	expected := domain.WebSearchConsentScopeV1{TenantID: s.TenantID, SessionID: s.SessionID, OwnerUserID: s.OwnerUserID, RunID: s.RunID, AttemptID: s.AttemptID, LeaseID: s.LeaseID, FenceToken: s.FenceToken, PhysicalInvocationClaimID: s.PhysicalInvocationClaimID,
		EffectReservationDigest: string(s.EffectReservationDigest), InvocationAuthorityDigest: string(s.InvocationAuthorityDigest), HarnessBindingDigest: string(s.HarnessBindingDigest), PolicyDigest: string(s.AgentLoopPolicyDigest)}
	if r.Scope != expected {
		return ErrContract
	}
	return nil
}
