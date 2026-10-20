// Package websearch implements bounded search request/result DATA only. It has
// no HTTP client, credential lookup, issuer, dispatch method or registration.
package websearch

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"

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
	if v.validateIdentity(g.TenantID, g.OwnerUserID) != nil || v.Scope.RunID != g.RunID || v.Scope.AttemptID != g.AttemptID {
		return ErrContract
	}
	return nil
}

// Reusable resource scope does not contain invocation IDs. The original
// binding remains mandatory and its whole invocation scope is never replaced.
func (v BindingV1) validateIdentity(tenant domain.TenantID, owner domain.UserID) error {
	if v.Scope.Validate() != nil || v.InitiatingUserID.Validate() != nil || v.ParentCostCeilingDigestV2.Validate() != nil || domain.ValidateOpaqueID("web_search.parent_price_revision", v.ParentPriceRevision) != nil || domain.ValidateSHA256Digest("web_search.model_response", v.ModelResponseDigest) != nil || (v.OperationDigest != "" && v.OperationDigest.Validate() != nil) || v.Sequence < 2 || v.Sequence > 12 || domain.ValidateOpaqueID("web_search.call", v.CallID) != nil ||
		v.Scope.TenantID != tenant || v.Scope.OwnerUserID != owner {
		return ErrContract
	}
	return nil
}

type HTTPMetadataV1 struct {
	StatusCode                   int
	ContentType, ContentEncoding string
}

// ValidateOperation checks DATA correlation only, never approval or freshness.
// Tavily always requires its explicit search-wire arm, including small bodies.
// Legacy argument-sized tool requests retain their original 4KiB cap; this
// DATA association never selects a fixture fallback or authorizes dispatch.
func (f *FrozenRequestV1) ValidateOperation(op domain.AttemptOperationV1) error {
	if f == nil {
		return ErrOperationCompatibility
	}
	return f.dataView().validateOperation(op)
}

// BindOperationV1 completes the DATA association AFTER the exact physical wire
// has been compiled and its digest sealed in the operation (no digest cycle).
// It cannot issue, reserve, approve, debit or send. Original legacy V1 limits
// remain unchanged; authentic new-arm producer/Store integration is required.
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
	if f == nil {
		return ErrContract
	}
	return f.dataView().validateConsentRequest(r)
}
