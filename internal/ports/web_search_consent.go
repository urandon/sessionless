package ports

import (
	"context"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// WebSearchConsentLocatorV1 is a bounded exact-row selector, not ownership,
// query disclosure or send authority. Resolve the tenant/session/run/attempt
// together; an opaque request ID alone never permits a cross-owner lookup.
type WebSearchConsentLocatorV1 struct {
	TenantID  domain.TenantID
	SessionID domain.SessionID
	RunID     domain.RunID
	AttemptID domain.AttemptID
	RequestID domain.WebSearchConsentRequestIDV1
}

func (value WebSearchConsentLocatorV1) Validate() error {
	for _, validate := range []func() error{value.TenantID.Validate, value.SessionID.Validate, value.RunID.Validate, value.AttemptID.Validate, value.RequestID.Validate} {
		if err := validate(); err != nil {
			return err
		}
	}
	return nil
}

func (value WebSearchConsentLocatorV1) ValidateForRequest(request domain.WebSearchConsentRequestV1) error {
	if err := value.Validate(); err != nil {
		return err
	}
	if err := request.Validate(); err != nil {
		return err
	}
	if value.TenantID != request.Scope.TenantID || value.SessionID != request.Scope.SessionID || value.RunID != request.Scope.RunID || value.AttemptID != request.Scope.AttemptID || value.RequestID != request.ID {
		return domain.ErrWebSearchConsentAuthority
	}
	return nil
}

// Creation is a trusted worker/store operation after durable complete model
// response validation. The store resolves current authority; neither a caller
// projection nor queue/native checkpoint facts may replace that resolution.
type CreateWebSearchConsentRequestV1 struct {
	Request domain.WebSearchConsentRequestV1
}

type ReadWebSearchConsentRequestV1 struct {
	Locator          WebSearchConsentLocatorV1
	WebSessionDigest domain.SecretDigest
}

func (value ReadWebSearchConsentRequestV1) Validate() error {
	if err := value.Locator.Validate(); err != nil {
		return err
	}
	return value.WebSessionDigest.Validate("web_search_consent.web_session_digest")
}

// DecideWebSearchConsentRequestV1 contains only an exact locator, expected CAS
// revision and choice, with the existing Web session/CSRF digest selectors.
// It cannot replace arguments, select an actor, extend a deadline or supply a
// prevalidated authority flag. The authenticated actor is loaded by the store.
type DecideWebSearchConsentRequestV1 struct {
	Locator          WebSearchConsentLocatorV1
	WebSessionDigest domain.SecretDigest
	CSRFTokenDigest  domain.SecretDigest
	ExpectedRevision uint64
	Choice           domain.WebSearchConsentChoiceV1
}

func (value DecideWebSearchConsentRequestV1) Validate() error {
	if err := (ReadWebSearchConsentRequestV1{Locator: value.Locator, WebSessionDigest: value.WebSessionDigest}).Validate(); err != nil {
		return err
	}
	if err := value.CSRFTokenDigest.Validate("web_search_consent.csrf_token_digest"); err != nil {
		return err
	}
	if value.ExpectedRevision != 1 {
		return domain.ErrWebSearchConsentConflict
	}
	return value.Choice.Validate()
}

// Reconciliation does not accept a desired status or human receipt. The store
// derives expiry/cancellation/revocation/claim loss from authoritative rows;
// fresh delivery and worker loss are reconcile-only, never resume authority.
type ReconcileWebSearchConsentRequestV1 struct {
	Locator          WebSearchConsentLocatorV1
	ExpectedRevision uint64
}

func (value ReconcileWebSearchConsentRequestV1) Validate() error {
	if err := value.Locator.Validate(); err != nil {
		return err
	}
	if value.ExpectedRevision != 1 && value.ExpectedRevision != 2 {
		return domain.ErrWebSearchConsentConflict
	}
	return nil
}

// WebSearchConsentStoreV1 extends the existing Attempt effect store; it is not
// a second scheduler or authority store. Each method is a bounded SAME-resource
// transaction using the store's authoritative clock, never a browser deadline.
//
// Create verifies canonical initiating user and grant consent permission, exact
// immutable argument bytes against the durable model response and all current
// claim/grant/policy windows. It permits one pending and two total requests per
// Attempt, charging rows/blobs to existing aggregate storage only; it performs
// no search reservation, monetary debit or network send. Exact duplicates may
// recover existing append evidence, but never a capability or replay permission.
//
// Read and Decide reload Web session, membership security version, initiating
// user participation and task/resource consent authority in that transaction.
// Read returns at most one bounded private state. It is not a public execution
// projection; its blob locator/query digests must not be published there.
// Decide additionally validates the same session's CSRF digest and current
// Run/Attempt/claim/fence/grant/policy/revisions/expiry, atomically CAS-appending
// the immutable authenticated receipt. Identical retries recover that receipt;
// conflicts/stale/foreign/late inputs deny. Reconcile never reopens a stop.
//
// Approval is necessary but insufficient: operation reserve and one-use egress
// must re-read the exact stored approved state and revalidate the typed receipt
// binding in their existing atomic authority gates. No serializable value
// returned here is a dispatch capability. #195 owns the production Web bridge;
// this port does not implement BFF transport, worker waiting or live approval.
type WebSearchConsentStoreV1 interface {
	CreateWebSearchConsent(context.Context, CreateWebSearchConsentRequestV1) (domain.WebSearchConsentStateV1, error)
	ReadWebSearchConsent(context.Context, ReadWebSearchConsentRequestV1) (domain.WebSearchConsentStateV1, error)
	DecideWebSearchConsent(context.Context, DecideWebSearchConsentRequestV1) (domain.WebSearchConsentStateV1, error)
	ReconcileWebSearchConsent(context.Context, ReconcileWebSearchConsentRequestV1) (domain.WebSearchConsentStateV1, error)
}
