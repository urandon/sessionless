package ydbstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Evidence can be assembled only from the first authenticated transaction's
// original profile and the trusted Source's exact bytes, never public input.
type ownedWebSearchResourceGrantEvidence struct {
	profile        domain.OwnedAgentProfileCurrentV1
	grant          domain.WebSearchResourceGrantV2
	sourceDone     <-chan struct{}
	sourceDeadline time.Time
}

func (e *ownedWebSearchResourceGrantEvidence) sourceOpen() bool {
	if e == nil || e.sourceDone == nil || e.sourceDeadline.IsZero() {
		return false
	}
	select {
	case <-e.sourceDone:
		return false
	default:
		return true
	}
}

func (e *ownedWebSearchResourceGrantEvidence) sourceCurrentAt(at time.Time) bool {
	return e.sourceOpen() && !at.IsZero() && at.Before(e.sourceDeadline)
}

func (store *Store) readOwnedWebSearchResourceGrant(ctx context.Context, profile domain.OwnedAgentProfileCurrentV1, at time.Time) (*ownedWebSearchResourceGrantEvidence, error) {
	deny := func() (*ownedWebSearchResourceGrantEvidence, error) { return nil, domain.ErrMembershipDenied }
	if store == nil {
		return deny()
	}
	custody := store.ownedWebSearchResourceGrantCustody
	if ctx == nil || ctx.Err() != nil || custody == nil || profile.WebSearchResourceGrant == nil || profile.WebSearchResourceGrant.Validate() != nil || profile.ValidateAt(at) != nil || profile.CostCeiling.WebSearchCost == nil {
		return deny()
	}
	lifetime, err := custody.ExecutionLifetime()
	if err != nil || lifetime == nil {
		return deny()
	}
	deadline, finite := lifetime.Deadline()
	if !finite || deadline.IsZero() || deadline.Year() < 1 || deadline.Year() > 9999 {
		return deny()
	}
	// Do not retain the context or inspect Values. This passive observation
	// keeps the original cancellation/deadline through the final SQL gate.
	evidence := &ownedWebSearchResourceGrantEvidence{sourceDone: lifetime.Done(), sourceDeadline: deadline}
	if !evidence.sourceCurrentAt(at) {
		return deny()
	}
	bodyRef := profile.WebSearchResourceGrant.Body
	// Reserve the bounded Source return, parse/copy/canonical body cohorts and
	// original-profile canonical copies BEFORE either keeper I/O or new copies.
	// This is the existing logical metadata budget, not a heap-fit certificate.
	if chargeOwnedAuthorityBody(ctx, 2*ownedAuthorityHeadBytes+4*bodyRef.Bytes) != nil {
		return deny()
	}
	pd, err := profile.Digest()
	if err != nil {
		return deny()
	}
	ref := domain.OwnedAgentProfileReferenceV1{Locator: profile.Locator, RevocationGeneration: profile.RevocationGeneration, Digest: pd}
	if ctx.Err() != nil || !evidence.sourceCurrentAt(at) {
		return deny()
	}
	body, err := custody.ReadWebSearchResourceGrant(ctx, ref, bodyRef.Bytes)
	defer clear(body) // custody returns caller-owned DATA, never a borrowed buffer
	if err != nil || ctx.Err() != nil || !evidence.sourceCurrentAt(at) || uint64(len(body)) != bodyRef.Bytes {
		return deny()
	}
	sum := sha256.Sum256(body)
	if hex.EncodeToString(sum[:]) != bodyRef.Digest {
		return deny()
	}
	g, err := domain.DecodeWebSearchResourceGrantV2(body)
	if err != nil {
		return deny()
	}
	gd, err := g.Digest()
	if err != nil || gd != profile.WebSearchGrantDigest || gd != profile.CostCeiling.WebSearchCost.ScopedGrantDigest || g.ValidateCostAt(*profile.CostCeiling.WebSearchCost, at) != nil || g.TenantID != profile.Locator.TenantID || g.OwnerUserID != profile.Locator.OwnerUserID || g.BillingOwnerUserID != profile.BillingOwnerUserID || ctx.Err() != nil || !evidence.sourceCurrentAt(at) {
		return deny()
	}
	evidence.profile, evidence.grant = profile.Clone(), g
	if ctx.Err() != nil || !evidence.sourceOpen() {
		return deny()
	}
	return evidence, nil
}

// The two grammars are explicitly tagged by the authenticated profile arm.
// This projection remains private DATA; it cannot supply a second authorizer.
type ownedCurrentConsentGrant struct {
	fixture  *ports.OwnedFixtureToolGrantV1
	resource *ownedWebSearchResourceGrantEvidence
}

func (e ownedCurrentConsentGrant) sourceOpen() bool {
	if e.resource != nil {
		return e.fixture == nil && e.resource.sourceOpen()
	}
	return e.fixture != nil
}

func (store *Store) readOwnedCurrentConsentGrant(ctx context.Context, r domain.WebSearchConsentRequestV1, p domain.OwnedAgentProfileCurrentV1, at time.Time, private bool) (ownedCurrentConsentGrant, error) {
	if p.WebSearchResourceGrant != nil {
		e, err := store.readOwnedWebSearchResourceGrant(ctx, p, at)
		if err != nil {
			return ownedCurrentConsentGrant{}, domain.ErrMembershipDenied
		}
		return ownedCurrentConsentGrant{resource: e}, nil
	}
	// Only the original deterministic fixture has the old fixture body grammar.
	// A missing V2 arm/custody in a genuine profile is never a fallback request.
	if p.Backend.BackendKind != domain.HarnessBackendDeterministicFixtureV1 {
		return ownedCurrentConsentGrant{}, domain.ErrMembershipDenied
	}
	if private && chargeOwnedAuthorityBody(ctx, 2*(4<<10)) != nil {
		return ownedCurrentConsentGrant{}, domain.ErrMembershipDenied
	}
	g, err := store.readOwnedConsentGrant(ctx, r)
	if err != nil {
		return ownedCurrentConsentGrant{}, domain.ErrMembershipDenied
	}
	return ownedCurrentConsentGrant{fixture: &g}, nil
}

func (e ownedCurrentConsentGrant) validate(parent domain.OwnedAttemptEffectParentV1, source domain.AttemptExecutionSourceV1, r domain.WebSearchConsentRequestV1, at time.Time) (uint64, uint64, time.Time, error) {
	deny := func() (uint64, uint64, time.Time, error) { return 0, 0, time.Time{}, domain.ErrMembershipDenied }
	cost := parent.Authority.AdmissionCostCeiling.WebSearchCost
	if cost == nil || parent.Authority.HarnessBinding.AgentLoopPolicy == nil || r.GrantDigest != parent.Authority.HarnessBinding.AgentLoopPolicy.WebSearchGrantDigest {
		return deny()
	}
	if e.fixture != nil && e.resource == nil {
		g := e.fixture
		if parent.Authority.HarnessBinding.Backend.BackendKind != domain.HarnessBackendDeterministicFixtureV1 || g.Kind != domain.ExternalServiceWebSearchV1 || g.ConsentRevision != r.ConsentRevision || g.RevocationGeneration != r.RevocationGeneration || g.ValidateForCost(*cost, r.GrantDigest, at) != nil {
			return deny()
		}
		return g.ConsentRevision, g.RevocationGeneration, g.ExpiresAt, nil
	}
	if e.resource == nil || e.fixture != nil || !e.resource.sourceCurrentAt(at) {
		return deny()
	}
	p, g := e.resource.profile, e.resource.grant
	pd, pe := p.Digest()
	gd, ge := g.Digest()
	pcd, pce := p.CostCeiling.Digest()
	acd, ace := parent.Authority.AdmissionCostCeiling.Digest()
	if pe != nil || ge != nil || pce != nil || ace != nil || pd != source.Profile.Digest || p.Locator != source.Profile.Locator || p.RevocationGeneration != source.Profile.RevocationGeneration || p.WebSearchResourceGrant == nil || p.ValidateAt(at) != nil || pcd != acd || gd != p.WebSearchGrantDigest || gd != r.GrantDigest || g.ValidateCostAt(*cost, at) != nil || g.TenantID != r.Scope.TenantID || g.OwnerUserID != r.Scope.OwnerUserID || g.BillingOwnerUserID != p.BillingOwnerUserID || g.ConsentRevision != r.ConsentRevision || g.RevocationGeneration != r.RevocationGeneration {
		return deny()
	}
	return g.ConsentRevision, g.RevocationGeneration, g.ExpiresAt, nil
}
