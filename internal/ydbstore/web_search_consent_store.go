package ydbstore

import (
	"bytes"
	"context"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func consentLocator(r domain.WebSearchConsentRequestV1) ports.WebSearchConsentLocatorV1 {
	return ports.WebSearchConsentLocatorV1{TenantID: r.Scope.TenantID, SessionID: r.Scope.SessionID, RunID: r.Scope.RunID, AttemptID: r.Scope.AttemptID, RequestID: r.ID}
}

func (store *Store) CreateWebSearchConsent(ctx context.Context, input ports.CreateWebSearchConsentRequestV1) (domain.WebSearchConsentStateV1, error) {
	if !store.validOwnedAllocationContext(ctx) || store.attemptOperationBodies == nil || input.Request.Validate() != nil {
		return domain.WebSearchConsentStateV1{}, domain.ErrMembershipDenied
	}
	ctx, cancel := ownedConsentContext(ctx)
	defer cancel()
	r := input.Request
	// Exact lost-ack recovery is immutable append evidence only. It must not
	// recharge, reopen a stop, renew time or recover dispatch authority.
	var existing domain.WebSearchConsentStateV1
	var found bool
	err := store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		existing, found = domain.WebSearchConsentStateV1{}, false
		q.metadataCache = make(map[string]ownedMetadataCacheEntry)
		parent, source, err := store.ownedConsentParent(ctx, q, consentLocator(r))
		if err != nil || r.Scope != ownedConsentScope(parent) || r.InitiatingUserID != source.Scope.OwnerUserID {
			return q.budget.deny()
		}
		states, err := readOwnedConsentStates(ctx, q, parent)
		if err != nil {
			return err
		}
		existing, found = findOwnedConsent(states, r.ID)
		if found {
			got, _ := existing.Request.Digest()
			want, _ := r.Digest()
			if got != want {
				return q.budget.deny()
			}
		}
		return nil
	})
	if err != nil {
		return domain.WebSearchConsentStateV1{}, domain.ErrMembershipDenied
	}
	if found {
		return existing, nil
	}
	scope := ownedConsentOperationScope(r.Scope)
	evidence, err := store.readOwnedFixtureToolEvidence(ctx, scope, r.ModelResponseDigest, r.CallID, r.ArgumentsDigest, r.GrantDigest, domain.AttemptOperationWebSearchV1)
	if err != nil || !consentCallBound(r, evidence) {
		return domain.WebSearchConsentStateV1{}, domain.ErrMembershipDenied
	}
	arguments, err := store.readOwnedOperationBody(ctx, scope, r.Arguments.Key, r.ArgumentsDigest, uint64(r.Arguments.Size), 4<<10)
	if err != nil {
		return domain.WebSearchConsentStateV1{}, domain.ErrMembershipDenied
	}
	defer clear(arguments)
	if !bytes.Equal(arguments, evidence.call.Arguments) || r.ValidateArguments(arguments) != nil {
		return domain.WebSearchConsentStateV1{}, domain.ErrMembershipDenied
	}
	var result domain.WebSearchConsentStateV1
	err = store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		result = domain.WebSearchConsentStateV1{}
		q.metadataCache = make(map[string]ownedMetadataCacheEntry)
		_, parent, source, _, err := store.operationParent(ctx, q, scope)
		if err != nil || store.validateOwnedFixtureToolEvidence(ctx, q, evidence, parent, source) != nil {
			return q.budget.deny()
		}
		if _, err := store.ownedConsentCurrentAuthority(ctx, q, parent, source, r, evidence.grant); err != nil {
			return err
		}
		accounting, err := readOwnedAttemptAccounting(ctx, q, parent, source)
		if err != nil || accounting == nil || accounting.Aggregate.Operations != evidence.model.Operation.Sequence {
			return q.budget.deny()
		}
		old, err := readOwnedConsentStates(ctx, q, parent)
		if err != nil {
			return err
		}
		next, debit, fresh, err := appendOwnedConsentState(old, *accounting, parent, r, q.at)
		if err != nil {
			return q.budget.deny()
		}
		if fresh {
			if writeOwnedAttemptAccounting(ctx, q, parent, source, debit) != nil || writeOwnedConsentStates(ctx, q, parent, next) != nil {
				return q.budget.deny()
			}
		}
		actual, err := readOwnedConsentStates(ctx, q, parent)
		var found bool
		result, found = findOwnedConsent(actual, r.ID)
		want, _ := r.Digest()
		got, _ := result.Request.Digest()
		if err != nil || !found || want != got {
			return q.budget.deny()
		}
		return nil
	})
	if err != nil || ctx.Err() != nil {
		return domain.WebSearchConsentStateV1{}, domain.ErrMembershipDenied
	}
	return result, nil
}

// Materialize only an actor-authorized immutable intent before the keeper read;
// a later transaction still reauthenticates actor/current authority and exact
// intent. This first read cannot approve or dispatch anything.
func (store *Store) readOwnedConsentIntent(ctx context.Context, input ports.ReadWebSearchConsentRequestV1) (domain.WebSearchConsentRequestV1, error) {
	var result domain.WebSearchConsentRequestV1
	err := store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		result = domain.WebSearchConsentRequestV1{}
		q.metadataCache = make(map[string]ownedMetadataCacheEntry)
		parent, source, err := store.ownedConsentParent(ctx, q, input.Locator)
		if err != nil || readOwnedAdmissionPrincipal(ctx, q, source.Scope) != nil {
			return q.budget.deny()
		}
		states, err := readOwnedConsentStates(ctx, q, parent)
		state, found := findOwnedConsent(states, input.Locator.RequestID)
		if err != nil || !found || input.Locator.ValidateForRequest(state.Request) != nil || state.Request.InitiatingUserID != source.Scope.OwnerUserID {
			return q.budget.deny()
		}
		a := domain.WebSearchConsentAuthorityV1{InitiatingUserID: source.Scope.OwnerUserID}
		if loadOwnedConsentActor(ctx, q, state.Request, input.WebSessionDigest, &a) != nil {
			return q.budget.deny()
		}
		result = state.Request
		return nil
	})
	return result, err
}

func (store *Store) ReadWebSearchConsent(ctx context.Context, input ports.ReadWebSearchConsentRequestV1) (domain.WebSearchConsentStateV1, error) {
	return store.readOrDecideOwnedConsent(ctx, input, nil)
}

func (store *Store) DecideWebSearchConsent(ctx context.Context, input ports.DecideWebSearchConsentRequestV1) (domain.WebSearchConsentStateV1, error) {
	if input.Validate() != nil {
		return domain.WebSearchConsentStateV1{}, domain.ErrMembershipDenied
	}
	return store.readOrDecideOwnedConsent(ctx, ports.ReadWebSearchConsentRequestV1{Locator: input.Locator, WebSessionDigest: input.WebSessionDigest}, &input)
}

func (store *Store) readOrDecideOwnedConsent(ctx context.Context, input ports.ReadWebSearchConsentRequestV1, decision *ports.DecideWebSearchConsentRequestV1) (domain.WebSearchConsentStateV1, error) {
	if !store.validOwnedAllocationContext(ctx) || store.attemptOperationBodies == nil || input.Validate() != nil {
		return domain.WebSearchConsentStateV1{}, domain.ErrMembershipDenied
	}
	ctx, cancel := ownedConsentContext(ctx)
	defer cancel()
	intent, err := store.readOwnedConsentIntent(ctx, input)
	if err != nil {
		return domain.WebSearchConsentStateV1{}, domain.ErrMembershipDenied
	}
	grant, err := store.readOwnedConsentGrant(ctx, intent)
	if err != nil {
		return domain.WebSearchConsentStateV1{}, domain.ErrMembershipDenied
	}
	var result domain.WebSearchConsentStateV1
	err = store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		result = domain.WebSearchConsentStateV1{}
		q.metadataCache = make(map[string]ownedMetadataCacheEntry)
		parent, source, err := store.ownedConsentParent(ctx, q, input.Locator)
		if err != nil {
			return err
		}
		states, err := readOwnedConsentStates(ctx, q, parent)
		state, found := findOwnedConsent(states, input.Locator.RequestID)
		want, _ := intent.Digest()
		got, _ := state.Request.Digest()
		if err != nil || !found || got != want || input.Locator.ValidateForRequest(state.Request) != nil {
			return q.budget.deny()
		}
		a, err := store.ownedConsentCurrentAuthority(ctx, q, parent, source, state.Request, grant)
		if err != nil || loadOwnedConsentActor(ctx, q, state.Request, input.WebSessionDigest, &a) != nil {
			return q.budget.deny()
		}
		if decision == nil {
			result = state.Clone()
			return nil
		}
		next, err := domain.DecideWebSearchConsentV1(state, decision.ExpectedRevision, decision.Choice, decision.WebSessionDigest, decision.CSRFTokenDigest, a, q.at)
		if err != nil {
			return q.budget.deny()
		}
		if next.Revision == state.Revision {
			result = state.Clone()
			return nil // immutable exact retry performs no projection rewrite
		}
		projection, err := replaceOwnedConsentState(*states, next, parent, q.at)
		if err != nil || writeOwnedConsentStates(ctx, q, parent, projection) != nil {
			return q.budget.deny()
		}
		actual, err := readOwnedConsentStates(ctx, q, parent)
		result, found = findOwnedConsent(actual, state.Request.ID)
		expected, _ := next.Decision.Digest()
		if err != nil || !found || result.Decision == nil {
			return q.budget.deny()
		}
		observed, _ := result.Decision.Digest()
		if observed != expected || result.Revision != next.Revision || result.Status != next.Status {
			return q.budget.deny()
		}
		return nil
	})
	if err != nil || ctx.Err() != nil {
		return domain.WebSearchConsentStateV1{}, domain.ErrMembershipDenied
	}
	return result, nil
}
