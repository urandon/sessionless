package ydbstore

import (
	"bytes"
	"context"
	"reflect"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localoperator"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type localSourcePublicationSnapshot struct {
	profile  domain.OwnedAgentProfileCurrentV1
	spending [4]domain.AttemptSpendingAuthorizationCurrentV1
	present  [4]bool
	end      time.Time
}

func (store *Store) originalLocalPublicationSource(source *localoperator.Source) bool {
	if store == nil || source == nil || store.db == nil || !store.localCompositionReady() {
		return false
	}
	profile, profileOK := store.ownedProfilePublicationVerifier.(*localoperator.Source)
	spending, spendingOK := store.attemptSpendingPublicationVerifier.(*localoperator.Source)
	return profileOK && spendingOK && profile == source && spending == source
}

// RecoverLocalOwnedExecutionPackage rejoins original installed funding and
// materializes only missing initial components using THIS Store's SAME concrete
// Source issuer. It does not admit a Run, reserve capacity or authorize effects.
// Existing publication heads/audits are never reissued or rewritten. The private
// implementation inherits this original call's deadline/counters across both
// normal transactions and all retries; it is not the future full ingress graph.
func (store *Store) RecoverLocalOwnedExecutionPackage(ctx context.Context, source *localoperator.Source, receipt ports.OwnedProfilePublicationReceiptV1, scope domain.AttemptExecutionSourceScopeV1) (domain.OwnedAgentExecutionPackageV1, error) {
	deny := func() (domain.OwnedAgentExecutionPackageV1, error) {
		return domain.OwnedAgentExecutionPackageV1{}, domain.ErrMembershipDenied
	}
	if ctx == nil || ctx.Err() != nil || !store.originalLocalPublicationSource(source) || scope.Validate() != nil {
		return deny()
	}
	call, cancel := ownedAuthorityCallContext(ctx)
	defer cancel()
	receipt, err := materializeLocalSourceRecoveryReceipt(call, receipt)
	if err != nil {
		return deny()
	}
	life, err := source.ExecutionLifetime()
	if err != nil || life.Err() != nil {
		return deny()
	}
	deadline, bounded := life.Deadline()
	if !bounded {
		return deny()
	}
	call, finish := context.WithDeadline(call, deadline)
	defer finish()
	stop := context.AfterFunc(life, finish)
	defer stop()
	result, err := store.recoverLocalOwnedExecutionPackage(call, source, receipt, scope, life)
	if err != nil || call.Err() != nil || life.Err() != nil || !store.originalLocalPublicationSource(source) {
		return deny()
	}
	return result, nil
}

// This is bounded immutable DATA materialization, not receipt authentication or
// current SQL approval. InputModalities is the profile's only dynamic collection;
// the remaining nested cost/body pointers have fixed arity. ValidateInitial
// bounds their strings/fields before any clone or canonical digest allocation.
func materializeLocalSourceRecoveryReceipt(ctx context.Context, receipt ports.OwnedProfilePublicationReceiptV1) (ports.OwnedProfilePublicationReceiptV1, error) {
	if ctx == nil || len(receipt.Profile.InputModalities) == 0 || len(receipt.Profile.InputModalities) > 3 || receipt.Profile.ValidateInitial() != nil || domain.ValidateSHA256Digest("recovery.receipt", receipt.Digest) != nil {
		return ports.OwnedProfilePublicationReceiptV1{}, domain.ErrMembershipDenied
	}
	if chargeOwnedAuthorityBody(ctx, ownedAuthorityHeadBytes) != nil {
		return ports.OwnedProfilePublicationReceiptV1{}, domain.ErrMembershipDenied
	}
	receipt.Profile = receipt.Profile.Clone()
	return receipt, nil
}

func (store *Store) recoverLocalOwnedExecutionPackage(ctx context.Context, source *localoperator.Source, receipt ports.OwnedProfilePublicationReceiptV1, scope domain.AttemptExecutionSourceScopeV1, life context.Context) (domain.OwnedAgentExecutionPackageV1, error) {
	deny := func() (domain.OwnedAgentExecutionPackageV1, error) {
		return domain.OwnedAgentExecutionPackageV1{}, domain.ErrMembershipDenied
	}
	// No helper-created budget/deadline, caller permission flag or Source is
	// accepted. This helper is composable only under an existing bounded call.
	if ctx == nil || life == nil {
		return deny()
	}
	if _, ok := ctx.Value(ownedAuthorityCallContextKey{}).(*ownedAuthorityBudget); !ok {
		return deny()
	}
	alive := func() bool {
		return ctx.Err() == nil && life.Err() == nil && store.originalLocalPublicationSource(source)
	}
	if !alive() || source.VerifyOriginalSpendingSources(receipt, nil) != nil {
		return deny()
	}
	var original localSourcePublicationSnapshot
	var result domain.OwnedAgentExecutionPackageV1
	err := store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		original, result = localSourcePublicationSnapshot{}, domain.OwnedAgentExecutionPackageV1{}
		if !alive() {
			return q.budget.deny()
		}
		var err error
		original, err = store.readLocalSourcePublicationSnapshot(ctx, q, source, receipt, scope)
		if err != nil {
			return err
		}
		if localSourceSnapshotComplete(original) {
			result, err = buildLocalSourceRecoveryPackage(source, scope, original, q.at)
		}
		if !alive() {
			return q.budget.deny()
		}
		return err
	})
	if err != nil || !alive() {
		return deny()
	}
	if localSourceSnapshotComplete(original) {
		return result, nil
	}
	var grants [4]ports.AttemptSpendingPublicationGrantV1
	defer func() {
		for i := range grants {
			clear(grants[i].Authenticator)
		}
	}()
	var intents [4]string
	end := original.end
	for i, kind := range ownedSpendingKinds {
		if original.present[i] {
			continue
		}
		// Reserve a conservative existing-head+intent allowance BEFORE asking
		// the original issuer to materialize bounded immutable grant bytes.
		if !alive() || chargeOwnedAuthorityBody(ctx, 2*ownedAuthorityHeadBytes) != nil {
			return deny()
		}
		g, err := source.SpendingPublicationBefore(ctx, scope, kind, end)
		if err != nil {
			return deny()
		}
		intent, err := g.IntentDigest()
		if err != nil {
			return deny()
		}
		grants[i], intents[i] = g, intent
		if g.Authorization.Window.ExpiresAt.Before(end) {
			end = g.Authorization.Window.ExpiresAt
		}
	}
	err = store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		result = domain.OwnedAgentExecutionPackageV1{}
		if !alive() {
			return q.budget.deny()
		}
		current, err := store.readLocalSourcePublicationSnapshot(ctx, q, source, receipt, scope)
		if err != nil || !q.at.Before(original.end) || !reflect.DeepEqual(current.profile, original.profile) {
			return q.budget.deny()
		}
		for i := range ownedSpendingKinds {
			if original.present[i] {
				if !current.present[i] || !reflect.DeepEqual(current.spending[i], original.spending[i]) {
					return q.budget.deny()
				}
				continue
			}
			g := grants[i]
			if current.present[i] {
				// SDK/lost-ACK replay must be THIS original prepared grant's exact
				// audit/postimage, not another concurrent initial publication.
				prior, found, err := readSpendingPublicationAudit(ctx, q, g, intents[i])
				if err != nil || !found || !reflect.DeepEqual(prior.Authorization, current.spending[i]) {
					return q.budget.deny()
				}
				continue
			}
			if !alive() || q.at.Before(g.IssuedAt) || !q.at.Before(g.ExpiresAt) || source.VerifyAttemptSpendingPublication(g, q.at) != nil {
				return q.budget.deny()
			}
			next := g.Authorization.Clone()
			next.UpdatedAt, next.Window.PublishedAt = q.at, q.at
			if next.ValidateInitial() != nil || next.ValidateAgainstProfile(current.profile, q.at) != nil || next.Window.ExpiresAt.After(original.end) {
				return q.budget.deny()
			}
			if _, err := writeAttemptSpendingPostimageTx(ctx, q, next, intents[i]); err != nil {
				return err
			}
			actual, found, err := loadOwnedSpendingProjection(ctx, q, next.Locator)
			if err != nil || !found || !reflect.DeepEqual(actual, next) || readLocalRecoveredSpendingAudit(ctx, q, actual) != nil {
				return q.budget.deny()
			}
			current.spending[i], current.present[i] = actual, true
		}
		if !alive() {
			return q.budget.deny()
		}
		result, err = buildLocalSourceRecoveryPackage(source, scope, current, q.at)
		return err
	})
	if err != nil || !alive() {
		return deny()
	}
	return result, nil
}

func localSourceSnapshotComplete(v localSourcePublicationSnapshot) bool {
	return v.present == [4]bool{true, true, true, true}
}

func buildLocalSourceRecoveryPackage(source *localoperator.Source, scope domain.AttemptExecutionSourceScopeV1, v localSourcePublicationSnapshot, now time.Time) (domain.OwnedAgentExecutionPackageV1, error) {
	if !localSourceSnapshotComplete(v) || v.profile.ValidateAt(now) != nil {
		return domain.OwnedAgentExecutionPackageV1{}, domain.ErrMembershipDenied
	}
	issued := v.profile.Window.PublishedAt
	for _, component := range v.spending {
		if component.ValidateAgainstProfile(v.profile, now) != nil {
			return domain.OwnedAgentExecutionPackageV1{}, domain.ErrMembershipDenied
		}
		if component.Window.PublishedAt.After(issued) {
			issued = component.Window.PublishedAt
		}
	}
	result, err := domain.BuildOwnedAgentExecutionPackageV1(scope, v.profile, v.spending, issued)
	if err != nil || source.VerifyOwnedAttemptEffectSource(result.Source) != nil || !now.Before(result.Source.ExpiresAt) {
		return domain.OwnedAgentExecutionPackageV1{}, domain.ErrMembershipDenied
	}
	return result, nil
}

func (store *Store) readLocalSourcePublicationSnapshot(ctx context.Context, q *ownedAuthorityQuery, source *localoperator.Source, receipt ports.OwnedProfilePublicationReceiptV1, scope domain.AttemptExecutionSourceScopeV1) (localSourcePublicationSnapshot, error) {
	deny := func() (localSourcePublicationSnapshot, error) {
		return localSourcePublicationSnapshot{}, q.budget.deny()
	}
	if !store.originalLocalPublicationSource(source) || source.VerifyOriginalSpendingSources(receipt, nil) != nil || scope.TenantID != receipt.Profile.Locator.TenantID || scope.OwnerUserID != receipt.Profile.Locator.OwnerUserID {
		return deny()
	}
	p, found, err := loadOwnedProfileProjection(ctx, q, receipt.Profile.Locator)
	digest, digestErr := p.Digest()
	if err != nil || !found || digestErr != nil || digest != receipt.Digest || !reflect.DeepEqual(p, receipt.Profile) || p.ValidateInitial() != nil || p.ValidateAt(q.at) != nil || readLocalRecoveredProfileAudit(ctx, q, p) != nil {
		return deny()
	}
	v := localSourcePublicationSnapshot{profile: p, end: p.Window.ExpiresAt}
	var components []domain.AttemptSpendingAuthorizationCurrentV1
	for i, kind := range ownedSpendingKinds {
		l := domain.AttemptSpendingAuthorizationLocatorV1{TenantID: scope.TenantID, AttemptID: scope.AttemptID, ComponentKind: kind, BillingOwnerUserID: p.BillingOwnerUserID}
		component, found, err := loadOwnedSpendingProjection(ctx, q, l)
		if err != nil {
			return deny()
		}
		if !found {
			continue
		}
		if component.ValidateInitial() != nil || component.ValidateAgainstProfile(p, q.at) != nil || component.RunID != scope.RunID || component.SessionID != scope.SessionID || component.OwnerUserID != scope.OwnerUserID || readLocalRecoveredSpendingAudit(ctx, q, component) != nil {
			return deny()
		}
		v.spending[i], v.present[i] = component, true
		components = append(components, component)
		if component.Window.ExpiresAt.Before(v.end) {
			v.end = component.Window.ExpiresAt
		}
	}
	if source.VerifyOriginalSpendingSources(receipt, components) != nil || ctx.Err() != nil {
		return deny()
	}
	return v, nil
}

// These immutable audits are original committed-writer DATA, not recovered
// signed grants. In particular the opaque profile intent hash has no persisted
// grant preimage; validate its shape/preservation, NEVER claim crypto recovery.
func readLocalRecoveredProfileAudit(ctx context.Context, q *ownedAuthorityQuery, p domain.OwnedAgentProfileCurrentV1) error {
	l, err := p.Locator.Digest()
	if err != nil {
		return q.budget.deny()
	}
	var intent, raw, digest string
	var generation uint64
	var at time.Time
	n, err := scanOwnedCurrentRow(q.QueryRowContext(ctx, ownedProfileAuditSQL, p.Locator.TenantID, l, p.MutationID), &intent, &raw, &digest, &generation, &at)
	if q.budget.materialized(q.callback, n) != nil || err != nil || len(raw) > ownedAuthorityHeadBytes || domain.ValidateSHA256Digest("profile.audit.intent", intent) != nil || len(digest) != 64 || at.After(q.at) {
		return q.budget.deny()
	}
	want, err := domain.EncodeOwnedAgentProfileCurrentV1(p)
	actual, digestErr := p.Digest()
	if err != nil || digestErr != nil || actual != digest || generation != p.RevocationGeneration || !at.Equal(p.UpdatedAt) || !at.Equal(p.Window.PublishedAt) || !bytes.Equal(want, []byte(raw)) {
		return q.budget.deny()
	}
	return nil
}

func readLocalRecoveredSpendingAudit(ctx context.Context, q *ownedAuthorityQuery, v domain.AttemptSpendingAuthorizationCurrentV1) error {
	l := v.Locator
	var intent, raw, digest, run, session string
	var generation uint64
	var at time.Time
	n, err := scanOwnedCurrentRow(q.QueryRowContext(ctx, spendingAuditSQL, l.TenantID, l.AttemptID, l.ComponentKind, l.BillingOwnerUserID, v.MutationID), &intent, &raw, &digest, &generation, &at, &run, &session)
	if q.budget.materialized(q.callback, n) != nil || err != nil || len(raw) > ownedAuthorityHeadBytes || domain.ValidateSHA256Digest("spending.audit.intent", intent) != nil || len(digest) != 64 || len(run) > 160 || len(session) > 160 || at.After(q.at) {
		return q.budget.deny()
	}
	want, err := domain.EncodeAttemptSpendingAuthorizationCurrentV1(v)
	actual, digestErr := v.Digest()
	if err != nil || digestErr != nil || actual != digest || generation != v.RevocationGeneration || !at.Equal(v.UpdatedAt) || !at.Equal(v.Window.PublishedAt) || run != string(v.RunID) || session != string(v.SessionID) || !bytes.Equal(want, []byte(raw)) {
		return q.budget.deny()
	}
	return nil
}
