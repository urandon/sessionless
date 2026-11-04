package ydbstore

import (
	"context"
	"reflect"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// The existing login port carries ephemeral exchange input. It is not the V2
// durable wire format and cannot issue a linking proof. Callback URI comes only
// from the trusted installation; the BFF must separately pin the actual callback
// route/client before exchange. No public V2 installation is exposed here.
func (store *Store) createLoginChallengeServingV2(ctx context.Context, request domain.OIDCLoginChallenge) error {
	if store == nil || store.webIdentityV2 == nil || request.Validate() != nil || request.ConsumedAt != nil {
		return domain.ErrMembershipDenied
	}
	bindings, err := store.webIdentityV2.bindings()
	if err != nil {
		return err
	}
	binding, enabled := bindings[request.Provider]
	if !enabled || request.Issuer != binding.Issuer || request.Audience != binding.ClientID {
		return domain.ErrMembershipDenied
	}
	if request.ExpiresAt.Sub(request.CreatedAt) > domain.IdentityOperationMaxLifetimeV2 {
		return domain.ErrMembershipDenied
	}
	// The existing BFF clock can have nanosecond precision. Canonicalize only
	// this initial port conversion to YDB Timestamp precision; expiry rounds
	// DOWN, never renews, and stored records are never rounded during reads.
	child := domain.IdentityChallengeV2{Version: 2, Purpose: domain.IdentityPurposeLoginV2,
		StateDigest: request.StateDigest, BrowserBindingDigest: request.BrowserBindingDigest, Binding: binding,
		CreatedAt: request.CreatedAt.UTC().Truncate(time.Microsecond), ExpiresAt: request.ExpiresAt.UTC().Truncate(time.Microsecond), DeleteAfter: request.ExpiresAt.UTC().Truncate(time.Microsecond),
		Login:   &domain.IdentityLoginChallengeV2{ReturnPath: request.RedirectPath},
		Secrets: &domain.IdentityChallengeSecretsV2{PKCEVerifier: request.PKCEVerifier, Nonce: request.Nonce}}
	if child.Validate() != nil {
		return domain.ErrMembershipDenied
	}
	return store.identityAuthorityTxV2(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		// Preserve the caller's original lifetime across transaction retries. The
		// database clock, not a caller's observation or asynchronous TTL, decides
		// whether that original challenge is still live.
		if q.at.Before(child.CreatedAt) || !q.at.Before(child.ExpiresAt) {
			return domain.ErrMembershipDenied
		}
		existing, found, err := readIdentityChallengeV2(ctx, q, child.StateDigest, domain.IdentityPurposeLoginV2)
		if err != nil {
			return err
		}
		if found {
			if existing.ConsumedAt == nil && reflect.DeepEqual(existing, child) {
				return nil
			}
			return domain.ErrMembershipDenied
		}
		return writeIdentityChallengeV2(ctx, q, child)
	})
}

func (store *Store) consumeLoginChallengeServingV2(ctx context.Context, digest domain.SecretDigest, browserSecret string) (domain.OIDCLoginChallenge, error) {
	var empty, result domain.OIDCLoginChallenge
	if store == nil || store.webIdentityV2 == nil || digest.Validate("identity_login_v2.state_digest") != nil || browserSecret == "" {
		return empty, domain.ErrMembershipDenied
	}
	bindings, err := store.webIdentityV2.bindings()
	if err != nil {
		return empty, err
	}
	err = store.identityAuthorityTxV2(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		result = empty
		child, found, err := readIdentityChallengeV2(ctx, q, digest, domain.IdentityPurposeLoginV2)
		if err != nil || !found {
			return domain.ErrMembershipDenied
		}
		cleared, exchange, err := child.Consume(bindings[child.Binding.Provider], browserSecret, nil, q.at)
		if err != nil {
			return domain.ErrMembershipDenied
		}
		if err := writeIdentityChallengeV2(ctx, q, cleared); err != nil {
			return err
		}
		result = domain.OIDCLoginChallenge{Provider: exchange.Binding.Provider, Issuer: exchange.Binding.Issuer, Audience: exchange.Binding.ClientID,
			StateDigest: child.StateDigest, BrowserBindingDigest: child.BrowserBindingDigest, PKCEVerifier: exchange.PKCEVerifier, Nonce: exchange.Nonce,
			RedirectPath: child.Login.ReturnPath, CreatedAt: child.CreatedAt, ExpiresAt: child.ExpiresAt, ConsumedAt: cleared.ConsumedAt}
		return nil
	})
	// A speculative callback result is never an exchange grant. In particular a
	// committed scrub followed by a lost reply stays consumed, with no recovery
	// of secrets or provider retry from the durable row.
	if err != nil {
		return empty, err
	}
	return result, nil
}
