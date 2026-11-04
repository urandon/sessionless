package ydbstore

import (
	"context"
	"crypto/rand"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// Selected by the existing disposable CI gate only. No local/shared database,
// provider exchange, registered OAuth client or public serving activation.
func TestWebIdentityV2YDBOrdinaryLoginChallengePorts(t *testing.T) {
	for _, provider := range []domain.IdentityProvider{domain.IdentityProviderYandex, domain.IdentityProviderTelegram} {
		t.Run(string(provider), func(t *testing.T) {
			db, ctx, identity, _, _ := newWebIdentityV2NativeFixture(t)
			store, err := New(db, Options{})
			if err != nil {
				t.Fatal(err)
			}
			binding := domain.IdentityProviderBindingV2{Provider: provider, Issuer: domain.YandexOAuthIssuer, ClientID: "native-login-synthetic", CallbackURI: "https://sessionless.example/auth/login/callback"}
			nonce := ""
			if provider == domain.IdentityProviderTelegram {
				binding.Issuer, binding.CallbackURI, nonce = "https://oauth.telegram.org", "https://sessionless.example/auth/telegram/callback", "native-login-synthetic-nonce"
			}
			store.webIdentityV2 = &identityConfigurationV2{ManifestDigest: domain.DigestSecret("native-login-manifest"), Providers: []domain.IdentityProviderBindingV2{binding}}
			key := rand.Text()
			challenge := domain.OIDCLoginChallenge{Provider: provider, Issuer: binding.Issuer, Audience: binding.ClientID,
				StateDigest: domain.DigestSecret("native-login-state-" + key), BrowserBindingDigest: domain.DigestSecret("native-login-browser-" + key),
				PKCEVerifier: strings.Repeat("p", 64), Nonce: nonce, RedirectPath: "/sessions", CreatedAt: identity.CreatedAt, ExpiresAt: identity.CreatedAt.Add(10 * time.Minute)}
			bucket, err := webBucket(string(challenge.StateDigest))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if _, err := db.ExecContext(cleanup, `DELETE FROM oidc_login_challenges WHERE shard_bucket=$1 AND state_digest=$2`, bucket, challenge.StateDigest); err != nil {
					t.Errorf("cleanup own native login key: %v", err)
				}
			})
			if err := store.CreateLoginChallenge(ctx, challenge); err != nil {
				t.Fatal("native create", err)
			}
			if err := store.CreateLoginChallenge(ctx, challenge); err != nil {
				t.Fatal("native exact pending duplicate", err)
			}
			if got, err := store.ConsumeLoginChallenge(ctx, challenge.StateDigest, "wrong-browser", challenge.CreatedAt); err == nil || !reflect.DeepEqual(got, domain.OIDCLoginChallenge{}) {
				t.Fatal("native wrong browser received exchange material")
			}
			got, err := store.ConsumeLoginChallenge(ctx, challenge.StateDigest, "native-login-browser-"+key, challenge.CreatedAt.Add(-time.Hour))
			if err != nil || got.ConsumedAt == nil || got.PKCEVerifier != challenge.PKCEVerifier || got.Nonce != nonce || got.Provider != provider {
				t.Fatalf("native consume did not commit before exchange: %v", err)
			}
			if err := store.identityAuthorityTxV2(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
				stored, found, err := readIdentityChallengeV2(ctx, q, challenge.StateDigest, domain.IdentityPurposeLoginV2)
				// The strict reader also checks every physical PKCE/nonce/time column
				// against JSON; empty Secrets alone is not sufficient.
				if err != nil || !found || stored.Secrets != nil || stored.ConsumedAt == nil || !stored.ConsumedAt.Equal(*got.ConsumedAt) || !stored.ExpiresAt.Equal(challenge.ExpiresAt) {
					return domain.ErrMembershipDenied
				}
				return nil
			}); err != nil {
				t.Fatal("native committed physical/JSON scrub readback", err)
			}
			if again, err := store.ConsumeLoginChallenge(ctx, challenge.StateDigest, "native-login-browser-"+key, challenge.CreatedAt); err == nil || !reflect.DeepEqual(again, domain.OIDCLoginChallenge{}) {
				t.Error("native replay released scrubbed secrets")
			}
			if err := store.CreateLoginChallenge(ctx, challenge); err == nil {
				t.Error("native creation resurrected consumed challenge")
			}
		})
	}
}
