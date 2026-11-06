package ydbstore

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func identityLoginCarrierV2(binding domain.IdentityProviderBindingV2, at time.Time) domain.OIDCLoginChallenge {
	nonce := ""
	if binding.Provider == domain.IdentityProviderTelegram {
		nonce = "synthetic-login-nonce"
	}
	return domain.OIDCLoginChallenge{Provider: binding.Provider, Issuer: binding.Issuer, Audience: binding.ClientID,
		StateDigest: domain.DigestSecret("login-state"), BrowserBindingDigest: domain.DigestSecret("login-browser"),
		PKCEVerifier: strings.Repeat("p", 64), Nonce: nonce, RedirectPath: "/sessions?from=login", CreatedAt: at, ExpiresAt: at.Add(10 * time.Minute)}
}

func TestIdentityLoginServingV2CreateDenials(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*domain.OIDCLoginChallenge, *identityConfigurationV2, *identityProofSQLV2)
	}{
		{"legacy_binding", func(c *domain.OIDCLoginChallenge, _ *identityConfigurationV2, _ *identityProofSQLV2) {
			c.Provider, c.Issuer, c.Audience = "", "", ""
		}},
		{"wrong_issuer", func(c *domain.OIDCLoginChallenge, _ *identityConfigurationV2, _ *identityProofSQLV2) {
			c.Issuer = "https://attacker.example"
		}},
		{"wrong_client", func(c *domain.OIDCLoginChallenge, _ *identityConfigurationV2, _ *identityProofSQLV2) {
			c.Audience = "another-client"
		}},
		{"disabled_provider", func(_ *domain.OIDCLoginChallenge, config *identityConfigurationV2, _ *identityProofSQLV2) {
			config.Providers = config.Providers[:1]
		}},
		{"invalid_callback", func(_ *domain.OIDCLoginChallenge, config *identityConfigurationV2, _ *identityProofSQLV2) {
			config.Providers[1].CallbackURI = "https://sessionless.example/wrong"
		}},
		{"no_manifest", func(_ *domain.OIDCLoginChallenge, config *identityConfigurationV2, _ *identityProofSQLV2) {
			config.ManifestDigest = ""
		}},
		{"duplicate_provider", func(_ *domain.OIDCLoginChallenge, config *identityConfigurationV2, _ *identityProofSQLV2) {
			config.Providers[0] = config.Providers[1]
		}},
		{"unsafe_return", func(c *domain.OIDCLoginChallenge, _ *identityConfigurationV2, _ *identityProofSQLV2) {
			c.RedirectPath = "//attacker.example/"
		}},
		{"missing_pkce", func(c *domain.OIDCLoginChallenge, _ *identityConfigurationV2, _ *identityProofSQLV2) {
			c.PKCEVerifier = ""
		}},
		{"missing_nonce", func(c *domain.OIDCLoginChallenge, _ *identityConfigurationV2, _ *identityProofSQLV2) { c.Nonce = "" }},
		{"same_state_browser", func(c *domain.OIDCLoginChallenge, _ *identityConfigurationV2, _ *identityProofSQLV2) {
			c.BrowserBindingDigest = c.StateDigest
		}},
		{"long_lifetime", func(c *domain.OIDCLoginChallenge, _ *identityConfigurationV2, _ *identityProofSQLV2) {
			c.ExpiresAt = c.ExpiresAt.Add(time.Microsecond)
		}},
		{"already_consumed", func(c *domain.OIDCLoginChallenge, _ *identityConfigurationV2, _ *identityProofSQLV2) {
			at := c.CreatedAt
			c.ConsumedAt = &at
		}},
		{"before_creation", func(c *domain.OIDCLoginChallenge, _ *identityConfigurationV2, f *identityProofSQLV2) {
			f.at = c.CreatedAt.Add(-time.Microsecond)
		}},
		{"at_expiry", func(c *domain.OIDCLoginChallenge, _ *identityConfigurationV2, f *identityProofSQLV2) {
			f.at = c.ExpiresAt
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f, store, ctx, _, config := newIdentityProofStorageV2(t, domain.IdentityOperationLinkV2)
			challenge := identityLoginCarrierV2(config.Providers[1], f.at)
			test.mutate(&challenge, &config, f)
			store.webIdentityV2 = &config
			if err := store.CreateLoginChallenge(ctx, challenge); err == nil || len(f.children) != 0 || f.base.writes != 0 {
				t.Errorf("invalid ordinary login persisted: err=%v writes=%d", err, f.base.writes)
			}
		})
	}
}

func TestIdentityLoginServingV2ConsumeDenials(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*identityProofSQLV2, *identityConfigurationV2, domain.SecretDigest, *string)
	}{
		{"wrong_browser", func(_ *identityProofSQLV2, _ *identityConfigurationV2, _ domain.SecretDigest, browser *string) {
			*browser = "another-browser"
		}},
		{"empty_browser", func(_ *identityProofSQLV2, _ *identityConfigurationV2, _ domain.SecretDigest, browser *string) {
			*browser = ""
		}},
		{"disabled_provider", func(_ *identityProofSQLV2, config *identityConfigurationV2, _ domain.SecretDigest, _ *string) {
			config.Providers = config.Providers[:1]
		}},
		{"changed_client", func(_ *identityProofSQLV2, config *identityConfigurationV2, _ domain.SecretDigest, _ *string) {
			config.Providers[1].ClientID = "changed-client"
		}},
		{"changed_callback", func(_ *identityProofSQLV2, config *identityConfigurationV2, _ domain.SecretDigest, _ *string) {
			config.Providers[1].CallbackURI = "https://new.example/auth/telegram/callback"
		}},
		{"before_creation", func(f *identityProofSQLV2, _ *identityConfigurationV2, _ domain.SecretDigest, _ *string) {
			f.at = f.at.Add(-time.Microsecond)
		}},
		{"at_expiry", func(f *identityProofSQLV2, _ *identityConfigurationV2, _ domain.SecretDigest, _ *string) {
			f.at = f.at.Add(10 * time.Minute)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			f, store, ctx, _, config := newIdentityProofStorageV2(t, domain.IdentityOperationLinkV2)
			store.webIdentityV2 = &config
			challenge := identityLoginCarrierV2(config.Providers[1], f.at)
			if err := store.CreateLoginChallenge(ctx, challenge); err != nil {
				t.Fatal(err)
			}
			before := cloneIdentityProofRowsV2(f.children)
			browser := "login-browser"
			test.mutate(f, &config, challenge.StateDigest, &browser)
			got, err := store.ConsumeLoginChallenge(ctx, challenge.StateDigest, browser, challenge.CreatedAt.Add(time.Second))
			if err == nil || !reflect.DeepEqual(got, domain.OIDCLoginChallenge{}) || !reflect.DeepEqual(before, f.children) {
				t.Errorf("invalid consume released secrets or changed durable state: err=%v", err)
			}
		})
	}
}

func TestIdentityLoginServingV2PhysicalJSONCorrespondence(t *testing.T) {
	for _, column := range []int{2, 3, 4, 5, 6, 7, 8, 10} {
		for _, null := range []bool{false, true} {
			name := []string{"bucket", "key", "browser", "pkce", "nonce", "return", "created", "expires", "consumed", "record", "delete_after"}[column]
			if null {
				name += "/null"
			}
			t.Run(name, func(t *testing.T) {
				f, store, ctx, _, config := newIdentityProofStorageV2(t, domain.IdentityOperationLinkV2)
				store.webIdentityV2 = &config
				challenge := identityLoginCarrierV2(config.Providers[1], f.at)
				if err := store.CreateLoginChallenge(ctx, challenge); err != nil {
					t.Fatal(err)
				}
				row := f.children[challenge.StateDigest]
				if null {
					row[column] = nil
				} else if column < 6 {
					row[column] = "different"
				} else {
					row[column] = f.at.Add(2 * time.Second)
				}
				before := cloneIdentityProofRowsV2(f.children)
				got, err := store.ConsumeLoginChallenge(ctx, challenge.StateDigest, "login-browser", f.at)
				if err == nil || !reflect.DeepEqual(got, domain.OIDCLoginChallenge{}) || !reflect.DeepEqual(before, f.children) {
					t.Errorf("physical column %d mismatch/null escaped strict read: err=%v", column, err)
				}
			})
		}
	}
}

func TestIdentityLoginServingV2StrictCollisionDoesNotOverwrite(t *testing.T) {
	for _, test := range []struct {
		name string
		raw  func(string) string
	}{
		{"legacy_v1", func(_ string) string { return `{"state_digest":"legacy"}` }},
		{"operation_purpose", func(raw string) string { return strings.Replace(raw, `"purpose":"login"`, `"purpose":"operation"`, 1) }},
		{"duplicate_version", func(raw string) string { return strings.Replace(raw, `"version":2`, `"version":2,"version":2`, 1) }},
		{"unknown_field", func(raw string) string { return strings.Replace(raw, `"version":2`, `"unknown":true,"version":2`, 1) }},
		{"missing_login", func(raw string) string {
			return strings.Replace(raw, `"login":{"return_path":"/sessions?from=login"}`, `"login":null`, 1)
		}},
		{"wrong_state", func(raw string) string {
			return strings.ReplaceAll(raw, string(domain.DigestSecret("login-state")), string(domain.DigestSecret("another-state")))
		}},
		{"oversized_record", func(_ string) string { return strings.Repeat(" ", domain.IdentityAuthMaxRecordBytesV2+1) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			f, store, ctx, _, config := newIdentityProofStorageV2(t, domain.IdentityOperationLinkV2)
			store.webIdentityV2 = &config
			challenge := identityLoginCarrierV2(config.Providers[1], f.at)
			if err := store.CreateLoginChallenge(ctx, challenge); err != nil {
				t.Fatal(err)
			}
			row := f.children[challenge.StateDigest]
			raw := row[9].(string)
			row[9] = test.raw(raw)
			if row[9] == raw {
				t.Fatal("mutation did not establish invalid input")
			}
			before := cloneIdentityProofRowsV2(f.children)
			if err := store.CreateLoginChallenge(ctx, challenge); err == nil {
				t.Error("occupied malformed key overwritten")
			}
			got, err := store.ConsumeLoginChallenge(ctx, challenge.StateDigest, "login-browser", f.at)
			if err == nil || !reflect.DeepEqual(got, domain.OIDCLoginChallenge{}) || !reflect.DeepEqual(before, f.children) {
				t.Errorf("invalid tagged record escaped or changed: %v", err)
			}
		})
	}
}

func TestIdentityLoginServingV2CommitAndRetryBoundaries(t *testing.T) {
	for _, name := range []string{"write_rollback", "lost_commit", "consume_retry_expiry", "create_retry_expiry", "live_retry"} {
		t.Run(name, func(t *testing.T) {
			f, store, ctx, _, config := newIdentityProofStorageV2(t, domain.IdentityOperationLinkV2)
			store.webIdentityV2 = &config
			challenge := identityLoginCarrierV2(config.Providers[1], f.at)
			if name == "create_retry_expiry" {
				f.rollbackOnce, f.retryAdvance = true, 10*time.Minute
				if err := store.CreateLoginChallenge(ctx, challenge); err == nil || len(f.children) != 0 {
					t.Errorf("create retry renewed original expiry: %v", err)
				}
				return
			}
			if err := store.CreateLoginChallenge(ctx, challenge); err != nil {
				t.Fatal(err)
			}
			before := cloneIdentityProofRowsV2(f.children)
			switch name {
			case "write_rollback":
				f.childFault = errors.New("controlled login write failure")
			case "lost_commit":
				f.commitReplyError = errors.New("controlled lost login commit reply")
			case "consume_retry_expiry":
				f.rollbackOnce, f.retryAdvance = true, 10*time.Minute
			case "live_retry":
				f.rollbackOnce, f.retryAdvance = true, time.Second
			}
			got, err := store.ConsumeLoginChallenge(ctx, challenge.StateDigest, "login-browser", f.at)
			if name == "live_retry" {
				if err != nil || got.ConsumedAt == nil || !got.ConsumedAt.Equal(f.at) || f.base.rollbacks != 1 {
					t.Fatalf("live retry did not use current SQL time: %v", err)
				}
				return
			}
			if err == nil || !reflect.DeepEqual(got, domain.OIDCLoginChallenge{}) {
				t.Fatalf("failed/ambiguous consume released exchange material: %v", err)
			}
			if name != "lost_commit" {
				if !reflect.DeepEqual(before, f.children) || f.base.rollbacks == 0 {
					t.Error("failed transaction altered durable challenge")
				}
				return
			}
			stored := f.children[challenge.StateDigest]
			cleared, err := domain.DecodeIdentityChallengeV2([]byte(stored[9].(string)))
			if err != nil || cleared.Secrets != nil || cleared.ConsumedAt == nil || stored[3] != "" || stored[4] != "" || f.base.commits != 2 {
				t.Fatalf("lost reply did not leave committed scrub: %v", err)
			}
			if got, err := store.ConsumeLoginChallenge(ctx, challenge.StateDigest, "login-browser", f.at); err == nil || !reflect.DeepEqual(got, domain.OIDCLoginChallenge{}) {
				t.Error("ambiguous consume was reconstructed/replayed")
			}
		})
	}
}

func TestIdentityLoginServingV2PurposeIsolation(t *testing.T) {
	for _, loginFirst := range []bool{false, true} {
		name := "proof_first"
		if loginFirst {
			name = "login_first"
		}
		t.Run(name, func(t *testing.T) {
			f, store, ctx, request, config := newIdentityProofStorageV2(t, domain.IdentityOperationLinkV2)
			store.webIdentityV2 = &config
			operation, err := store.startIdentityOperationV2(ctx, request, config)
			if err != nil {
				t.Fatal(err)
			}
			challenge := identityLoginCarrierV2(config.Providers[1], f.at)
			challenge.StateDigest = request.TargetChallengeDigest
			childRequest := identityProofChildRequestV2(operation, request.Phase)
			if loginFirst {
				if err := store.CreateLoginChallenge(ctx, challenge); err != nil {
					t.Fatal(err)
				}
				before := cloneIdentityProofRowsV2(f.children)
				if _, err := store.createIdentityProofV2(ctx, childRequest, config); err == nil {
					t.Error("proof writer repurposed ordinary login")
				}
				got, err := store.consumeIdentityProofV2(ctx, identityProofConsumeV2{Phase: request.Phase, ChallengeDigest: challenge.StateDigest, BrowserSecret: "login-browser"}, config)
				if err == nil || !reflect.DeepEqual(got, identityConsumedProofV2{}) || !reflect.DeepEqual(before, f.children) {
					t.Error("ordinary login acquired proof authority or was changed")
				}
			} else {
				if _, err := store.createIdentityProofV2(ctx, childRequest, config); err != nil {
					t.Fatal(err)
				}
				before := cloneIdentityProofRowsV2(f.children)
				if err := store.CreateLoginChallenge(ctx, challenge); err == nil {
					t.Error("login writer repurposed linking proof")
				}
				got, err := store.ConsumeLoginChallenge(ctx, challenge.StateDigest, "proof-browser", f.at)
				if err == nil || !reflect.DeepEqual(got, domain.OIDCLoginChallenge{}) || !reflect.DeepEqual(before, f.children) {
					t.Error("linking proof became ordinary login or was changed")
				}
			}
		})
	}
}

func TestIdentityLoginServingV2StoragePrecision(t *testing.T) {
	f, store, ctx, _, config := newIdentityProofStorageV2(t, domain.IdentityOperationLinkV2)
	store.webIdentityV2 = &config
	original := f.at
	challenge := identityLoginCarrierV2(config.Providers[1], original.Add(789*time.Nanosecond).In(time.FixedZone("test-offset", 3*60*60)))
	// Exactly ten original minutes remain ten; conversion never rounds expiry up.
	if err := store.CreateLoginChallenge(ctx, challenge); err != nil {
		t.Fatal(err)
	}
	stored, err := domain.DecodeIdentityChallengeV2([]byte(f.children[challenge.StateDigest][9].(string)))
	if err != nil || !stored.CreatedAt.Equal(original) || !stored.ExpiresAt.Equal(original.Add(10*time.Minute)) || stored.ExpiresAt.After(challenge.ExpiresAt) || !stored.DeleteAfter.Equal(stored.ExpiresAt) {
		t.Fatalf("initial precision conversion renewed/changed lifetime: %v", err)
	}
	if err := store.CreateLoginChallenge(ctx, challenge); err != nil {
		t.Fatalf("same original nanosecond/offset request not idempotent: %v", err)
	}
	for _, name := range []string{"long_by_nanosecond", "collapsed_lifetime", "changed_secrets", "changed_return", "changed_deadline"} {
		t.Run(name, func(t *testing.T) {
			c := challenge
			switch name {
			case "long_by_nanosecond":
				c.ExpiresAt = c.ExpiresAt.Add(time.Nanosecond)
			case "collapsed_lifetime":
				c.ExpiresAt = c.CreatedAt.Add(time.Nanosecond)
			case "changed_secrets":
				c.PKCEVerifier = strings.Repeat("s", 64)
			case "changed_return":
				c.RedirectPath = "/different"
			case "changed_deadline":
				c.ExpiresAt = c.ExpiresAt.Add(-time.Microsecond)
			}
			before := cloneIdentityProofRowsV2(f.children)
			if err := store.CreateLoginChallenge(ctx, c); err == nil || !reflect.DeepEqual(before, f.children) {
				t.Errorf("invalid conversion or collision refreshed challenge: %v", err)
			}
		})
	}
	f.at = stored.ExpiresAt
	if got, err := store.ConsumeLoginChallenge(ctx, challenge.StateDigest, "login-browser", challenge.CreatedAt); err == nil || !reflect.DeepEqual(got, domain.OIDCLoginChallenge{}) {
		t.Error("sub-microsecond tail extended logical expiry")
	}
}

func TestIdentityLoginServingV2BoundedReturnPathAndCancelledCall(t *testing.T) {
	f, store, ctx, _, config := newIdentityProofStorageV2(t, domain.IdentityOperationLinkV2)
	store.webIdentityV2 = &config
	challenge := identityLoginCarrierV2(config.Providers[1], f.at)
	challenge.RedirectPath = "/sessions?return=" + strings.Repeat("x", 300)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := store.CreateLoginChallenge(cancelled, challenge); err == nil || f.base.begins != 0 {
		t.Error("cancelled creation began SQL")
	}
	if err := store.CreateLoginChallenge(ctx, challenge); err != nil {
		t.Fatal(err)
	}
	before := cloneIdentityProofRowsV2(f.children)
	if got, err := store.ConsumeLoginChallenge(cancelled, challenge.StateDigest, "login-browser", f.at); err == nil || !reflect.DeepEqual(got, domain.OIDCLoginChallenge{}) || !reflect.DeepEqual(before, f.children) {
		t.Error("cancelled call consumed or released secrets")
	}
	got, err := store.ConsumeLoginChallenge(ctx, challenge.StateDigest, "login-browser", f.at)
	if err != nil || got.RedirectPath != challenge.RedirectPath {
		t.Fatalf("safe path within original 8KiB record bound lost by physical projection: %v", err)
	}
}

// Exercise the actual serving ports with an isolated serializable SQL peer.
// This is not provider verification, a public V2 installer or native YDB proof.
func TestIdentityLoginServingV2RoundTrip(t *testing.T) {
	for _, provider := range []domain.IdentityProvider{domain.IdentityProviderYandex, domain.IdentityProviderTelegram} {
		for _, both := range []bool{false, true} {
			name := string(provider) + "/single"
			if both {
				name = string(provider) + "/both"
			}
			t.Run(name, func(t *testing.T) {
				f, store, ctx, _, config := newIdentityProofStorageV2(t, domain.IdentityOperationLinkV2)
				bindings, err := config.bindings()
				if err != nil {
					t.Fatal(err)
				}
				config.SensitiveOperationsEnabled = false
				if !both {
					config.Providers = []domain.IdentityProviderBindingV2{bindings[provider]}
				}
				store.webIdentityV2 = &config
				challenge := identityLoginCarrierV2(bindings[provider], f.at)
				before := f.base.durable.clone()
				if err := store.CreateLoginChallenge(ctx, challenge); err != nil {
					t.Fatalf("create ordinary %s login with linking disabled: %v", provider, err)
				}
				pending := append([]any(nil), f.children[challenge.StateDigest]...)
				if err := store.CreateLoginChallenge(ctx, challenge); err != nil || !reflect.DeepEqual(pending, f.children[challenge.StateDigest]) {
					t.Fatalf("identical pending creation changed original deadline/secrets: %v", err)
				}
				f.at = f.at.Add(time.Second)
				// A caller's skewed clock is not authority in V2.
				got, err := store.ConsumeLoginChallenge(ctx, challenge.StateDigest, "login-browser", f.at.Add(-24*time.Hour))
				want := challenge
				consumed := f.at
				want.ConsumedAt = &consumed
				if err != nil || !reflect.DeepEqual(got, want) {
					t.Fatalf("one ephemeral exchange carrier differs: err=%v got=%+v", err, got)
				}
				stored := f.children[challenge.StateDigest]
				cleared, err := domain.DecodeIdentityChallengeV2([]byte(stored[9].(string)))
				if err != nil || cleared.Purpose != domain.IdentityPurposeLoginV2 || cleared.Secrets != nil || cleared.ConsumedAt == nil || !cleared.ConsumedAt.Equal(f.at) || stored[3] != "" || stored[4] != "" || stored[5] != challenge.RedirectPath {
					t.Fatalf("committed challenge not purpose-bound and scrubbed: err=%v", err)
				}
				if err := store.CreateLoginChallenge(ctx, challenge); err == nil {
					t.Error("consumed state was resurrected by duplicate creation")
				}
				if retry, err := store.ConsumeLoginChallenge(ctx, challenge.StateDigest, "login-browser", f.at); err == nil || !reflect.DeepEqual(retry, domain.OIDCLoginChallenge{}) {
					t.Error("second exchange escaped consumed state")
				}
				if !reflect.DeepEqual(before, f.base.durable) || f.operation != "" || len(f.audit) != 0 {
					t.Error("ordinary login modified identity, session, parent or audit authority")
				}
			})
		}
	}
}
