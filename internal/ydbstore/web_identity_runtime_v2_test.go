package ydbstore

import (
	"bytes"
	"database/sql"
	"errors"
	"reflect"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func runtimeOptionsV2() WebIdentityRuntimeOptionsV2 {
	return WebIdentityRuntimeOptionsV2{
		Providers: []IdentityAccountProviderV2{
			{Binding: domain.IdentityProviderBindingV2{Provider: domain.IdentityProviderYandex, Issuer: domain.YandexOAuthIssuer, ClientID: "runtime-yandex", CallbackURI: "https://web.example.invalid/auth/login/callback"}, Prover: accountNoProbeV2{}},
			{Binding: domain.IdentityProviderBindingV2{Provider: domain.IdentityProviderTelegram, Issuer: "https://oauth.telegram.org", ClientID: "123456", CallbackURI: "https://web.example.invalid/auth/telegram/callback"}, Prover: accountNoProbeV2{}},
		},
		HMACKey: bytes.Repeat([]byte{41}, 32), SensitiveOperationsEnabled: true,
	}
}

func TestWebIdentityRuntimeV2PublishesOneImmutablePairWithoutIO(t *testing.T) {
	t.Parallel()
	for _, enabled := range []bool{true, false} {
		t.Run(map[bool]string{true: "on", false: "off"}[enabled], func(t *testing.T) {
			options := runtimeOptionsV2()
			options.SensitiveOperationsEnabled = enabled
			originalKey := bytes.Clone(options.HMACKey)
			// An uninitialized DB deliberately panics if construction tries I/O.
			db := &sql.DB{}
			store, service, err := NewWebIdentityRuntimeV2(db, Options{}, options)
			if err != nil || store == nil || service == nil {
				t.Fatal("complete runtime constructor", err)
			}
			actual := service.(*identityAccountServiceV2)
			if actual.store != store || store.db != db || store.webIdentityV2 == nil || actual.config.SensitiveOperationsEnabled != enabled || !reflect.DeepEqual(*store.webIdentityV2, actual.config) {
				t.Fatal("unpaired store/service or wrong version/gate")
			}
			bindings := append([]domain.IdentityProviderBindingV2(nil), store.webIdentityV2.Providers...)
			manifest, err := WebIdentityManifestDigestV2(bindings)
			if err != nil || manifest != actual.config.ManifestDigest || service.ValidateLoginBindings(bindings) != nil {
				t.Fatal("manifest mismatch", err)
			}
			clear(options.HMACKey)
			options.Providers[0].Binding.ClientID = "changed-after-construction"
			options.Providers[1].Prover = nil
			bindings[0].ClientID = "changed-read-copy"
			if !bytes.Equal(actual.key, originalKey) || actual.config.Providers[0].ClientID != "runtime-yandex" || !reflect.DeepEqual(*store.webIdentityV2, actual.config) || service.ValidateLoginBindings(actual.config.Providers) != nil {
				t.Fatal("caller mutation changed live authority")
			}
			if legacy, err := New(db, Options{}); err != nil || legacy.webIdentityV2 != nil {
				t.Fatal("legacy constructor silently promoted", err)
			}
		})
	}
}

func TestWebIdentityRuntimeV2RejectsIncompletePairWithoutPublishingStore(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		mutate func(*WebIdentityRuntimeOptionsV2)
	}{
		{"no_providers", func(o *WebIdentityRuntimeOptionsV2) { o.Providers = nil }},
		{"reversed", func(o *WebIdentityRuntimeOptionsV2) { o.Providers[0], o.Providers[1] = o.Providers[1], o.Providers[0] }},
		{"duplicate", func(o *WebIdentityRuntimeOptionsV2) { o.Providers[1] = o.Providers[0] }},
		{"nil_prover", func(o *WebIdentityRuntimeOptionsV2) { o.Providers[0].Prover = nil }},
		{"typed_nil_prover", func(o *WebIdentityRuntimeOptionsV2) { var p *accountNoProbeV2; o.Providers[0].Prover = p }},
		{"callback_mixup", func(o *WebIdentityRuntimeOptionsV2) {
			o.Providers[1].Binding.CallbackURI = o.Providers[0].Binding.CallbackURI
		}},
		{"issuer_mixup", func(o *WebIdentityRuntimeOptionsV2) { o.Providers[1].Binding.Issuer = o.Providers[0].Binding.Issuer }},
		{"short_key", func(o *WebIdentityRuntimeOptionsV2) { o.HMACKey = make([]byte, 31) }},
		{"oversized_key", func(o *WebIdentityRuntimeOptionsV2) { o.HMACKey = make([]byte, 4097) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := runtimeOptionsV2()
			tc.mutate(&o)
			s, p, err := NewWebIdentityRuntimeV2(&sql.DB{}, Options{}, o)
			if err == nil || s != nil || p != nil {
				t.Fatalf("%s published incomplete runtime: store=%v service=%v err=%v", tc.name, s != nil, p != nil, err)
			}
		})
	}
	if s, p, err := NewWebIdentityRuntimeV2(nil, Options{}, runtimeOptionsV2()); err == nil || s != nil || p != nil {
		t.Fatal("nil DB published runtime")
	}
}

func TestWebIdentityRuntimeV2ManifestClosedAndRedeployStable(t *testing.T) {
	t.Parallel()
	o := runtimeOptionsV2()
	bindings := []domain.IdentityProviderBindingV2{o.Providers[0].Binding, o.Providers[1].Binding}
	want := domain.DigestSecret(`{"version":"web-identity-runtime.v2.session-link-v1","yandex_proof":"yandex_fresh_authorization","telegram_proof":"telegram_signed_auth_time","providers":[{"provider":"yandex","issuer":"https://oauth.yandex.ru","client_id":"runtime-yandex","callback_uri":"https://web.example.invalid/auth/login/callback"},{"provider":"telegram","issuer":"https://oauth.telegram.org","client_id":"123456","callback_uri":"https://web.example.invalid/auth/telegram/callback"}]}`)
	got, err := WebIdentityManifestDigestV2(bindings)
	if err != nil || got != want {
		t.Fatalf("canonical manifest differs: got=%s want=%s err=%v", got, want, err)
	}
	for _, provider := range o.Providers {
		if _, err := WebIdentityManifestDigestV2([]domain.IdentityProviderBindingV2{provider.Binding}); err != nil {
			t.Fatal("valid single provider", err)
		}
	}
	s1, _, err := NewWebIdentityRuntimeV2(&sql.DB{}, Options{}, o)
	if err != nil {
		t.Fatal(err)
	}
	o.SensitiveOperationsEnabled = false
	o.HMACKey = bytes.Repeat([]byte{42}, 32)
	s2, _, err := NewWebIdentityRuntimeV2(&sql.DB{}, Options{}, o)
	if err != nil || s1.webIdentityV2.ManifestDigest != s2.webIdentityV2.ManifestDigest {
		t.Fatal("gate/key changed proof configuration pin", err)
	}
	bindings[1].ClientID = "234567"
	changed, err := WebIdentityManifestDigestV2(bindings)
	if err != nil || changed == want {
		t.Fatal("changed client not bound", err)
	}
}

func TestWebIdentityRuntimeV2SensitiveOffRetainsCurrentReadsAndRetiresLegacyWriters(t *testing.T) {
	f, legacy, ctx, _, config := newIdentityProofStorageV2(t, domain.IdentityOperationLinkV2)
	o := runtimeOptionsV2()
	o.Providers = nil
	o.SensitiveOperationsEnabled = false
	for _, b := range config.Providers {
		o.Providers = append(o.Providers, IdentityAccountProviderV2{Binding: b, Prover: accountNoProbeV2{}})
	}
	s, service, err := NewWebIdentityRuntimeV2(legacy.db, Options{}, o)
	if err != nil {
		t.Fatal(err)
	}
	methods, err := service.Methods(ctx, ports.IdentityAccountMethodsRequestV2{Auth: ports.IdentityAccountReadAuthV2{SessionCookie: "rows-session"}})
	if err != nil || methods.Revision != 7 || len(methods.Methods) != 2 || !methods.Methods[0].Linked || !methods.Methods[0].Current {
		t.Fatalf("sensitiveOFF lost existing current V2 read: %+v err=%v", methods, err)
	}
	for _, m := range methods.Methods {
		if len(m.AllowedActions) != 0 {
			t.Fatal("sensitiveOFF exposed new mutation")
		}
	}
	before := f.base.writes
	if r, err := service.Start(ctx, ports.IdentityAccountStartRequestV2{Auth: accountAuthV2(), Kind: domain.IdentityOperationLinkV2, TargetProvider: domain.IdentityProviderTelegram, ExpectedRevision: 7}); err != ports.IdentityAccountAccessDeniedV2 || r.View.OperationID != "" {
		t.Fatalf("OFF start=%+v err=%v", r, err)
	}
	if _, err := s.Enroll(ctx, ports.EnrollmentRequest{}); !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatal("retired V2 enrollment admitted", err)
	}
	if _, err := s.BootstrapDevelopmentMembership(ctx, domain.DevelopmentBootstrapGrant{}); !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatal("retired V2 bootstrap admitted", err)
	}
	if _, err := s.EnsureTelegramIdentity(ctx, ports.TelegramIdentityRequest{}); !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatal("retired V2 Telegram chat admitted", err)
	}
	if f.base.writes != before {
		t.Fatal("OFF/retired writer mutated storage")
	}
}
