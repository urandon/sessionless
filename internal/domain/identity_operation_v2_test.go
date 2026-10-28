package domain_test

import (
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

func operationV2At() time.Time { return time.Date(2026, 10, 26, 14, 0, 0, 0, time.UTC) }

func operationV2Fixture(kind domain.IdentityOperationKindV2) domain.IdentityOperationV2 {
	authenticated := domain.ExternalSubject{Provider: domain.IdentityProviderYandex, Subject: "subject-original"}
	scope := domain.IdentityOperationScopeV2{UserID: "operation-user", SessionDigest: domain.DigestSecret("original-session"), AuthenticatedSubject: authenticated, TenantID: "operation-tenant", MembershipSecurityVersion: 3, BrowserBindingDigest: domain.DigestSecret("operation-browser"), ConfigurationDigest: domain.DigestSecret("configured-manifest"), ExpectedRevision: 7}
	pending := &domain.IdentityOperationPendingV2{Scope: scope, SourceSubject: authenticated, TargetProvider: domain.IdentityProviderTelegram, SourceChallengeDigest: domain.DigestSecret("source-child"), TargetChallengeDigest: domain.DigestSecret("target-child")}
	if kind == domain.IdentityOperationUnlinkV2 {
		target := authenticated
		pending.SourceSubject = domain.ExternalSubject{Provider: domain.IdentityProviderTelegram, Subject: "subject-surviving"}
		pending.TargetProvider = target.Provider
		pending.TargetSubject = &target
		pending.TargetChallengeDigest = ""
	}
	created := operationV2At()
	return domain.IdentityOperationV2{Version: 2, Purpose: domain.IdentityPurposeOperationV2, OperationDigest: domain.DigestSecret("operation-address"), Kind: kind, Phase: domain.IdentityPhaseSourceProofV2, CreatedAt: created, ExpiresAt: created.Add(10 * time.Minute), DeleteAfter: created.Add(10 * time.Minute), Pending: pending}
}

func operationV2ProofChallenge(operation domain.IdentityOperationV2) domain.IdentityChallengeV2 {
	pending := operation.Pending
	provider := pending.SourceSubject.Provider
	state := pending.SourceChallengeDigest
	created := operation.CreatedAt.Add(10 * time.Second)
	expected := pending.SourceSubject
	proof := &domain.IdentityProofChallengeV2{ParentOperationDigest: operation.OperationDigest, ParentPhase: operation.Phase, Scope: pending.Scope, ExpectedSubject: &expected}
	if operation.Phase == domain.IdentityPhaseTargetProofV2 {
		provider, state = pending.TargetProvider, pending.TargetChallengeDigest
		created = pending.SourceProof.CompletedAt.Add(time.Second)
		proof.ExpectedSubject = nil
	}
	nonce := ""
	if provider == domain.IdentityProviderTelegram {
		nonce = "one-use-operation-nonce"
	}
	return domain.IdentityChallengeV2{Version: 2, Purpose: domain.IdentityPurposeProviderProofV2, StateDigest: state, BrowserBindingDigest: pending.Scope.BrowserBindingDigest, Binding: challengeV2Binding(provider), CreatedAt: created, ExpiresAt: operation.ExpiresAt, DeleteAfter: operation.ExpiresAt, Proof: proof, Secrets: &domain.IdentityChallengeSecretsV2{PKCEVerifier: strings.Repeat("a", 64), Nonce: nonce}}
}

func operationV2VerifiedStep(t *testing.T, operation domain.IdentityOperationV2) (domain.IdentityChallengeV2, domain.IdentityProviderProofV2) {
	t.Helper()
	challenge := operationV2ProofChallenge(operation)
	consumed, _, err := challenge.Consume(challenge.Binding, "operation-browser", &operation, challenge.CreatedAt.Add(time.Second))
	if err != nil {
		t.Fatalf("consume original proof challenge: %v", err)
	}
	subject := operation.Pending.SourceSubject
	if operation.Phase == domain.IdentityPhaseTargetProofV2 {
		subject = domain.ExternalSubject{Provider: operation.Pending.TargetProvider, Subject: "subject-new-target"}
	}
	proof := domain.IdentityProviderProofV2{ChallengeDigest: consumed.StateDigest, ParentOperationDigest: operation.OperationDigest, ParentPhase: operation.Phase, Binding: consumed.Binding, Subject: subject, Kind: domain.IdentityProofYandexAuthorizationV2, ChallengeCreatedAt: consumed.CreatedAt, ChallengeExpiresAt: consumed.ExpiresAt, CompletedAt: consumed.ConsumedAt.Add(2 * time.Second)}
	if subject.Provider == domain.IdentityProviderTelegram {
		authTime := consumed.CreatedAt
		proof.Kind = domain.IdentityProofTelegramAuthTimeV2
		proof.SignedAuthTime = &authTime
	}
	return consumed, proof
}

func operationV2Confirm(t *testing.T, kind domain.IdentityOperationKindV2) domain.IdentityOperationV2 {
	t.Helper()
	operation := operationV2Fixture(kind)
	for operation.Phase != domain.IdentityPhaseConfirmV2 {
		challenge, proof := operationV2VerifiedStep(t, operation)
		next, err := operation.RecordProof(challenge, proof, proof.CompletedAt)
		if err != nil {
			t.Fatalf("record exact %s proof: %v", operation.Phase, err)
		}
		operation = next
	}
	return operation
}

func TestIdentityOperationV2LinkAndUnlinkPhases(t *testing.T) {
	for _, kind := range []domain.IdentityOperationKindV2{domain.IdentityOperationLinkV2, domain.IdentityOperationUnlinkV2} {
		t.Run(string(kind), func(t *testing.T) {
			original := operationV2Fixture(kind)
			if err := original.Validate(); err != nil {
				t.Fatal(err)
			}
			challenge, proof := operationV2VerifiedStep(t, original)
			next, err := original.RecordProof(challenge, proof, proof.CompletedAt)
			if err != nil {
				t.Fatal(err)
			}
			if original.Pending.SourceProof != nil || original.Phase != domain.IdentityPhaseSourceProofV2 || !next.ExpiresAt.Equal(original.ExpiresAt) || !next.DeleteAfter.Equal(original.DeleteAfter) || next.Pending.Scope != original.Pending.Scope {
				t.Error("proof transition mutated original scope/deadline/input")
			}
			if kind == domain.IdentityOperationLinkV2 {
				if next.Phase != domain.IdentityPhaseTargetProofV2 || next.Pending.TargetProof != nil || next.Pending.TargetSubject != nil {
					t.Fatalf("link source phase skipped independent target: %+v", next)
				}
				challenge, proof = operationV2VerifiedStep(t, next)
				next, err = next.RecordProof(challenge, proof, proof.CompletedAt)
				if err != nil {
					t.Fatal(err)
				}
			}
			if next.Phase != domain.IdentityPhaseConfirmV2 || next.ValidateConfirm(proof.CompletedAt) != nil {
				t.Fatalf("complete proofs did not reach pure confirm: %+v", next)
			}
			if next.Pending.SourceProof == nil || (kind == domain.IdentityOperationLinkV2 && next.Pending.TargetProof == nil) {
				t.Error("required provider proof missing")
			}
			if kind == domain.IdentityOperationUnlinkV2 && (next.Pending.SourceSubject == *next.Pending.TargetSubject || next.Pending.SourceSubject.Provider == next.Pending.TargetProvider) {
				t.Error("unlink proved removed rather than surviving method")
			}
			if replay, err := next.RecordProof(challenge, proof, proof.CompletedAt); err == nil || !reflect.DeepEqual(replay, domain.IdentityOperationV2{}) {
				t.Errorf("proof replay at confirm = (%+v,%v), want zero/error", replay, err)
			}
		})
	}
}

func TestIdentityOperationV2OriginalScopeAndLifetime(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*domain.IdentityOperationScopeV2)
	}{
		{name: "user", mutate: func(s *domain.IdentityOperationScopeV2) { s.UserID = "foreign" }},
		{name: "session", mutate: func(s *domain.IdentityOperationScopeV2) { s.SessionDigest = domain.DigestSecret("foreign") }},
		{name: "subject", mutate: func(s *domain.IdentityOperationScopeV2) { s.AuthenticatedSubject.Subject = "foreign" }},
		{name: "tenant", mutate: func(s *domain.IdentityOperationScopeV2) { s.TenantID = "foreign" }},
		{name: "membership revision", mutate: func(s *domain.IdentityOperationScopeV2) { s.MembershipSecurityVersion++ }},
		{name: "browser", mutate: func(s *domain.IdentityOperationScopeV2) { s.BrowserBindingDigest = domain.DigestSecret("foreign") }},
		{name: "manifest", mutate: func(s *domain.IdentityOperationScopeV2) { s.ConfigurationDigest = domain.DigestSecret("foreign") }},
		{name: "identity revision", mutate: func(s *domain.IdentityOperationScopeV2) { s.ExpectedRevision++ }},
	} {
		t.Run(test.name, func(t *testing.T) {
			operation := operationV2Fixture(domain.IdentityOperationLinkV2)
			scope := operation.Pending.Scope
			if err := operation.ValidateCurrentScope(scope, operation.CreatedAt); err != nil {
				t.Fatal(err)
			}
			test.mutate(&scope)
			if err := operation.ValidateCurrentScope(scope, operation.CreatedAt.Add(time.Second)); err == nil {
				t.Errorf("changed %s scope accepted", test.name)
			}
		})
	}
	for _, test := range []struct {
		name   string
		mutate func(*domain.IdentityOperationV2)
	}{
		{name: "V1", mutate: func(o *domain.IdentityOperationV2) { o.Version = 1 }},
		{name: "missing purpose", mutate: func(o *domain.IdentityOperationV2) { o.Purpose = "" }},
		{name: "login purpose", mutate: func(o *domain.IdentityOperationV2) { o.Purpose = domain.IdentityPurposeLoginV2 }},
		{name: "unknown phase", mutate: func(o *domain.IdentityOperationV2) { o.Phase = "ready" }},
		{name: "original lifetime extended", mutate: func(o *domain.IdentityOperationV2) {
			o.ExpiresAt = o.ExpiresAt.Add(time.Nanosecond)
			o.DeleteAfter = o.ExpiresAt
		}},
		{name: "pending retention extended", mutate: func(o *domain.IdentityOperationV2) { o.DeleteAfter = o.CreatedAt.Add(24 * time.Hour) }},
		{name: "missing pending", mutate: func(o *domain.IdentityOperationV2) { o.Pending = nil }},
		{name: "source subject changed", mutate: func(o *domain.IdentityOperationV2) { o.Pending.SourceSubject.Subject = "foreign" }},
		{name: "same provider target", mutate: func(o *domain.IdentityOperationV2) { o.Pending.TargetProvider = o.Pending.SourceSubject.Provider }},
		{name: "same child IDs", mutate: func(o *domain.IdentityOperationV2) { o.Pending.TargetChallengeDigest = o.Pending.SourceChallengeDigest }},
		{name: "child is parent", mutate: func(o *domain.IdentityOperationV2) { o.Pending.SourceChallengeDigest = o.OperationDigest }},
		{name: "preselected link target", mutate: func(o *domain.IdentityOperationV2) {
			subject := domain.ExternalSubject{Provider: o.Pending.TargetProvider, Subject: "guessed"}
			o.Pending.TargetSubject = &subject
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			operation := operationV2Fixture(domain.IdentityOperationLinkV2)
			test.mutate(&operation)
			if err := operation.Validate(); err == nil {
				t.Errorf("malformed original operation accepted: %+v", operation)
			}
		})
	}
	operation := operationV2Fixture(domain.IdentityOperationLinkV2)
	for _, at := range []time.Time{time.Time{}, operation.CreatedAt.Add(-time.Nanosecond), operation.ExpiresAt, operation.ExpiresAt.Add(time.Hour)} {
		if err := operation.ValidateCurrentScope(operation.Pending.Scope, at); err == nil {
			t.Errorf("original scope accepted out-of-lifetime time %v", at)
		}
	}
}

func TestIdentityProviderProofV2FreshnessAndDistinctAssurance(t *testing.T) {
	for _, kind := range []domain.IdentityOperationKindV2{domain.IdentityOperationLinkV2, domain.IdentityOperationUnlinkV2} {
		t.Run(string(kind), func(t *testing.T) {
			operation := operationV2Fixture(kind)
			_, proof := operationV2VerifiedStep(t, operation)
			if err := proof.ValidateFresh(proof.CompletedAt.Add(5 * time.Minute)); err != nil {
				t.Errorf("exact five minute proof should remain usable: %v", err)
			}
			for _, at := range []time.Time{proof.CompletedAt.Add(-time.Nanosecond), proof.CompletedAt.Add(5*time.Minute + time.Nanosecond)} {
				if err := proof.ValidateFresh(at); err == nil {
					t.Errorf("future/stale proof accepted at %v", at)
				}
			}
			if proof.Subject.Provider == domain.IdentityProviderTelegram {
				missing := proof
				missing.SignedAuthTime = nil
				if missing.Validate() == nil {
					t.Error("missing signed auth_time accepted")
				}
				old := proof
				at := proof.ChallengeCreatedAt.Add(-domain.IdentityProofClockSkewV2 - time.Nanosecond)
				old.SignedAuthTime = &at
				if old.Validate() == nil {
					t.Error("old signed auth_time accepted")
				}
				future := proof
				at = proof.CompletedAt.Add(domain.IdentityProofClockSkewV2 + time.Nanosecond)
				future.SignedAuthTime = &at
				if future.Validate() == nil {
					t.Error("future signed auth_time accepted")
				}
				wrong := proof
				wrong.Kind = domain.IdentityProofYandexAuthorizationV2
				if wrong.Validate() == nil {
					t.Error("Yandex authorization kind substituted for signed Telegram proof")
				}
			} else {
				wrong := proof
				at := proof.ChallengeCreatedAt
				wrong.SignedAuthTime = &at
				if wrong.Validate() == nil {
					t.Error("Yandex authorization invented an auth_time assurance")
				}
				wrong = proof
				wrong.Kind = domain.IdentityProofTelegramAuthTimeV2
				if wrong.Validate() == nil {
					t.Error("Telegram proof kind substituted for Yandex authorization")
				}
			}
		})
	}
}

func TestIdentityOperationV2RejectsWrongChildAndProof(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*domain.IdentityChallengeV2, *domain.IdentityProviderProofV2)
	}{
		{name: "unconsumed child", mutate: func(c *domain.IdentityChallengeV2, _ *domain.IdentityProviderProofV2) { c.ConsumedAt = nil }},
		{name: "retained verifier", mutate: func(c *domain.IdentityChallengeV2, _ *domain.IdentityProviderProofV2) {
			c.Secrets = &domain.IdentityChallengeSecretsV2{PKCEVerifier: strings.Repeat("b", 64)}
		}},
		{name: "wrong parent", mutate: func(_ *domain.IdentityChallengeV2, p *domain.IdentityProviderProofV2) {
			p.ParentOperationDigest = domain.DigestSecret("other")
		}},
		{name: "wrong child", mutate: func(_ *domain.IdentityChallengeV2, p *domain.IdentityProviderProofV2) {
			p.ChallengeDigest = domain.DigestSecret("other")
		}},
		{name: "wrong phase", mutate: func(_ *domain.IdentityChallengeV2, p *domain.IdentityProviderProofV2) {
			p.ParentPhase = domain.IdentityPhaseTargetProofV2
		}},
		{name: "wrong subject", mutate: func(_ *domain.IdentityChallengeV2, p *domain.IdentityProviderProofV2) { p.Subject.Subject = "other" }},
		{name: "wrong client", mutate: func(_ *domain.IdentityChallengeV2, p *domain.IdentityProviderProofV2) { p.Binding.ClientID = "other" }},
		{name: "proof starts before parent", mutate: func(_ *domain.IdentityChallengeV2, p *domain.IdentityProviderProofV2) {
			p.ChallengeCreatedAt = operationV2At().Add(-time.Second)
		}},
		{name: "proof lifetime extended", mutate: func(_ *domain.IdentityChallengeV2, p *domain.IdentityProviderProofV2) {
			p.ChallengeExpiresAt = p.ChallengeExpiresAt.Add(time.Nanosecond)
		}},
		{name: "completion before consume", mutate: func(c *domain.IdentityChallengeV2, p *domain.IdentityProviderProofV2) {
			p.CompletedAt = c.ConsumedAt.Add(-time.Nanosecond)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			operation := operationV2Fixture(domain.IdentityOperationLinkV2)
			challenge, proof := operationV2VerifiedStep(t, operation)
			test.mutate(&challenge, &proof)
			next, err := operation.RecordProof(challenge, proof, operation.CreatedAt.Add(time.Minute))
			if err == nil || !reflect.DeepEqual(next, domain.IdentityOperationV2{}) {
				t.Errorf("bad child/proof advanced phase: (%+v,%v)", next, err)
			}
		})
	}
}

func TestIdentityOperationV2ConfirmFreshnessAndDeepCopy(t *testing.T) {
	operation := operationV2Confirm(t, domain.IdentityOperationLinkV2)
	at := operation.Pending.TargetProof.CompletedAt
	if operation.ValidateConfirm(at) != nil {
		t.Fatal("complete fresh operation denied")
	}
	stale := operation.Pending.SourceProof.CompletedAt.Add(5*time.Minute + time.Nanosecond)
	if stale.Before(operation.ExpiresAt) && operation.ValidateConfirm(stale) == nil {
		t.Error("stale source proof accepted while target proof is newer")
	}
	for _, at := range []time.Time{operation.CreatedAt.Add(-time.Nanosecond), operation.ExpiresAt} {
		if operation.ValidateConfirm(at) == nil {
			t.Errorf("confirm accepted beyond original window: %v", at)
		}
	}
	original := operationV2Fixture(domain.IdentityOperationUnlinkV2)
	challenge, proof := operationV2VerifiedStep(t, original)
	next, err := original.RecordProof(challenge, proof, proof.CompletedAt)
	if err != nil {
		t.Fatal(err)
	}
	storedTime := *next.Pending.SourceProof.SignedAuthTime
	*proof.SignedAuthTime = proof.SignedAuthTime.Add(time.Minute)
	challenge.Proof.ExpectedSubject.Subject = "mutated-input"
	if *next.Pending.SourceProof.SignedAuthTime != storedTime || next.Pending.SourceSubject != original.Pending.SourceSubject || original.Pending.SourceProof != nil {
		t.Error("proof transition aliased input-owned pointers")
	}
	*next.Pending.TargetSubject = domain.ExternalSubject{Provider: domain.IdentityProviderYandex, Subject: "mutated-output"}
	if original.Pending.TargetSubject.Subject == "mutated-output" {
		t.Error("copied operation aliases original target")
	}
}

func TestIdentityOperationV2TerminalPrivacyAndReceiptIsNotGrant(t *testing.T) {
	for _, kind := range []domain.IdentityOperationKindV2{domain.IdentityOperationLinkV2, domain.IdentityOperationUnlinkV2} {
		t.Run(string(kind), func(t *testing.T) {
			operation := operationV2Confirm(t, kind)
			completed := operation.Pending.SourceProof.CompletedAt.Add(time.Minute)
			outcome := domain.IdentityOutcomeUnlinkedV2
			if kind == domain.IdentityOperationLinkV2 {
				outcome = domain.IdentityOutcomeLinkedV2
			}
			after := domain.AuthRevisionV2(8)
			terminal, err := operation.Terminalize(outcome, &after, completed)
			if err != nil {
				t.Fatal(err)
			}
			if terminal.Pending != nil || terminal.Receipt == nil || !terminal.CreatedAt.Equal(operation.CreatedAt) || !terminal.ExpiresAt.Equal(operation.ExpiresAt) || !terminal.DeleteAfter.Equal(operation.CreatedAt.Add(24*time.Hour)) || terminal.Receipt.BeforeRevision != 7 || *terminal.Receipt.AfterRevision != 8 {
				t.Fatalf("terminal prune/deadline/revision mismatch: %+v", terminal)
			}
			raw, err := json.Marshal(terminal)
			if err != nil {
				t.Fatal(err)
			}
			for _, secretKey := range []string{"pending", "source_subject", "target_subject", "authenticated_subject", "source_proof", "target_proof", "session_digest", "browser_binding_digest", "configuration_digest", "secrets", "pkce_verifier", "nonce"} {
				if strings.Contains(string(raw), `"`+secretKey+`"`) {
					t.Errorf("receipt retained private key %s: %s", secretKey, raw)
				}
			}
			after = 99
			if *terminal.Receipt.AfterRevision != 8 {
				t.Error("receipt aliases caller revision pointer")
			}
			if terminal.ValidateCurrentScope(operation.Pending.Scope, completed) == nil || terminal.ValidateConfirm(completed) == nil {
				t.Error("terminal receipt became an authorization/confirm grant")
			}
			if again, err := terminal.Terminalize(outcome, terminal.Receipt.AfterRevision, completed); err == nil || !reflect.DeepEqual(again, domain.IdentityOperationV2{}) {
				t.Errorf("receipt resurrected terminal operation: (%+v,%v)", again, err)
			}
		})
	}
}

func TestIdentityOperationV2FailurePruneExpiryAndRevisionStrings(t *testing.T) {
	for _, revision := range []domain.AuthRevisionV2{1<<53 - 1, 1 << 53, 1<<53 + 1, math.MaxUint64} {
		t.Run(revision.String(), func(t *testing.T) {
			operation := operationV2Fixture(domain.IdentityOperationLinkV2)
			operation.Pending.Scope.ExpectedRevision = revision
			terminal, err := operation.Terminalize(domain.IdentityOutcomeCancelledV2, nil, operation.CreatedAt.Add(time.Second))
			if err != nil {
				t.Fatal(err)
			}
			for _, record := range []any{operation, terminal} {
				raw, err := json.Marshal(record)
				if err != nil {
					t.Fatal(err)
				}
				if !strings.Contains(string(raw), `:"`+revision.String()+`"`) {
					t.Errorf("identity-derived revision emitted nonstring: %s", raw)
				}
			}
			if terminal.Receipt.AfterRevision != nil || terminal.Pending != nil {
				t.Error("failed receipt retained pending state or claimed committed revision")
			}
		})
	}
	operation := operationV2Fixture(domain.IdentityOperationLinkV2)
	if terminal, err := operation.Terminalize(domain.IdentityOutcomeExpiredV2, nil, operation.ExpiresAt); err != nil || terminal.Pending != nil || terminal.Receipt.Outcome != domain.IdentityOutcomeExpiredV2 {
		t.Errorf("logical expiry prune=(%+v,%v)", terminal, err)
	}
	for _, test := range []struct {
		name    string
		outcome domain.IdentityOperationOutcomeV2
		after   *domain.AuthRevisionV2
		at      time.Time
	}{
		{name: "success without proofs", outcome: domain.IdentityOutcomeLinkedV2, at: operation.CreatedAt.Add(time.Second)},
		{name: "absence is no receipt", outcome: domain.IdentityOutcomeUnavailableV2, at: operation.CreatedAt.Add(time.Second)},
		{name: "expiry before deadline", outcome: domain.IdentityOutcomeExpiredV2, at: operation.CreatedAt.Add(time.Second)},
		{name: "nonexpiry after deadline", outcome: domain.IdentityOutcomeCancelledV2, at: operation.ExpiresAt},
		{name: "retention already over", outcome: domain.IdentityOutcomeExpiredV2, at: operation.CreatedAt.Add(24 * time.Hour)},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, err := operation.Terminalize(test.outcome, test.after, test.at)
			if err == nil || !reflect.DeepEqual(result, domain.IdentityOperationV2{}) {
				t.Errorf("invalid terminal transition=(%+v,%v)", result, err)
			}
		})
	}
}

func TestIdentityOperationV2CodecRoundTripAllPhases(t *testing.T) {
	for _, kind := range []domain.IdentityOperationKindV2{domain.IdentityOperationLinkV2, domain.IdentityOperationUnlinkV2} {
		t.Run(string(kind), func(t *testing.T) {
			operation := operationV2Fixture(kind)
			for {
				raw, err := domain.EncodeIdentityOperationV2(operation)
				if err != nil {
					t.Fatalf("encode %s phase: %v", operation.Phase, err)
				}
				decoded, err := domain.DecodeIdentityOperationV2(raw)
				if err != nil || !reflect.DeepEqual(decoded, operation) {
					t.Fatalf("roundtrip %s phase = (%+v,%v), want %+v", operation.Phase, decoded, err, operation)
				}
				if operation.Phase == domain.IdentityPhaseTerminalV2 {
					break
				}
				if operation.Phase == domain.IdentityPhaseConfirmV2 {
					outcome := domain.IdentityOutcomeLinkedV2
					if kind == domain.IdentityOperationUnlinkV2 {
						outcome = domain.IdentityOutcomeUnlinkedV2
					}
					after := domain.AuthRevisionV2(8)
					operation, err = operation.Terminalize(outcome, &after, operation.CreatedAt.Add(time.Minute))
					if err != nil {
						t.Fatal(err)
					}
				} else {
					challenge, proof := operationV2VerifiedStep(t, operation)
					operation, err = operation.RecordProof(challenge, proof, proof.CompletedAt)
					if err != nil {
						t.Fatal(err)
					}
				}
			}
		})
	}
}

func TestIdentityOperationV2SourceCannotExpireDuringTargetProof(t *testing.T) {
	operation := operationV2Fixture(domain.IdentityOperationLinkV2)
	challenge, proof := operationV2VerifiedStep(t, operation)
	operation, err := operation.RecordProof(challenge, proof, proof.CompletedAt)
	if err != nil {
		t.Fatal(err)
	}
	challenge, proof = operationV2VerifiedStep(t, operation)
	proof.CompletedAt = operation.Pending.SourceProof.CompletedAt.Add(domain.IdentityProofMaxAgeV2 + time.Nanosecond)
	next, err := operation.RecordProof(challenge, proof, proof.CompletedAt)
	if err == nil || !reflect.DeepEqual(next, domain.IdentityOperationV2{}) {
		t.Errorf("fresh target revived stale source: (%+v,%v)", next, err)
	}
}

func TestIdentityOperationV2MaxUint64CommittedReceipt(t *testing.T) {
	operation := operationV2Fixture(domain.IdentityOperationLinkV2)
	operation.Pending.Scope.ExpectedRevision = math.MaxUint64 - 1
	for operation.Phase != domain.IdentityPhaseConfirmV2 {
		challenge, proof := operationV2VerifiedStep(t, operation)
		next, err := operation.RecordProof(challenge, proof, proof.CompletedAt)
		if err != nil {
			t.Fatal(err)
		}
		operation = next
	}
	after := domain.AuthRevisionV2(math.MaxUint64)
	terminal, err := operation.Terminalize(domain.IdentityOutcomeLinkedV2, &after, operation.CreatedAt.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := domain.EncodeIdentityOperationV2(terminal)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := domain.DecodeIdentityOperationV2(raw)
	if err != nil || decoded.Receipt == nil || decoded.Receipt.BeforeRevision != math.MaxUint64-1 || decoded.Receipt.AfterRevision == nil || *decoded.Receipt.AfterRevision != math.MaxUint64 || !strings.Contains(string(raw), `"after_revision":"18446744073709551615"`) {
		t.Errorf("MaxUint64 committed receipt roundtrip=(%+v,%v), raw=%s", decoded, err, raw)
	}
}
