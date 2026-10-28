package domain

import (
	"encoding/json"
	"time"
)

type IdentityRecordPurposeV2 string
type IdentityOperationPhaseV2 string
type IdentityOperationOutcomeV2 string
type IdentityProviderProofKindV2 string

const (
	IdentityPurposeOperationV2           IdentityRecordPurposeV2     = "identity_operation"
	IdentityPurposeLoginV2               IdentityRecordPurposeV2     = "login"
	IdentityPurposeProviderProofV2       IdentityRecordPurposeV2     = "provider_proof"
	IdentityPhaseSourceProofV2           IdentityOperationPhaseV2    = "source_proof"
	IdentityPhaseTargetProofV2           IdentityOperationPhaseV2    = "target_proof"
	IdentityPhaseConfirmV2               IdentityOperationPhaseV2    = "confirm"
	IdentityPhaseTerminalV2              IdentityOperationPhaseV2    = "terminal"
	IdentityOutcomeLinkedV2              IdentityOperationOutcomeV2  = "linked"
	IdentityOutcomeUnlinkedV2            IdentityOperationOutcomeV2  = "unlinked"
	IdentityOutcomeCancelledV2           IdentityOperationOutcomeV2  = "cancelled"
	IdentityOutcomeConflictV2            IdentityOperationOutcomeV2  = "conflict"
	IdentityOutcomeStaleRevisionV2       IdentityOperationOutcomeV2  = "stale_revision"
	IdentityOutcomeProofRequiredV2       IdentityOperationOutcomeV2  = "proof_required"
	IdentityOutcomeProofUnavailableV2    IdentityOperationOutcomeV2  = "proof_unavailable"
	IdentityOutcomeLastMethodV2          IdentityOperationOutcomeV2  = "last_method"
	IdentityOutcomeProviderUnavailableV2 IdentityOperationOutcomeV2  = "provider_unavailable"
	IdentityOutcomeExpiredV2             IdentityOperationOutcomeV2  = "expired"
	IdentityOutcomeUnavailableV2         IdentityOperationOutcomeV2  = "outcome_unavailable"
	IdentityOutcomeAccessDeniedV2        IdentityOperationOutcomeV2  = "access_denied"
	IdentityProofYandexAuthorizationV2   IdentityProviderProofKindV2 = "yandex_fresh_authorization"
	IdentityProofTelegramAuthTimeV2      IdentityProviderProofKindV2 = "telegram_signed_auth_time"
	IdentityOperationMaxLifetimeV2                                   = 10 * time.Minute
	IdentityProofMaxAgeV2                                            = 5 * time.Minute
	IdentityReceiptRetentionV2                                       = 24 * time.Hour
	// Matches the trusted Telegram startup policy; never extends operation,
	// challenge, completion or five-minute proof deadlines.
	IdentityProofClockSkewV2 = 30 * time.Second
)

// IdentityOperationScopeV2 is an immutable comparison snapshot, NOT an
// authorization receipt. Current identity/session/membership and CSRF must be
// checked again in the same transaction as each protected effect.
type IdentityOperationScopeV2 struct {
	UserID                    UserID          `json:"user_id"`
	SessionDigest             SecretDigest    `json:"session_digest"`
	AuthenticatedSubject      ExternalSubject `json:"authenticated_subject"`
	TenantID                  TenantID        `json:"tenant_id"`
	MembershipSecurityVersion uint64          `json:"membership_security_version"`
	BrowserBindingDigest      SecretDigest    `json:"browser_binding_digest"`
	ConfigurationDigest       SecretDigest    `json:"configuration_digest"`
	ExpectedRevision          AuthRevisionV2  `json:"expected_revision"`
}

func (scope IdentityOperationScopeV2) Validate() error {
	if scope.UserID.Validate() != nil || scope.TenantID.Validate() != nil || validateIdentitySubjectV2(scope.AuthenticatedSubject) != nil || scope.MembershipSecurityVersion == 0 {
		return identityOperationErrorV2("scope", "requires exact original user, subject, tenant and membership version")
	}
	for _, digest := range []SecretDigest{scope.SessionDigest, scope.BrowserBindingDigest, scope.ConfigurationDigest} {
		if digest.Validate("identity_operation_v2.scope_digest") != nil {
			return identityOperationErrorV2("scope", "requires complete original digests")
		}
	}
	return scope.ExpectedRevision.Validate()
}

// Proof metadata is accepted only from the verified provider adapter after its
// one-shot exchange. These data and validators do NOT verify signatures or mint
// reusable grants. No token, code, authorization URL, nonce or verifier belongs
// here. The pinned callback URI is nonsecret client-binding metadata.
type IdentityProviderProofV2 struct {
	ChallengeDigest       SecretDigest                `json:"challenge_digest"`
	ParentOperationDigest SecretDigest                `json:"parent_operation_digest"`
	ParentPhase           IdentityOperationPhaseV2    `json:"parent_phase"`
	Binding               IdentityProviderBindingV2   `json:"binding"`
	Subject               ExternalSubject             `json:"subject"`
	Kind                  IdentityProviderProofKindV2 `json:"kind"`
	ChallengeCreatedAt    time.Time                   `json:"challenge_created_at"`
	ChallengeExpiresAt    time.Time                   `json:"challenge_expires_at"`
	CompletedAt           time.Time                   `json:"completed_at"`
	SignedAuthTime        *time.Time                  `json:"signed_auth_time,omitempty"`
}

func (proof IdentityProviderProofV2) Validate() error {
	if proof.ChallengeDigest.Validate("identity_proof_v2.challenge_digest") != nil || proof.ParentOperationDigest.Validate("identity_proof_v2.parent_operation_digest") != nil || proof.ChallengeDigest == proof.ParentOperationDigest {
		return identityOperationErrorV2("proof", "requires distinct exact parent and child digests")
	}
	if proof.ParentPhase != IdentityPhaseSourceProofV2 && proof.ParentPhase != IdentityPhaseTargetProofV2 {
		return identityOperationErrorV2("proof.phase", "requires a provider-proof phase")
	}
	if proof.Binding.Validate() != nil || validateIdentitySubjectV2(proof.Subject) != nil || proof.Subject.Provider != proof.Binding.Provider {
		return identityOperationErrorV2("proof.binding", "requires a matching fixed provider")
	}
	if !identityLifetimeV2(proof.ChallengeCreatedAt, proof.ChallengeExpiresAt) || !proof.CompletedAt.After(proof.ChallengeCreatedAt) || !proof.CompletedAt.Before(proof.ChallengeExpiresAt) {
		return identityOperationErrorV2("proof.time", "completion must be inside the original challenge lifetime")
	}
	switch proof.Binding.Provider {
	case IdentityProviderYandex:
		if proof.Kind != IdentityProofYandexAuthorizationV2 || proof.SignedAuthTime != nil {
			return identityOperationErrorV2("proof.kind", "Yandex proves new authorization, not password or MFA age")
		}
	case IdentityProviderTelegram:
		if proof.Kind != IdentityProofTelegramAuthTimeV2 || proof.SignedAuthTime == nil || proof.SignedAuthTime.IsZero() || proof.SignedAuthTime.Before(proof.ChallengeCreatedAt.Add(-IdentityProofClockSkewV2)) || proof.SignedAuthTime.After(proof.CompletedAt.Add(IdentityProofClockSkewV2)) {
			return identityOperationErrorV2("proof.auth_time", "requires signed fresh Telegram auth_time; iat is not a substitute")
		}
	}
	return nil
}

func (proof IdentityProviderProofV2) ValidateFresh(at time.Time) error {
	if err := proof.Validate(); err != nil {
		return err
	}
	if at.IsZero() || at.Before(proof.CompletedAt) || at.Sub(proof.CompletedAt) > IdentityProofMaxAgeV2 {
		return identityOperationErrorV2("proof.freshness", "requires a nonfuture proof no older than five minutes")
	}
	return nil
}

func (proof IdentityProviderProofV2) ValidateForChallenge(challenge IdentityChallengeV2) error {
	if proof.Validate() != nil || challenge.Validate() != nil || challenge.Purpose != IdentityPurposeProviderProofV2 || challenge.Proof == nil || challenge.ConsumedAt == nil || challenge.Secrets != nil {
		return identityOperationErrorV2("proof.challenge", "requires its exact consumed and scrubbed provider challenge")
	}
	parent := challenge.Proof
	if proof.ChallengeDigest != challenge.StateDigest || proof.ParentOperationDigest != parent.ParentOperationDigest || proof.ParentPhase != parent.ParentPhase || proof.Binding != challenge.Binding || !proof.ChallengeCreatedAt.Equal(challenge.CreatedAt) || !proof.ChallengeExpiresAt.Equal(challenge.ExpiresAt) || proof.CompletedAt.Before(*challenge.ConsumedAt) {
		return identityOperationErrorV2("proof.challenge", "must match original consumed child and parent")
	}
	if parent.ExpectedSubject != nil && proof.Subject != *parent.ExpectedSubject {
		return identityOperationErrorV2("proof.subject", "does not match the fixed source subject")
	}
	return nil
}

type IdentityOperationPendingV2 struct {
	Scope                 IdentityOperationScopeV2 `json:"scope"`
	SourceSubject         ExternalSubject          `json:"source_subject"`
	TargetProvider        IdentityProvider         `json:"target_provider"`
	TargetSubject         *ExternalSubject         `json:"target_subject,omitempty"`
	SourceChallengeDigest SecretDigest             `json:"source_challenge_digest"`
	TargetChallengeDigest SecretDigest             `json:"target_challenge_digest,omitempty"`
	SourceProof           *IdentityProviderProofV2 `json:"source_proof,omitempty"`
	TargetProof           *IdentityProviderProofV2 `json:"target_proof,omitempty"`
}

// Receipt has no subject, browser/session binding, configuration or proof. Its
// operation digest is an address, not a bearer; the store must freshly authorize
// a same-user reader, even for a repeated successful confirm.
type IdentityOperationReceiptV2 struct {
	UserID         UserID                     `json:"user_id"`
	TenantID       TenantID                   `json:"tenant_id"`
	Provider       IdentityProvider           `json:"provider"`
	Outcome        IdentityOperationOutcomeV2 `json:"outcome"`
	BeforeRevision AuthRevisionV2             `json:"before_revision"`
	AfterRevision  *AuthRevisionV2            `json:"after_revision,omitempty"`
	CompletedAt    time.Time                  `json:"completed_at"`
}

type IdentityOperationV2 struct {
	Version         uint32                      `json:"version"`
	Purpose         IdentityRecordPurposeV2     `json:"purpose"`
	OperationDigest SecretDigest                `json:"operation_digest"`
	Kind            IdentityOperationKindV2     `json:"kind"`
	Phase           IdentityOperationPhaseV2    `json:"phase"`
	CreatedAt       time.Time                   `json:"created_at"`
	ExpiresAt       time.Time                   `json:"expires_at"`
	DeleteAfter     time.Time                   `json:"delete_after"`
	Pending         *IdentityOperationPendingV2 `json:"pending,omitempty"`
	Receipt         *IdentityOperationReceiptV2 `json:"receipt,omitempty"`
}

func (operation IdentityOperationV2) Validate() error {
	if operation.Version != 2 || operation.Purpose != IdentityPurposeOperationV2 || operation.OperationDigest.Validate("identity_operation_v2.operation_digest") != nil || (operation.Kind != IdentityOperationLinkV2 && operation.Kind != IdentityOperationUnlinkV2) || !identityLifetimeV2(operation.CreatedAt, operation.ExpiresAt) {
		return identityOperationErrorV2("record", "requires explicit V2 purpose, kind, address and original lifetime")
	}
	if operation.Phase == IdentityPhaseTerminalV2 {
		if err := operation.validateReceipt(); err != nil {
			return err
		}
	} else {
		if err := operation.validatePending(); err != nil {
			return err
		}
	}
	type wire IdentityOperationV2
	raw, err := json.Marshal(wire(operation))
	if err != nil || len(raw) > IdentityAuthMaxRecordBytesV2 {
		return identityOperationErrorV2("record", "must fit the original 8 KiB record bound")
	}
	return nil
}

func (operation IdentityOperationV2) validatePending() error {
	pending := operation.Pending
	if pending == nil || operation.Receipt != nil || !operation.DeleteAfter.Equal(operation.ExpiresAt) || pending.Scope.Validate() != nil || validateIdentitySubjectV2(pending.SourceSubject) != nil || !identityProviderV2(pending.TargetProvider) || pending.SourceSubject.Provider == pending.TargetProvider || pending.SourceChallengeDigest.Validate("identity_operation_v2.source_challenge_digest") != nil || pending.SourceChallengeDigest == operation.OperationDigest {
		return identityOperationErrorV2("pending", "requires original scope, distinct fixed methods and unextended pending retention")
	}
	if operation.Kind == IdentityOperationLinkV2 {
		if pending.SourceSubject != pending.Scope.AuthenticatedSubject || pending.TargetChallengeDigest.Validate("identity_operation_v2.target_challenge_digest") != nil || pending.TargetChallengeDigest == pending.SourceChallengeDigest || pending.TargetChallengeDigest == operation.OperationDigest {
			return identityOperationErrorV2("pending.link", "requires current authenticating source and independent target challenge")
		}
	} else if pending.TargetSubject == nil || pending.TargetChallengeDigest != "" || pending.TargetProof != nil {
		return identityOperationErrorV2("pending.unlink", "requires exact removed method and no target-proof arm")
	}
	if pending.TargetSubject != nil && (validateIdentitySubjectV2(*pending.TargetSubject) != nil || pending.TargetSubject.Provider != pending.TargetProvider || *pending.TargetSubject == pending.SourceSubject) {
		return identityOperationErrorV2("pending.target", "requires the fixed other provider subject")
	}
	if pending.SourceProof != nil && operation.validateStoredProof(*pending.SourceProof, IdentityPhaseSourceProofV2) != nil {
		return identityOperationErrorV2("pending.source_proof", "does not correspond to the original source phase")
	}
	if pending.TargetProof != nil && operation.validateStoredProof(*pending.TargetProof, IdentityPhaseTargetProofV2) != nil {
		return identityOperationErrorV2("pending.target_proof", "does not correspond to the original target phase")
	}
	switch operation.Phase {
	case IdentityPhaseSourceProofV2:
		if pending.SourceProof != nil || pending.TargetProof != nil || (operation.Kind == IdentityOperationLinkV2 && pending.TargetSubject != nil) {
			return identityOperationErrorV2("phase", "source phase cannot carry completed proof or selected link subject")
		}
	case IdentityPhaseTargetProofV2:
		if operation.Kind != IdentityOperationLinkV2 || pending.SourceProof == nil || pending.TargetProof != nil || pending.TargetSubject != nil {
			return identityOperationErrorV2("phase", "target phase requires exactly completed source proof")
		}
	case IdentityPhaseConfirmV2:
		if pending.SourceProof == nil || (operation.Kind == IdentityOperationLinkV2 && (pending.TargetProof == nil || pending.TargetSubject == nil)) {
			return identityOperationErrorV2("phase", "confirmation requires all exact proofs")
		}
	default:
		return identityOperationErrorV2("phase", "is not a closed pending phase")
	}
	return nil
}

func (operation IdentityOperationV2) validateStoredProof(proof IdentityProviderProofV2, phase IdentityOperationPhaseV2) error {
	pending := operation.Pending
	if proof.Validate() != nil || proof.ParentOperationDigest != operation.OperationDigest || proof.ParentPhase != phase || proof.ChallengeCreatedAt.Before(operation.CreatedAt) || proof.ChallengeExpiresAt.After(operation.ExpiresAt) || !proof.CompletedAt.Before(operation.ExpiresAt) {
		return identityOperationErrorV2("proof", "does not match the original operation")
	}
	if phase == IdentityPhaseSourceProofV2 {
		if proof.ChallengeDigest != pending.SourceChallengeDigest || proof.Subject != pending.SourceSubject {
			return identityOperationErrorV2("proof.source", "requires the exact original source child and subject")
		}
	} else if operation.Kind != IdentityOperationLinkV2 || proof.ChallengeDigest != pending.TargetChallengeDigest || proof.Subject.Provider != pending.TargetProvider || pending.TargetSubject == nil || proof.Subject != *pending.TargetSubject || pending.SourceProof == nil || proof.ChallengeCreatedAt.Before(pending.SourceProof.CompletedAt) {
		return identityOperationErrorV2("proof.target", "requires the independent target child after source completion")
	}
	return nil
}

func (operation IdentityOperationV2) validateReceipt() error {
	receipt := operation.Receipt
	if operation.Pending != nil || receipt == nil || !operation.DeleteAfter.Equal(operation.CreatedAt.Add(IdentityReceiptRetentionV2)) || receipt.UserID.Validate() != nil || receipt.TenantID.Validate() != nil || !identityProviderV2(receipt.Provider) || receipt.BeforeRevision.Validate() != nil || receipt.CompletedAt.Before(operation.CreatedAt) || !receipt.CompletedAt.Before(operation.DeleteAfter) || !identityTerminalOutcomeV2(receipt.Outcome) {
		return identityOperationErrorV2("receipt", "requires only a minimal original-lifetime outcome")
	}
	success := receipt.Outcome == IdentityOutcomeLinkedV2 || receipt.Outcome == IdentityOutcomeUnlinkedV2
	if success {
		if (receipt.Outcome == IdentityOutcomeLinkedV2) != (operation.Kind == IdentityOperationLinkV2) || receipt.AfterRevision == nil || receipt.AfterRevision.Validate() != nil || *receipt.AfterRevision <= receipt.BeforeRevision || !receipt.CompletedAt.Before(operation.ExpiresAt) {
			return identityOperationErrorV2("receipt.revision", "requires the matching committed strictly advanced revision")
		}
	} else if receipt.AfterRevision != nil {
		return identityOperationErrorV2("receipt.revision", "failure cannot claim a committed revision")
	}
	if (receipt.Outcome == IdentityOutcomeExpiredV2) != (!receipt.CompletedAt.Before(operation.ExpiresAt)) {
		return identityOperationErrorV2("receipt.expiry", "requires truthful expiry against the original deadline")
	}
	return nil
}

func (operation IdentityOperationV2) ValidateCurrentScope(current IdentityOperationScopeV2, at time.Time) error {
	if operation.Validate() != nil || operation.Pending == nil || current.Validate() != nil || operation.Pending.Scope != current || at.Before(operation.CreatedAt) || !at.Before(operation.ExpiresAt) {
		return identityOperationErrorV2("current_scope", "requires exact original live scope; it is not a current authorization grant")
	}
	return nil
}

// RecordProof only advances a copied structural record. The caller must have
// persisted child consumption before external exchange, then recheck original
// current authority and adapter-verified metadata in its owning transaction.
func (operation IdentityOperationV2) RecordProof(challenge IdentityChallengeV2, proof IdentityProviderProofV2, at time.Time) (IdentityOperationV2, error) {
	if operation.Validate() != nil || operation.Pending == nil || at.Before(operation.CreatedAt) || !at.Before(operation.ExpiresAt) || challenge.ValidateForOperation(operation) != nil || proof.ValidateForChallenge(challenge) != nil || proof.ValidateFresh(at) != nil {
		return IdentityOperationV2{}, identityOperationErrorV2("proof", "requires exact current consumed child and fresh verified metadata")
	}
	if operation.Pending.SourceProof != nil && operation.Pending.SourceProof.ValidateFresh(at) != nil {
		return IdentityOperationV2{}, identityOperationErrorV2("proof.source", "source proof is no longer fresh")
	}
	next := operation.clone()
	proof = proof.clone()
	switch operation.Phase {
	case IdentityPhaseSourceProofV2:
		next.Pending.SourceProof = &proof
		if next.Kind == IdentityOperationLinkV2 {
			next.Phase = IdentityPhaseTargetProofV2
		} else {
			next.Phase = IdentityPhaseConfirmV2
		}
	case IdentityPhaseTargetProofV2:
		next.Pending.TargetProof = &proof
		subject := proof.Subject
		next.Pending.TargetSubject = &subject
		next.Phase = IdentityPhaseConfirmV2
	default:
		return IdentityOperationV2{}, identityOperationErrorV2("phase", "cannot append another proof")
	}
	if err := next.Validate(); err != nil {
		return IdentityOperationV2{}, err
	}
	return next, nil
}

// ValidateConfirm is pure consistency/freshness validation, not authorization.
// Identity ownership, enabled methods, configuration, membership, CSRF and audit
// must still be checked together in the actual mutation transaction.
func (operation IdentityOperationV2) ValidateConfirm(at time.Time) error {
	if operation.Validate() != nil || operation.Phase != IdentityPhaseConfirmV2 || at.Before(operation.CreatedAt) || !at.Before(operation.ExpiresAt) || operation.Pending.SourceProof.ValidateFresh(at) != nil {
		return identityOperationErrorV2("confirm", "requires the original live confirm phase and fresh source proof")
	}
	if operation.Kind == IdentityOperationLinkV2 && operation.Pending.TargetProof.ValidateFresh(at) != nil {
		return identityOperationErrorV2("confirm", "requires independent fresh target proof")
	}
	return nil
}

// Terminalize strips every pending subject/proof/secret binding before extending
// retention. Only the owning atomic mutation may persist a success receipt.
func (operation IdentityOperationV2) Terminalize(outcome IdentityOperationOutcomeV2, after *AuthRevisionV2, at time.Time) (IdentityOperationV2, error) {
	if operation.Validate() != nil || operation.Pending == nil || at.Before(operation.CreatedAt) || !at.Before(operation.CreatedAt.Add(IdentityReceiptRetentionV2)) {
		return IdentityOperationV2{}, identityOperationErrorV2("terminal", "requires the original nonterminal record within receipt retention")
	}
	if (outcome == IdentityOutcomeLinkedV2 || outcome == IdentityOutcomeUnlinkedV2) && operation.ValidateConfirm(at) != nil {
		return IdentityOperationV2{}, identityOperationErrorV2("terminal", "success requires complete fresh proof, not only a receipt request")
	}
	pending := operation.Pending
	result := IdentityOperationV2{Version: 2, Purpose: IdentityPurposeOperationV2, OperationDigest: operation.OperationDigest, Kind: operation.Kind, Phase: IdentityPhaseTerminalV2, CreatedAt: operation.CreatedAt, ExpiresAt: operation.ExpiresAt, DeleteAfter: operation.CreatedAt.Add(IdentityReceiptRetentionV2), Receipt: &IdentityOperationReceiptV2{UserID: pending.Scope.UserID, TenantID: pending.Scope.TenantID, Provider: pending.TargetProvider, Outcome: outcome, BeforeRevision: pending.Scope.ExpectedRevision, CompletedAt: at}}
	if after != nil {
		revision := *after
		result.Receipt.AfterRevision = &revision
	}
	if err := result.Validate(); err != nil {
		return IdentityOperationV2{}, err
	}
	return result, nil
}

func (operation IdentityOperationV2) clone() IdentityOperationV2 {
	if operation.Pending != nil {
		pending := *operation.Pending
		if pending.TargetSubject != nil {
			subject := *pending.TargetSubject
			pending.TargetSubject = &subject
		}
		if pending.SourceProof != nil {
			proof := pending.SourceProof.clone()
			pending.SourceProof = &proof
		}
		if pending.TargetProof != nil {
			proof := pending.TargetProof.clone()
			pending.TargetProof = &proof
		}
		operation.Pending = &pending
	}
	if operation.Receipt != nil {
		receipt := *operation.Receipt
		if receipt.AfterRevision != nil {
			revision := *receipt.AfterRevision
			receipt.AfterRevision = &revision
		}
		operation.Receipt = &receipt
	}
	return operation
}
func (proof IdentityProviderProofV2) clone() IdentityProviderProofV2 {
	if proof.SignedAuthTime != nil {
		at := *proof.SignedAuthTime
		proof.SignedAuthTime = &at
	}
	return proof
}
func identityLifetimeV2(created, expires time.Time) bool {
	return !created.IsZero() && expires.After(created) && expires.Sub(created) <= IdentityOperationMaxLifetimeV2
}
func identityProviderV2(provider IdentityProvider) bool {
	return provider == IdentityProviderYandex || provider == IdentityProviderTelegram
}
func identityTerminalOutcomeV2(outcome IdentityOperationOutcomeV2) bool {
	switch outcome {
	case IdentityOutcomeLinkedV2, IdentityOutcomeUnlinkedV2, IdentityOutcomeCancelledV2, IdentityOutcomeConflictV2, IdentityOutcomeStaleRevisionV2, IdentityOutcomeProofRequiredV2, IdentityOutcomeProofUnavailableV2, IdentityOutcomeLastMethodV2, IdentityOutcomeProviderUnavailableV2, IdentityOutcomeExpiredV2, IdentityOutcomeAccessDeniedV2:
		return true
	default:
		return false
	}
}
func identityOperationErrorV2(field, reason string) error {
	return ValidationError{Field: "identity_operation_v2." + field, Reason: reason}
}
