package ports

import "gitcode.com/urandon/sessionless/internal/domain"

// OwnedAttemptEffectSourceVerifierV1 authenticates the exact original installed,
// independently reviewed source and payer snapshots. The Store calls it on
// each DB retry. Implementations must be local, bounded and perform no I/O.
// A source digest, arbitrary MAC or fixture-approved boolean alone cannot prove
// provenance. The production keeper/issuer is a separate implementation gate.
//
// Verification may accept preserved historical source bytes after their expiry;
// it is NOT current permission, lease/fence validation or held-quota authority.
// Those checks belong to the same current canonical Store transaction. A nil
// verifier must fail closed before SQL or capability minting.
type OwnedAttemptEffectSourceVerifierV1 interface {
	VerifyOwnedAttemptEffectSource(domain.AttemptExecutionSourceV1) error
}

// OwnedPreparationGrantVerifierV1 authenticates the process-local sole owner's
// grant before a durable preparation burn. It is NOT installed-source approval
// or proof that physical preparation succeeded. Verification is local, bounded,
// has no I/O and is repeated after the fresh burn's confirmed commit.
type OwnedPreparationGrantVerifierV1 interface {
	VerifyGrant(AttemptEffectOwnershipGrantV1) error
}
