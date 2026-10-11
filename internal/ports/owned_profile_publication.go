package ports

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// OwnedProfilePublicationGrantV1 is an authenticated process-local transition
// intent, not installed-source approval by itself. A trusted composition must
// verify actual installed/reviewed/source bytes before issuing it. No issuer or
// serving composition is provided by this storage prerequisite.
type OwnedProfilePublicationGrantV1 struct {
	Profile            domain.OwnedAgentProfileCurrentV1
	ExpectedGeneration uint64
	ExpectedDigest     string
	IssuedAt           time.Time
	ExpiresAt          time.Time
	Authenticator      []byte
}

func (g OwnedProfilePublicationGrantV1) Clone() OwnedProfilePublicationGrantV1 {
	g.Profile = g.Profile.Clone()
	g.Authenticator = append([]byte(nil), g.Authenticator...)
	g.IssuedAt, g.ExpiresAt = g.IssuedAt.UTC(), g.ExpiresAt.UTC()
	return g
}

func (g OwnedProfilePublicationGrantV1) Validate() error {
	denied := domain.ErrMembershipDenied
	if g.Profile.Validate() != nil || g.ExpectedGeneration == math.MaxUint64 || g.Profile.RevocationGeneration != g.ExpectedGeneration+1 || len(g.Authenticator) != sha256.Size {
		return denied
	}
	if g.ExpectedGeneration == 0 {
		if g.ExpectedDigest != "" || g.Profile.ValidateInitial() != nil {
			return denied
		}
	} else if domain.ValidateSHA256Digest("publication.expected_digest", g.ExpectedDigest) != nil {
		return denied
	}
	for _, at := range []time.Time{g.IssuedAt, g.ExpiresAt} {
		if at.IsZero() || at.Year() < 1 || at.Year() > 9999 || at.Nanosecond()%1000 != 0 {
			return denied
		}
	}
	if !g.ExpiresAt.After(g.IssuedAt) || g.ExpiresAt.Sub(g.IssuedAt) > 10*time.Second {
		return denied
	}
	// Closed first publication class: live/platform/foreign funding remains
	// unsupported even if an injected verifier authenticates an intent.
	p := g.Profile
	if p.Backend.BackendKind != domain.HarnessBackendDeterministicFixtureV1 || p.Backend.ProviderContractKind != domain.ProviderContractCredentiallessFixtureV1 || p.Resource.Kind != domain.ProviderResourceCredentiallessFixtureV1 || p.Substrate.Kind != domain.SubstrateDeterministicFixtureV1 {
		return denied
	}
	return nil
}

// IntentBytes seals every template, predecessor and original grant-window field.
// DB publication/update time is assigned by the Store, never by the issuer.
// Authentication must bind these exact bytes, not only a row or approval label.
func (g OwnedProfilePublicationGrantV1) IntentBytes() ([]byte, error) {
	if g.Validate() != nil {
		return nil, domain.ErrMembershipDenied
	}
	raw, err := domain.EncodeOwnedAgentProfileCurrentV1(g.Profile)
	if err != nil {
		return nil, domain.ErrMembershipDenied
	}
	return json.Marshal(struct {
		Kind               string          `json:"kind"`
		Profile            json.RawMessage `json:"profile"`
		ExpectedGeneration uint64          `json:"expected_generation"`
		ExpectedDigest     string          `json:"expected_digest"`
		IssuedAt           time.Time       `json:"issued_at"`
		ExpiresAt          time.Time       `json:"expires_at"`
	}{"sessionless.owned-profile-publication.v1", raw, g.ExpectedGeneration, g.ExpectedDigest, g.IssuedAt.UTC(), g.ExpiresAt.UTC()})
}

func (g OwnedProfilePublicationGrantV1) IntentDigest() (string, error) {
	raw, err := g.IntentBytes()
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func (OwnedProfilePublicationGrantV1) MarshalJSON() ([]byte, error) {
	return nil, errors.New("process-local publication grant cannot be serialized")
}

func (*OwnedProfilePublicationGrantV1) UnmarshalJSON([]byte) error {
	return errors.New("process-local publication grant cannot be deserialized")
}

// Implementations are installed trusted composition, not public input. Verify
// must be a bounded local check with no I/O and authenticate all IntentBytes.
// It must reject absent/unreviewed installations and revoked source authority.
// Store calls it for each DB-time retry snapshot; a signature alone is not source
// verification. The production issuer/composition remains a required #189 gate.
type OwnedProfilePublicationVerifierV1 interface {
	VerifyOwnedProfilePublication(OwnedProfilePublicationGrantV1, time.Time) error
}

// A historical publication receipt is not current execution permission. Exact
// replay recovers this original record even after a later revocation; it does
// not restore that record to the current head or mint an operation capability.
type OwnedProfilePublicationReceiptV1 struct {
	Profile  domain.OwnedAgentProfileCurrentV1
	Digest   string
	Replayed bool
}
