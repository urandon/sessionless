package ports

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// AttemptSpendingPublicationGrantV1 is an exact process-local source-owner
// intent, not payer approval by itself. Its trusted verifier must derive the
// scope and finite synthetic amount from an independently installed source.
// No production issuer or admission composition is supplied by this port.
type AttemptSpendingPublicationGrantV1 struct {
	Authorization      domain.AttemptSpendingAuthorizationCurrentV1
	ExpectedGeneration uint64
	ExpectedDigest     string
	IssuedAt           time.Time
	ExpiresAt          time.Time
	Authenticator      []byte
}

func (g AttemptSpendingPublicationGrantV1) Clone() AttemptSpendingPublicationGrantV1 {
	g.Authorization = g.Authorization.Clone()
	g.Authenticator = append([]byte(nil), g.Authenticator...)
	g.IssuedAt, g.ExpiresAt = g.IssuedAt.UTC(), g.ExpiresAt.UTC()
	return g
}

func (g AttemptSpendingPublicationGrantV1) Validate() error {
	denied := domain.ErrMembershipDenied
	v := g.Authorization
	if v.Validate() != nil || v.PriceState == domain.ExternalServicePriceUnknownV1 || len(g.Authenticator) != sha256.Size {
		return denied
	}
	switch g.ExpectedGeneration {
	case 0:
		if g.ExpectedDigest != "" || v.ValidateInitial() != nil {
			return denied
		}
	case 1:
		if v.State != domain.OwnedAgentCurrentRevokedV1 || v.RevocationGeneration != 2 || domain.ValidateSHA256Digest("spending.expected_digest", g.ExpectedDigest) != nil {
			return denied
		}
	default:
		return denied // one initial envelope and one monotonic revocation; never top-up
	}
	for _, at := range []time.Time{g.IssuedAt, g.ExpiresAt} {
		if at.IsZero() || at.Year() < 1 || at.Year() > 9999 || at.Nanosecond()%1000 != 0 {
			return denied
		}
	}
	if !g.ExpiresAt.After(g.IssuedAt) || g.ExpiresAt.Sub(g.IssuedAt) > 10*time.Second {
		return denied
	}
	return nil
}

func (g AttemptSpendingPublicationGrantV1) IntentBytes() ([]byte, error) {
	if g.Validate() != nil {
		return nil, domain.ErrMembershipDenied
	}
	raw, err := domain.EncodeAttemptSpendingAuthorizationCurrentV1(g.Authorization)
	if err != nil {
		return nil, domain.ErrMembershipDenied
	}
	return json.Marshal(struct {
		Kind               string          `json:"kind"`
		Authorization      json.RawMessage `json:"authorization"`
		ExpectedGeneration uint64          `json:"expected_generation"`
		ExpectedDigest     string          `json:"expected_digest"`
		IssuedAt           time.Time       `json:"issued_at"`
		ExpiresAt          time.Time       `json:"expires_at"`
	}{"sessionless.attempt-spending-publication.v1", raw, g.ExpectedGeneration, g.ExpectedDigest, g.IssuedAt.UTC(), g.ExpiresAt.UTC()})
}

func (g AttemptSpendingPublicationGrantV1) IntentDigest() (string, error) {
	raw, err := g.IntentBytes()
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func (AttemptSpendingPublicationGrantV1) MarshalJSON() ([]byte, error) {
	return nil, errors.New("process-local spending grant cannot be serialized")
}
func (*AttemptSpendingPublicationGrantV1) UnmarshalJSON([]byte) error {
	return errors.New("process-local spending grant cannot be deserialized")
}

// Installed admission/source-owner composition only; no I/O during SQL. It
// authenticates all IntentBytes and the exact original Attempt/source/payer
// authorization, not a fixture label or a copied price/capacity quota. The Store
// rechecks it on every retry. No serving implementation is installed yet.
type AttemptSpendingPublicationVerifierV1 interface {
	VerifyAttemptSpendingPublication(AttemptSpendingPublicationGrantV1, time.Time) error
}

// Historical readback is not a current spending permission or debit receipt.
type AttemptSpendingPublicationReceiptV1 struct {
	Authorization domain.AttemptSpendingAuthorizationCurrentV1
	Digest        string
	Replayed      bool
}
