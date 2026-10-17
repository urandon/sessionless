package localoperator

import (
	"bytes"
	"context"
	"encoding/json"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// ReadInvoiceTerms returns original nonsecret custody DATA, not a tariff,
// current DB approval or send capability. The Store must retain this Source's
// original lifetime and independently rejoin its current profile/spending.
// Only the original accepted installation's retained <=4KiB body is returned;
// no file, key, manifest or caller-selected evidence is read here.
func (s *Source) ReadInvoiceTerms(ctx context.Context, ref domain.OwnedAgentProfileReferenceV1, cap uint64) ([]byte, error) {
	if s == nil || ctx == nil || ctx.Err() != nil || cap == 0 || cap > 4096 || ref.Validate() != nil {
		return nil, ErrDenied
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.aliveLocked() || s.receipt == nil || s.config.ValidateForOwnedProfile(s.receipt.Profile) != nil {
		return nil, ErrDenied
	}
	p := s.receipt.Profile
	if ref.Locator != p.Locator || ref.RevocationGeneration != p.RevocationGeneration || ref.Digest != s.receipt.Digest {
		return nil, ErrDenied
	}
	want := s.config.OperatorTerms
	if want.Validate() != nil || want.Bytes == 0 || want.Bytes > cap {
		return nil, ErrDenied
	}
	for _, body := range s.bodies {
		if body.Reference != want {
			continue
		}
		if uint64(len(body.Body)) != want.Bytes || hash(body.Body) != want.Digest {
			return nil, ErrDenied
		}
		var terms InvoiceTermsV1
		if json.Unmarshal(body.Body, &terms) != nil || !validTerms(terms, s.config, p) {
			return nil, ErrDenied
		}
		canonical, err := json.Marshal(terms)
		if err != nil || !bytes.Equal(canonical, body.Body) || ctx.Err() != nil {
			return nil, ErrDenied
		}
		return bytes.Clone(body.Body), nil
	}
	return nil, ErrDenied
}
