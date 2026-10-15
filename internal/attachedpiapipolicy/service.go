// Package attachedpiapipolicy exposes the owner-policy operations behind the
// existing WebBFF authenticated mutation boundary. It has no provider keys,
// signing key, ingestion permission, model client or serving activation flag.
package attachedpiapipolicy

import (
	"context"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type Service struct {
	store ports.AttachedPiAPIOwnerPolicyStoreV1
}

func New(store ports.AttachedPiAPIOwnerPolicyStoreV1) (*Service, error) {
	if store == nil {
		return nil, domain.ErrMembershipDenied
	}
	return &Service{store: store}, nil
}
func (s *Service) Read(ctx context.Context, r ports.ReadAttachedPiAPIPolicyV1) (ports.AttachedPiAPIPolicyOfferV1, error) {
	if s == nil || ctx == nil || ctx.Err() != nil || r.Locator.Validate() != nil || r.WebSessionDigest.Validate("pi_policy.web_session") != nil {
		return ports.AttachedPiAPIPolicyOfferV1{}, domain.ErrMembershipDenied
	}
	return s.store.ReadAttachedPiAPIPolicy(ctx, r)
}
func (s *Service) Confirm(ctx context.Context, r ports.ConfirmAttachedPiAPIPolicyV1) (ports.AttachedPiAPIPolicyDecisionV1, error) {
	if s == nil || ctx == nil || ctx.Err() != nil {
		return ports.AttachedPiAPIPolicyDecisionV1{}, domain.ErrMembershipDenied
	}
	return s.store.ConfirmAttachedPiAPIPolicy(ctx, r)
}
func (s *Service) Revoke(ctx context.Context, r ports.RevokeAttachedPiAPIPolicyV1) error {
	if s == nil || ctx == nil || ctx.Err() != nil {
		return domain.ErrMembershipDenied
	}
	return s.store.RevokeAttachedPiAPIPolicy(ctx, r)
}
