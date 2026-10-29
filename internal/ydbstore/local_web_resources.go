package ydbstore

import (
	"context"
	"reflect"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localoperator"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// localWebResources changes only trusted compute selection. All other resource
// reads retain the original Store's membership/session/object authorization.
type localWebResources struct {
	*Store
	source  *localoperator.Source
	receipt ports.OwnedProfilePublicationReceiptV1
}

func NewLocalWebResourceStore(store *Store, source *localoperator.Source, receipt ports.OwnedProfilePublicationReceiptV1) (ports.WebResourceStore, error) {
	if !store.originalLocalPublicationSource(source) || source.VerifyOriginalSpendingSources(receipt, nil) != nil {
		return nil, domain.ErrMembershipDenied
	}
	receipt.Profile = receipt.Profile.Clone()
	return &localWebResources{Store: store, source: source, receipt: receipt}, nil
}

// This is a current read, not admission or a capacity reservation. In
// particular it never invents Attempt IDs or calls spending recovery/publish.
func (s *localWebResources) ResolveComputeConnectionsForUser(ctx context.Context, r ports.ComputeConnectionResolveRequest) ([]ports.ComputeConnectionState, error) {
	if s == nil || ctx == nil || ctx.Err() != nil || validateSessionAPIIdentity(r.TenantID, r.UserID, r.SessionID) != nil {
		return nil, domain.ErrMembershipDenied
	}
	call, cancel := ownedAuthorityCallContext(ctx)
	defer cancel()
	lifetime, lifetimeErr := s.source.ExecutionLifetime()
	if lifetimeErr == nil && lifetime.Err() == nil {
		deadline, bounded := lifetime.Deadline()
		if !bounded {
			return nil, domain.ErrMembershipDenied
		}
		linked, finish := context.WithDeadline(call, deadline)
		defer finish()
		stop := context.AfterFunc(lifetime, finish)
		defer stop()
		call = linked
	}
	var result []ports.ComputeConnectionState
	err := s.ownedAuthorityTx(call, func(ctx context.Context, q *ownedAuthorityQuery) error {
		result = nil
		if _, _, err := readOwnedSessionPrincipal(ctx, q, r.TenantID, r.SessionID, r.UserID); err != nil {
			return err
		}
		if r.TenantID != s.receipt.Profile.Locator.TenantID || r.UserID != s.receipt.Profile.Locator.OwnerUserID {
			return q.budget.deny()
		}
		// Authorized readers may observe an unavailable local installation, but
		// never fall back to a subscription or renew its original authority.
		life, err := s.source.ExecutionLifetime()
		if err != nil || life.Err() != nil || !s.originalLocalPublicationSource(s.source) || s.source.VerifyOriginalSpendingSources(s.receipt, nil) != nil {
			return nil
		}
		configuration, err := s.source.Configuration()
		if err != nil {
			return nil
		}
		p, found, err := loadOwnedProfileProjection(ctx, q, s.receipt.Profile.Locator)
		if err != nil {
			return err
		}
		digest, digestErr := p.Digest()
		if !found || digestErr != nil || digest != s.receipt.Digest || !reflect.DeepEqual(p, s.receipt.Profile) || p.ValidateAt(q.at) != nil || configuration.ValidateForOwnedProfile(p) != nil {
			return nil
		}
		if err := readLocalRecoveredProfileAudit(ctx, q, p); err != nil {
			return err
		}
		association := &domain.LocalCapacityAssociationV1{Version: 1, Kind: domain.ProviderResourceLocalEndpointV1, TenantID: r.TenantID, OwnerUserID: r.UserID, ResourceID: p.Locator.ResourceID}
		if association.Validate() != nil || ctx.Err() != nil || life.Err() != nil {
			return q.budget.deny()
		}
		result = []ports.ComputeConnectionState{{LocalCapacityAssociation: association, Provider: "local_model", ObservedAt: q.at}}
		return nil
	})
	if err != nil || ctx.Err() != nil {
		return nil, domain.ErrMembershipDenied
	}
	// Recheck the ORIGINAL lifetime after the final read/commit; no detached
	// status response can survive cancellation during its DB operation.
	if len(result) != 0 {
		life, err := s.source.ExecutionLifetime()
		if err != nil || life.Err() != nil {
			return nil, nil
		}
	}
	return result, nil
}

var _ ports.WebResourceStore = (*localWebResources)(nil)
