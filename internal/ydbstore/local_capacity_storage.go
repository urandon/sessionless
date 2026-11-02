package ydbstore

import (
	"database/sql"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// These values are physical lookup/projection data only. In particular, key is
// never converted into a domain SubscriptionConnectionID on the local arm.
type capacityStorageV1 struct {
	key         string
	association sql.NullString
	digest      sql.NullString
}

func encodeCapacityStorageV1(tenant domain.TenantID, subscription domain.SubscriptionConnectionID, local *domain.LocalCapacityAssociationV1) (capacityStorageV1, error) {
	if tenant.Validate() != nil {
		return capacityStorageV1{}, domain.ErrMembershipDenied
	}
	if local == nil {
		if subscription.Validate() != nil {
			return capacityStorageV1{}, domain.ErrMembershipDenied
		}
		return capacityStorageV1{key: string(subscription)}, nil
	}
	if subscription != "" || local.Validate() != nil || local.TenantID != tenant {
		return capacityStorageV1{}, domain.ErrMembershipDenied
	}
	raw, err := domain.EncodeLocalCapacityAssociationV1(*local)
	if err != nil {
		return capacityStorageV1{}, domain.ErrMembershipDenied
	}
	digest, err := local.Digest()
	if err != nil {
		return capacityStorageV1{}, domain.ErrMembershipDenied
	}
	key, err := local.CapacityKey()
	if err != nil {
		return capacityStorageV1{}, domain.ErrMembershipDenied
	}
	return capacityStorageV1{key: key, association: sql.NullString{String: string(raw), Valid: true}, digest: sql.NullString{String: digest, Valid: true}}, nil
}

func decodeCapacityStorageV1(tenant domain.TenantID, stored capacityStorageV1) (domain.SubscriptionConnectionID, *domain.LocalCapacityAssociationV1, error) {
	if tenant.Validate() != nil || len(stored.key) > 160 || stored.association.Valid != stored.digest.Valid {
		return "", nil, domain.ErrMembershipDenied
	}
	if !stored.association.Valid {
		// NULL is not an empty local record, and the prefix never chooses an arm.
		subscription := domain.SubscriptionConnectionID(stored.key)
		if subscription.Validate() != nil {
			return "", nil, domain.ErrMembershipDenied
		}
		return subscription, nil, nil
	}
	local, err := domain.DecodeLocalCapacityAssociationV1([]byte(stored.association.String))
	if err != nil || local.TenantID != tenant {
		return "", nil, domain.ErrMembershipDenied
	}
	want, err := encodeCapacityStorageV1(tenant, "", &local)
	if err != nil || want != stored {
		return "", nil, domain.ErrMembershipDenied
	}
	return "", &local, nil
}

func equalLocalCapacityV1(left, right *domain.LocalCapacityAssociationV1) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}
