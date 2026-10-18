package ydbstore

import (
	"context"
	"database/sql"
	"reflect"
	"sync"
	"sync/atomic"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localoperator"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
)

// LocalOwnedStoreBootstrapV1 is one trusted constructor, not a runtime registry,
// source verifier, operation issuer or approval callback. The connector retains
// this original object from its constructor; it never separately binds a Store.
// Only the completed immutable tuple exposes its serving handles.
type LocalOwnedStoreBootstrapV1 struct {
	mu      sync.Mutex
	self    *LocalOwnedStoreBootstrapV1
	source  *localoperator.Source
	prepare *serverlessharness.CapabilityIssuer
	operate *serverlessharness.OperationCapabilityIssuer
	// Immutable decoding DATA from the original source, not current permission.
	configuration domain.ConfiguredInferenceProfileV1
	store         *Store
	keeper        ports.OwnedProxyPublicationVerifierV2
	ready         atomic.Pointer[localOwnedStoreTupleV1]
	bodySlot      *localOperationBodySlot
	startClaimed  atomic.Bool
	entry         atomic.Pointer[LocalInvocationEntryV1]
	// Installed only by this original Store's confirmed fresh reserve. The
	// external result is DATA; neither its mutable Status nor a valid grant
	// creates this one-use ownership. b.mu protects the single pending slot.
	pendingStart    *localFreshReservationV1
	reserveFinished bool
}

type localFreshReservationV1 struct {
	invocation context.Context
	request    ports.ReserveAttemptEffectRequestV1
	result     ports.ReserveAttemptEffectResultV1
	tuple      *localOwnedStoreTupleV1
}

type localOwnedStoreTupleV1 struct {
	store  *Store
	bridge *serverlessharness.BoundedOwnedAllocationPreparationV1
	keeper ports.OwnedProxyPublicationVerifierV2
}

func NewLocalOwnedStoreBootstrapV1(source *localoperator.Source, prepare *serverlessharness.CapabilityIssuer, operate *serverlessharness.OperationCapabilityIssuer) (*LocalOwnedStoreBootstrapV1, error) {
	if source == nil || prepare == nil || operate == nil {
		return nil, domain.ErrMembershipDenied
	}
	c, err := source.Configuration()
	if err != nil || c.Auth != domain.ConfiguredInferenceAuthNoneV1 {
		return nil, domain.ErrMembershipDenied
	}
	b := &LocalOwnedStoreBootstrapV1{source: source, prepare: prepare, operate: operate, configuration: c.Clone()}
	b.self = b
	return b, nil
}

// HasLocalConnectorIdentity is constructor metadata, NOT current permission.
func (b *LocalOwnedStoreBootstrapV1) HasLocalConnectorIdentity(source *localoperator.Source, operate *serverlessharness.OperationCapabilityIssuer) bool {
	return b != nil && b.self == b && source != nil && operate != nil && b.source == source && b.operate == operate
}

func sameLocalOwnerPointer(a, b any) bool {
	x, y := reflect.ValueOf(a), reflect.ValueOf(b)
	return x.IsValid() && y.IsValid() && x.Kind() == reflect.Pointer && y.Kind() == reflect.Pointer &&
		!x.IsNil() && !y.IsNil() && x.Type() == y.Type() && x.Pointer() == y.Pointer()
}

// CreateStore creates exactly one inert Store, with original owners fixed.
// Nil owner options may select those explicit constructor owners; foreign
// preinstalled owners are rejected, never silently replaced. Source is not a
// substitute for the complete effect-source verifier or other service custody.
func (b *LocalOwnedStoreBootstrapV1) CreateStore(db *sql.DB, options Options, keeper ports.OwnedProxyPublicationVerifierV2) (*Store, error) {
	if b == nil || b.self != b || db == nil || !sameLocalOwnerPointer(keeper, keeper) ||
		options.OwnedAllocationPublicationVerifier != nil || options.OwnedAttemptEffectSourceVerifier == nil {
		return nil, domain.ErrMembershipDenied
	}
	for _, pair := range [][2]any{
		{options.AttemptEffectGrantIssuer, b.prepare}, {options.OwnedPreparationGrantVerifier, b.prepare},
		{options.AttemptOperationIssuer, b.operate}, {options.OwnedProxyPublicationVerifier, keeper},
	} {
		if pair[0] != nil && !sameLocalOwnerPointer(pair[0], pair[1]) {
			return nil, domain.ErrMembershipDenied
		}
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.store != nil || b.ready.Load() != nil {
		return nil, domain.ErrMembershipDenied
	}
	options.AttemptEffectGrantIssuer, options.OwnedPreparationGrantVerifier = b.prepare, b.prepare
	options.AttemptOperationIssuer, options.OwnedProxyPublicationVerifier = b.operate, keeper
	var bodySlot *localOperationBodySlot
	if options.AttemptOperationBodies == nil && options.OwnedOperationResidualCustody == nil {
		bodySlot = &localOperationBodySlot{owner: b}
		options.AttemptOperationBodies = bodySlot
	}
	store, err := New(db, options) // constructor only; no SQL or supplied methods
	if err != nil || (bodySlot != nil && store.attemptOperationBodies != bodySlot) {
		if err == nil {
			err = domain.ErrMembershipDenied
		}
		return nil, err
	}
	store.localBootstrap = b
	b.store, b.keeper, b.bodySlot = store, keeper, bodySlot
	return store, nil
}

// Complete publishes the exact one-time tuple. The connector already retains
// b from construction and resolves RuntimeFor itself; no mutable BindStore
// callback, prior-binding assertion or caller Boolean participates here.
func (b *LocalOwnedStoreBootstrapV1) Complete(bridge *serverlessharness.BoundedOwnedAllocationPreparationV1) error {
	if b == nil || b.self != b || bridge == nil || !bridge.MatchesLocalComposition(b.source, b.prepare, b) {
		return domain.ErrMembershipDenied
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.store == nil || b.store.localBootstrap != b || b.ready.Load() != nil ||
		!sameLocalOwnerPointer(b.store.attemptOperationIssuer, b.operate) || !sameLocalOwnerPointer(b.store.ownedProxyPublicationVerifier, b.keeper) ||
		!sameLocalOwnerPointer(b.store.attemptEffectGrantIssuer, b.prepare) || !sameLocalOwnerPointer(b.store.ownedPreparationGrantVerifier, b.prepare) ||
		b.store.ownedAllocationPublicationVerifier != nil {
		return domain.ErrMembershipDenied
	}
	b.store.ownedAllocationPublicationVerifier = bridge
	b.ready.Store(&localOwnedStoreTupleV1{store: b.store, bridge: bridge, keeper: b.keeper})
	return nil
}

// RuntimeFor authenticates actual retained object identities only. It calls no
// keeper method and cannot authorize a socket or skip Store's current joins.
func (b *LocalOwnedStoreBootstrapV1) RuntimeFor(keeper ports.OwnedProxyPublicationVerifierV2) (*Store, *serverlessharness.BoundedOwnedAllocationPreparationV1, error) {
	if b == nil || b.self != b {
		return nil, nil, domain.ErrMembershipDenied
	}
	tuple := b.ready.Load()
	if tuple == nil || tuple.store == nil || tuple.store.localBootstrap != b || !sameLocalOwnerPointer(keeper, tuple.keeper) {
		return nil, nil, domain.ErrMembershipDenied
	}
	return tuple.store, tuple.bridge, nil
}

func (store *Store) localCompositionReady() bool {
	if store == nil {
		return false
	}
	b := store.localBootstrap
	if b == nil {
		return true // ordinary existing Store profiles retain their contracts
	}
	if b.self != b {
		return false
	}
	tuple := b.ready.Load()
	return tuple != nil && tuple.store == store
}

// The actual canonical burn, not an adapter that forwards to a caller callback.
func (b *LocalOwnedStoreBootstrapV1) BeginOwnedAttemptPreparation(ctx context.Context, grant ports.AttemptEffectOwnershipGrantV1) (bool, error) {
	if b == nil || b.self != b {
		return false, domain.ErrMembershipDenied
	}
	tuple := b.ready.Load()
	if tuple == nil || tuple.store == nil || !tuple.store.localCompositionReady() {
		return false, domain.ErrMembershipDenied
	}
	return tuple.store.beginBoundedOwnedAttemptPreparation(ctx, grant)
}

func (*LocalOwnedStoreBootstrapV1) String() string {
	return "LocalOwnedStoreBootstrapV1{private:[redacted]}"
}
func (b *LocalOwnedStoreBootstrapV1) GoString() string { return b.String() }
