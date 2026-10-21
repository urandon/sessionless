package codexexec

import (
	"context"
	"reflect"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
	"gitcode.com/urandon/sessionless/internal/domain"
)

// NewNativePreparedProcessBoundaryV2 explicitly selects the successor while
// retaining the SAME consumed/process/cancellation/observation owner. It does
// not register a driver, issue credentials or qualify a native/OCI artifact.
func NewNativePreparedProcessBoundaryV2(ctx context.Context, config PreparedProcessBoundaryConfigV1,
	session *attachedworkersession.Session, input *attachedworkerdaemon.VerifiedAttachedInputV1,
	bundle attachedworkerdaemon.NativeLaunchV2) (*PreparedProcessBoundaryV1, error) {
	if config.Supervisor == nil || !config.Supervisor.NativeV2() {
		return nil, ErrContract
	}
	// Validate before borrowing: an invalid/losing constructor must never retire
	// the original child already held by a successful constructor.
	boundary, err := newPreparedProcessBoundary(config)
	if err != nil {
		return nil, err
	}
	owner, err := newNativeConduitOwner(ctx, session, input, config.Authority, config.Supervisor, bundle)
	if err != nil {
		return nil, err
	}
	boundary.native = owner
	boundary.nativeIdentity = boundary
	bound, err := owner.child.BindPreparedEntryV2(boundary)
	if err != nil {
		return nil, ErrAuthorityUnavailable
	}
	original, ok := bound.(*PreparedProcessBoundaryV1)
	if !ok || original == nil || original.nativeIdentity != original || original.native == nil ||
		original.native.self != original.native || original.native.child != owner.child ||
		original.supervisor != boundary.supervisor || original.authority != boundary.authority ||
		original.executable != boundary.executable || original.digest != boundary.digest ||
		!reflect.DeepEqual(original.arguments, boundary.arguments) || !reflect.DeepEqual(original.native.bundle, owner.bundle) ||
		owner.child.Current() != nil {
		return nil, ErrAuthorityUnavailable
	}
	return original, nil
}

// NativePreparedEntryIdentityV2 is a compile-time marker, not an approval call.
func (*PreparedProcessBoundaryV1) NativePreparedEntryIdentityV2() {}

// NewPreparedAttachedWorkerDriverV1 is the exact outer composition for one
// already accepted attached-worker attempt. It remains disabled whenever
// Config.Enabled is false, but its immutable authority snapshot and prepared
// process boundaries are complete and testable without central-store access.
func NewPreparedAttachedWorkerDriverV1(
	config Config,
	attempt domain.AttachedWorkerAttemptV1,
	resource domain.ProviderResourceBindingV1,
	supervisor *attachedworkerdaemon.Supervisor,
) (*Driver, error) {
	resolver, err := NewAcceptedAuthorityResolverV1(attempt, resource, config.Now)
	if err != nil {
		return nil, err
	}
	boundary, err := NewPreparedProcessBoundaryV1(PreparedProcessBoundaryConfigV1{
		Supervisor: supervisor, Authority: resolver.authority, Executable: config.Executable,
		ExecutableDigest: config.ExecutableDigest,
		Arguments:        processArguments(config.Model),
		Now:              config.Now,
	})
	if err != nil {
		return nil, err
	}
	return NewDriver(config, resolver, boundary)
}
