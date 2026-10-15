package providercredential

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"time"

	"gitcode.com/urandon/sessionless/internal/credentiallifecycle"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// PreparedImmutableAPIFileV1 is local pre-Issue sizing and callback-only file
// verification by the SAME lifecycle owner. It exposes neither a private file
// object nor plaintext, and cannot issue, finalize, release or authorize use.
// A zero value is invalid. It is not a wire DTO or a Source/native-fit grant.
type PreparedImmutableAPIFileV1 struct {
	ctx       context.Context
	service   *InvocationService
	files     *credentiallifecycle.PinnedSecretFileSystem
	scope     ports.ProviderInvocationCredentialV1 // exact expected tuple except not-yet-issued HandleID
	binding   preparedAPIFileBinding
	fileName  string
	maxBytes  int64
	pathBytes uint64
}

func (PreparedImmutableAPIFileV1) String() string {
	return "PreparedImmutableAPIFileV1{scope:[redacted]}"
}
func (p PreparedImmutableAPIFileV1) GoString() string { return p.String() }

type immutableAPIFileUse struct {
	gate        chan struct{}
	ctx         context.Context
	handle      ports.ProviderInvocationCredentialV1
	binding     preparedAPIFileBinding
	plan        ports.ProviderCredentialMaterializationV1
	files       *credentiallifecycle.PinnedSecretFileSystem
	file        *credentiallifecycle.PinnedSecretFile
	fingerprint domain.CredentialFingerprint
	active      atomic.Bool
}

// Only the supported single-turn scalar binding is retained. Freeze its sole
// optional time pointer by value, never retain an owner-agent policy graph.
type preparedAPIFileBinding struct {
	value          domain.HarnessBindingV2
	evidenceExpiry time.Time
}

func snapshotAPIFileBinding(b domain.HarnessBindingV2) preparedAPIFileBinding {
	if b.Profile != domain.HarnessExecutionSingleTurnV2 || b.AgentLoopPolicy != nil {
		return preparedAPIFileBinding{}
	}
	v := preparedAPIFileBinding{value: b}
	if b.EvidenceExpiresAt != nil {
		v.evidenceExpiry = b.EvidenceExpiresAt.UTC()
	}
	v.value.EvidenceExpiresAt = nil
	return v
}

// PrepareImmutableAPIFileV1 establishes the genuine namespace and exact future
// locator size before Issue. No planner, binding, secret or ID backend is read.
// The later original Issue still validates its real delivery template/resource.
func (s *InvocationService) PrepareImmutableAPIFileV1(ctx context.Context, request ports.ProviderCredentialIssueRequestV1, fileName string, maxBytes int64) (*PreparedImmutableAPIFileV1, error) {
	if s == nil || s.clock == nil || ctx == nil || ctx.Err() != nil || request.Validate() != nil ||
		request.HarnessBinding.Resource.Kind != domain.ProviderResourceAPIAccountV1 ||
		request.HarnessBinding.Profile != domain.HarnessExecutionSingleTurnV2 ||
		request.HarnessBinding.Backend.CredentialDeliveryKind != domain.ProviderCredentialDeliveryFileV1 ||
		maxBytes != s.maxBytes || !s.clock.Now().Before(request.ExpiresAt) ||
		domain.ValidateOpaqueID("credential.materialization.file_name", fileName) != nil || filepath.Base(fileName) != fileName {
		return nil, ErrInvalid
	}
	if s.ensureFileSystem() != nil {
		return nil, ErrDeliveryMismatch
	}
	s.mu.Lock()
	files := s.files
	s.mu.Unlock()
	n, err := files.ImmutableFilePathBound(ctx, fileName)
	if err != nil || ctx.Err() != nil {
		return nil, ErrDeliveryMismatch
	}
	b := request.HarnessBinding
	return &PreparedImmutableAPIFileV1{ctx: ctx, service: s, files: files, fileName: fileName, maxBytes: maxBytes, pathBytes: n, binding: snapshotAPIFileBinding(b),
		scope: ports.ProviderInvocationCredentialV1{TenantID: b.TenantID, OwnerUserID: b.OwnerUserID, RunID: b.RunID, AttemptID: b.AttemptID,
			WorkerID: request.WorkerID, LeaseID: request.LeaseID, LeaseFence: request.LeaseFence,
			ProviderResource: b.Resource, ExpiresAt: canonicalTime(request.ExpiresAt)}}, nil
}

// MatchesOriginalBinding compares frozen DATA, not eligibility or authority.
// b must have its optional time removed and supplied independently by value.
func (p *PreparedImmutableAPIFileV1) MatchesOriginalBinding(b domain.HarnessBindingV2, evidenceExpiry time.Time) bool {
	return p != nil && b.Profile == domain.HarnessExecutionSingleTurnV2 && b.AgentLoopPolicy == nil && b.EvidenceExpiresAt == nil &&
		p.binding.value == b && p.binding.evidenceExpiry.Equal(evidenceExpiry)
}

// Scope is comparison DATA from the original pre-Issue request, not a handle.
func (p *PreparedImmutableAPIFileV1) Scope() ports.ProviderInvocationCredentialV1 {
	if p == nil {
		return ports.ProviderInvocationCredentialV1{}
	}
	return p.scope
}

func (p *PreparedImmutableAPIFileV1) FilePathBound(ctx context.Context, maxBytes uint64) (uint64, error) {
	if p == nil || p.service == nil || p.ctx == nil || p.ctx.Err() != nil || ctx == nil || ctx.Err() != nil ||
		p.maxBytes < 1 || uint64(p.maxBytes) != maxBytes || p.files == nil {
		return 0, ErrInvalid
	}
	n, err := p.files.ImmutableFilePathBound(ctx, p.fileName)
	if err != nil || n != p.pathBytes {
		return 0, ErrDeliveryMismatch
	}
	return n, nil
}

// VerifyMaterialized accepts only the exact acquired handle and original
// materialization while its genuine synchronous consumer is active. It never
// locks state.mu, calls a backend, reenters the lifecycle or returns file/key
// custody. The callback's close joins this local verifier before cleanup.
func (p *PreparedImmutableAPIFileV1) VerifyMaterialized(ctx context.Context, handle ports.ProviderInvocationCredentialV1, plan ports.ProviderCredentialMaterializationV1) error {
	if p == nil || p.service == nil || p.ctx == nil || p.ctx.Err() != nil || ctx == nil || ctx.Err() != nil || handle.Validate() != nil || plan.Validate() != nil {
		return ErrInvalid
	}
	want := handle
	want.HandleID = ""
	if want != p.scope || plan.Kind != domain.ProviderCredentialDeliveryFileV1 ||
		filepath.Base(plan.FilePath) != p.fileName || uint64(len(plan.FilePath)) != p.pathBytes {
		return ErrDeliveryMismatch
	}
	// Only immutable handle and atomic use-pointer are inspected outside the
	// state lock. No mutable callback/finalizer/revocation flags are raced.
	p.service.mu.Lock()
	state := p.service.handles[handle.HandleID]
	var use *immutableAPIFileUse
	if state != nil && state.handle == handle {
		use = state.fileUse.Load()
	}
	p.service.mu.Unlock()
	if use == nil {
		return ErrConsumed
	}
	// The original Init has one consumer. Reentry/concurrent validation does
	// not queue or hold up retirement with an unbounded set of waiters.
	select {
	case <-use.gate:
	default:
		return ErrConsumed
	}
	defer func() { use.gate <- struct{}{} }()
	if !use.active.Load() || use.ctx == nil || use.ctx.Err() != nil || p.ctx.Err() != nil || ctx.Err() != nil ||
		use.handle != handle || use.binding != p.binding || use.plan != plan || use.files != p.files ||
		!p.service.clock.Now().Before(handle.ExpiresAt) || use.file == nil {
		return ErrConsumed
	}
	if p.files.VerifyImmutable(ctx, use.file, use.fingerprint, p.maxBytes) != nil {
		return ErrDeliveryMismatch
	}
	if !use.active.Load() || use.ctx.Err() != nil || p.ctx.Err() != nil || ctx.Err() != nil || !p.service.clock.Now().Before(handle.ExpiresAt) {
		return ErrExpired
	}
	return nil
}
