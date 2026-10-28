package attachedworkerdaemontransport

import (
	"context"
	"errors"
	"slices"

	"gitcode.com/urandon/sessionless/internal/attachedresourcegenerationsession"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
	"gitcode.com/urandon/sessionless/internal/attachedworkerstack"
	"gitcode.com/urandon/sessionless/internal/codexexec"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ownerlocalcredential"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// NewNativeGenerationPreparedAdapterV3 is the explicit post-Ready generation
// composition. It binds/synchronizes only the original Stack/Session and the
// normal subscription materializer. It neither connects nor issues credentials.
// The caller retains original cleanup ownership on every return.
func NewNativeGenerationPreparedAdapterV3(ctx context.Context, session *attachedworkersession.Session, materializer Materializer, config Config,
	stack *attachedworkerstack.Stack, mode attachedworkersession.ResourceGenerationConnectionModeV1) (*Adapter, error) {
	if ctx == nil || ctx.Err() != nil || !normalNativeGenerationMaterializerV3(materializer) {
		return nil, ErrInvalidConfiguration
	}
	adapter, err := preparePreparedAdapter(session, materializer, config, stack)
	if err != nil {
		return nil, err
	}
	if err := stack.BindPreparedGenerationConnectionV3(ctx, session, mode); err != nil {
		return nil, errors.Join(ErrReconciliationRequired, err)
	}
	if _, err := adapter.readySnapshot(); err != nil {
		return nil, errors.Join(ErrReconciliationRequired, err)
	}
	adapter.generationStack = stack
	adapter.config.preparedReceipt.nativeGenerationV3 = true
	return adapter, nil
}

// Called only by the actual initial/idle-reconnect assembly, with a mode fixed
// by that path. Contextless New remains incapable of selecting this branch.
func newRuntimeAdapterV3(ctx context.Context, session SessionPort, materializer Materializer, config Config,
	mode attachedworkersession.ResourceGenerationConnectionModeV1) (*Adapter, error) {
	if config.preparedReceipt == nil || !config.preparedReceipt.nativeGenerationV3 {
		return New(session, materializer, config)
	}
	original, ok := session.(*attachedworkersession.Session)
	if !ok || original == nil {
		return nil, ErrInvalidAuthority
	}
	stack, err := config.preparedReceipt.preparedStack()
	if err != nil {
		return nil, err
	}
	config.ReceiptCandidate, config.preparedReceipt, config.preparedInlineV2 = nil, nil, false
	return NewNativeGenerationPreparedAdapterV3(ctx, original, materializer, config, stack, mode)
}

func normalNativeGenerationMaterializerV3(materializer Materializer) bool {
	actual, ok := materializer.(*BoundMaterializer)
	return ok && actual != nil && actual.source != nil && actual.nativeSubscription && !actual.allowTestProvider
}

// Identity checks happen before constructing a Connector or running lease-held
// preflight. A matching declaration or replacement interface cannot stand in
// for either original concrete owner. Reconnect callers supply NEW owners.
func validateNativeGenerationRuntimeV3(config attachedworkerstack.Config, exchange attachedworkersession.ExchangeFactory,
	credentials ports.CredentialLifecycle, materializer Materializer) error {
	if config.PreparedCodexNativeGeneration == nil {
		return nil
	}
	native := config.PreparedCodexNativeGeneration
	owner, ownerOK := exchange.(*attachedresourcegenerationsession.OwnerV1)
	lifecycle, lifecycleOK := credentials.(*ownerlocalcredential.Lifecycle)
	if !ownerOK || owner == nil || owner != native.GenerationOwner || !lifecycleOK || lifecycle == nil || lifecycle != native.Lifecycle ||
		!normalNativeGenerationMaterializerV3(materializer) {
		return ErrInvalidConfiguration
	}
	return nil
}

// This freezes complete installed data and exact invocation/capability metadata,
// not admission, an ACK or a qualification shortcut through the V2 profile.
func freezeNativeGenerationRuntimeV3(capability attachedworkerprotocol.CapabilityManifestV1, profile LocalProfileV1,
	configured *codexexec.NativePreparedConfigV3, credentials ports.CredentialLifecycle) (codexexec.NativePreparedConfigV3, error) {
	zero := codexexec.NativePreparedConfigV3{}
	lifecycle, ok := credentials.(*ownerlocalcredential.Lifecycle)
	if configured == nil || !ok || lifecycle == nil || lifecycle != configured.Lifecycle {
		return zero, ErrInvalidConfiguration
	}
	native, err := configured.Clone()
	if err != nil {
		return zero, ErrInvalidConfiguration
	}
	if native.InlineOutputV2 && (!slices.Contains(capability.Features, attachedworkerprotocol.FeatureInlineOutputV2) ||
		!slices.Contains(capability.Features, attachedworkerprotocol.FeatureOutputReceipt)) {
		return zero, ErrInvalidConfiguration
	}
	arguments, err := native.Profile.ArgumentsV3()
	if err != nil || !native.Profile.Backend.Enabled || profile.Executable != native.Profile.Backend.Executable ||
		profile.ExecutableDigest != native.Profile.Backend.ExecutableDigest || !slices.Equal(profile.Arguments, arguments) || len(profile.Environment) != 0 ||
		capability.OperatingSystem != native.Profile.OperatingSystem || capability.Architecture != native.Profile.Architecture ||
		capability.HarnessVersion != native.Profile.Backend.ExecutableVersion ||
		native.Baseline.CapabilityDigest != profile.CapabilityDigest || native.Baseline.WorkerID != domain.AttachedWorkerID(capability.WorkerID) ||
		native.Baseline.EnrollmentGeneration != capability.EnrollmentGeneration {
		return zero, ErrInvalidAuthority
	}
	return native, nil
}
