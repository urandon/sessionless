package attachedworkerstack

import (
	"context"
	"errors"

	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// NewNativeGenerationV3 explicitly selects generation-aware native semantics.
// It verifies the installed stack but performs no metadata request, connection
// activation, credential Issue or provider operation.
func NewNativeGenerationV3(ctx context.Context, manifest attachedworkerlocal.ManifestV1, config Config, credentials ports.CredentialLifecycle) (*Stack, error) {
	return newNativeGenerationStackV3(ctx, manifest, config, credentials, defaultNativeDependenciesV3())
}

func newNativeGenerationStackV3(ctx context.Context, manifest attachedworkerlocal.ManifestV1, config Config, credentials ports.CredentialLifecycle, deps nativeDependenciesV3) (*Stack, error) {
	return newNativeStackCompositionV3(ctx, manifest, config, credentials, deps, nativeStackGenerationV3)
}

// BindPreparedGenerationConnectionV3 joins the exact configured metadata owner,
// original connected Session and sole prepared runner. The closed connection
// mode comes from the actual initial/idle-reconnect runtime path, not a grant.
// An attempted bind is one-shot, including a failed or cancelled metadata call.
func (stack *Stack) BindPreparedGenerationConnectionV3(ctx context.Context, session *attachedworkersession.Session, mode attachedworkersession.ResourceGenerationConnectionModeV1) error {
	if stack == nil || ctx == nil || session == nil || ctx.Err() != nil ||
		mode != attachedworkersession.ResourceGenerationInitialV1 && mode != attachedworkersession.ResourceGenerationReconnectV1 {
		return ErrInvalidConfiguration
	}
	stack.mu.Lock()
	if stack.closed || stack.prepared == nil || stack.nativeGeneration == nil || stack.generationBindAttempted || stack.connection != nil {
		stack.mu.Unlock()
		return ErrReconciliationRequired
	}
	stack.generationBindAttempted = true
	configured := stack.nativeGeneration
	snapshot := session.Snapshot()
	if !stack.validPreparedConnection(snapshot) || snapshot.CapabilityDigest != configured.Baseline.CapabilityDigest {
		stack.mu.Unlock()
		return ErrReconciliationRequired
	}
	stack.mu.Unlock()

	// Metadata requests must not hold the stack lock: Close remains able to
	// fence execution while the original Session owns cancellation/retirement.
	var err error
	if mode == attachedworkersession.ResourceGenerationInitialV1 {
		err = configured.GenerationOwner.BindInitialSessionV1(ctx, session)
	} else {
		err = configured.GenerationOwner.BindReconnectedSessionV1(ctx, session)
	}
	if err == nil {
		err = configured.GenerationOwner.SynchronizeV1(ctx)
	}
	if err != nil {
		return errors.Join(ErrReconciliationRequired, err)
	}
	stack.mu.Lock()
	defer stack.mu.Unlock()
	current := session.Snapshot()
	if ctx.Err() != nil || stack.closed || stack.nativeGeneration != configured || stack.connection != nil ||
		!stack.validPreparedConnection(current) || current.ConnectionID != snapshot.ConnectionID || current.CapabilityDigest != snapshot.CapabilityDigest {
		return ErrReconciliationRequired
	}
	if err := stack.prepared.BindNativeSessionV3(session); err != nil {
		return errors.Join(ErrReconciliationRequired, err)
	}
	stack.connection = session
	stack.boundID, stack.boundCapability = current.ConnectionID, current.CapabilityDigest
	return nil
}

// SynchronizePreparedGenerationV3 is only a fresh-poll metadata fence, never a
// grant for launch, credential use or an old result. It is deliberately absent
// from receipt/finalizer/terminal paths. A failed synchronization leaves this
// stack fenced; a new original connection requires a new stack/metadata owner.
func (stack *Stack) SynchronizePreparedGenerationV3(ctx context.Context, session *attachedworkersession.Session) error {
	if stack == nil || ctx == nil || session == nil || ctx.Err() != nil {
		return ErrInvalidConfiguration
	}
	stack.mu.Lock()
	if stack.closed || stack.nativeGeneration == nil || stack.prepared == nil || stack.connection != session ||
		!stack.currentGenerationConnectionV3(session) {
		stack.mu.Unlock()
		return ErrReconciliationRequired
	}
	configured := stack.nativeGeneration
	stack.mu.Unlock()

	err := configured.GenerationOwner.SynchronizeV1(ctx)
	stack.mu.Lock()
	defer stack.mu.Unlock()
	if err != nil || ctx.Err() != nil || stack.closed || stack.nativeGeneration != configured || stack.connection != session ||
		!stack.currentGenerationConnectionV3(session) {
		// Preserve the prepared result/receipt owner. Only new-input authority
		// is removed; no metadata failure can delete or replace an old slot.
		stack.connection = nil
		return errors.Join(ErrReconciliationRequired, err)
	}
	return nil
}

// Called under Stack.mu; the pointer is checked separately from its snapshot.
func (stack *Stack) currentGenerationConnectionV3(session *attachedworkersession.Session) bool {
	snapshot := session.Snapshot()
	return stack.validPreparedConnection(snapshot) && snapshot.ConnectionID == stack.boundID && snapshot.CapabilityDigest == stack.boundCapability
}
