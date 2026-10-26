package attachedworkersession

import (
	"context"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
)

type ResourceGenerationConnectionModeV1 uint8

const (
	ResourceGenerationInitialV1   ResourceGenerationConnectionModeV1 = 1
	ResourceGenerationReconnectV1 ResourceGenerationConnectionModeV1 = 2
)

// CheckResourceGenerationConnectionV1 checks the ORIGINAL connected owner, not
// a copied Session or readiness snapshot. This is metadata currentness only:
// it does not claim an attempt, renew a deadline or authorize execution.
// The generation owner passes its installed baseline's original expiry for
// positive eligibility; nil permits identity-only readback/revocation checks.
// Revocation publication must not depend on positive evidence remaining valid.
func (session *Session) CheckResourceGenerationConnectionV1(ctx context.Context, expected ConnectionBindingV1,
	mode ResourceGenerationConnectionModeV1, positiveEvidenceExpiresAt *time.Time,
) error {
	if session == nil || ctx == nil || ctx.Err() != nil || !validBinding(expected) ||
		mode != ResourceGenerationInitialV1 && mode != ResourceGenerationReconnectV1 {
		return ErrInvalidAuthority
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.nativeIdentity != session || session.state != StateReady || session.machine == nil ||
		session.exchange == nil || session.lease == nil || session.now == nil || session.binding != expected ||
		session.nativeFresh != (mode == ResourceGenerationInitialV1) || ctx.Err() != nil {
		return ErrInvalidAuthority
	}
	now := session.now().UTC()
	if now.IsZero() || !now.Before(session.binding.AuthenticationExpires) ||
		positiveEvidenceExpiresAt != nil && !now.Before(*positiveEvidenceExpiresAt) {
		return ErrInvalidAuthority
	}
	current, err := session.machine.Snapshot()
	if err != nil || current.Connection != attachedworkerprotocol.ConnectionReady || current.Revoke != nil {
		return ErrInvalidAuthority
	}
	return nil
}
