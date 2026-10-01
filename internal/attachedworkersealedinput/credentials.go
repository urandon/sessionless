package attachedworkersealedinput

import (
	"context"

	"gitcode.com/urandon/sessionless/internal/ports"
)

// DeniedCredentialLifecycle is the only lifecycle supported by the synthetic
// sealed-input path. It satisfies the pinned stack's required port while
// making any unexpected provider-credential request fail closed. A real
// provider lifecycle requires a separate reviewed composition.
type DeniedCredentialLifecycle struct{}

func (DeniedCredentialLifecycle) Issue(context.Context, ports.CredentialIssueRequest) (ports.CredentialHandle, error) {
	return ports.CredentialHandle{}, ErrUnsupported
}

func (DeniedCredentialLifecycle) Materialize(context.Context, ports.CredentialHandle) (ports.CredentialMaterialization, error) {
	return ports.CredentialMaterialization{}, ErrUnsupported
}

func (DeniedCredentialLifecycle) WriteBack(context.Context, ports.CredentialHandle, ports.CredentialMaterialization) (ports.CredentialWriteBackResult, error) {
	return ports.CredentialWriteBackResult{}, ErrUnsupported
}

func (DeniedCredentialLifecycle) Release(context.Context, ports.CredentialHandle) error {
	return ErrUnsupported
}

func (DeniedCredentialLifecycle) RevokeConnection(context.Context, ports.CredentialRevokeRequest) error {
	return ErrUnsupported
}

var _ ports.CredentialLifecycle = DeniedCredentialLifecycle{}
