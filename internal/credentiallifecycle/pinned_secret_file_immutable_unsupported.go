//go:build !darwin && !linux

package credentiallifecycle

import (
	"context"
	"gitcode.com/urandon/sessionless/internal/domain"
)

type immutableNamespace struct{}

func (*PinnedSecretFileSystem) captureNamespace() error { return ErrCredentialMaterialization }
func (*PinnedSecretFileSystem) CreateImmutable(string, []byte, int64) (*PinnedSecretFile, error) {
	return nil, ErrCredentialMaterialization
}
func (*PinnedSecretFileSystem) VerifyImmutable(context.Context, *PinnedSecretFile, domain.CredentialFingerprint, int64) error {
	return ErrCredentialMaterialization
}
func (*PinnedSecretFileSystem) closedResult(*pinnedMaterialization) error {
	return ErrCredentialMaterialization
}
func (*PinnedSecretFileSystem) drain(*pinnedMaterialization) error {
	return ErrCredentialMaterialization
}
