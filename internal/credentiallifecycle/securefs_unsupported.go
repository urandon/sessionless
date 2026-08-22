//go:build !darwin && !linux

package credentiallifecycle

import "gitcode.com/urandon/sessionless/internal/ports"

type secureCredentialFS struct{ rootPath string }

type pinnedMaterialization struct {
	public ports.CredentialMaterialization
}

func newSecureCredentialFS(string) (*secureCredentialFS, error) {
	return nil, ErrCredentialMaterialization
}

func (*secureCredentialFS) create([]byte, int64) (*pinnedMaterialization, error) {
	return nil, ErrCredentialMaterialization
}

func (*secureCredentialFS) read(*pinnedMaterialization, int64) ([]byte, error) {
	return nil, ErrCredentialMaterialization
}

func (*secureCredentialFS) cleanup(*pinnedMaterialization) error {
	return ErrCredentialMaterialization
}

func (*secureCredentialFS) close() error { return nil }
