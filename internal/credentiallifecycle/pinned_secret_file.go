package credentiallifecycle

import "sync"

const providerRootPrefix = "sessionless-provider-credentials-"

// PinnedSecretFileSystem is the canonical local secret-file primitive shared
// by subscription and provider-resource credential lifecycles. Its private
// implementation pins directory descriptors and uses no-follow relative
// operations; callers receive only the path required by the isolated child.
type PinnedSecretFileSystem struct {
	// Lock order is wrapper mu, then pinnedMaterialization.mu. All namespace
	// registration, verification, cleanup and descriptor draining uses this order.
	mu         sync.Mutex
	filesystem *secureCredentialFS
	scratchFD  int
	owned      map[*pinnedMaterialization]struct{}
	identity   *immutableNamespace
}

type PinnedSecretFile struct {
	RootDir  string
	FilePath string
	pinned   *pinnedMaterialization
	owner    *PinnedSecretFileSystem
}

func NewPinnedSecretFileSystem(canonicalScratchRoot string) (*PinnedSecretFileSystem, error) {
	scratchFD, err := acquireProviderScratchRoot(canonicalScratchRoot)
	if err != nil {
		return nil, err
	}
	filesystem, err := newSecureCredentialFSWithPrefix(canonicalScratchRoot, providerRootPrefix)
	if err != nil {
		_ = releaseProviderScratchRoot(scratchFD)
		return nil, err
	}
	wrapper := &PinnedSecretFileSystem{filesystem: filesystem, scratchFD: scratchFD, owned: make(map[*pinnedMaterialization]struct{})}
	if err := wrapper.captureNamespace(); err != nil {
		_ = wrapper.Close()
		return nil, err
	}
	return wrapper, nil
}

func (filesystem *PinnedSecretFileSystem) Create(fileName string, secret []byte, maxBytes int64) (*PinnedSecretFile, error) {
	if filesystem == nil {
		return nil, ErrCredentialMaterialization
	}
	filesystem.mu.Lock()
	defer filesystem.mu.Unlock()
	if filesystem.filesystem == nil {
		return nil, ErrCredentialMaterialization
	}
	pinned, err := filesystem.filesystem.createNamedMode(fileName, secret, maxBytes, false, filesystem.owned)
	if err != nil {
		return nil, err
	}
	return &PinnedSecretFile{RootDir: pinned.public.RootDir, FilePath: pinned.public.AuthFile, pinned: pinned, owner: filesystem}, nil
}

func (filesystem *PinnedSecretFileSystem) Cleanup(file *PinnedSecretFile) error {
	if filesystem == nil {
		return ErrCredentialMaterialization
	}
	filesystem.mu.Lock()
	defer filesystem.mu.Unlock()
	if file == nil || file.owner != filesystem || file.pinned == nil || file.RootDir != file.pinned.public.RootDir || file.FilePath != file.pinned.public.AuthFile {
		return ErrCredentialMaterialization
	}
	if _, ok := filesystem.owned[file.pinned]; !ok {
		return filesystem.closedResult(file.pinned)
	}
	if filesystem.filesystem == nil {
		return filesystem.closedResult(file.pinned)
	}
	err := filesystem.filesystem.cleanup(file.pinned)
	if err == nil {
		delete(filesystem.owned, file.pinned)
	}
	return err
}

func (filesystem *PinnedSecretFileSystem) Close() error {
	if filesystem == nil {
		return nil
	}
	filesystem.mu.Lock()
	defer filesystem.mu.Unlock()
	if filesystem.filesystem == nil {
		return nil
	}
	var drainErr error
	for pinned := range filesystem.owned {
		if filesystem.drain(pinned) != nil {
			drainErr = ErrCredentialMaterialization
		}
	}
	closeErr := filesystem.filesystem.close()
	filesystem.filesystem = nil
	unlockErr := releaseProviderScratchRoot(filesystem.scratchFD)
	filesystem.scratchFD = -1
	if drainErr != nil || closeErr != nil || unlockErr != nil {
		return ErrCredentialMaterialization
	}
	return nil
}
