package credentiallifecycle

const providerRootPrefix = "sessionless-provider-credentials-"

// PinnedSecretFileSystem is the canonical local secret-file primitive shared
// by subscription and provider-resource credential lifecycles. Its private
// implementation pins directory descriptors and uses no-follow relative
// operations; callers receive only the path required by the isolated child.
type PinnedSecretFileSystem struct {
	filesystem *secureCredentialFS
	scratchFD  int
}

type PinnedSecretFile struct {
	RootDir  string
	FilePath string
	pinned   *pinnedMaterialization
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
	return &PinnedSecretFileSystem{filesystem: filesystem, scratchFD: scratchFD}, nil
}

func (filesystem *PinnedSecretFileSystem) Create(fileName string, secret []byte, maxBytes int64) (*PinnedSecretFile, error) {
	if filesystem == nil || filesystem.filesystem == nil {
		return nil, ErrCredentialMaterialization
	}
	pinned, err := filesystem.filesystem.createNamed(fileName, secret, maxBytes)
	if err != nil {
		return nil, err
	}
	return &PinnedSecretFile{RootDir: pinned.public.RootDir, FilePath: pinned.public.AuthFile, pinned: pinned}, nil
}

func (filesystem *PinnedSecretFileSystem) Cleanup(file *PinnedSecretFile) error {
	if filesystem == nil || filesystem.filesystem == nil || file == nil || file.pinned == nil || file.RootDir != file.pinned.public.RootDir || file.FilePath != file.pinned.public.AuthFile {
		return ErrCredentialMaterialization
	}
	return filesystem.filesystem.cleanup(file.pinned)
}

func (filesystem *PinnedSecretFileSystem) Close() error {
	if filesystem == nil || filesystem.filesystem == nil {
		return nil
	}
	closeErr := filesystem.filesystem.close()
	filesystem.filesystem = nil
	unlockErr := releaseProviderScratchRoot(filesystem.scratchFD)
	filesystem.scratchFD = -1
	if closeErr != nil || unlockErr != nil {
		return ErrCredentialMaterialization
	}
	return nil
}
