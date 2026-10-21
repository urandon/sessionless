package codexexec

import (
	"crypto/sha256"
	"io"
	"os"
	"path/filepath"
	"runtime"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
)

// The companion is Go code in this host process, not a separately invoked
// bundle member. Linux binds the actual running inode through /proc/self/exe.
// Other platforms support source fixtures only, not the Linux serving profile.
func openNativeCompanionHost(expected attachedworkerdaemon.NativeArtifactV2) (*os.File, os.FileInfo, error) {
	path, err := os.Executable()
	if err != nil {
		return nil, nil, ErrContract
	}
	path, err = filepath.EvalSymlinks(path)
	if err != nil || path != expected.Executable {
		return nil, nil, ErrContract
	}
	locator := path
	if runtime.GOOS == "linux" {
		locator = "/proc/self/exe"
	}
	running, err := os.Open(locator)
	if err != nil {
		return nil, nil, ErrContract
	}
	fail := func() (*os.File, os.FileInfo, error) {
		_ = running.Close()
		return nil, nil, ErrContract
	}
	identity, err := running.Stat()
	if err != nil || !identity.Mode().IsRegular() || identity.Mode().Perm()&0111 == 0 {
		return fail()
	}
	// Reject symlink locators and same-byte copies: the admitted file must be
	// the original host inode, not merely content with a matching hash.
	named, err := os.Lstat(path)
	if err != nil || !sameNativeCompanionHost(identity, named) {
		return fail()
	}
	admitted, err := os.Open(path)
	if err != nil {
		return fail()
	}
	opened, statErr := admitted.Stat()
	closeErr := admitted.Close()
	if statErr != nil || closeErr != nil || !sameNativeCompanionHost(identity, opened) {
		return fail()
	}
	return running, identity, nil
}

func sameNativeCompanionHost(a, b os.FileInfo) bool {
	return a != nil && b != nil && a.Mode().IsRegular() && b.Mode().IsRegular() && os.SameFile(a, b)
}

func verifyNativeCompanionHost(expected attachedworkerdaemon.NativeArtifactV2) (os.FileInfo, error) {
	running, identity, err := openNativeCompanionHost(expected)
	if err != nil {
		return nil, err
	}
	// Hash the trusted opened running executable, never a later caller-path
	// lookup. No host argv or credentials enter the policy envelope.
	hash := sha256.New()
	n, readErr := io.Copy(hash, io.LimitReader(running, identity.Size()+1))
	closeErr := running.Close()
	var actual attachedworkerdaemon.ExecutableDigest
	copy(actual[:], hash.Sum(nil))
	if readErr != nil || closeErr != nil || n != identity.Size() || actual != expected.Digest {
		return nil, ErrContract
	}
	return identity, nil
}

func nativeCompanionHostCurrent(expected attachedworkerdaemon.NativeArtifactV2, captured os.FileInfo) bool {
	running, identity, err := openNativeCompanionHost(expected)
	if err != nil {
		return false
	}
	closeErr := running.Close()
	return closeErr == nil && sameNativeCompanionHost(captured, identity)
}
