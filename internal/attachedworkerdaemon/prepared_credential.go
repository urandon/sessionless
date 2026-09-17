package attachedworkerdaemon

import "path/filepath"

// BindPreparedCredential grants one already-materialized invocation credential
// to an attempt without issuing, materializing, finalizing, or releasing it.
// The outer canonical worker retains lifecycle ownership of the credential.
func BindPreparedCredential(spec AttemptSpec, rootDir, authFile string) (AttemptSpec, error) {
	root, rootErr := validateDirectoryPath(rootDir)
	file, fileErr := validateDataFilePath(authFile)
	if rootErr != nil || fileErr != nil || root != rootDir || file != authFile ||
		filepath.Dir(file) != root || filepath.Base(file) != "auth.json" ||
		spec.credentialWriteFile != "" {
		return AttemptSpec{}, ErrSupervisorConfig
	}
	for _, existing := range spec.AdditionalReadRoots {
		if existing == root {
			return AttemptSpec{}, ErrSupervisorConfig
		}
	}
	bound := cloneAttemptSpec(spec)
	bound.AdditionalReadRoots = append(bound.AdditionalReadRoots, root)
	bound.credentialWriteFile = file
	return bound, nil
}
