package codexexec

import (
	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/domain"
)

// NewPreparedAttachedWorkerDriverV1 is the exact outer composition for one
// already accepted attached-worker attempt. It remains disabled whenever
// Config.Enabled is false, but its immutable authority snapshot and prepared
// process boundaries are complete and testable without central-store access.
func NewPreparedAttachedWorkerDriverV1(
	config Config,
	attempt domain.AttachedWorkerAttemptV1,
	resource domain.ProviderResourceBindingV1,
	supervisor *attachedworkerdaemon.Supervisor,
) (*Driver, error) {
	resolver, err := NewAcceptedAuthorityResolverV1(attempt, resource, config.Now)
	if err != nil {
		return nil, err
	}
	boundary, err := NewPreparedProcessBoundaryV1(PreparedProcessBoundaryConfigV1{
		Supervisor: supervisor, Executable: config.Executable,
		ExecutableDigest: config.ExecutableDigest,
		Arguments:        processArguments(config.Model),
	})
	if err != nil {
		return nil, err
	}
	return NewDriver(config, resolver, boundary)
}
