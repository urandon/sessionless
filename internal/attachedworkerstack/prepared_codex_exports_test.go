//go:build darwin || linux

package attachedworkerstack

import (
	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"testing"
)

// PreparedCodexFixtureForTest is compiled only into this package's test binary.
// It exposes one fixed admitted unit fixture to the external receipt test,
// never a production factory, authority producer or transport injection.
func PreparedCodexFixtureForTest(t *testing.T) (*Stack, attachedworkerdaemon.Invocation, attachedworkerprotocol.CapabilityManifestV1) {
	t.Helper()
	stack, invocation, _, _ := newPreparedStackFixture(t)
	return stack, invocation, preparedCapability(attachedworkerlocal.ManifestV1{WorkerID: stack.scope.worker, EnrollmentGeneration: stack.scope.enrollment,
		Harness: attachedworkerlocal.HarnessConfigV1{SHA256: fmtDigest(invocation.Process.ExecutableDigest[:])}})
}
