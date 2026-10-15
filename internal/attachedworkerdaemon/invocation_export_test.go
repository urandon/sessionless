package attachedworkerdaemon

import (
	"context"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/ports"
)

// Test-only bridge preserves the actual original receiver comparison after
// moving its genuine-service integration test out of the daemon test package.
var APIHandleMatchesForTest = apiHandleMatches

var _ func(ports.ProviderInvocationCredentialV1, ports.ProviderCredentialIssueRequestV1) bool = APIHandleMatchesForTest

// ExerciseOriginalAPIRunnerForTest uses the existing synthetic parent/Source
// fixture, not native eligibility. It lets an external integration test supply
// a GENUINE credential service during original preparation, avoiding a test
// import cycle while retaining the real runner/Issue/Materialize/Release code.
func ExerciseOriginalAPIRunnerForTest(t *testing.T, prepare func(context.Context, *PreparedAPIDataV1) ports.ProviderResourceCredentialLifecycleV1,
	executor PreparedProcessRunner) (InvocationResult, error, []string) {
	t.Helper()
	inv, parent, _ := longLivedAPIOwnerFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Hour)
	t.Cleanup(cancel)
	life := &integrationAPILifecycle{}
	parent.mutate = func(data *PreparedAPIDataV1) { life.ProviderResourceCredentialLifecycleV1 = prepare(ctx, data) }
	runner, err := NewPreparedAPIInvocationRunner(executor, life, parent)
	if err != nil {
		t.Fatal(err)
	}
	result, err := runner.Run(ctx, inv)
	return result, err, append([]string(nil), parent.order...)
}

type integrationAPILifecycle struct {
	ports.ProviderResourceCredentialLifecycleV1
}

func (l *integrationAPILifecycle) FinalizeProviderCredentialFile(ctx context.Context, h ports.ProviderInvocationCredentialV1, p ports.ProviderCredentialMaterializationV1) error {
	return l.ProviderResourceCredentialLifecycleV1.(ports.ProviderCredentialImmutableFileFinalizerV1).FinalizeProviderCredentialFile(ctx, h, p)
}
