//go:build ydbintegration && (darwin || linux)

package ydbintegration

import (
	"context"
	"errors"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/codexexec"
)

// These are fixed native fixture pins, not an operator activation mode. The
// descriptor uses the actual pinned executable before Web admission/claim.
func aw133PreparedArguments() []string {
	return []string{"exec", "--json", "--ephemeral", "--ignore-user-config", "--ignore-rules", "--strict-config", "--sandbox", "read-only", "--skip-git-repo-check", "--color", "never", "--model", "gpt-fixture", "-"}
}

func aw133PreparedConfig(t *testing.T, path string, now func() time.Time) codexexec.Config {
	t.Helper()
	digest, err := attachedworkerdaemon.DigestExecutable(path)
	if err != nil {
		t.Fatal(err)
	}
	return codexexec.Config{Enabled: true, Executable: path, ExecutableDigest: digest, ExecutableVersion: "native-fixture-v1", Model: "gpt-fixture", Now: now}
}

// New is used only to calculate the original descriptor; this runner can
// never spawn or fabricate an observation. Actual execution uses normal Stack.
type aw133DescriptorOnlyRunner struct{}

func (aw133DescriptorOnlyRunner) Run(context.Context, attachedworkerdaemon.Invocation) (attachedworkerdaemon.InvocationResult, error) {
	return attachedworkerdaemon.InvocationResult{}, errors.New("descriptor-only fixture cannot execute")
}

// Real Web ingress/admission, current YDB acceptance, strict TLS sealed input,
// and the normal Stack join are used. The OCI engine/provider remain pinned
// test executables: this is not real platform, paid provider, or egress proof.
func TestWebAttachedPreparedCodexCanonicalReceipt(t *testing.T) {
	testJoinedProviderDaemonReceipts(t, true, true)
}
