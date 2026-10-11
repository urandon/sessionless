//go:build ydbintegration

package ydbintegration

import (
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroci"
)

// This contract check needs no database, Docker engine or executable. The
// production declarative validator must accept the same installation IDs that
// the native onboarding fixture feeds into its signed manifest.
func TestYDBFixtureInstallationIDsRespectCanonicalOCIContract(t *testing.T) {
	seen := make(map[string]bool)
	for range 64 {
		id := uniqueID("installation")
		if seen[id] {
			t.Fatal("process-local fixture counter reused an installation ID")
		}
		seen[id] = true
		config := attachedworkeroci.Config{
			DockerPath: "/private/synthetic/docker", DockerSHA256: strings.Repeat("a", 64),
			CLIConfigDir: "/private/synthetic/cli", Host: "unix:///run/user/1000/docker.sock",
			EngineID: "onboarding-test-engine", InstallationID: id,
			Boundary: attachedworkeroci.BoundaryLinuxRootless,
			Image:    "registry.example/worker@sha256:" + strings.Repeat("b", 64),
			UserID:   1000, GroupID: 1000, DiskBytes: 1 << 30, CredentialFileBytes: 1024,
			MemoryBytes: 64 << 20, PIDsLimit: 64, StopSeconds: 10,
		}
		if err := attachedworkeroci.ValidateDeclarativeConfig(config); err != nil {
			t.Fatalf("fixture installation ID violates unchanged canonical OCI configuration: %v", err)
		}
	}
}
