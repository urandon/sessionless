package attachedworkerlocal

import (
	"testing"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroci"
)

func TestNativeBoundaryV3IsDisjointFromV1UIDContract(t *testing.T) {
	_, manifest, _ := newFixture(t)
	original := manifest.OCI
	manifest.OCI.UserID, manifest.OCI.GroupID = 0, 0
	if err := manifest.Validate(); err == nil {
		t.Fatal("old boundary accepted namespace root")
	}
	manifest.OCI.Boundary = BoundaryLinuxRootlessNativeV3
	if err := manifest.Validate(); err != nil {
		t.Fatalf("explicit native namespace mapping denied: %v", err)
	}
	if toLauncherConfig(manifest.OCI).Boundary != attachedworkeroci.BoundaryKind(BoundaryLinuxRootlessNativeV3) {
		t.Fatal("generic conversion silently normalized native boundary into V1")
	}
	if err := attachedworkeroci.ValidateDeclarativeConfig(toLauncherConfig(manifest.OCI)); err == nil {
		t.Fatal("ordinary launcher accepted native installed contract")
	}
	for _, pair := range [][2]uint32{{1000, 0}, {0, 1000}, {1000, 1000}} {
		manifest.OCI.UserID, manifest.OCI.GroupID = pair[0], pair[1]
		if err := manifest.Validate(); err == nil {
			t.Errorf("native boundary accepted noncanonical UID/GID %v", pair)
		}
	}
	manifest.OCI = original
	if err := manifest.Validate(); err != nil {
		t.Fatalf("original V1 contract regressed: %v", err)
	}
	manifest.OCI.Boundary = "unknown-native"
	if err := manifest.Validate(); err == nil {
		t.Fatal("unknown variant accepted")
	}
}
