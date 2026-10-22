//go:build darwin || linux

package codexexec

import (
	"context"
	"errors"
	"testing"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
)

type unselectedV3Launcher struct{}

func (unselectedV3Launcher) ProfileV3() attachedworkerdaemon.IsolationProfileV3 {
	return attachedworkerdaemon.IsolationProfileV3{Version: 3, Name: "unselected-v3-source-fixture", FilesystemReadBoundary: true, FilesystemWriteBoundary: true, ProcessBoundary: true, DiskBytesBounded: true, Network: attachedworkerdaemon.NativeConnectUnixV2}
}
func (unselectedV3Launcher) PrepareV3(context.Context, attachedworkerdaemon.LaunchEnvelopeV3) (attachedworkerdaemon.IsolationBoundaryV3, error) {
	return nil, errors.New("must not prepare")
}

// Reject cross-version composition at construction, not after the outer
// credential owner has already issued/materialized an execution credential.
func TestNativeV3SupervisorCannotEnterV1PreparedConstructor(t *testing.T) {
	f := newNativeConstructorFixture(t)
	supervisor, err := attachedworkerdaemon.NewSupervisorV3(attachedworkerdaemon.SupervisorConfigV3{Base: attachedworkerdaemon.SupervisorConfig{ScratchRoot: canonicalTempDir(t)}, Launcher: unselectedV3Launcher{}})
	if err != nil {
		t.Fatal(err)
	}
	f.config.Supervisor = supervisor
	if _, err := NewPreparedProcessBoundaryV1(f.config); !errors.Is(err, ErrContract) {
		t.Fatalf("V3 supervisor accepted by V1 constructor: %v", err)
	}
	if _, err := NewNativePreparedProcessBoundaryV2(t.Context(), f.config, f.session, f.input, f.bundle); !errors.Is(err, ErrContract) {
		t.Fatalf("V3 supervisor accepted by V2 constructor: %v", err)
	}
}
