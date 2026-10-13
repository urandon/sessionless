//go:build darwin || linux

package codexexec

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
)

type witnessTeardownLauncher struct {
	delegate         *fixtureIsolationLauncher
	entered, proceed chan struct{}
}

func (launcher *witnessTeardownLauncher) Profile() attachedworkerdaemon.IsolationProfile {
	return launcher.delegate.Profile()
}
func (launcher *witnessTeardownLauncher) Prepare(ctx context.Context, spec attachedworkerdaemon.LaunchSpec) (attachedworkerdaemon.IsolationBoundary, error) {
	boundary, err := launcher.delegate.Prepare(ctx, spec)
	if err != nil {
		return nil, err
	}
	return &witnessTeardownBoundary{IsolationBoundary: boundary, owner: launcher}, nil
}

type witnessTeardownBoundary struct {
	attachedworkerdaemon.IsolationBoundary
	owner *witnessTeardownLauncher
}

func (boundary *witnessTeardownBoundary) Release(ctx context.Context) error {
	err := boundary.IsolationBoundary.Release(ctx)
	close(boundary.owner.entered)
	select {
	case <-boundary.owner.proceed:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestPreparedWitnessAgreesWithRealSupervisorDeferredCancellation(t *testing.T) {
	for _, cancelWins := range []bool{true, false} {
		name := "finish wins"
		if cancelWins {
			name = "cancel wins after natural exit"
		}
		t.Run(name, func(t *testing.T) {
			fixture := writePreparedCodexFixture(t, "#!/bin/sh\nwhile IFS= read -r line; do :; done\nprintf '%s' '"+successfulJSONL+"'\n")
			request, _, driver, launcher, _, recording := preparedDriverFixture(t, fixture)
			gate := &witnessTeardownLauncher{delegate: launcher, entered: make(chan struct{}), proceed: make(chan struct{})}
			defer func() {
				select {
				case <-gate.proceed:
				default:
					close(gate.proceed)
				}
			}()
			supervisor, err := attachedworkerdaemon.NewSupervisor(attachedworkerdaemon.SupervisorConfig{ScratchRoot: canonicalTempDir(t), Launcher: gate,
				Timeout: 20 * time.Second, TerminationGrace: 5 * time.Second, MaxStdoutBytes: maxJSONLOutputBytes, MaxStderrBytes: maxProcessStderrBytesV1,
				AllowedEnvironmentNames: []string{CredentialHomeEnvironmentV1}, AllowedReadRoots: []string{filepath.Dir(request.CredentialMaterialization.RootDir)}})
			if err != nil {
				t.Fatal(err)
			}
			boundary := recording.delegate.(*PreparedProcessBoundaryV1)
			boundary.supervisor = supervisor
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			finished := make(chan error, 1)
			go func() { _, err := driver.Execute(ctx, request, &fixtureEventSink{}); finished <- err }()
			select {
			case <-gate.entered:
			case <-ctx.Done():
				t.Fatal("real natural-exit supervisor did not enter teardown")
			}
			if cancelWins {
				if err := driver.Cancel(context.Background(), executionIdentity(request)); err != nil {
					t.Fatalf("active cancellation after actual exit: %v", err)
				}
			}
			close(gate.proceed)
			var executeErr error
			select {
			case executeErr = <-finished:
			case <-ctx.Done():
				t.Fatal("real supervisor/driver did not settle")
			}
			observed := recording.snapshot()
			process, runErr := observed.Process, observed.Err
			if !observed.Returned {
				t.Fatalf("settled Execute has pending boundary observation: %+v", observed)
			}
			if process.original == nil || process.original.boundary != boundary || process.original.process.Cancelled != process.Cancelled || process.Cancelled != cancelWins ||
				!process.original.process.DescendantsReaped || !process.original.process.BoundaryReleased || !process.original.process.CleanupSucceeded {
				t.Fatalf("final-arbitrated same-run witness: process=%+v original_present=%t run_err=%v", process, process.original != nil, runErr)
			}
			if cancelWins && (executeErr == nil || !errors.Is(runErr, context.Canceled)) {
				t.Fatalf("cancel-winner execute=%v boundary=%v", executeErr, runErr)
			}
			if !cancelWins && executeErr != nil {
				t.Fatalf("finish-winner execute=%v", executeErr)
			}
			if err := boundary.Cancel(context.Background(), boundary.authority); !errors.Is(err, ErrProcessNotActive) {
				t.Fatalf("late cancel=%v want not-active", err)
			}
		})
	}
}
