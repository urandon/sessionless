package registrygc

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// fakeSharedDeploymentLock models the deployment-lock command's environment
// namespace. The one-element channel makes acquisition ordering observable in
// tests without timing assumptions or sleeps.
type fakeSharedDeploymentLock struct {
	environment string
	token       chan struct{}
}

func newFakeSharedDeploymentLock(environment string) *fakeSharedDeploymentLock {
	lock := &fakeSharedDeploymentLock{environment: environment, token: make(chan struct{}, 1)}
	lock.token <- struct{}{}
	return lock
}

func (lock *fakeSharedDeploymentLock) Lock(environment string) error {
	if environment != lock.environment {
		return fmt.Errorf("lock environment %q does not match %q", environment, lock.environment)
	}
	<-lock.token
	return nil
}

func (lock *fakeSharedDeploymentLock) Unlock() { lock.token <- struct{}{} }

type coordinatedRunResult struct {
	report Report
	err    error
}

func TestDeploymentAndGCShareLockEnvironment(t *testing.T) {
	t.Run("deployment promotes while GC waits and GC observes promoted digest", func(t *testing.T) {
		f := newGCFixture(t, ModeDryRun)
		lock := newFakeSharedDeploymentLock(f.inventory.LockEnvironment)
		promotedDigest := digest(210 + indexOf("control-api"))
		deploymentLocked := make(chan struct{})
		releaseDeployment := make(chan struct{})
		deploymentDone := make(chan error, 1)
		go func() {
			if err := lock.Lock(f.inventory.LockEnvironment); err != nil {
				deploymentDone <- err
				return
			}
			promoteControlBlue(&f, promotedDigest)
			close(deploymentLocked)
			<-releaseDeployment
			lock.Unlock()
			deploymentDone <- nil
		}()
		<-deploymentLocked

		gcAttempted := make(chan struct{})
		gcLocked := make(chan struct{})
		gcResult := make(chan coordinatedRunResult, 1)
		go func() {
			close(gcAttempted)
			if err := lock.Lock(f.inventory.LockEnvironment); err != nil {
				gcResult <- coordinatedRunResult{err: err}
				return
			}
			close(gcLocked)
			cloud := &fakeCloud{live: f.live, get: func(string) (CloudImage, error) {
				return CloudImage{}, fmt.Errorf("GetImage called during dry-run")
			}}
			report, err := Run(context.Background(), f.config, f.inventory, f.manifests, f.protected, cloud)
			lock.Unlock()
			gcResult <- coordinatedRunResult{report: report, err: err}
		}()
		<-gcAttempted
		assertChannelBlocked(t, gcLocked, "GC acquired the shared lock while deployment still held it")
		close(releaseDeployment)
		if err := <-deploymentDone; err != nil {
			t.Fatalf("deployment lock: %v", err)
		}
		<-gcLocked
		result := <-gcResult
		if result.err != nil {
			t.Fatalf("Run() after promotion error = %v", result.err)
		}
		decision := decisionFor(t, result.report, "control-api", promotedDigest)
		if decision.Decision != DecisionRetain || !hasReason(decision, "active_revision", "stable/blue") {
			t.Fatalf("promoted digest decision = %#v, want active stable retention", decision)
		}
	})

	t.Run("GC plans first and retains a fresh digest while deployment waits", func(t *testing.T) {
		f := newGCFixture(t, ModeDryRun)
		lock := newFakeSharedDeploymentLock(f.inventory.LockEnvironment)
		freshDigest := digest(210 + indexOf("control-api"))
		gcLocked := make(chan struct{})
		allowGCPlan := make(chan struct{})
		allowGCUnlock := make(chan struct{})
		gcResult := make(chan coordinatedRunResult, 1)
		go func() {
			if err := lock.Lock(f.inventory.LockEnvironment); err != nil {
				gcResult <- coordinatedRunResult{err: err}
				return
			}
			close(gcLocked)
			<-allowGCPlan
			cloud := &fakeCloud{live: f.live, get: func(string) (CloudImage, error) {
				return CloudImage{}, fmt.Errorf("GetImage called during dry-run")
			}}
			report, err := Run(context.Background(), f.config, f.inventory, f.manifests, f.protected, cloud)
			gcResult <- coordinatedRunResult{report: report, err: err}
			<-allowGCUnlock
			lock.Unlock()
		}()
		<-gcLocked

		deploymentAttempted := make(chan struct{})
		deploymentLocked := make(chan struct{})
		deploymentDone := make(chan error, 1)
		go func() {
			close(deploymentAttempted)
			if err := lock.Lock(f.inventory.LockEnvironment); err != nil {
				deploymentDone <- err
				return
			}
			promoteControlBlue(&f, freshDigest)
			close(deploymentLocked)
			lock.Unlock()
			deploymentDone <- nil
		}()
		<-deploymentAttempted
		assertChannelBlocked(t, deploymentLocked, "deployment acquired the shared lock while GC still held it")

		close(allowGCPlan)
		result := <-gcResult
		if result.err != nil {
			t.Fatalf("Run() before deployment error = %v", result.err)
		}
		decision := decisionFor(t, result.report, "control-api", freshDigest)
		if decision.Decision != DecisionRetain || !hasReason(decision, "safety_window", "48h0m0s") {
			t.Fatalf("fresh digest decision = %#v, want safety-window retention", decision)
		}
		assertChannelBlocked(t, deploymentLocked, "deployment acquired lock before GC released its completed plan")
		close(allowGCUnlock)
		<-deploymentLocked
		if err := <-deploymentDone; err != nil {
			t.Fatalf("deployment lock: %v", err)
		}
	})
}

func promoteControlBlue(f *gcFixture, promotedDigest string) {
	container := f.inventory.Containers["control-blue"]
	oldRevision := f.live.Containers[container.ContainerID].Revisions[0]
	container.RevisionID = "revisionpromoted"
	container.SourceSHA = sha(250)
	container.ImageRef = imageReference(container.Component, promotedDigest)
	f.inventory.Containers["control-blue"] = container

	liveContainer := f.live.Containers[container.ContainerID]
	liveContainer.Labels["source-commit"] = container.SourceSHA
	oldRevision.Status = "OBSOLETE"
	liveContainer.Revisions = []LiveRevision{
		oldRevision,
		{
			ID:          container.RevisionID,
			Status:      "ACTIVE",
			CreatedAt:   f.now,
			ImageURL:    container.ImageRef,
			ImageDigest: promotedDigest,
		},
	}
	f.live.Containers[container.ContainerID] = liveContainer
}

func assertChannelBlocked(t *testing.T, channel <-chan struct{}, failure string) {
	t.Helper()
	select {
	case <-channel:
		t.Fatal(failure)
	default:
	}
}

func hasReason(decision Decision, kind, referenceSubstring string) bool {
	for _, reason := range decision.Reasons {
		if reason.Kind == kind && strings.Contains(reason.Reference, referenceSubstring) {
			return true
		}
	}
	return false
}
