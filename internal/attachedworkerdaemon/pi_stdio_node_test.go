//go:build darwin || linux

package attachedworkerdaemon

import (
	"context"
	"crypto/sha256"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/pisdkphasewire"
)

// Real local Node/stdin/stdout through SAME actual Supervisor.Run. Synthetic
// profile/Source/opaque Init, no Pi SDK, credentials, provider or platform claim.
func TestPiStdioNodeSameSupervisor(t *testing.T) {
	// The prerequisite probe itself must not execute ambient preloads before
	// the actual Supervisor's replacement environment has been established.
	t.Setenv("NODE_OPTIONS", "--public-invalid-preload-sentinel")
	node, err := exec.LookPath("node")
	if err != nil {
		t.Fatal("pinned Node prerequisite missing:", err)
	}
	node, err = filepath.EvalSymlinks(node)
	if err != nil {
		t.Fatal(err)
	}
	checkCtx, cancelCheck := context.WithTimeout(t.Context(), 30*time.Second)
	t.Cleanup(cancelCheck)
	probe := exec.CommandContext(checkCtx, node, "--version")
	probe.Env = []string{}
	version, err := probe.Output()
	if err != nil || strings.TrimSpace(string(version)) != "v24.19.0" {
		t.Fatalf("Node prerequisite version=%q err=%v, want v24.19.0", version, err)
	}
	sourceRoot, err := filepath.Abs("../pisdktransport")
	if err != nil {
		t.Fatal(err)
	}
	sourceRoot, err = filepath.EvalSymlinks(sourceRoot)
	if err != nil {
		t.Fatal(err)
	}
	// Existing supervisor policy allows only strict descendants of an approved
	// root. Stage exactly two test-owned files, not the repository parent or a
	// new broad host read allowance; Node receives only this child directory.
	staged := newCanonicalTempDir(t)
	assetRoot := filepath.Join(staged, "modules")
	if err := os.Mkdir(assetRoot, 0700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"stdio.mjs", "stdio_process_fixture.mjs"} {
		b, err := os.ReadFile(filepath.Join(sourceRoot, name))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(assetRoot, name), b, 0600); err != nil {
			t.Fatal(err)
		}
	}
	digest, err := DigestExecutable(node)
	if err != nil {
		t.Fatal(err)
	}
	s, _, _, _ := newFixtureSupervisor(t, SupervisorConfig{TerminationGrace: maxSupervisorGrace})
	s.timeout = time.Hour // non-expiry resources outlive the enclosing suite
	s.allowedReadRoots = []string{staged}
	p := apiOwnerProfile(t, digest)
	desc, err := p.Descriptor()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	t.Cleanup(cancel)
	sendCtx, cancelSend := context.WithCancel(ctx)
	t.Cleanup(cancelSend)
	var calls atomic.Int32
	send := &preparedAPIPhysicalSend{ctx: sendCtx, ownerCtx: ctx,
		body: []byte(piSyntheticRequest), profile: desc.BackendProfileDigest,
		authorize: func() (time.Time, error) { calls.Add(1); return time.Now().Add(30 * time.Minute), nil }}
	send.cancel.Store(&preparedAPICancel{cancel: cancelSend})
	ledger := &apiTestLedger{}
	plan, err := s.PreparePiStdioV1(p, ledger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := plan.Close(); err != nil {
			t.Error("plan close:", err)
		}
	})
	in := PreparedInvocationV1{APIRequest: &PreparedAPIRequestV1{Profile: p, Body: []byte(piSyntheticRequest)},
		PhysicalSend: send, Ledger: ledger, apiIssueStarted: time.Now(),
		Process: AttemptSpec{Executable: node, ExecutableDigest: digest,
			Arguments:           []string{"--permission", "--allow-fs-read=" + assetRoot, "--no-addons", "--disable-proto=throw", filepath.Join(assetRoot, "stdio_process_fixture.mjs")},
			AdditionalReadRoots: []string{assetRoot}, Stdin: []byte(desc.BackendProfileDigest)}}
	result, err := s.RunPreparedPi(ctx, in, plan)
	if err != nil || result.ExitCode != 0 || result.FailureCode != "" || !result.CleanupSucceeded || !result.DescendantsReaped || !result.BoundaryReleased {
		t.Fatalf("same Node supervisor result=%+v err=%v", result, err)
	}
	obs, ok := result.PiStdioObservationV1()
	if !ok || string(obs.Final) != piSyntheticFinal || calls.Load() != 1 || len(result.Stdout) != 0 || !send.retired.Load() {
		t.Fatalf("Node observation ok=%v calls=%d result=%+v", ok, calls.Load(), result)
	}
	request, err := pisdkphasewire.EncodeFrame(pisdkphasewire.Frame{Kind: pisdkphasewire.PhysicalRequest, BackendProfileDigest: desc.BackendProfileDigest, Body: []byte(piSyntheticRequest)}, plan.limits)
	if err != nil {
		t.Fatal(err)
	}
	final, err := pisdkphasewire.EncodeFrame(pisdkphasewire.Frame{Kind: pisdkphasewire.StructuredFinal, Body: []byte(piSyntheticFinal)}, plan.limits)
	if err != nil {
		t.Fatal(err)
	}
	if obs.StdoutDigest != sha256.Sum256(append(request, final...)) || obs.StdoutDigest == obs.FinalDigest {
		t.Fatal("actual Node stdout digest differs from canonical Go wire")
	}
}
