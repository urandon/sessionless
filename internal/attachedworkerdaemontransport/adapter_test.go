package attachedworkerdaemontransport

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
	"gitcode.com/urandon/sessionless/internal/domain"
)

var adapterTestTime = time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)

func TestAdapterMaterializesOnlyAcceptedAuthorityAndCommitsTypedTerminal(t *testing.T) {
	fixture := newAdapterFixture(t)
	invocation, available, err := fixture.adapter.Next(context.Background())
	if err != nil || !available {
		t.Fatalf("Next available=%t error=%v", available, err)
	}
	if invocation.Identity != fixture.identity() {
		t.Errorf("identity=%+v want=%+v", invocation.Identity, fixture.identity())
	}
	if invocation.Process.Executable != fixture.config.Profile.Executable ||
		invocation.Process.ExecutableDigest != fixture.config.Profile.ExecutableDigest ||
		fmt.Sprint(invocation.Process.Arguments) != fmt.Sprint(fixture.config.Profile.Arguments) ||
		fmt.Sprint(invocation.Process.Environment) != fmt.Sprint(fixture.config.Profile.Environment) {
		t.Errorf("process configuration escaped local profile: %+v", invocation.Process)
	}
	if !bytes.Equal(invocation.Process.Stdin, []byte("bounded canonical input")) ||
		len(invocation.Process.AdditionalReadRoots) != 1 || invocation.Process.AdditionalReadRoots[0] != fixture.readRoot {
		t.Errorf("materialized input=%+v", invocation.Process)
	}
	if fixture.materializer.calls != 1 || fixture.materializer.request.AttemptSequence != 1 ||
		!sameAttemptBinding(fixture.materializer.request.Attempt, fixture.binding) ||
		fixture.materializer.request.ConnectionID != fixture.snapshot.ConnectionID {
		t.Errorf("materialization request=%+v calls=%d", fixture.materializer.request, fixture.materializer.calls)
	}
	if len(fixture.session.actions) != 2 || fixture.session.actions[0].Heartbeat == nil || fixture.session.actions[1].LeaseClaim == nil {
		t.Fatalf("pre-run actions=%+v", fixture.session.actions)
	}

	result := successfulResult()
	result.Process.Stdout = []byte("secret stdout A")
	evidenceA, err := terminalEvidenceDigest(
		materializationRequest(fixture.snapshot, fixture.binding, 1), invocation.Identity, result, nil,
		attachedworkerprotocol.TerminalSucceeded, attachedworkerprotocol.TerminalResultCompleted,
	)
	if err != nil {
		t.Fatal(err)
	}
	resultB := result
	resultB.Process.Stdout = []byte("secret stdout B")
	evidenceB, err := terminalEvidenceDigest(
		materializationRequest(fixture.snapshot, fixture.binding, 1), invocation.Identity, resultB, nil,
		attachedworkerprotocol.TerminalSucceeded, attachedworkerprotocol.TerminalResultCompleted,
	)
	if err != nil || !bytes.Equal(evidenceA, evidenceB) {
		t.Fatalf("stdout changed typed terminal evidence: equal=%t error=%v", bytes.Equal(evidenceA, evidenceB), err)
	}
	if err := fixture.adapter.Complete(context.Background(), invocation.Identity, result, nil); err != nil {
		t.Fatalf("Complete error=%v", err)
	}
	if len(fixture.session.actions) != 3 || fixture.session.actions[2].Terminal == nil {
		t.Fatalf("terminal actions=%+v", fixture.session.actions)
	}
	terminal := fixture.session.actions[2].Terminal
	if terminal.Status != attachedworkerprotocol.TerminalSucceeded || terminal.Result != attachedworkerprotocol.TerminalResultCompleted ||
		terminal.AttemptSequence != 2 || terminal.TerminalSequence != 1 || len(terminal.EvidenceDigest) != sha256.Size {
		t.Errorf("terminal=%+v", terminal)
	}
	if !bytes.Equal(terminal.EvidenceDigest, evidenceA) {
		t.Fatal("terminal evidence differs from content-free typed evidence")
	}

	// Stdout content is intentionally excluded from both evidence and the
	// idempotency fingerprint. The bounded byte count remains authoritative.
	result.Process.Stdout = []byte("secret stdout B")
	if err := fixture.adapter.Complete(context.Background(), invocation.Identity, result, nil); err != nil {
		t.Fatalf("duplicate Complete error=%v", err)
	}
	if len(fixture.session.actions) != 3 {
		t.Fatalf("duplicate completion crossed session actions=%d", len(fixture.session.actions))
	}
	result.Process.StderrBytes++
	if err := fixture.adapter.Complete(context.Background(), invocation.Identity, result, nil); !errors.Is(err, ErrAttemptUnavailable) {
		t.Fatalf("divergent duplicate error=%v", err)
	}
	assertAdapterFormattingIsSecretFree(t, fixture.adapter, fixture.config.Profile, fixture.materializer.result)
}

func TestAdapterFailsClosedBeforeMaterialization(t *testing.T) {
	t.Run("snapshot version", func(t *testing.T) {
		fixture := newAdapterFixture(t)
		fixture.session.snapshot.Version++
		if _, _, err := fixture.adapter.Next(context.Background()); !errors.Is(err, ErrSessionFenced) {
			t.Fatalf("error=%v", err)
		}
		if len(fixture.session.actions) != 0 || fixture.materializer.calls != 0 {
			t.Fatalf("invalid snapshot crossed boundary actions=%d materialize=%d", len(fixture.session.actions), fixture.materializer.calls)
		}
	})

	t.Run("offer exceeds authentication", func(t *testing.T) {
		fixture := newAdapterFixture(t)
		fixture.binding.ExpiresAtUnixMicro = fixture.snapshot.AuthenticationExpires.Add(time.Microsecond).UnixMicro()
		fixture.session.binding.ExpiresAtUnixMicro = fixture.binding.ExpiresAtUnixMicro
		if _, _, err := fixture.adapter.Next(context.Background()); !errors.Is(err, ErrInvalidAuthority) {
			t.Fatalf("error=%v", err)
		}
		if len(fixture.session.actions) != 1 || fixture.materializer.calls != 0 {
			t.Fatalf("invalid lease crossed claim/materializer actions=%d materialize=%d", len(fixture.session.actions), fixture.materializer.calls)
		}
	})

	t.Run("ambiguous claim", func(t *testing.T) {
		fixture := newAdapterFixture(t)
		fixture.session.claimErr = attachedworkersession.ErrReconciliationRequired
		if _, _, err := fixture.adapter.Next(context.Background()); !errors.Is(err, ErrReconciliationRequired) {
			t.Fatalf("error=%v", err)
		}
		if len(fixture.session.actions) != 2 || fixture.materializer.calls != 0 {
			t.Fatalf("ambiguous claim actions=%d materialize=%d", len(fixture.session.actions), fixture.materializer.calls)
		}
	})

	t.Run("caller cancellation remains observable", func(t *testing.T) {
		fixture := newAdapterFixture(t)
		fixture.session.heartbeatErr = context.Canceled
		if _, _, err := fixture.adapter.Next(context.Background()); !errors.Is(err, context.Canceled) {
			t.Fatalf("error=%v", err)
		}
	})

	t.Run("profile capability mismatch", func(t *testing.T) {
		fixture := newAdapterFixture(t)
		fixture.session.snapshot.CapabilityDigest = domain.DigestAttachedWorkerCapability([]byte("different capability"))
		if _, _, err := fixture.adapter.Next(context.Background()); !errors.Is(err, ErrInvalidAuthority) {
			t.Fatalf("error=%v", err)
		}
		if len(fixture.session.actions) != 0 || fixture.materializer.calls != 0 {
			t.Fatalf("profile mismatch crossed boundary actions=%d materialize=%d", len(fixture.session.actions), fixture.materializer.calls)
		}
	})
}

func TestAdapterReportsInvalidMaterializationWithoutRunning(t *testing.T) {
	fixture := newAdapterFixture(t)
	outside := canonicalTempDir(t)
	link := filepath.Join(fixture.config.MaterializationRoot, "escape")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	fixture.materializer.result.ReadRoots = []string{link}
	if _, available, err := fixture.adapter.Next(context.Background()); !errors.Is(err, ErrMaterializationFailed) || available {
		t.Fatalf("Next available=%t error=%v", available, err)
	}
	if fixture.materializer.calls != 1 || len(fixture.session.actions) != 3 || fixture.session.actions[2].Terminal == nil {
		t.Fatalf("materialize=%d actions=%+v", fixture.materializer.calls, fixture.session.actions)
	}
	terminal := fixture.session.actions[2].Terminal
	if terminal.Status != attachedworkerprotocol.TerminalFailed || terminal.Result != attachedworkerprotocol.TerminalResultFailed ||
		terminal.AttemptSequence != 2 {
		t.Fatalf("failure terminal=%+v", terminal)
	}
}

func TestAdapterBoundsMaterializedInputAndPreservesCancellation(t *testing.T) {
	t.Run("oversized stdin", func(t *testing.T) {
		fixture := newAdapterFixture(t)
		fixture.materializer.result.Stdin = bytes.Repeat([]byte{0x31}, fixture.config.MaxInputBytes+1)
		if _, available, err := fixture.adapter.Next(context.Background()); !errors.Is(err, ErrMaterializationFailed) || available {
			t.Fatalf("Next available=%t error=%v", available, err)
		}
		if len(fixture.session.actions) != 3 || fixture.session.actions[2].Terminal == nil {
			t.Fatalf("actions=%+v", fixture.session.actions)
		}
	})

	t.Run("too many read roots", func(t *testing.T) {
		fixture := newAdapterFixture(t)
		fixture.materializer.result.ReadRoots = make([]string, maxReadRoots+1)
		for index := range fixture.materializer.result.ReadRoots {
			fixture.materializer.result.ReadRoots[index] = fmt.Sprintf("%s/root-%03d", fixture.config.MaterializationRoot, index)
		}
		if _, available, err := fixture.adapter.Next(context.Background()); !errors.Is(err, ErrMaterializationFailed) || available {
			t.Fatalf("Next available=%t error=%v", available, err)
		}
		if len(fixture.session.actions) != 3 || fixture.session.actions[2].Terminal == nil {
			t.Fatalf("actions=%+v", fixture.session.actions)
		}
	})

	t.Run("materializer cancellation", func(t *testing.T) {
		fixture := newAdapterFixture(t)
		fixture.materializer.err = context.Canceled
		if _, available, err := fixture.adapter.Next(context.Background()); !errors.Is(err, ErrMaterializationFailed) || !errors.Is(err, context.Canceled) || available {
			t.Fatalf("Next available=%t error=%v", available, err)
		}
		if len(fixture.session.actions) != 3 || fixture.session.actions[2].Terminal == nil ||
			fixture.session.actions[2].Terminal.Status != attachedworkerprotocol.TerminalFailed {
			t.Fatalf("actions=%+v", fixture.session.actions)
		}
	})

	t.Run("lease expires during materialization", func(t *testing.T) {
		fixture := newAdapterFixture(t)
		now := adapterTestTime
		fixture.adapter.config.Now = func() time.Time { return now }
		fixture.materializer.hook = func() { now = adapterTestTime.Add(31 * time.Minute) }
		if _, available, err := fixture.adapter.Next(context.Background()); !errors.Is(err, ErrReconciliationRequired) || available {
			t.Fatalf("Next available=%t error=%v", available, err)
		}
		if len(fixture.session.actions) != 2 {
			t.Fatalf("expired authority emitted terminal actions=%+v", fixture.session.actions)
		}
	})

	t.Run("connection generation changes during materialization", func(t *testing.T) {
		fixture := newAdapterFixture(t)
		fixture.materializer.hook = func() { fixture.session.snapshot.ConnectionGeneration++ }
		if _, available, err := fixture.adapter.Next(context.Background()); !errors.Is(err, ErrReconciliationRequired) || available {
			t.Fatalf("Next available=%t error=%v", available, err)
		}
		if len(fixture.session.actions) != 2 {
			t.Fatalf("stale authority emitted terminal actions=%+v", fixture.session.actions)
		}
	})

	t.Run("ignored cancellation retains operation ownership", func(t *testing.T) {
		fixture := newAdapterFixture(t)
		started := make(chan struct{})
		release := make(chan struct{})
		finished := make(chan struct{})
		fixture.materializer.started = started
		fixture.materializer.release = release
		fixture.materializer.finished = finished
		var releaseOnce bool
		t.Cleanup(func() {
			if !releaseOnce {
				close(release)
			}
			select {
			case <-finished:
			case <-time.After(time.Second):
				t.Error("ignored materializer did not finish during cleanup")
			}
		})
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, _, err := fixture.adapter.Next(ctx)
			done <- err
		}()
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("materializer did not start")
		}
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) || !errors.Is(err, ErrReconciliationRequired) {
				t.Fatalf("error=%v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("Next did not honor cancellation bound")
		}
		secondCtx, secondCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer secondCancel()
		if _, _, err := fixture.adapter.Next(secondCtx); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("second owner error=%v", err)
		}
		if len(fixture.session.actions) != 2 {
			t.Fatalf("second owner crossed session actions=%+v", fixture.session.actions)
		}
		close(release)
		releaseOnce = true
	})
}

func TestAdapterDeepOwnsAuthorityAndClearsFailedMaterialization(t *testing.T) {
	t.Run("materializer cannot mutate active digests", func(t *testing.T) {
		fixture := newAdapterFixture(t)
		original := materializationRequest(fixture.snapshot, fixture.binding, 1)
		fixture.materializer.hook = func() {
			fixture.materializer.request.Attempt.ContextDigest[0] ^= 0xff
			fixture.materializer.request.Attempt.CapabilityDigest[0] ^= 0xff
			fixture.materializer.request.Attempt.PolicyDigest[0] ^= 0xff
		}
		invocation, available, err := fixture.adapter.Next(context.Background())
		if err != nil || !available {
			t.Fatalf("Next available=%t error=%v", available, err)
		}
		result := successfulResult()
		if err := fixture.adapter.Complete(context.Background(), invocation.Identity, result, nil); err != nil {
			t.Fatal(err)
		}
		want, err := terminalEvidenceDigest(
			original, invocation.Identity, result, nil,
			attachedworkerprotocol.TerminalSucceeded, attachedworkerprotocol.TerminalResultCompleted,
		)
		if err != nil || !bytes.Equal(fixture.session.actions[2].Terminal.EvidenceDigest, want) {
			t.Fatalf("active authority was aliased: equal=%t error=%v", bytes.Equal(fixture.session.actions[2].Terminal.EvidenceDigest, want), err)
		}
	})

	t.Run("partial sensitive input is cleared on error", func(t *testing.T) {
		fixture := newAdapterFixture(t)
		partial := []byte("partial sensitive input")
		fixture.materializer.result.Stdin = partial
		fixture.materializer.err = errors.New("fixture materialization failed")
		if _, available, err := fixture.adapter.Next(context.Background()); !errors.Is(err, ErrMaterializationFailed) || available {
			t.Fatalf("Next available=%t error=%v", available, err)
		}
		if !bytes.Equal(partial, make([]byte, len(partial))) {
			t.Fatal("partial materialized input was not cleared")
		}
	})
}

func TestAdapterReconcilesTerminalAcknowledgementWithoutInventingSuccess(t *testing.T) {
	t.Run("lost acknowledgement can retry exact terminal", func(t *testing.T) {
		fixture := newAdapterFixture(t)
		invocation, available, err := fixture.adapter.Next(context.Background())
		if err != nil || !available {
			t.Fatalf("Next available=%t error=%v", available, err)
		}
		fixture.session.terminalErr = attachedworkersession.ErrReconciliationRequired
		result := successfulResult()
		if err := fixture.adapter.Complete(context.Background(), invocation.Identity, result, nil); !errors.Is(err, ErrReconciliationRequired) {
			t.Fatalf("first Complete error=%v", err)
		}
		first := fixture.session.actions[2].Terminal
		fixture.session.terminalErr = nil
		if err := fixture.adapter.Complete(context.Background(), invocation.Identity, result, nil); err != nil {
			t.Fatalf("retry Complete error=%v", err)
		}
		second := fixture.session.actions[3].Terminal
		firstJSON, _ := json.Marshal(first)
		secondJSON, _ := json.Marshal(second)
		if !bytes.Equal(firstJSON, secondJSON) {
			t.Fatalf("terminal retry diverged: first=%s second=%s", firstJSON, secondJSON)
		}
	})

	t.Run("divergent acknowledgement remains uncommitted", func(t *testing.T) {
		fixture := newAdapterFixture(t)
		invocation, available, err := fixture.adapter.Next(context.Background())
		if err != nil || !available {
			t.Fatalf("Next available=%t error=%v", available, err)
		}
		fixture.session.mutateTerminalAck = func(ack *attachedworkerprotocol.TerminalAckV1) {
			ack.EvidenceDigest[0] ^= 0xff
		}
		if err := fixture.adapter.Complete(context.Background(), invocation.Identity, successfulResult(), nil); !errors.Is(err, ErrReconciliationRequired) {
			t.Fatalf("Complete error=%v", err)
		}
		if fixture.adapter.currentActive() == nil {
			t.Fatal("divergent acknowledgement cleared active attempt")
		}
	})
}

func TestTerminalClassificationAndEvidenceKeepFailureModesDistinct(t *testing.T) {
	fixture := newAdapterFixture(t)
	request := materializationRequest(fixture.snapshot, fixture.binding, 1)
	identity := fixture.identity()
	cases := []struct {
		name           string
		cancelRevision uint64
		result         attachedworkerdaemon.InvocationResult
		runErr         error
		wantStatus     attachedworkerprotocol.TerminalStatus
		wantResult     attachedworkerprotocol.TerminalResult
	}{
		{name: "success", result: successfulResult(), wantStatus: attachedworkerprotocol.TerminalSucceeded, wantResult: attachedworkerprotocol.TerminalResultCompleted},
		{name: "deadline", result: attachedworkerdaemon.InvocationResult{Process: attachedworkerdaemon.AttemptResult{Deadline: true}}, wantStatus: attachedworkerprotocol.TerminalFailed, wantResult: attachedworkerprotocol.TerminalResultFailed},
		{name: "cleanup", result: attachedworkerdaemon.InvocationResult{Process: attachedworkerdaemon.AttemptResult{ExitCode: 0, DescendantsReaped: true, BoundaryReleased: true}}, wantStatus: attachedworkerprotocol.TerminalFailed, wantResult: attachedworkerprotocol.TerminalResultFailed},
		{name: "runner", result: attachedworkerdaemon.InvocationResult{}, runErr: errors.New("fixture runner failure"), wantStatus: attachedworkerprotocol.TerminalFailed, wantResult: attachedworkerprotocol.TerminalResultFailed},
		{name: "cancelled", cancelRevision: 1, result: attachedworkerdaemon.InvocationResult{Process: attachedworkerdaemon.AttemptResult{Cancelled: true}}, runErr: context.Canceled, wantStatus: attachedworkerprotocol.TerminalCancelled, wantResult: attachedworkerprotocol.TerminalResultCancelled},
	}
	seen := make(map[string]string, len(cases))
	for _, test := range cases {
		status, result := classifyTerminal(test.cancelRevision, test.result, test.runErr)
		if status != test.wantStatus || result != test.wantResult {
			t.Errorf("case=%s status/result=%s/%s want=%s/%s", test.name, status, result, test.wantStatus, test.wantResult)
			continue
		}
		digest, err := terminalEvidenceDigest(request, identity, test.result, test.runErr, status, result)
		if err != nil {
			t.Errorf("case=%s evidence error=%v", test.name, err)
			continue
		}
		encoded := hex.EncodeToString(digest)
		if prior, duplicate := seen[encoded]; duplicate {
			t.Errorf("cases=%s/%s collapsed to one evidence digest", prior, test.name)
		}
		seen[encoded] = test.name
	}
}

func TestAdapterAcknowledgesPreRunCancellationAndSkipsMaterializer(t *testing.T) {
	fixture := newAdapterFixture(t)
	fixture.session.cancelOnClaim = true
	if _, available, err := fixture.adapter.Next(context.Background()); !errors.Is(err, ErrAttemptCancelled) || available {
		t.Fatalf("Next available=%t error=%v", available, err)
	}
	if fixture.materializer.calls != 0 || len(fixture.session.actions) != 4 {
		t.Fatalf("materialize=%d actions=%+v", fixture.materializer.calls, fixture.session.actions)
	}
	if fixture.session.actions[2].CancelAck == nil || fixture.session.actions[2].CancelAck.AttemptSequence != 2 ||
		fixture.session.actions[3].Terminal == nil || fixture.session.actions[3].Terminal.AttemptSequence != 3 ||
		fixture.session.actions[3].Terminal.Status != attachedworkerprotocol.TerminalCancelled {
		t.Fatalf("cancellation actions=%+v", fixture.session.actions)
	}
}

func TestAdapterRejectsUnsafeConfigurationAndTerminalEvidence(t *testing.T) {
	t.Run("reserved profile environment", func(t *testing.T) {
		fixture := newAdapterFixture(t)
		config := fixture.config
		config.Profile.Environment = []attachedworkerdaemon.EnvironmentVariable{{Name: "OPENAI_API_KEY", Value: "must-not-pass"}}
		if _, err := New(fixture.session, fixture.materializer, config); !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("error=%v", err)
		}
	})

	t.Run("symlink materialization root", func(t *testing.T) {
		fixture := newAdapterFixture(t)
		parent := canonicalTempDir(t)
		link := filepath.Join(parent, "root-link")
		if err := os.Symlink(fixture.config.MaterializationRoot, link); err != nil {
			t.Fatal(err)
		}
		config := fixture.config
		config.MaterializationRoot = link
		if _, err := New(fixture.session, fixture.materializer, config); !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("error=%v", err)
		}
	})

	t.Run("invalid isolation profile", func(t *testing.T) {
		fixture := newAdapterFixture(t)
		invocation, available, err := fixture.adapter.Next(context.Background())
		if err != nil || !available {
			t.Fatalf("Next available=%t error=%v", available, err)
		}
		result := successfulResult()
		result.Process.IsolationProfile = "profile with spaces"
		if err := fixture.adapter.Complete(context.Background(), invocation.Identity, result, nil); !errors.Is(err, ErrTerminalEvidenceInvalid) {
			t.Fatalf("error=%v", err)
		}
		if len(fixture.session.actions) != 2 {
			t.Fatalf("invalid evidence crossed terminal boundary actions=%d", len(fixture.session.actions))
		}
	})

	t.Run("lease expires before completion", func(t *testing.T) {
		fixture := newAdapterFixture(t)
		now := adapterTestTime
		fixture.adapter.config.Now = func() time.Time { return now }
		invocation, available, err := fixture.adapter.Next(context.Background())
		if err != nil || !available {
			t.Fatalf("Next available=%t error=%v", available, err)
		}
		now = adapterTestTime.Add(31 * time.Minute)
		if err := fixture.adapter.Complete(context.Background(), invocation.Identity, successfulResult(), nil); !errors.Is(err, ErrReconciliationRequired) {
			t.Fatalf("error=%v", err)
		}
		if len(fixture.session.actions) != 2 {
			t.Fatalf("expired authority emitted terminal actions=%+v", fixture.session.actions)
		}
	})

	t.Run("invalid bounded metadata", func(t *testing.T) {
		for _, test := range []struct {
			name   string
			mutate func(*attachedworkerdaemon.InvocationResult)
		}{
			{name: "negative stdout", mutate: func(result *attachedworkerdaemon.InvocationResult) { result.Process.StdoutBytes = -1 }},
			{name: "stdout exceeds maximum", mutate: func(result *attachedworkerdaemon.InvocationResult) {
				result.Process.StdoutBytes = maxTerminalStdoutBytes + 1
			}},
			{name: "stderr exceeds maximum", mutate: func(result *attachedworkerdaemon.InvocationResult) {
				result.Process.StderrBytes = maxTerminalStderrBytes + 1
			}},
			{name: "stdout count smaller than content", mutate: func(result *attachedworkerdaemon.InvocationResult) {
				result.Process.Stdout = []byte("content")
				result.Process.StdoutBytes = 1
			}},
			{name: "credential generation missing", mutate: func(result *attachedworkerdaemon.InvocationResult) {
				result.CredentialChanged = true
				result.CredentialGeneration = 0
			}},
			{name: "duplicate committed artifact digest", mutate: func(result *attachedworkerdaemon.InvocationResult) {
				result.CommittedArtifactDigests = append(result.CommittedArtifactDigests, result.CommittedArtifactDigests[0])
			}},
			{name: "zero committed event digest", mutate: func(result *attachedworkerdaemon.InvocationResult) {
				result.CommittedEventDigests = []attachedworkerdaemon.CommittedEvidenceDigest{{}}
			}},
			{name: "too many committed artifact digests", mutate: func(result *attachedworkerdaemon.InvocationResult) {
				result.CommittedArtifactDigests = orderedEvidenceDigests(maxCommittedDigests + 1)
			}},
			{name: "noncanonical committed event digest order", mutate: func(result *attachedworkerdaemon.InvocationResult) {
				result.CommittedEventDigests = orderedEvidenceDigests(2)
				result.CommittedEventDigests[0], result.CommittedEventDigests[1] =
					result.CommittedEventDigests[1], result.CommittedEventDigests[0]
			}},
		} {
			t.Run(test.name, func(t *testing.T) {
				fixture := newAdapterFixture(t)
				invocation, available, err := fixture.adapter.Next(context.Background())
				if err != nil || !available {
					t.Fatalf("Next available=%t error=%v", available, err)
				}
				result := successfulResult()
				test.mutate(&result)
				if err := fixture.adapter.Complete(context.Background(), invocation.Identity, result, nil); !errors.Is(err, ErrTerminalEvidenceInvalid) {
					t.Fatalf("error=%v", err)
				}
				if len(fixture.session.actions) != 2 {
					t.Fatalf("invalid metadata crossed terminal boundary actions=%+v", fixture.session.actions)
				}
			})
		}
	})
}

func TestTerminalEvidenceAndCompletionFingerprintBindCommittedDigests(t *testing.T) {
	fixture := newAdapterFixture(t)
	request := materializationRequest(fixture.snapshot, fixture.binding, 1)
	identity := fixture.identity()
	baseline := successfulResult()

	for _, test := range []struct {
		name   string
		mutate func(*attachedworkerdaemon.InvocationResult)
	}{
		{name: "artifact", mutate: func(result *attachedworkerdaemon.InvocationResult) {
			result.CommittedArtifactDigests = []attachedworkerdaemon.CommittedEvidenceDigest{
				attachedworkerdaemon.CommittedEvidenceDigest(sha256.Sum256([]byte("different committed artifact"))),
			}
		}},
		{name: "event", mutate: func(result *attachedworkerdaemon.InvocationResult) {
			result.CommittedEventDigests = []attachedworkerdaemon.CommittedEvidenceDigest{
				attachedworkerdaemon.CommittedEvidenceDigest(sha256.Sum256([]byte("different committed event"))),
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := baseline
			test.mutate(&changed)
			baselineEvidence, err := terminalEvidenceDigest(
				request, identity, baseline, nil,
				attachedworkerprotocol.TerminalSucceeded, attachedworkerprotocol.TerminalResultCompleted,
			)
			if err != nil {
				t.Fatal(err)
			}
			changedEvidence, err := terminalEvidenceDigest(
				request, identity, changed, nil,
				attachedworkerprotocol.TerminalSucceeded, attachedworkerprotocol.TerminalResultCompleted,
			)
			if err != nil {
				t.Fatal(err)
			}
			if bytes.Equal(baselineEvidence, changedEvidence) {
				t.Fatalf("%s digest did not affect terminal evidence", test.name)
			}
			baselineFingerprint, err := completionFingerprint(identity, baseline, nil)
			if err != nil {
				t.Fatal(err)
			}
			changedFingerprint, err := completionFingerprint(identity, changed, nil)
			if err != nil {
				t.Fatal(err)
			}
			if baselineFingerprint == changedFingerprint {
				t.Fatalf("%s digest did not affect completion fingerprint", test.name)
			}
		})
	}
}

func orderedEvidenceDigests(count int) []attachedworkerdaemon.CommittedEvidenceDigest {
	result := make([]attachedworkerdaemon.CommittedEvidenceDigest, count)
	for index := range result {
		result[index][sha256.Size-1] = byte(index + 1)
	}
	return result
}

type adapterFixture struct {
	adapter      *Adapter
	session      *fakeSession
	materializer *fakeMaterializer
	config       Config
	snapshot     attachedworkersession.SnapshotV1
	binding      attachedworkerprotocol.AttemptBindingV1
	readRoot     string
}

func newAdapterFixture(t *testing.T) *adapterFixture {
	t.Helper()
	root := canonicalTempDir(t)
	readRoot := filepath.Join(root, "input")
	if err := os.Mkdir(readRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	capabilityBytes := bytes.Repeat([]byte{0x41}, sha256.Size)
	capability := domain.AttachedWorkerCapabilityDigest(hex.EncodeToString(capabilityBytes))
	authenticationExpires := adapterTestTime.Add(time.Hour)
	snapshot := attachedworkersession.SnapshotV1{
		Version: attachedworkersession.SnapshotVersionV1, State: attachedworkersession.StateReady,
		TenantID: "tenant-001", OwnerUserID: "user-001", WorkerID: "worker-001",
		EnrollmentGeneration: 2, ConnectionGeneration: 3,
		ProtocolVersion: attachedworkerprotocol.ProtocolVersionV1, ConnectionID: "connection-001",
		CapabilityDigest: capability, AuthenticationExpires: &authenticationExpires,
	}
	binding := attachedworkerprotocol.AttemptBindingV1{
		RunID: "run-001", AttemptID: "attempt-001", LeaseID: "lease-001", LeaseGeneration: 7,
		FenceToken: "fence-token-001", ExpiresAtUnixMicro: adapterTestTime.Add(30 * time.Minute).UnixMicro(),
		ContextDigest: bytes.Repeat([]byte{0x42}, sha256.Size), CapabilityDigest: capabilityBytes,
		PolicyDigest: bytes.Repeat([]byte{0x43}, sha256.Size),
	}
	executableDigest := attachedworkerdaemon.ExecutableDigest(sha256.Sum256([]byte("pinned executable")))
	config := Config{
		Profile: LocalProfileV1{
			Name: "codex-local-v1", CapabilityDigest: capability, Executable: "/fixture/pinned-harness",
			ExecutableDigest: executableDigest, Arguments: []string{"--attached"},
			Environment: []attachedworkerdaemon.EnvironmentVariable{{Name: "SESSIONLESS_MODE", Value: "attached"}},
		},
		MaterializationRoot: root, MaxInputBytes: 1024, ReportTimeout: time.Second,
		Now: func() time.Time { return adapterTestTime },
	}
	session := &fakeSession{snapshot: snapshot, binding: binding}
	materializer := &fakeMaterializer{result: MaterializedInputV1{
		Stdin: []byte("bounded canonical input"), ReadRoots: []string{readRoot},
	}}
	adapter, err := New(session, materializer, config)
	if err != nil {
		t.Fatal(err)
	}
	return &adapterFixture{
		adapter: adapter, session: session, materializer: materializer, config: config,
		snapshot: snapshot, binding: binding, readRoot: readRoot,
	}
}

func (fixture *adapterFixture) identity() attachedworkerdaemon.InvocationIdentity {
	return attachedworkerdaemon.InvocationIdentity{
		TenantID: fixture.snapshot.TenantID, OwnerUserID: fixture.snapshot.OwnerUserID, WorkerID: fixture.snapshot.WorkerID,
		RunID: domain.RunID(fixture.binding.RunID), AttemptID: domain.AttemptID(fixture.binding.AttemptID),
		LeaseID: domain.LeaseID(fixture.binding.LeaseID), FenceToken: fixture.binding.LeaseGeneration,
	}
}

type fakeSession struct {
	snapshot          attachedworkersession.SnapshotV1
	binding           attachedworkerprotocol.AttemptBindingV1
	actions           []attachedworkersession.ActionV1
	heartbeatErr      error
	claimErr          error
	cancelOnClaim     bool
	terminalErr       error
	mutateTerminalAck func(*attachedworkerprotocol.TerminalAckV1)
}

func (fake *fakeSession) Snapshot() attachedworkersession.SnapshotV1 { return fake.snapshot }

func (fake *fakeSession) ExchangeAction(_ context.Context, action attachedworkersession.ActionV1) (*attachedworkerprotocol.FrameV1, error) {
	fake.actions = append(fake.actions, action)
	snapshot := fake.snapshot
	switch {
	case action.Heartbeat != nil:
		if fake.heartbeatErr != nil {
			return nil, fake.heartbeatErr
		}
		return platformFrame(snapshot, 3, attachedworkerprotocol.MessageLeaseOffer, func(frame *attachedworkerprotocol.FrameV1) {
			frame.LeaseOffer = &attachedworkerprotocol.LeaseOfferV1{Binding: cloneAttemptBinding(fake.binding), AttemptSequence: 1}
		}), nil
	case action.LeaseClaim != nil:
		if fake.claimErr != nil {
			return nil, fake.claimErr
		}
		if fake.cancelOnClaim {
			return platformFrame(snapshot, 4, attachedworkerprotocol.MessageCancel, func(frame *attachedworkerprotocol.FrameV1) {
				frame.Cancel = &attachedworkerprotocol.CancelV1{
					Binding: cloneAttemptBinding(fake.binding), AttemptSequence: 2,
					CancelRevision: 1, Code: attachedworkerprotocol.CancelRequested,
				}
			}), nil
		}
		return platformFrame(snapshot, 4, attachedworkerprotocol.MessageLeaseAccepted, func(frame *attachedworkerprotocol.FrameV1) {
			frame.LeaseAccepted = &attachedworkerprotocol.LeaseAcceptedV1{Binding: cloneAttemptBinding(fake.binding), AttemptSequence: 2}
		}), nil
	case action.CancelAck != nil:
		return nil, nil
	case action.Terminal != nil:
		if fake.terminalErr != nil {
			return nil, fake.terminalErr
		}
		terminal := action.Terminal
		return platformFrame(snapshot, 5, attachedworkerprotocol.MessageTerminalAck, func(frame *attachedworkerprotocol.FrameV1) {
			frame.TerminalAck = &attachedworkerprotocol.TerminalAckV1{
				Binding: cloneAttemptBinding(fake.binding), AttemptSequence: 3,
				TerminalSequence: terminal.TerminalSequence, Status: terminal.Status, Result: terminal.Result,
				EvidenceDigest: append([]byte(nil), terminal.EvidenceDigest...),
			}
			if fake.mutateTerminalAck != nil {
				fake.mutateTerminalAck(frame.TerminalAck)
			}
		}), nil
	default:
		return nil, errors.New("unexpected action")
	}
}

type fakeMaterializer struct {
	request  MaterializationRequestV1
	result   MaterializedInputV1
	err      error
	calls    int
	hook     func()
	started  chan struct{}
	release  chan struct{}
	finished chan struct{}
}

func (fake *fakeMaterializer) Materialize(_ context.Context, request MaterializationRequestV1) (MaterializedInputV1, error) {
	fake.calls++
	fake.request = request
	if fake.hook != nil {
		fake.hook()
	}
	if fake.started != nil {
		close(fake.started)
	}
	if fake.release != nil {
		<-fake.release
	}
	if fake.finished != nil {
		close(fake.finished)
	}
	return fake.result, fake.err
}

func platformFrame(
	snapshot attachedworkersession.SnapshotV1,
	sequence uint64,
	kind attachedworkerprotocol.MessageKind,
	populate func(*attachedworkerprotocol.FrameV1),
) *attachedworkerprotocol.FrameV1 {
	frame := &attachedworkerprotocol.FrameV1{
		Version:   snapshot.ProtocolVersion,
		MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionPlatformToWorker, sequence),
		WorkerID:  string(snapshot.WorkerID), EnrollmentGeneration: snapshot.EnrollmentGeneration,
		ConnectionGeneration: snapshot.ConnectionGeneration, Sequence: sequence, Ack: sequence,
		Kind: kind,
	}
	populate(frame)
	return frame
}

func successfulResult() attachedworkerdaemon.InvocationResult {
	return attachedworkerdaemon.InvocationResult{
		Process: attachedworkerdaemon.AttemptResult{
			ExitCode: 0, DescendantsReaped: true, StdoutBytes: 15, StderrBytes: 0,
			IsolationProfile: "darwin-vm", BoundaryReleased: true, CleanupSucceeded: true,
		},
		CommittedArtifactDigests: []attachedworkerdaemon.CommittedEvidenceDigest{
			attachedworkerdaemon.CommittedEvidenceDigest(sha256.Sum256([]byte("committed artifact"))),
		},
		CommittedEventDigests: []attachedworkerdaemon.CommittedEvidenceDigest{
			attachedworkerdaemon.CommittedEvidenceDigest(sha256.Sum256([]byte("committed event"))),
		},
	}
}

func canonicalTempDir(t *testing.T) string {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func assertAdapterFormattingIsSecretFree(t *testing.T, adapter *Adapter, profile LocalProfileV1, input MaterializedInputV1) {
	t.Helper()
	secret := "bounded canonical input"
	formatted := strings.Join([]string{
		fmt.Sprintf("%+v %#v", adapter, adapter),
		fmt.Sprintf("%+v %#v", profile, profile),
		fmt.Sprintf("%+v %#v", input, input),
	}, " ")
	if strings.Contains(formatted, secret) || strings.Contains(formatted, profile.Executable) {
		t.Fatalf("formatting leaked secret/process path: %s", formatted)
	}
}
