//go:build darwin || linux

package attachedworkerstack_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	transport "gitcode.com/urandon/sessionless/internal/attachedworkerdaemontransport"
	"gitcode.com/urandon/sessionless/internal/attachedworkerstack"
	"gitcode.com/urandon/sessionless/internal/codexexec"
)

// This observer is not a runner, authority, receipt producer or replacement
// finalizer. The original Adapter receives the exact Complete arguments once.
// It exposes only closed flags/counts on failure, never error text, output,
// credential material, identifiers, references or paths.
type joinedGenerationTerminalDiagnostic struct {
	t            *testing.T
	adapter      *transport.Adapter
	installation *attachedworkerstack.NativeGenerationJoinedInstallationV3ForTest
}

func (d *joinedGenerationTerminalDiagnostic) Complete(ctx context.Context, identity attachedworkerdaemon.InvocationIdentity, result attachedworkerdaemon.InvocationResult, runErr error) error {
	reportErr := d.adapter.Complete(ctx, identity, result, runErr)
	if reportErr != nil {
		d.t.Logf("joined terminal failure DATA: launches=%d flags=%+v", d.installation.LaunchCount(), classifyJoinedGenerationTerminal(result, runErr, reportErr))
	}
	return reportErr
}

type joinedGenerationTerminalFlags struct {
	RunError, RunContract, RunAuthority, RunConsumed                bool
	RunInvocationInvalid, RunCredentialUnavailable                  bool
	RunCredentialFinalization, RunCanceled, RunDeadline, RunUnknown bool
	RunStackMismatch, RunStackReconciliation                        bool
	PreparedContractStage                                           string
	ReportError, ReportTerminalInvalid, ReportReconciliation        bool
	ReportAuthority                                                 bool
	Succeeded, CleanupComplete                                      bool
	DescendantsReaped, BoundaryReleased, ProcessCleanup             bool
	CredentialRequired, CredentialReleased, CredentialChanged       bool
	ProcessFailure, ProcessCancelled, ProcessDeadline               bool
	NegativeDuration                                                bool
	ExitCode, StdoutBytes, StderrBytes, BufferedStdoutBytes         int
	CredentialGeneration                                            uint64
	InvocationCode                                                  uint8
}

// InvocationCode is a closed diagnostic enum: 0 none, 1 lifecycle unavailable,
// 2 issue, 3 handle mismatch, 4 materialization, 5 writeback, 6 release,
// 255 any other nonempty code. No original string is retained or printed.
func classifyJoinedGenerationTerminal(result attachedworkerdaemon.InvocationResult, runErr, reportErr error) joinedGenerationTerminalFlags {
	f := joinedGenerationTerminalFlags{
		RunError: runErr != nil, RunContract: errors.Is(runErr, codexexec.ErrContract),
		RunAuthority: errors.Is(runErr, codexexec.ErrAuthorityUnavailable), RunConsumed: errors.Is(runErr, codexexec.ErrProcessAlreadyConsumed),
		RunInvocationInvalid:      errors.Is(runErr, attachedworkerdaemon.ErrInvocationInvalid),
		RunCredentialUnavailable:  errors.Is(runErr, attachedworkerdaemon.ErrCredentialUnavailable),
		RunCredentialFinalization: errors.Is(runErr, attachedworkerdaemon.ErrCredentialFinalization),
		RunCanceled:               errors.Is(runErr, context.Canceled), RunDeadline: errors.Is(runErr, context.DeadlineExceeded),
		RunStackMismatch:       errors.Is(runErr, attachedworkerstack.ErrInvocationMismatch),
		RunStackReconciliation: errors.Is(runErr, attachedworkerstack.ErrReconciliationRequired),
		PreparedContractStage:  codexexec.PreparedContractStageV1(runErr),
		ReportError:            reportErr != nil, ReportTerminalInvalid: errors.Is(reportErr, transport.ErrTerminalEvidenceInvalid),
		ReportReconciliation: errors.Is(reportErr, transport.ErrReconciliationRequired), ReportAuthority: errors.Is(reportErr, transport.ErrInvalidAuthority),
		Succeeded: result.Succeeded(runErr), CleanupComplete: result.CleanupComplete(),
		DescendantsReaped: result.Process.DescendantsReaped, BoundaryReleased: result.Process.BoundaryReleased, ProcessCleanup: result.Process.CleanupSucceeded,
		CredentialRequired: result.CredentialReleaseRequired, CredentialReleased: result.CredentialReleased, CredentialChanged: result.CredentialChanged,
		CredentialGeneration: result.CredentialGeneration, ProcessFailure: result.Process.FailureCode != "",
		ProcessCancelled: result.Process.Cancelled, ProcessDeadline: result.Process.Deadline, NegativeDuration: result.Process.Duration < 0,
		ExitCode: result.Process.ExitCode, StdoutBytes: result.Process.StdoutBytes, StderrBytes: result.Process.StderrBytes, BufferedStdoutBytes: len(result.Process.Stdout),
	}
	f.RunUnknown = f.RunError && !(f.RunContract || f.RunAuthority || f.RunConsumed || f.RunInvocationInvalid || f.RunCredentialUnavailable || f.RunCredentialFinalization || f.RunCanceled || f.RunDeadline || f.RunStackMismatch || f.RunStackReconciliation)
	switch result.FailureCode {
	case "":
	case "credential_lifecycle_unavailable":
		f.InvocationCode = 1
	case "credential_issue_failed":
		f.InvocationCode = 2
	case "credential_handle_mismatch":
		f.InvocationCode = 3
	case "credential_materialization_failed":
		f.InvocationCode = 4
	case "credential_writeback_failed":
		f.InvocationCode = 5
	case "credential_release_failed":
		f.InvocationCode = 6
	default:
		f.InvocationCode = 255
	}
	return f
}

// Pure diagnostic DATA tests only. They neither reproduce the unresolved
// remote runner failure nor exercise a DB, adapter completion or native process.
func TestNativeGenerationTerminalDiagnosticFlags(t *testing.T) {
	t.Run("original runner error survives report classification", func(t *testing.T) {
		f := classifyJoinedGenerationTerminal(attachedworkerdaemon.InvocationResult{}, errors.Join(codexexec.ErrContract, context.Canceled), transport.ErrTerminalEvidenceInvalid)
		if !f.RunError || !f.RunContract || !f.RunCanceled || f.RunUnknown || !f.ReportError || !f.ReportTerminalInvalid || f.CleanupComplete || f.Succeeded {
			t.Fatalf("closed original runner/report classification: %+v", f)
		}
	})
	t.Run("credential finalization and cleanup counts", func(t *testing.T) {
		result := attachedworkerdaemon.InvocationResult{Process: attachedworkerdaemon.AttemptResult{DescendantsReaped: true, BoundaryReleased: true, CleanupSucceeded: true, StdoutBytes: 9, StderrBytes: 2, Stdout: []byte("synthetic")}, CredentialReleaseRequired: true, CredentialChanged: true, CredentialGeneration: 2, FailureCode: "credential_release_failed"}
		f := classifyJoinedGenerationTerminal(result, attachedworkerdaemon.ErrCredentialFinalization, transport.ErrTerminalEvidenceInvalid)
		if !f.RunCredentialFinalization || f.RunUnknown || f.InvocationCode != 6 || !f.CredentialChanged || f.CredentialGeneration != 2 || f.CredentialReleased || !f.ProcessCleanup || f.CleanupComplete || f.Succeeded || f.StdoutBytes != 9 || f.BufferedStdoutBytes != 9 || f.StderrBytes != 2 {
			t.Fatalf("closed credential finalization classification: %+v", f)
		}
	})
	t.Run("no original text retained", func(t *testing.T) {
		const marker = "synthetic-private-diagnostic-marker"
		result := attachedworkerdaemon.InvocationResult{Process: attachedworkerdaemon.AttemptResult{Stdout: []byte(marker), StdoutBytes: len(marker), FailureCode: marker}, FailureCode: marker}
		f := classifyJoinedGenerationTerminal(result, errors.New(marker), errors.New(marker))
		if !f.RunUnknown || !f.ReportError || !f.ProcessFailure || f.InvocationCode != 255 || strings.Contains(fmt.Sprintf("%+v", f), marker) {
			t.Fatal("diagnostic retained original failure/output text")
		}
	})
	t.Run("successful result remains distinct", func(t *testing.T) {
		result := attachedworkerdaemon.InvocationResult{Process: attachedworkerdaemon.AttemptResult{DescendantsReaped: true, BoundaryReleased: true, CleanupSucceeded: true}, CredentialReleaseRequired: true, CredentialReleased: true, CredentialGeneration: 1}
		f := classifyJoinedGenerationTerminal(result, nil, nil)
		if !f.Succeeded || !f.CleanupComplete || f.RunError || f.RunUnknown || f.ReportError || f.InvocationCode != 0 {
			t.Fatalf("successful diagnostic DATA classification: %+v", f)
		}
	})
}
