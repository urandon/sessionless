package codexexec

import (
	"reflect"

	"gitcode.com/urandon/sessionless/internal/domain"
)

type nativeReduction struct {
	evidence     domain.ProviderExecutionEvidenceV1
	final        []byte
	failedClosed bool
}

// reduceNativeResult consumes only original private observations. Public
// ProcessResultV1 exit/cleanup/effect fields cannot manufacture success or a
// definitive failed turn. Calling this private reducer does not select or
// register a native profile, issue a credential, or authorize an execution.
func (boundary *PreparedProcessBoundaryV1) reduceNativeResult(binding domain.HarnessBindingV2, process ProcessResultV1) (nativeReduction, error) {
	original := process.original
	if boundary == nil || boundary.native == nil || boundary.nativeIdentity != boundary ||
		original == nil || original.self != original || original.boundary != boundary || original.authority != boundary.authority ||
		original.native != boundary.native || boundary.native.self != boundary.native ||
		!reflect.DeepEqual(original.binding, binding) || binding.Validate() != nil {
		return nativeReduction{}, ErrContract
	}
	parsed, actual := original.protocol, original.process
	evidence := domain.ProviderExecutionEvidenceV1{
		AcceptanceClass: domain.ProviderAcceptanceUnknownV1,
		FinishClass:     domain.ProviderFinishUnknownV1, FailureCode: domain.ProviderExecutionFailureAcceptedUnknownV1,
		RouteState: domain.ProviderEvidenceUnknownV1, PolicyVerdict: domain.ProviderPolicyConditionalV1,
		UsageProvenance: domain.ProviderUsageUnknownV1,
	}
	if parsed.accepted {
		evidence.AcceptanceClass = domain.ProviderAcceptanceAcceptedV1
	}
	bounded := actual.StdoutBytes == parsed.stdoutBytes && actual.StdoutBytes >= 0 && actual.StdoutBytes <= maxJSONLOutputBytes &&
		actual.StderrBytes >= 0 && actual.StderrBytes <= maxProcessStderrBytesV1
	clean := actual.DescendantsReaped && actual.BoundaryReleased && actual.CleanupSucceeded
	failedClosed := false
	switch {
	case actual.Cancelled || actual.Deadline:
		evidence.FinishClass, evidence.FailureCode = domain.ProviderFinishCancelledV1, domain.ProviderExecutionFailureCancelledV1
	case parsed.protocolDrift || parsed.terminalDrift:
		evidence.FinishClass, evidence.FailureCode = domain.ProviderFinishFailedV1, domain.ProviderExecutionFailureProtocolDriftV1
	case !bounded || !clean || original.runErr != nil:
		// Even a valid terminal cannot publish through unknown cleanup. Keep
		// the existing consumed claim; nothing here proves safe external replay.
		evidence.FinishClass, evidence.FailureCode = domain.ProviderFinishFailedV1, domain.ProviderExecutionFailureBackendV1
	case parsed.accepted && parsed.failedTerminal && (actual.FailureCode == "" || actual.FailureCode == "process_exit_failed"):
		evidence.FinishClass, evidence.FailureCode = domain.ProviderFinishFailedV1, domain.ProviderExecutionFailureProviderFailedV1
		failedClosed = true
	case parsed.accepted && parsed.terminal && len(parsed.final) != 0 && actual.ExitCode == 0 && actual.FailureCode == "":
		evidence.FinishClass, evidence.FailureCode = domain.ProviderFinishCompletedV1, ""
	}
	sealed, err := evidence.SealForBinding(binding)
	if err != nil {
		return nativeReduction{}, ErrContract
	}
	result := nativeReduction{evidence: sealed, failedClosed: failedClosed}
	if sealed.FinishClass == domain.ProviderFinishCompletedV1 {
		result.final = append([]byte(nil), parsed.final...)
	}
	return result, nil
}
