package attachedworkeroutput

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// ProcessObservationV1 contains bounded, content-free teardown evidence. A
// server derives credentialRequired from the pinned job, never from this
// untrusted observation alone. It is distinct from canonical output and from
// the legacy synthetic Terminal digest.
type ProcessObservationV1 struct {
	Version                   uint32 `json:"version"`
	ExitCode                  int    `json:"exit_code"`
	Cancelled                 bool   `json:"cancelled"`
	Deadline                  bool   `json:"deadline"`
	ProcessFailureCode        string `json:"process_failure_code,omitempty"`
	InvocationFailureCode     string `json:"invocation_failure_code,omitempty"`
	RunnerFailed              bool   `json:"runner_failed"`
	DescendantsReaped         bool   `json:"descendants_reaped"`
	BoundaryReleased          bool   `json:"boundary_released"`
	CleanupSucceeded          bool   `json:"cleanup_succeeded"`
	CredentialReleaseRequired bool   `json:"credential_release_required"`
	CredentialReleased        bool   `json:"credential_released"`
}

func (observation ProcessObservationV1) ValidateFor(status domain.AttachedWorkerTerminalStatus, credentialRequired bool) error {
	if observation.Version != 1 || !status.Valid() ||
		observation.ProcessFailureCode != "" && domain.ValidateOpaqueID("process_failure_code", observation.ProcessFailureCode) != nil ||
		observation.InvocationFailureCode != "" && domain.ValidateOpaqueID("invocation_failure_code", observation.InvocationFailureCode) != nil ||
		!observation.DescendantsReaped || !observation.BoundaryReleased || !observation.CleanupSucceeded ||
		observation.CredentialReleaseRequired != credentialRequired ||
		(credentialRequired && !observation.CredentialReleased) ||
		(!credentialRequired && observation.CredentialReleased) {
		return ErrCandidateInvalid
	}
	switch status {
	case domain.AttachedWorkerTerminalSucceeded:
		if observation.ExitCode != 0 || observation.Cancelled || observation.Deadline ||
			observation.ProcessFailureCode != "" || observation.InvocationFailureCode != "" || observation.RunnerFailed {
			return ErrCandidateInvalid
		}
	case domain.AttachedWorkerTerminalCancelled:
		if !observation.Cancelled || observation.Deadline || observation.InvocationFailureCode != "" {
			return ErrCandidateInvalid
		}
	case domain.AttachedWorkerTerminalFailed:
		if observation.ExitCode == 0 && !observation.Deadline && !observation.RunnerFailed &&
			observation.ProcessFailureCode == "" && observation.InvocationFailureCode == "" {
			return ErrCandidateInvalid
		}
	}
	return nil
}

func (observation ProcessObservationV1) Digest() (string, error) {
	if observation.Version != 1 {
		return "", ErrCandidateInvalid
	}
	encoded, err := json.Marshal(observation)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(append([]byte("sessionless:attached-worker-process-observation:v1\x00"), encoded...))
	return hex.EncodeToString(digest[:]), nil
}
