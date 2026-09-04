// Package codexexec implements the bounded Codex exec subscription backend.
// It is deliberately not a ports.HarnessDriver and is not wired into a
// runtime: the future Sessionless-owned routing harness remains the canonical
// cross-backend boundary for Codex, OpenCode, Pi, and direct providers.
package codexexec

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

const (
	ContractVersionV1 uint32 = 1

	LifecycleCompleted                    LifecycleClass = "completed"
	LifecyclePreAcceptance                LifecycleClass = "pre_acceptance"
	LifecycleAmbiguous                    LifecycleClass = "ambiguous"
	LifecycleCompletedWithTeardownFailure LifecycleClass = "completed_with_teardown_failure"
	LifecycleCancelledAmbiguous           LifecycleClass = "cancelled_ambiguous"
	LifecycleProtocolDrift                LifecycleClass = "protocol_drift"
	LifecycleTerminalProtocolDrift        LifecycleClass = "terminal_protocol_drift"

	FailureLocalRunner = "local_runner_failed"

	ObservationUnknown Observation = "unknown"
)

var (
	ErrDisabled = errors.New("codex exec backend is disabled")
	ErrContract = errors.New("codex exec backend contract is invalid")
)

type LifecycleClass string
type Observation string

func (class LifecycleClass) Valid() bool {
	switch class {
	case LifecycleCompleted, LifecyclePreAcceptance, LifecycleAmbiguous,
		LifecycleCompletedWithTeardownFailure, LifecycleCancelledAmbiguous,
		LifecycleProtocolDrift, LifecycleTerminalProtocolDrift:
		return true
	default:
		return false
	}
}

// AuthorityV1 is adapter-local verification input, not a scheduling decision.
// A future SessionlessHarnessV1 must construct it from the already-admitted
// immutable harness/resource binding. Remote workers never choose these fields.
type AuthorityV1 struct {
	Version              uint32
	TenantID             domain.TenantID
	OwnerUserID          domain.UserID
	WorkerID             domain.AttachedWorkerID
	ConnectionID         domain.AttachedWorkerConnectionID
	EnrollmentGeneration uint64
	ConnectionGeneration uint64
	RunID                domain.RunID
	AttemptID            domain.AttemptID
	ReservationID        domain.QuotaReservationID
	LeaseID              domain.LeaseID
	LeaseGeneration      uint64
	FenceToken           domain.AttachedWorkerFenceToken
	LeaseExpiresAt       time.Time
	ContextDigest        domain.AttachedWorkerContextDigest
	CapabilityDigest     domain.AttachedWorkerCapabilityDigest
	PolicyDigest         domain.AttachedWorkerPolicyDigest
	ProviderResource     domain.ProviderResourceBindingV1
}

func (authority AuthorityV1) Validate() error {
	if authority.Version != ContractVersionV1 || authority.TenantID.Validate() != nil ||
		authority.OwnerUserID.Validate() != nil || authority.WorkerID.Validate() != nil ||
		authority.ConnectionID.Validate() != nil || authority.RunID.Validate() != nil ||
		authority.AttemptID.Validate() != nil || authority.ReservationID.Validate() != nil ||
		authority.LeaseID.Validate() != nil ||
		authority.EnrollmentGeneration == 0 || authority.ConnectionGeneration == 0 ||
		authority.LeaseGeneration == 0 || authority.ProviderResource.Validate() != nil ||
		authority.LeaseExpiresAt.IsZero() || authority.ContextDigest.Validate() != nil ||
		authority.CapabilityDigest.Validate() != nil || authority.PolicyDigest.Validate() != nil {
		return ErrContract
	}
	resource := authority.ProviderResource
	if resource.Kind != domain.ProviderResourceSubscriptionV1 ||
		resource.OwnerUserID != authority.OwnerUserID ||
		resource.CredentialMode != domain.ProviderCredentialInvocationV1 ||
		domain.SubscriptionConnectionID(resource.ResourceID).Validate() != nil {
		return ErrContract
	}
	expectedFence, err := domain.NewAttachedWorkerFenceTokenV1(
		authority.TenantID, authority.OwnerUserID, authority.WorkerID,
		authority.RunID, authority.AttemptID, authority.LeaseID, authority.LeaseGeneration,
	)
	if err != nil || authority.FenceToken != expectedFence {
		return ErrContract
	}
	return nil
}

type RequestV1 struct {
	Authority   AuthorityV1                  `json:"-"`
	Credential  ports.CredentialIssueRequest `json:"-"`
	Instruction []byte                       `json:"-"`
}

func (request RequestV1) Validate() error {
	if request.Authority.Validate() != nil || len(request.Instruction) == 0 ||
		len(request.Instruction) > maxInstructionBytes || !utf8.Valid(request.Instruction) ||
		strings.IndexByte(string(request.Instruction), 0) >= 0 {
		return ErrContract
	}
	authority := request.Authority
	credential := request.Credential
	if credential.ValidateAt(time.Now().UTC()) != nil ||
		credential.OwnerUserID != authority.OwnerUserID || credential.Run.TenantID != authority.TenantID ||
		credential.Run.ID != authority.RunID ||
		credential.Run.SubscriptionConnectionID != domain.SubscriptionConnectionID(authority.ProviderResource.ResourceID) ||
		credential.ProviderResource != authority.ProviderResource ||
		credential.Attempt.ID != authority.AttemptID || credential.Attempt.WorkerID != string(authority.WorkerID) ||
		credential.Lease.ID != authority.LeaseID || credential.Lease.WorkerID != string(authority.WorkerID) ||
		credential.Lease.FenceToken != authority.LeaseGeneration ||
		!credential.Lease.ExpiresAt.Equal(authority.LeaseExpiresAt) {
		return ErrContract
	}
	return nil
}

func (request RequestV1) String() string {
	return fmt.Sprintf(
		"CodexExecRequest{tenant:%s owner:%s worker:%s connection:%s run:%s attempt:%s lease:%s lease_generation:%d credential_generation:%d instruction:[redacted]}",
		request.Authority.TenantID, request.Authority.OwnerUserID, request.Authority.WorkerID,
		request.Authority.ConnectionID, request.Authority.RunID, request.Authority.AttemptID,
		request.Authority.LeaseID, request.Authority.LeaseGeneration,
		request.Authority.ProviderResource.CredentialGeneration,
	)
}

func (request RequestV1) GoString() string { return request.String() }

// ResultV1 keeps provider lifecycle, local cleanup, and credential finalization
// orthogonal. FinalCandidate is private attempt material and must never be
// logged or treated as canonical success unless Lifecycle is completed.
type ResultV1 struct {
	Lifecycle           LifecycleClass
	Accepted            bool
	NativeTerminal      bool
	ProcessStopped      bool
	CleanupSucceeded    bool
	CredentialFinalized bool
	FailureCode         string
	EvidenceDigest      string
	FinalCandidate      []byte `json:"-"`
	BillingRoute        Observation
	Quota               Observation
}

func (result ResultV1) String() string {
	return fmt.Sprintf(
		"CodexExecResult{lifecycle:%s accepted:%t terminal:%t stopped:%t cleanup:%t credential_finalized:%t failure:%s evidence:%s final:[redacted] billing:%s quota:%s}",
		result.Lifecycle, result.Accepted, result.NativeTerminal, result.ProcessStopped,
		result.CleanupSucceeded, result.CredentialFinalized, result.FailureCode,
		result.EvidenceDigest, result.BillingRoute, result.Quota,
	)
}

func (result ResultV1) GoString() string { return result.String() }

func evidenceDigest(authority AuthorityV1, executableVersion string, executableDigest attachedworkerdaemon.ExecutableDigest, model string, instruction []byte, parsed parseResult, lifecycle LifecycleClass, failureCode string) string {
	hash := sha256.New()
	instructionDigest := sha256.Sum256(instruction)
	fields := []string{
		"sessionless.codex-exec.evidence.v1", string(authority.TenantID), string(authority.OwnerUserID),
		string(authority.WorkerID), string(authority.ConnectionID), fmt.Sprint(authority.EnrollmentGeneration),
		fmt.Sprint(authority.ConnectionGeneration), string(authority.RunID), string(authority.AttemptID),
		string(authority.ReservationID), string(authority.LeaseID), fmt.Sprint(authority.LeaseGeneration),
		string(authority.FenceToken), authority.LeaseExpiresAt.UTC().Format(time.RFC3339Nano),
		string(authority.ContextDigest), string(authority.CapabilityDigest),
		string(authority.PolicyDigest), string(authority.ProviderResource.Kind),
		authority.ProviderResource.ResourceID, string(authority.ProviderResource.OwnerUserID),
		fmt.Sprint(authority.ProviderResource.Revision), string(authority.ProviderResource.CredentialMode),
		fmt.Sprint(authority.ProviderResource.CredentialGeneration), executableVersion, hex.EncodeToString(executableDigest[:]),
		model, hex.EncodeToString(instructionDigest[:]),
		string(lifecycle), failureCode,
	}
	if len(parsed.final) != 0 {
		digest := sha256.Sum256(parsed.final)
		fields = append(fields, hex.EncodeToString(digest[:]))
	} else {
		fields = append(fields, "")
	}
	for _, field := range fields {
		_, _ = fmt.Fprintf(hash, "%d:%s", len(field), field)
	}
	return hex.EncodeToString(hash.Sum(nil))
}
