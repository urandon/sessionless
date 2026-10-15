package attachedworkerdaemon

import (
	"context"
	"reflect"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/pisdkphasewire"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// PreparedInvocationV1 is private, invocation-scoped input for a configured
// executor. The outer InvocationRunner has already validated, issued and
// materialized its exact credential. It retains WriteBack/Release ownership.
// Process is a cloned, credential-bound spec; its stdin is borrowed only until
// RunPrepared returns and must not be retained. This is not a wire DTO.
type PreparedInvocationV1 struct {
	Identity        InvocationIdentity                        `json:"-"`
	Process         AttemptSpec                               `json:"-"`
	Credential      ports.ProviderInvocationCredentialV1      `json:"-"`
	Materialization ports.ProviderCredentialMaterializationV1 `json:"-"`
	VerifiedInput   *VerifiedAttachedInputV1                  `json:"-"`
	APIRequest      *PreparedAPIRequestV1                     `json:"-"`
	PhysicalSend    PreparedAPIPhysicalSendV1                 `json:"-"`
	Ledger          PreparedAPILedgerV1                       `json:"-"`
	apiIssueStarted time.Time                                 // parent-only original Issue boundary, never wire data
}

func (PreparedInvocationV1) String() string {
	return "PreparedInvocationV1{scope:[redacted] process:[redacted] credential:[redacted] materialization:[redacted]}"
}

func (invocation PreparedInvocationV1) GoString() string { return invocation.String() }

// PreparedProcessRunner executes using the already prepared projection. It
// returns actual process observations only, never credential finalization
// flags. An executor must not wrap another InvocationRunner or repeat an
// invocation after an ambiguous result. Accepted-attempt authority, canonical
// input staging and semantic output collection remain its explicit contracts.
type PreparedProcessRunner interface {
	RunPrepared(context.Context, PreparedInvocationV1) (AttemptResult, error)
}

// PreparedAPIPhysicalSendV1 is private parent-side supervision, not a child
// wire grant or a substitute for the genuine Source. The executor must submit
// the complete physical request and recheck the returned original monotonic
// cutoff at the actual send. It receives neither a bearer nor a lifecycle port.
// A future native IPC adapter must preserve this boundary, not serialize a Go
// deadline or treat this interface alone as hard native enforcement.
type PreparedAPIPhysicalSendV1 interface {
	Authorize(body []byte, backendProfileDigest string) (time.Time, error)
}

// NewPreparedInvocationRunner selects an explicit executor instead of the
// ordinary process runner, with exactly the same credential lifecycle owner.
// It rejects credentialless input; there is no fallback to the ordinary path.
// No shipped foreground/profile constructor uses this default-off library seam.
func NewPreparedInvocationRunner(config InvocationRunnerConfig, executor PreparedProcessRunner, credentials ports.CredentialLifecycle) (*InvocationRunner, error) {
	return newInvocationRunner(config, nil, executor, credentials)
}

// PreparedAPIRequestV1 is complete borrowed request DATA, never admission,
// native fit, currentness, or a physical-send capability. Every buffer is
// prospectively funded by the SAME parent ledger before it is transferred.
type PreparedAPIRequestV1 struct {
	Profile                      domain.AttachedPiSDKNativeProfileV1 `json:"-"`
	Body, Model, Schema, Headers []byte                              `json:"-"`
}

func (PreparedAPIRequestV1) String() string     { return "PreparedAPIRequestV1{data:[redacted]}" }
func (v PreparedAPIRequestV1) GoString() string { return v.String() }

// PreparedAPILedgerV1 borrows the existing canonical ledger's actual methods.
// It is NOT a snapshot certificate or fresh budget; prior debits survive.
type PreparedAPILedgerV1 interface {
	Reserve(copyBytes, liveBytes, nodes uint64) error
	EndLive(uint64) error
}

// PreparedInputAcquisitionV1 is the trusted local source input's original
// accounting/custody owner, not authorization or a native-fit certificate.
// The genuine parent consumes the same ledger/entry; it cannot replenish them
// after HTTP decode or derive a new context from remaining durations.
type PreparedInputAcquisitionV1 interface {
	OriginalContext() context.Context
	OriginalEntry() time.Time
	OriginalLedger() PreparedAPILedgerV1
	Clear()
}

// PreparedAPIRetirementV1 belongs to the original retirement owner. Context
// returns its once-selected applicable cutoff, including elapsed retirement
// already begun during callback/process cleanup. It must not restart grace.
// First retirement after normal completion is allowed; model-use permission
// never follows from this independently cancellable cleanup context.
type PreparedAPIRetirementV1 interface {
	Context() (context.Context, error)
}

// PreparedAPIDataV1 transfers funded independent parent/child copies, not a
// grant. Prepare must fund all cloning, validation and comparison work BEFORE
// allocation, including the process projection. No runner-side Clone/stringify
// replenishes that ledger. A real source-derived implementation is absent.
type PreparedAPIDataV1 struct {
	Issue                       ports.ProviderCredentialIssueRequestV1 `json:"-"`
	ParentInput, ChildInput     *VerifiedAttachedInputV1               `json:"-"`
	ParentRequest, ChildRequest PreparedAPIRequestV1                   `json:"-"`
	ParentProcess, ChildProcess AttemptSpec                            `json:"-"`
	// InitArguments is the exact ten already-funded decimal values emitted by
	// the SAME Init plan before Issue. Original Invocation.Process is the pinned
	// manifest prefix with empty stdin; both owned processes append only these
	// values. This DATA does not certify a native artifact, SDK or allocation fit.
	InitArguments                                              [10]string                                   `json:"-"`
	Authorization                                              ports.AttachedWorkerSealedInputAuthorization `json:"-"`
	Entry, CallerDeadline, InvocationDeadline, ProcessDeadline time.Time                                    `json:"-"`
	SafetySlack                                                time.Duration                                `json:"-"`
	Ledger                                                     PreparedAPILedgerV1                          `json:"-"`
	Retirement                                                 PreparedAPIRetirementV1                      `json:"-"`
	// Original composite data owner clears its buffers/live reservations; it
	// never refunds cumulative work/nodes or manufactures credential cleanup.
	Custody interface{ Clear() } `json:"-"`
}

func (PreparedAPIDataV1) String() string     { return "PreparedAPIDataV1{data:[redacted]}" }
func (v PreparedAPIDataV1) GoString() string { return v.String() }

// PreparedAPIParentV1 is trusted engineering DI, NOT child/wire configuration.
// Prepare owns the full original canonical/native/profile/fit admission and
// original deadline provenance; returned DATA cannot certify those facts.
// Observe must use the genuine SAME authenticated owner/source transaction and
// sole requested RPC. Matching PiAPIClient metadata alone DOES NOT implement
// this contract. No shipped production composition supplies an implementation;
// ordinary constructors/materializers remain API-denying. Synthetic local
// collaborators qualify orchestration only, never serving/source/S/hard-F.
type PreparedAPIParentV1 interface {
	// Every nonnil returned record transfers SAME-parent staging retirement,
	// even when accompanied by an error or later found ineligible/expired.
	// This does not transfer permission to Clear caller-aliased buffers. The
	// parent must retain private resource ownership, not derive cleanup from
	// paths in an unvalidated record. With a nil record it retains cleanup.
	PrepareOriginalAPIInvocation(context.Context, InvocationIdentity, *VerifiedAttachedInputV1, AttemptSpec, PreparedInputAcquisitionV1) (*PreparedAPIDataV1, error)
	ObserveOriginalAPIPhase(context.Context, pisdkphasewire.Request) (pisdkphasewire.Reply, error)
	// RetireOriginalAPIStaging invokes the SAME original preparation's actual
	// resource owner after any acquired credential's Release attempt, or on
	// pre-Issue rejection, using its once-selected original
	// retirement context. Clear only relinquishes buffers and is NOT this proof.
	// A nil error from arbitrary DI is not provenance: the fixed trusted parent
	// must own the exact data/resources (including a proven empty resource set).
	// Missing/malformed retirement receives nil context, fails closed, and may
	// not mint a fresh grace; the parent must report unresolved resource cleanup.
	// No shipped production parent currently implements that complete contract.
	RetireOriginalAPIStaging(context.Context, InvocationIdentity, *PreparedAPIDataV1) error
}

func nilPreparedAPIDependency(value any) bool {
	if value == nil {
		return true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Func, reflect.Map, reflect.Slice, reflect.Chan:
		return v.IsNil()
	}
	return false
}

// NewPreparedAPIInvocationRunner is an explicit default-off library seam.
// Merely implementing the DATA interfaces does not supply source authority;
// normal production registration is intentionally untouched/unavailable.
func NewPreparedAPIInvocationRunner(executor PreparedProcessRunner, credentials ports.ProviderResourceCredentialLifecycleV1, parent PreparedAPIParentV1) (*InvocationRunner, error) {
	if nilPreparedAPIDependency(executor) || nilPreparedAPIDependency(credentials) || nilPreparedAPIDependency(parent) {
		return nil, ErrInvocationInvalid
	}
	finalizer, ok := credentials.(ports.ProviderCredentialImmutableFileFinalizerV1)
	if !ok || nilPreparedAPIDependency(finalizer) {
		return nil, ErrCredentialUnavailable
	}
	return &InvocationRunner{prepared: executor, apiCredentials: credentials, apiFinalizer: finalizer, apiParent: parent}, nil
}
