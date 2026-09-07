// Package attachedworkerdaemontransport adapts one authenticated attached
// worker session to the daemon Source and ResultSink ports. It deliberately
// remains feature-disabled composition: the materializer and session are
// injected contracts, and no production foreground constructor wires them.
package attachedworkerdaemontransport

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
	"gitcode.com/urandon/sessionless/internal/domain"
)

const (
	defaultMaxInputBytes   = 1 << 20
	maxProfileArguments    = 128
	maxProfileVariables    = 64
	maxProfileValueBytes   = 4096
	maxReadRoots           = 64
	maxPathBytes           = 4096
	maxTerminalStdoutBytes = 16 << 20
	maxTerminalStderrBytes = 1 << 20
	maxCommittedDigests    = 32
)

var (
	ErrInvalidConfiguration    = errors.New("attached worker daemon transport configuration is invalid")
	ErrInvalidAuthority        = errors.New("attached worker daemon transport authority is invalid")
	ErrAttemptActive           = errors.New("attached worker daemon transport attempt is already active")
	ErrAttemptUnavailable      = errors.New("attached worker daemon transport attempt is unavailable")
	ErrAttemptCancelled        = errors.New("attached worker daemon transport attempt was cancelled before execution")
	ErrMaterializationFailed   = errors.New("attached worker daemon transport materialization failed")
	ErrReconciliationRequired  = errors.New("attached worker daemon transport requires reconciliation")
	ErrSessionFenced           = errors.New("attached worker daemon transport session is fenced")
	ErrTerminalEvidenceInvalid = errors.New("attached worker daemon transport terminal evidence is invalid")
)

var environmentNamePattern = regexp.MustCompile(`^[A-Z_][A-Z0-9_]*$`)

// SessionPort is intentionally semantic. The concrete Session owns raw frame
// envelopes, connection watermarks, validation, and bearer custody.
type SessionPort interface {
	ExchangeAction(context.Context, attachedworkersession.ActionV1) (*attachedworkerprotocol.FrameV1, error)
	Snapshot() attachedworkersession.SnapshotV1
}

// Materializer receives only accepted immutable authority. It cannot select a
// process executable, arguments, environment, or host path outside the
// configured materialization root.
type Materializer interface {
	Materialize(context.Context, MaterializationRequestV1) (MaterializedInputV1, error)
}

type LocalProfileV1 struct {
	Name             string
	CapabilityDigest domain.AttachedWorkerCapabilityDigest
	Executable       string
	ExecutableDigest attachedworkerdaemon.ExecutableDigest
	Arguments        []string
	Environment      []attachedworkerdaemon.EnvironmentVariable
}

func (profile LocalProfileV1) String() string {
	return fmt.Sprintf(
		"LocalProfileV1{name:%s capability:%t executable:[redacted] digest:%x arguments:%d environment:%d}",
		profile.Name, profile.CapabilityDigest != "", profile.ExecutableDigest, len(profile.Arguments), len(profile.Environment),
	)
}

func (profile LocalProfileV1) GoString() string { return profile.String() }

type Config struct {
	Profile                LocalProfileV1
	MaterializationRoot    string
	MaxInputBytes          int
	MaterializationTimeout time.Duration
	ReportTimeout          time.Duration
	Now                    func() time.Time
}

type MaterializationRequestV1 struct {
	TenantID             domain.TenantID
	OwnerUserID          domain.UserID
	WorkerID             domain.AttachedWorkerID
	EnrollmentGeneration uint64
	ConnectionGeneration uint64
	ConnectionID         domain.AttachedWorkerConnectionID
	Attempt              attachedworkerprotocol.AttemptBindingV1
	AttemptSequence      uint64
}

func (request MaterializationRequestV1) String() string {
	return fmt.Sprintf(
		"MaterializationRequestV1{scope:[redacted] generations:%d/%d attempt:[redacted] sequence:%d digests:[redacted]}",
		request.EnrollmentGeneration, request.ConnectionGeneration, request.AttemptSequence,
	)
}

func (request MaterializationRequestV1) GoString() string { return request.String() }

type MaterializedInputV1 struct {
	Stdin      []byte
	ReadRoots  []string
	Credential *attachedworkerdaemon.CredentialInvocation
}

func (input MaterializedInputV1) String() string {
	return fmt.Sprintf(
		"MaterializedInputV1{stdin:[redacted:%d] read_roots:%d credential:%t}",
		len(input.Stdin), len(input.ReadRoots), input.Credential != nil,
	)
}

func (input MaterializedInputV1) GoString() string { return input.String() }

type activeAttempt struct {
	request                     MaterializationRequestV1
	identity                    attachedworkerdaemon.InvocationIdentity
	nextWorkerAttemptSequence   uint64
	nextPlatformAttemptSequence uint64
	cancelRevision              uint64
}

type completedAttempt struct {
	identity    attachedworkerdaemon.InvocationIdentity
	fingerprint [sha256.Size]byte
}

type Adapter struct {
	session      SessionPort
	materializer Materializer
	config       Config
	gate         chan struct{}

	mu     sync.Mutex
	active *activeAttempt
	last   *completedAttempt
}

func New(session SessionPort, materializer Materializer, config Config) (*Adapter, error) {
	if session == nil || materializer == nil {
		return nil, ErrInvalidConfiguration
	}
	config.Profile = cloneProfile(config.Profile)
	if config.MaxInputBytes == 0 {
		config.MaxInputBytes = defaultMaxInputBytes
	}
	if config.ReportTimeout == 0 {
		config.ReportTimeout = 15 * time.Second
	}
	if config.MaterializationTimeout == 0 {
		config.MaterializationTimeout = 30 * time.Second
	}
	if config.Now == nil {
		config.Now = func() time.Time { return time.Now().UTC() }
	}
	canonicalRoot, err := canonicalDirectory(config.MaterializationRoot)
	if err != nil || canonicalRoot != config.MaterializationRoot {
		return nil, ErrInvalidConfiguration
	}
	if validateConfig(config) != nil {
		return nil, ErrInvalidConfiguration
	}
	adapter := &Adapter{session: session, materializer: materializer, config: config, gate: make(chan struct{}, 1)}
	adapter.gate <- struct{}{}
	return adapter, nil
}

func (adapter *Adapter) Next(ctx context.Context) (attachedworkerdaemon.Invocation, bool, error) {
	if adapter == nil || ctx == nil {
		return attachedworkerdaemon.Invocation{}, false, ErrInvalidConfiguration
	}
	if err := adapter.acquire(ctx); err != nil {
		return attachedworkerdaemon.Invocation{}, false, err
	}
	releaseGate := true
	defer func() {
		if releaseGate {
			adapter.release()
		}
	}()
	if adapter.currentActive() != nil {
		return attachedworkerdaemon.Invocation{}, false, ErrAttemptActive
	}
	snapshot, err := adapter.readySnapshot()
	if err != nil {
		return attachedworkerdaemon.Invocation{}, false, err
	}
	response, err := adapter.session.ExchangeAction(ctx, attachedworkersession.ActionV1{Heartbeat: &attachedworkerprotocol.HeartbeatV1{
		ObservedAtUnixMicro: adapter.config.Now().UTC().UnixMicro(), Available: true, ActiveAttempts: 0,
	}})
	if err != nil {
		return attachedworkerdaemon.Invocation{}, false, classifySessionError(err)
	}
	if response == nil {
		return attachedworkerdaemon.Invocation{}, false, nil
	}
	if !frameMatchesSnapshot(*response, snapshot) {
		return attachedworkerdaemon.Invocation{}, false, ErrInvalidAuthority
	}
	switch response.Kind {
	case attachedworkerprotocol.MessageLeaseOffer:
		return adapter.claimAndMaterialize(ctx, snapshot, *response.LeaseOffer, &releaseGate)
	case attachedworkerprotocol.MessageRevoke:
		_, ackErr := adapter.session.ExchangeAction(ctx, attachedworkersession.ActionV1{Revoked: &attachedworkerprotocol.RevokedV1{
			Revision: response.Revoke.Revision, NextEnrollmentGeneration: response.Revoke.NextEnrollmentGeneration,
			NextConnectionGeneration: response.Revoke.NextConnectionGeneration,
		}})
		return attachedworkerdaemon.Invocation{}, false, errors.Join(ErrSessionFenced, classifySessionError(ackErr))
	case attachedworkerprotocol.MessageDrain:
		_, ackErr := adapter.session.ExchangeAction(ctx, attachedworkersession.ActionV1{Drained: &attachedworkerprotocol.DrainedV1{Revision: response.Drain.Revision}})
		if ackErr != nil {
			return attachedworkerdaemon.Invocation{}, false, classifySessionError(ackErr)
		}
		return attachedworkerdaemon.Invocation{}, false, ErrAttemptUnavailable
	default:
		return attachedworkerdaemon.Invocation{}, false, ErrInvalidAuthority
	}
}

func (adapter *Adapter) Complete(
	ctx context.Context,
	identity attachedworkerdaemon.InvocationIdentity,
	result attachedworkerdaemon.InvocationResult,
	runErr error,
) error {
	if adapter == nil || ctx == nil {
		return ErrInvalidConfiguration
	}
	if err := adapter.acquire(ctx); err != nil {
		return err
	}
	defer adapter.release()
	return adapter.completeOwned(ctx, identity, result, runErr)
}

func (adapter *Adapter) claimAndMaterialize(
	ctx context.Context,
	snapshot attachedworkersession.SnapshotV1,
	offer attachedworkerprotocol.LeaseOfferV1,
	releaseGate *bool,
) (attachedworkerdaemon.Invocation, bool, error) {
	if offer.Validate() != nil || offer.AttemptSequence != 1 || !adapter.attemptMatchesSnapshot(offer.Binding, snapshot) {
		return attachedworkerdaemon.Invocation{}, false, ErrInvalidAuthority
	}
	claim := attachedworkerprotocol.LeaseClaimV1{Binding: cloneAttemptBinding(offer.Binding), AttemptSequence: offer.AttemptSequence}
	response, err := adapter.session.ExchangeAction(ctx, attachedworkersession.ActionV1{LeaseClaim: &claim})
	if err != nil {
		return attachedworkerdaemon.Invocation{}, false, errors.Join(ErrReconciliationRequired, classifySessionError(err))
	}
	if response == nil || !frameMatchesSnapshot(*response, snapshot) {
		return attachedworkerdaemon.Invocation{}, false, ErrReconciliationRequired
	}
	request := materializationRequest(snapshot, offer.Binding, offer.AttemptSequence)
	active, err := newActiveAttempt(request)
	if err != nil {
		return attachedworkerdaemon.Invocation{}, false, err
	}
	switch response.Kind {
	case attachedworkerprotocol.MessageLeaseAccepted:
		accepted := response.LeaseAccepted
		if accepted == nil || accepted.Validate() != nil || accepted.AttemptSequence != offer.AttemptSequence+1 ||
			!sameAttemptBinding(accepted.Binding, offer.Binding) {
			return attachedworkerdaemon.Invocation{}, false, ErrInvalidAuthority
		}
		active.nextWorkerAttemptSequence = offer.AttemptSequence + 1
		active.nextPlatformAttemptSequence = accepted.AttemptSequence + 1
		adapter.setActive(active)
	case attachedworkerprotocol.MessageCancel:
		cancel := response.Cancel
		if cancel == nil || cancel.Validate() != nil || cancel.AttemptSequence != offer.AttemptSequence+1 ||
			!sameAttemptBinding(cancel.Binding, offer.Binding) {
			return attachedworkerdaemon.Invocation{}, false, ErrInvalidAuthority
		}
		active.nextWorkerAttemptSequence = offer.AttemptSequence + 1
		active.nextPlatformAttemptSequence = cancel.AttemptSequence + 1
		active.cancelRevision = cancel.CancelRevision
		adapter.setActive(active)
		if err := adapter.acknowledgeCancel(ctx, active, *cancel); err != nil {
			return attachedworkerdaemon.Invocation{}, false, err
		}
		reportCtx, cancelReport := context.WithTimeout(context.WithoutCancel(ctx), adapter.config.ReportTimeout)
		reportErr := adapter.completeOwned(reportCtx, active.identity, attachedworkerdaemon.InvocationResult{
			Process:     attachedworkerdaemon.AttemptResult{Cancelled: true, DescendantsReaped: true, BoundaryReleased: true, CleanupSucceeded: true},
			FailureCode: "cancelled_before_materialization",
		}, context.Canceled)
		cancelReport()
		return attachedworkerdaemon.Invocation{}, false, errors.Join(ErrAttemptCancelled, reportErr)
	default:
		return attachedworkerdaemon.Invocation{}, false, ErrInvalidAuthority
	}

	input, materializeErr, materializationAmbiguous := adapter.materializeOwned(ctx, cloneMaterializationRequest(request), releaseGate)
	if materializationAmbiguous {
		return attachedworkerdaemon.Invocation{}, false, errors.Join(ErrMaterializationFailed, ErrReconciliationRequired, materializeErr)
	}
	defer clearMaterializedInput(&input)
	if materializeErr != nil || ctx.Err() != nil {
		failureCode := "invocation_materialization_failed"
		var cancellationErr error
		causalErr := materializeErr
		if ctx.Err() != nil {
			causalErr = ctx.Err()
		}
		if errors.Is(causalErr, context.Canceled) {
			failureCode = "invocation_materialization_cancelled"
			cancellationErr = causalErr
		} else if errors.Is(causalErr, context.DeadlineExceeded) {
			failureCode = "invocation_materialization_deadline"
			cancellationErr = causalErr
		}
		reportCtx, cancelReport := context.WithTimeout(context.WithoutCancel(ctx), adapter.config.ReportTimeout)
		reportErr := adapter.completeOwned(reportCtx, active.identity, attachedworkerdaemon.InvocationResult{FailureCode: failureCode}, materializeErr)
		cancelReport()
		return attachedworkerdaemon.Invocation{}, false, errors.Join(ErrMaterializationFailed, cancellationErr, reportErr)
	}
	if authorityErr := adapter.validateActiveAuthority(active); authorityErr != nil {
		return attachedworkerdaemon.Invocation{}, false, authorityErr
	}
	invocation, err := adapter.invocationFromInput(active.identity, input)
	if err != nil {
		reportCtx, cancelReport := context.WithTimeout(context.WithoutCancel(ctx), adapter.config.ReportTimeout)
		reportErr := adapter.completeOwned(reportCtx, active.identity, attachedworkerdaemon.InvocationResult{FailureCode: "invocation_materialization_invalid"}, err)
		cancelReport()
		return attachedworkerdaemon.Invocation{}, false, errors.Join(ErrMaterializationFailed, reportErr)
	}
	return invocation, true, nil
}

func (adapter *Adapter) acknowledgeCancel(ctx context.Context, active *activeAttempt, cancel attachedworkerprotocol.CancelV1) error {
	ack := attachedworkerprotocol.CancelAckV1{
		Binding: cloneAttemptBinding(cancel.Binding), AttemptSequence: active.nextWorkerAttemptSequence,
		CancelRevision: cancel.CancelRevision,
	}
	response, err := adapter.session.ExchangeAction(ctx, attachedworkersession.ActionV1{CancelAck: &ack})
	if err != nil || response != nil {
		return errors.Join(ErrReconciliationRequired, classifySessionError(err))
	}
	active.nextWorkerAttemptSequence++
	return nil
}

func (adapter *Adapter) invocationFromInput(
	identity attachedworkerdaemon.InvocationIdentity,
	input MaterializedInputV1,
) (attachedworkerdaemon.Invocation, error) {
	if len(input.Stdin) == 0 || len(input.Stdin) > adapter.config.MaxInputBytes {
		return attachedworkerdaemon.Invocation{}, ErrInvalidAuthority
	}
	readRoots, err := validateReadRoots(adapter.config.MaterializationRoot, input.ReadRoots)
	if err != nil {
		return attachedworkerdaemon.Invocation{}, err
	}
	credential, err := cloneCredential(input.Credential)
	if err != nil {
		return attachedworkerdaemon.Invocation{}, err
	}
	invocation := attachedworkerdaemon.Invocation{
		Identity: identity,
		Process: attachedworkerdaemon.AttemptSpec{
			Executable: adapter.config.Profile.Executable, ExecutableDigest: adapter.config.Profile.ExecutableDigest,
			Arguments:           append([]string(nil), adapter.config.Profile.Arguments...),
			Environment:         append([]attachedworkerdaemon.EnvironmentVariable(nil), adapter.config.Profile.Environment...),
			AdditionalReadRoots: readRoots, Stdin: append([]byte(nil), input.Stdin...),
		},
		Credential: credential,
	}
	if invocation.Validate() != nil {
		clearInvocation(&invocation)
		return attachedworkerdaemon.Invocation{}, ErrInvalidAuthority
	}
	return invocation, nil
}

func (adapter *Adapter) completeOwned(
	ctx context.Context,
	identity attachedworkerdaemon.InvocationIdentity,
	result attachedworkerdaemon.InvocationResult,
	runErr error,
) error {
	active := adapter.currentActive()
	if validateInvocationResult(result) != nil {
		return ErrTerminalEvidenceInvalid
	}
	fingerprint, err := completionFingerprint(identity, result, runErr)
	if err != nil {
		return ErrTerminalEvidenceInvalid
	}
	if active == nil {
		adapter.mu.Lock()
		last := adapter.last
		adapter.mu.Unlock()
		if last != nil && last.identity == identity && last.fingerprint == fingerprint {
			return nil
		}
		return ErrAttemptUnavailable
	}
	if active.identity != identity || identity.Validate() != nil || active.nextWorkerAttemptSequence == 0 ||
		active.nextWorkerAttemptSequence == math.MaxUint64 || active.nextPlatformAttemptSequence == 0 ||
		active.nextPlatformAttemptSequence == math.MaxUint64 {
		return ErrInvalidAuthority
	}
	if authorityErr := adapter.validateActiveAuthority(active); authorityErr != nil {
		return authorityErr
	}
	status, terminalResult := classifyTerminal(active.cancelRevision, result, runErr)
	evidence, err := terminalEvidenceDigest(active.request, identity, result, runErr, status, terminalResult)
	if err != nil {
		return ErrTerminalEvidenceInvalid
	}
	terminal := attachedworkerprotocol.TerminalV1{
		Binding: cloneAttemptBinding(active.request.Attempt), AttemptSequence: active.nextWorkerAttemptSequence,
		TerminalSequence: 1, Status: status, Result: terminalResult, EvidenceDigest: evidence,
	}
	response, exchangeErr := adapter.session.ExchangeAction(ctx, attachedworkersession.ActionV1{Terminal: &terminal})
	if exchangeErr != nil || response == nil {
		return errors.Join(ErrReconciliationRequired, classifySessionError(exchangeErr))
	}
	if !frameMatchesCurrentSession(*response, adapter.session.Snapshot()) || response.Kind != attachedworkerprotocol.MessageTerminalAck ||
		response.TerminalAck == nil || !terminalMatchesAck(terminal, *response.TerminalAck, active.nextPlatformAttemptSequence) {
		return ErrReconciliationRequired
	}
	adapter.mu.Lock()
	adapter.active = nil
	adapter.last = &completedAttempt{identity: identity, fingerprint: fingerprint}
	adapter.mu.Unlock()
	return nil
}

func (adapter *Adapter) readySnapshot() (attachedworkersession.SnapshotV1, error) {
	snapshot := adapter.session.Snapshot()
	if snapshot.Version != attachedworkersession.SnapshotVersionV1 || snapshot.State != attachedworkersession.StateReady ||
		snapshot.TenantID.Validate() != nil ||
		snapshot.OwnerUserID.Validate() != nil || snapshot.WorkerID.Validate() != nil ||
		snapshot.EnrollmentGeneration == 0 || snapshot.ConnectionGeneration == 0 ||
		snapshot.ProtocolVersion != attachedworkerprotocol.ProtocolVersionV1 || snapshot.ConnectionID.Validate() != nil ||
		snapshot.CapabilityDigest.Validate() != nil || snapshot.AuthenticationExpires == nil ||
		!adapter.config.Now().UTC().Before(snapshot.AuthenticationExpires.UTC()) {
		return attachedworkersession.SnapshotV1{}, ErrSessionFenced
	}
	if snapshot.CapabilityDigest != adapter.config.Profile.CapabilityDigest {
		return attachedworkersession.SnapshotV1{}, ErrInvalidAuthority
	}
	return snapshot, nil
}

func (adapter *Adapter) attemptMatchesSnapshot(
	binding attachedworkerprotocol.AttemptBindingV1,
	snapshot attachedworkersession.SnapshotV1,
) bool {
	if binding.Validate() != nil || adapter.config.Now().UTC().UnixMicro() >= binding.ExpiresAtUnixMicro ||
		snapshot.AuthenticationExpires == nil || binding.ExpiresAtUnixMicro > snapshot.AuthenticationExpires.UTC().UnixMicro() {
		return false
	}
	digest, err := hex.DecodeString(string(snapshot.CapabilityDigest))
	return err == nil && bytes.Equal(binding.CapabilityDigest, digest)
}

func (adapter *Adapter) validateActiveAuthority(active *activeAttempt) error {
	if active == nil {
		return ErrAttemptUnavailable
	}
	snapshot, err := adapter.readySnapshot()
	if err != nil {
		return errors.Join(ErrReconciliationRequired, err)
	}
	request := active.request
	if request.TenantID != snapshot.TenantID || request.OwnerUserID != snapshot.OwnerUserID ||
		request.WorkerID != snapshot.WorkerID || request.EnrollmentGeneration != snapshot.EnrollmentGeneration ||
		request.ConnectionGeneration != snapshot.ConnectionGeneration || request.ConnectionID != snapshot.ConnectionID ||
		!adapter.attemptMatchesSnapshot(request.Attempt, snapshot) {
		return errors.Join(ErrReconciliationRequired, ErrInvalidAuthority)
	}
	return nil
}

func materializationRequest(
	snapshot attachedworkersession.SnapshotV1,
	binding attachedworkerprotocol.AttemptBindingV1,
	attemptSequence uint64,
) MaterializationRequestV1 {
	return MaterializationRequestV1{
		TenantID: snapshot.TenantID, OwnerUserID: snapshot.OwnerUserID, WorkerID: snapshot.WorkerID,
		EnrollmentGeneration: snapshot.EnrollmentGeneration, ConnectionGeneration: snapshot.ConnectionGeneration,
		ConnectionID: snapshot.ConnectionID, Attempt: cloneAttemptBinding(binding), AttemptSequence: attemptSequence,
	}
}

func newActiveAttempt(request MaterializationRequestV1) (*activeAttempt, error) {
	request = cloneMaterializationRequest(request)
	identity := attachedworkerdaemon.InvocationIdentity{
		TenantID: request.TenantID, OwnerUserID: request.OwnerUserID, WorkerID: request.WorkerID,
		RunID: domain.RunID(request.Attempt.RunID), AttemptID: domain.AttemptID(request.Attempt.AttemptID),
		LeaseID: domain.LeaseID(request.Attempt.LeaseID), FenceToken: request.Attempt.LeaseGeneration,
	}
	if identity.Validate() != nil || request.AttemptSequence == 0 {
		return nil, ErrInvalidAuthority
	}
	return &activeAttempt{request: request, identity: identity}, nil
}

func validateConfig(config Config) error {
	profile := config.Profile
	if domain.ValidateOpaqueID("attached_worker.local_profile.name", profile.Name) != nil ||
		profile.CapabilityDigest.Validate() != nil || profile.ExecutableDigest == (attachedworkerdaemon.ExecutableDigest{}) ||
		!filepath.IsAbs(profile.Executable) || filepath.Clean(profile.Executable) != profile.Executable || len(profile.Executable) > maxPathBytes ||
		len(profile.Arguments) == 0 || len(profile.Arguments) > maxProfileArguments ||
		len(profile.Environment) > maxProfileVariables || !filepath.IsAbs(config.MaterializationRoot) ||
		filepath.Clean(config.MaterializationRoot) != config.MaterializationRoot || config.MaxInputBytes <= 0 ||
		config.MaxInputBytes > attachedworkerprotocol.MaxBatchBytes*64 || config.MaterializationTimeout <= 0 ||
		config.MaterializationTimeout > time.Minute || config.ReportTimeout <= 0 ||
		config.ReportTimeout > time.Minute || config.Now == nil {
		return ErrInvalidConfiguration
	}
	for _, argument := range profile.Arguments {
		if argument == "" || len(argument) > maxProfileValueBytes || strings.ContainsRune(argument, '\x00') {
			return ErrInvalidConfiguration
		}
	}
	seen := make(map[string]struct{}, len(profile.Environment))
	for _, variable := range profile.Environment {
		if !environmentNamePattern.MatchString(variable.Name) || reservedEnvironmentName(variable.Name) || len(variable.Value) > maxProfileValueBytes ||
			strings.ContainsRune(variable.Value, '\x00') {
			return ErrInvalidConfiguration
		}
		if _, exists := seen[variable.Name]; exists {
			return ErrInvalidConfiguration
		}
		seen[variable.Name] = struct{}{}
	}
	return nil
}

func validateReadRoots(root string, candidates []string) ([]string, error) {
	if len(candidates) > maxReadRoots {
		return nil, ErrInvalidAuthority
	}
	result := make([]string, 0, len(candidates))
	seen := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		if !filepath.IsAbs(candidate) || filepath.Clean(candidate) != candidate || len(candidate) > maxPathBytes {
			return nil, ErrInvalidAuthority
		}
		canonical, err := canonicalDirectory(candidate)
		if err != nil || canonical != candidate {
			return nil, ErrInvalidAuthority
		}
		relative, err := filepath.Rel(root, canonical)
		if err != nil || relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return nil, ErrInvalidAuthority
		}
		if _, exists := seen[canonical]; exists {
			return nil, ErrInvalidAuthority
		}
		seen[canonical] = struct{}{}
		result = append(result, canonical)
	}
	return result, nil
}

func canonicalDirectory(value string) (string, error) {
	if value == "" || len(value) > maxPathBytes || !filepath.IsAbs(value) || filepath.Clean(value) != value {
		return "", ErrInvalidAuthority
	}
	canonical, err := filepath.EvalSymlinks(value)
	if err != nil || !filepath.IsAbs(canonical) || filepath.Clean(canonical) != canonical {
		return "", ErrInvalidAuthority
	}
	info, err := os.Stat(canonical)
	if err != nil || !info.IsDir() {
		return "", ErrInvalidAuthority
	}
	return canonical, nil
}

func frameMatchesSnapshot(frame attachedworkerprotocol.FrameV1, snapshot attachedworkersession.SnapshotV1) bool {
	return frame.Validate() == nil && frameMatchesCurrentSession(frame, snapshot)
}

func frameMatchesCurrentSession(frame attachedworkerprotocol.FrameV1, snapshot attachedworkersession.SnapshotV1) bool {
	return frame.Version == snapshot.ProtocolVersion && frame.WorkerID == string(snapshot.WorkerID) &&
		frame.EnrollmentGeneration == snapshot.EnrollmentGeneration && frame.ConnectionGeneration == snapshot.ConnectionGeneration
}

func sameAttemptBinding(left, right attachedworkerprotocol.AttemptBindingV1) bool {
	return left.RunID == right.RunID && left.AttemptID == right.AttemptID && left.LeaseID == right.LeaseID &&
		left.LeaseGeneration == right.LeaseGeneration && left.FenceToken == right.FenceToken &&
		left.ExpiresAtUnixMicro == right.ExpiresAtUnixMicro && bytes.Equal(left.ContextDigest, right.ContextDigest) &&
		bytes.Equal(left.CapabilityDigest, right.CapabilityDigest) && bytes.Equal(left.PolicyDigest, right.PolicyDigest)
}

func terminalMatchesAck(terminal attachedworkerprotocol.TerminalV1, ack attachedworkerprotocol.TerminalAckV1, expectedAttemptSequence uint64) bool {
	return ack.Validate() == nil && sameAttemptBinding(terminal.Binding, ack.Binding) &&
		ack.AttemptSequence == expectedAttemptSequence && terminal.TerminalSequence == ack.TerminalSequence &&
		terminal.Status == ack.Status && terminal.Result == ack.Result && bytes.Equal(terminal.EvidenceDigest, ack.EvidenceDigest)
}

func classifyTerminal(
	cancelRevision uint64,
	result attachedworkerdaemon.InvocationResult,
	runErr error,
) (attachedworkerprotocol.TerminalStatus, attachedworkerprotocol.TerminalResult) {
	process := result.Process
	if cancelRevision > 0 && process.Cancelled {
		return attachedworkerprotocol.TerminalCancelled, attachedworkerprotocol.TerminalResultCancelled
	}
	if runErr == nil && !process.Cancelled && !process.Deadline && process.ExitCode == 0 &&
		process.FailureCode == "" && result.FailureCode == "" && process.DescendantsReaped &&
		process.BoundaryReleased && process.CleanupSucceeded {
		return attachedworkerprotocol.TerminalSucceeded, attachedworkerprotocol.TerminalResultCompleted
	}
	return attachedworkerprotocol.TerminalFailed, attachedworkerprotocol.TerminalResultFailed
}

type terminalEvidenceV1 struct {
	Version              uint32   `json:"version"`
	TenantID             string   `json:"tenant_id"`
	OwnerUserID          string   `json:"owner_user_id"`
	WorkerID             string   `json:"worker_id"`
	RunID                string   `json:"run_id"`
	AttemptID            string   `json:"attempt_id"`
	LeaseID              string   `json:"lease_id"`
	LeaseGeneration      uint64   `json:"lease_generation"`
	ContextDigest        string   `json:"context_digest"`
	CapabilityDigest     string   `json:"capability_digest"`
	PolicyDigest         string   `json:"policy_digest"`
	Status               string   `json:"status"`
	Result               string   `json:"result"`
	ExitCode             int      `json:"exit_code"`
	Cancelled            bool     `json:"cancelled"`
	Deadline             bool     `json:"deadline"`
	TermSent             bool     `json:"term_sent"`
	KillSent             bool     `json:"kill_sent"`
	DescendantsReaped    bool     `json:"descendants_reaped"`
	StdoutBytes          int      `json:"stdout_bytes"`
	StderrBytes          int      `json:"stderr_bytes"`
	ProcessFailure       string   `json:"process_failure"`
	IsolationProfile     string   `json:"isolation_profile"`
	BoundaryReleased     bool     `json:"boundary_released"`
	CleanupSucceeded     bool     `json:"cleanup_succeeded"`
	CredentialChanged    bool     `json:"credential_changed"`
	CredentialGeneration uint64   `json:"credential_generation"`
	InvocationFailure    string   `json:"invocation_failure"`
	RunnerFailed         bool     `json:"runner_failed"`
	CommittedArtifacts   []string `json:"committed_artifacts"`
	CommittedEvents      []string `json:"committed_events"`
}

func terminalEvidenceDigest(
	request MaterializationRequestV1,
	identity attachedworkerdaemon.InvocationIdentity,
	result attachedworkerdaemon.InvocationResult,
	runErr error,
	status attachedworkerprotocol.TerminalStatus,
	terminalResult attachedworkerprotocol.TerminalResult,
) ([]byte, error) {
	if identity.Validate() != nil || request.Attempt.Validate() != nil {
		return nil, ErrTerminalEvidenceInvalid
	}
	processFailure := safeFailureCode(result.Process.FailureCode)
	invocationFailure := safeFailureCode(result.FailureCode)
	isolationProfile := safeFailureCode(result.Process.IsolationProfile)
	if (result.Process.FailureCode != "" && processFailure == "") ||
		(result.FailureCode != "" && invocationFailure == "") ||
		(result.Process.IsolationProfile != "" && isolationProfile == "") {
		return nil, ErrTerminalEvidenceInvalid
	}
	evidence := terminalEvidenceV1{
		Version: 1, TenantID: string(identity.TenantID), OwnerUserID: string(identity.OwnerUserID), WorkerID: string(identity.WorkerID),
		RunID: string(identity.RunID), AttemptID: string(identity.AttemptID), LeaseID: string(identity.LeaseID),
		LeaseGeneration: identity.FenceToken, ContextDigest: hex.EncodeToString(request.Attempt.ContextDigest),
		CapabilityDigest: hex.EncodeToString(request.Attempt.CapabilityDigest), PolicyDigest: hex.EncodeToString(request.Attempt.PolicyDigest),
		Status: string(status), Result: string(terminalResult), ExitCode: result.Process.ExitCode,
		Cancelled: result.Process.Cancelled, Deadline: result.Process.Deadline, TermSent: result.Process.TermSent,
		KillSent: result.Process.KillSent, DescendantsReaped: result.Process.DescendantsReaped,
		StdoutBytes: result.Process.StdoutBytes, StderrBytes: result.Process.StderrBytes,
		ProcessFailure: processFailure, IsolationProfile: isolationProfile,
		BoundaryReleased: result.Process.BoundaryReleased, CleanupSucceeded: result.Process.CleanupSucceeded,
		CredentialChanged: result.CredentialChanged, CredentialGeneration: result.CredentialGeneration,
		InvocationFailure: invocationFailure, RunnerFailed: runErr != nil,
		CommittedArtifacts: encodeCommittedDigests(result.CommittedArtifactDigests),
		CommittedEvents:    encodeCommittedDigests(result.CommittedEventDigests),
	}
	encoded, err := json.Marshal(evidence)
	if err != nil {
		return nil, ErrTerminalEvidenceInvalid
	}
	digest := sha256.Sum256(append([]byte("sessionless:attached-worker-terminal-evidence:v1\x00"), encoded...))
	return append([]byte(nil), digest[:]...), nil
}

func validateInvocationResult(result attachedworkerdaemon.InvocationResult) error {
	process := result.Process
	if process.StdoutBytes < 0 || process.StdoutBytes > maxTerminalStdoutBytes ||
		process.StderrBytes < 0 || process.StderrBytes > maxTerminalStderrBytes ||
		len(process.Stdout) > process.StdoutBytes || len(process.Stdout) > maxTerminalStdoutBytes ||
		process.Duration < 0 || (result.CredentialChanged && result.CredentialGeneration == 0) {
		return ErrTerminalEvidenceInvalid
	}
	if !validCommittedDigests(result.CommittedArtifactDigests) || !validCommittedDigests(result.CommittedEventDigests) {
		return ErrTerminalEvidenceInvalid
	}
	processFailure := safeFailureCode(process.FailureCode)
	invocationFailure := safeFailureCode(result.FailureCode)
	isolationProfile := safeFailureCode(process.IsolationProfile)
	if (process.FailureCode != "" && processFailure == "") ||
		(result.FailureCode != "" && invocationFailure == "") ||
		(process.IsolationProfile != "" && isolationProfile == "") {
		return ErrTerminalEvidenceInvalid
	}
	return nil
}

func completionFingerprint(
	identity attachedworkerdaemon.InvocationIdentity,
	result attachedworkerdaemon.InvocationResult,
	runErr error,
) ([sha256.Size]byte, error) {
	type completion struct {
		Identity attachedworkerdaemon.InvocationIdentity `json:"identity"`
		Result   attachedworkerdaemon.InvocationResult   `json:"result"`
		Failed   bool                                    `json:"failed"`
	}
	owned := result
	owned.Process.Stdout = nil
	owned.CommittedArtifactDigests = append([]attachedworkerdaemon.CommittedEvidenceDigest{}, result.CommittedArtifactDigests...)
	owned.CommittedEventDigests = append([]attachedworkerdaemon.CommittedEvidenceDigest{}, result.CommittedEventDigests...)
	encoded, err := json.Marshal(completion{Identity: identity, Result: owned, Failed: runErr != nil})
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return sha256.Sum256(append([]byte("sessionless:attached-worker-completion:v1\x00"), encoded...)), nil
}

func validCommittedDigests(values []attachedworkerdaemon.CommittedEvidenceDigest) bool {
	if len(values) > maxCommittedDigests {
		return false
	}
	var previous attachedworkerdaemon.CommittedEvidenceDigest
	for index, value := range values {
		if value == (attachedworkerdaemon.CommittedEvidenceDigest{}) ||
			(index > 0 && bytes.Compare(previous[:], value[:]) >= 0) {
			return false
		}
		previous = value
	}
	return true
}

func encodeCommittedDigests(values []attachedworkerdaemon.CommittedEvidenceDigest) []string {
	encoded := make([]string, len(values))
	for index, value := range values {
		encoded[index] = hex.EncodeToString(value[:])
	}
	return encoded
}

func safeFailureCode(value string) string {
	if value == "" {
		return ""
	}
	if domain.ValidateOpaqueID("failure_code", value) != nil {
		return ""
	}
	return value
}

func cloneProfile(profile LocalProfileV1) LocalProfileV1 {
	profile.Arguments = append([]string(nil), profile.Arguments...)
	profile.Environment = append([]attachedworkerdaemon.EnvironmentVariable(nil), profile.Environment...)
	return profile
}

func cloneAttemptBinding(binding attachedworkerprotocol.AttemptBindingV1) attachedworkerprotocol.AttemptBindingV1 {
	binding.ContextDigest = append([]byte(nil), binding.ContextDigest...)
	binding.CapabilityDigest = append([]byte(nil), binding.CapabilityDigest...)
	binding.PolicyDigest = append([]byte(nil), binding.PolicyDigest...)
	return binding
}

func cloneMaterializationRequest(request MaterializationRequestV1) MaterializationRequestV1 {
	request.Attempt = cloneAttemptBinding(request.Attempt)
	return request
}

type materializationOutcome struct {
	input MaterializedInputV1
	err   error
}

func (adapter *Adapter) materializeOwned(
	ctx context.Context,
	request MaterializationRequestV1,
	releaseGate *bool,
) (MaterializedInputV1, error, bool) {
	materializeCtx, cancel := context.WithTimeout(ctx, adapter.config.MaterializationTimeout)
	result := make(chan materializationOutcome, 1)
	go func() {
		input, err := adapter.materializer.Materialize(materializeCtx, request)
		result <- materializationOutcome{input: input, err: err}
	}()
	select {
	case outcome := <-result:
		operationErr := materializeCtx.Err()
		cancel()
		if operationErr != nil {
			clearMaterializedInput(&outcome.input)
			return MaterializedInputV1{}, operationErr, false
		}
		return outcome.input, outcome.err, false
	case <-materializeCtx.Done():
		operationErr := materializeCtx.Err()
		cancel()
		if releaseGate != nil {
			*releaseGate = false
		}
		go func() {
			outcome := <-result
			clearMaterializedInput(&outcome.input)
			adapter.release()
		}()
		return MaterializedInputV1{}, operationErr, true
	}
}

func cloneCredential(value *attachedworkerdaemon.CredentialInvocation) (*attachedworkerdaemon.CredentialInvocation, error) {
	if value == nil {
		return nil, nil
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, ErrInvalidAuthority
	}
	var result attachedworkerdaemon.CredentialInvocation
	if err := json.Unmarshal(encoded, &result); err != nil {
		return nil, ErrInvalidAuthority
	}
	return &result, nil
}

func clearMaterializedInput(input *MaterializedInputV1) {
	if input == nil {
		return
	}
	clearBytes(input.Stdin)
	*input = MaterializedInputV1{}
}

func clearInvocation(invocation *attachedworkerdaemon.Invocation) {
	if invocation == nil {
		return
	}
	clearBytes(invocation.Process.Stdin)
	*invocation = attachedworkerdaemon.Invocation{}
}

func clearBytes(value []byte) {
	for index := range value {
		value[index] = 0
	}
}

func classifySessionError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, attachedworkersession.ErrSessionFenced) {
		return ErrSessionFenced
	}
	if errors.Is(err, attachedworkersession.ErrReconciliationRequired) {
		return ErrReconciliationRequired
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return ErrInvalidAuthority
}

func reservedEnvironmentName(name string) bool {
	switch name {
	case "HOME", "TMPDIR", "TMP", "TEMP", "PATH", "LANG", "LC_ALL", "NO_COLOR",
		"XDG_CONFIG_HOME", "XDG_CACHE_HOME", "XDG_DATA_HOME", "OPENAI_API_KEY",
		"ANTHROPIC_API_KEY", "GEMINI_API_KEY", "GOOGLE_API_KEY":
		return true
	}
	return strings.HasSuffix(name, "_API_KEY") || strings.HasSuffix(name, "_ACCESS_TOKEN") ||
		strings.HasSuffix(name, "_SECRET")
}

func (adapter *Adapter) acquire(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-adapter.gate:
		return nil
	}
}

func (adapter *Adapter) release() { adapter.gate <- struct{}{} }

func (adapter *Adapter) currentActive() *activeAttempt {
	adapter.mu.Lock()
	defer adapter.mu.Unlock()
	return adapter.active
}

func (adapter *Adapter) setActive(active *activeAttempt) {
	adapter.mu.Lock()
	adapter.active = active
	adapter.mu.Unlock()
}

func (adapter *Adapter) String() string {
	if adapter == nil {
		return "Adapter{state:invalid}"
	}
	adapter.mu.Lock()
	active, completed := adapter.active != nil, adapter.last != nil
	adapter.mu.Unlock()
	return fmt.Sprintf("Adapter{active:%t completed:%t authority:[redacted]}", active, completed)
}

func (adapter *Adapter) GoString() string { return adapter.String() }

var _ attachedworkerdaemon.Source = (*Adapter)(nil)
var _ attachedworkerdaemon.ResultSink = (*Adapter)(nil)
