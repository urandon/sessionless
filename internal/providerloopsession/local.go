// Package providerloopsession coordinates the original entered invocation and
// its existing per-operation owners. It is not a serving registration, model
// qualification, token-fit proof, scheduler or another accounting authority.
package providerloopsession

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"sync"
	"time"

	"gitcode.com/urandon/sessionless/internal/configuredinference"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localloopback"
	"gitcode.com/urandon/sessionless/internal/localoperator"
	"gitcode.com/urandon/sessionless/internal/operationbody"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

var (
	ErrSession    = errors.New("local provider session unavailable")
	ErrBusy       = errors.New("local provider session already active")
	ErrIncomplete = errors.New("local model operation did not produce a verified durable success")
)

// LocalConfigV1 is trusted worker composition only. Source and DB are borrowed;
// this factory creates and owns its sole Store/bootstrap/bridge/keeper tuple.
// No credential lifecycle is accepted in the noauth arm. Complete source
// authenticity and executed calibrated token fit remain composer obligations;
// no public serving registry or real profile is enabled by this package.
type LocalConfigV1 struct {
	Source       *localoperator.Source
	DB           *sql.DB
	StoreOptions ydbstore.Options
	Prepare      *serverlessharness.CapabilityIssuer
	Operate      *serverlessharness.OperationCapabilityIssuer
	Backend      domain.HarnessBackendDescriptorV1
	Substrate    domain.SubstrateBindingV1
	Blobs        ports.BlobStore
}

// Copies share all entry/operation/close state. No caller-provided allocation,
// observation grant, body adapter or permission callback enters this session.
type LocalV1 struct{ state *localState }
type localState struct {
	mu                            sync.Mutex
	gate                          chan struct{}
	ctx                           context.Context
	caller                        context.Context
	cancel                        context.CancelFunc
	stopSource                    func() bool
	stopEntryCaller               func() bool
	entryCancel                   context.CancelFunc
	entryContext                  context.Context
	entryCaller                   context.Context
	entryHandoffCaller            context.Context
	entryRoute                    localEntryRoute
	workerState                   *localWorkerState
	executionCancel               context.CancelFunc
	execution                     context.Context
	profile                       domain.ConfiguredInferenceProfileV1
	bootstrap                     *ydbstore.LocalOwnedStoreBootstrapV1
	store                         *ydbstore.Store
	bridge                        *serverlessharness.BoundedOwnedAllocationPreparationV1
	keeper                        *localloopback.KeeperV1
	issuer                        *serverlessharness.OperationCapabilityIssuer
	blobs                         ports.BlobStore
	entry                         *ydbstore.LocalInvocationEntryV1
	parent                        domain.OwnedAttemptEffectParentV1
	source                        domain.AttemptExecutionSourceV1
	allocation                    domain.RegisteredAllocationV2
	bodies                        *operationbody.StoreV1
	started, attempted, closed    bool
	runtimeStarted, runtimeFailed bool
	closeOnce                     sync.Once
	closeErr                      error
	terminalRequested             bool
	terminalDeadline              time.Time
	terminalOutput                *PreparedLocalTerminalV1
}

// NewLocalV1 constructs inert owners only. There is no SQL, socket, provider or
// credential call. Start and the single-invocation worker facade reserve each
// actual parent through the SAME original bootstrap and Store.
func NewLocalV1(ctx context.Context, c LocalConfigV1) (*LocalV1, error) {
	if ctx == nil || ctx.Err() != nil || c.Source == nil || c.DB == nil || c.Blobs == nil ||
		c.StoreOptions.AttemptOperationBodies != nil || c.StoreOptions.OwnedOperationResidualCustody != nil {
		return nil, ErrSession
	}
	p, err := c.Source.Configuration()
	if err != nil || p.Auth != domain.ConfiguredInferenceAuthNoneV1 {
		return nil, ErrSession
	}
	lifetime, err := c.Source.ExecutionLifetime()
	if err != nil || lifetime.Err() != nil {
		return nil, ErrSession
	}
	b, err := ydbstore.NewLocalOwnedStoreBootstrapV1(c.Source, c.Prepare, c.Operate)
	if err != nil {
		return nil, ErrSession
	}
	k, err := localloopback.NewKeeperV1(c.Source, c.Operate, b)
	if err != nil {
		return nil, ErrSession
	}
	store, err := b.CreateStore(c.DB, c.StoreOptions, k)
	if err != nil {
		_ = k.Close()
		return nil, ErrSession
	}
	bridge, err := serverlessharness.NewBoundedOwnedLocalAllocationPreparationV1(c.Prepare, c.Backend, c.Substrate, c.Source, b)
	if err != nil {
		_ = k.Close()
		return nil, ErrSession
	}
	if err := b.Complete(bridge); err != nil {
		_ = bridge.Close()
		_ = k.Close()
		return nil, ErrSession
	}
	// Source revocation synchronously cancels the root. An asynchronous hook
	// alone would permit fresh body I/O after Source.Close has returned.
	call, cancel := context.WithCancel(lifetime)
	if deadline, ok := ctx.Deadline(); ok {
		cancel()
		call, cancel = context.WithDeadline(lifetime, deadline)
	}
	s := &localState{gate: make(chan struct{}, 1), ctx: call, caller: ctx, cancel: cancel, profile: p.Clone(),
		bootstrap: b, store: store, bridge: bridge, keeper: k, issuer: c.Operate, blobs: c.Blobs}
	s.stopSource = context.AfterFunc(ctx, cancel)
	if ctx.Err() != nil || lifetime.Err() != nil || call.Err() != nil {
		cancel()
		s.stopSource()
		_ = bridge.Close()
		_ = k.Close()
		return nil, ErrSession
	}
	return &LocalV1{state: s}, nil
}

// begin never holds a mutex across I/O. Closing first cancels the common
// lifetime, so it can interrupt an active operation before joining its gate.
func (v *LocalV1) begin(ctx context.Context) (*localState, context.Context, func(), error) {
	if v == nil || v.state == nil || ctx == nil || ctx.Err() != nil {
		return nil, nil, nil, ErrSession
	}
	s := v.state
	select {
	case s.gate <- struct{}{}:
	default:
		return nil, nil, nil, ErrBusy
	}
	s.mu.Lock()
	base := s.ctx
	if s.execution != nil {
		base = s.execution
	}
	closed := s.closed || s.terminalRequested || s.callersCanceled() || base.Err() != nil
	s.mu.Unlock()
	if closed {
		<-s.gate
		return nil, nil, nil, ErrSession
	}
	call, cancel := context.WithCancel(base)
	if deadline, ok := ctx.Deadline(); ok {
		cancel()
		call, cancel = context.WithDeadline(base, deadline)
	}
	stop := context.AfterFunc(ctx, cancel)
	if ctx.Err() != nil {
		cancel()
	}
	return s, call, func() { cancel(); stop(); <-s.gate }, nil
}

// Start reserves/burns/publishes/enters exactly once through the original
// bootstrap. A failed start cannot prepare a replacement or renew a window.
// The returned parent is immutable DATA for the trusted loop, not authority.
func (v *LocalV1) Start(ctx context.Context, request ports.ReserveAttemptEffectRequestV1) (domain.OwnedAttemptEffectParentV1, error) {
	s, call, finish, err := v.begin(ctx)
	if err != nil {
		return domain.OwnedAttemptEffectParentV1{}, err
	}
	defer finish()
	if s.started || s.entryRoute != localEntryUnselected {
		return domain.OwnedAttemptEffectParentV1{}, ErrSession
	}
	s.entryRoute = localEntryDirect
	s.started = true
	entryCtx, err := s.retainEntryContext(ctx)
	if err != nil {
		return domain.OwnedAttemptEffectParentV1{}, ErrSession
	}
	entry, err := s.bootstrap.StartLocalInvocation(entryCtx, request, s.blobs)
	if err != nil {
		s.cancel()
		return domain.OwnedAttemptEffectParentV1{}, ErrSession
	}
	return s.installEntry(ctx, call, entry)
}

// WriteModelRequest freezes the configured route's own codec and writes DATA
// into the original-parent-derived adapter. The caller must still establish
// calibrated token fit and construct the bounded canonical operation. This
// method neither reserves an operation nor returns a send capability.
func (v *LocalV1) WriteModelRequest(ctx context.Context, limits domain.ConfiguredInferenceLimitsV1, request configuredinference.RequestV1) (operationbody.ReferenceV1, error) {
	s, call, finish, err := v.begin(ctx)
	if err != nil {
		return operationbody.ReferenceV1{}, err
	}
	defer finish()
	if s.entry == nil || s.attempted {
		return operationbody.ReferenceV1{}, ErrSession
	}
	frozen, err := configuredinference.FreezeRequestV1(s.profile, limits, request)
	if err != nil {
		return operationbody.ReferenceV1{}, ErrSession
	}
	if err := s.entry.CheckFirstModelTerminalFit(call); err != nil {
		s.cancel()
		return operationbody.ReferenceV1{}, localPreparationError(ErrSession, err)
	}
	body := frozen.Body()
	defer clear(body)
	ref, err := s.bodies.Write(call, body)
	if err != nil {
		s.cancel()
		return operationbody.ReferenceV1{}, ErrSession
	}
	return ref, nil
}

type ModelResultV1 struct {
	SendAttempted bool
	Observation   ports.AuthenticatedAttemptOperationObservationV1
}

// ModelStepResultV1 keeps the optional compaction observation distinct from the
// actual MODEL result. A summarizer response is never an assistant answer.
type ModelStepResultV1 struct {
	Model      ModelResultV1
	Compaction *ModelResultV1
}

// A terminal-capacity denial is not context overflow. Preserve that closed
// classification without exposing underlying authority or storage errors.
func localPreparationError(base, cause error) error {
	if errors.Is(cause, ydbstore.ErrTerminalCapacity) {
		return errors.Join(base, ydbstore.ErrTerminalCapacity)
	}
	return base
}

// RunModelStep prepares the actual full ordinary request through the original
// entry. Only a classified context-no-fit may start one compaction, followed by
// publication and a MODEL prepared from the actual committed retained head.
// All effects use the same Store, issuer, body custody, allocation and keeper.
// It is a bounded step, not a tool loop, canonical finalizer or serving switch.
func (v *LocalV1) RunModelStep(ctx context.Context) (result ModelStepResultV1, err error) {
	s, call, finish, err := v.begin(ctx)
	if err != nil {
		return result, err
	}
	defer finish()
	if s.entry == nil || s.runtimeFailed || (s.attempted && !s.runtimeStarted) {
		return result, ErrSession
	}
	s.attempted, s.runtimeStarted = true, true
	defer func() {
		if err != nil {
			// An ambiguous or partial step cannot be retried through a wrapper
			// copy. Independent observation reconciliation never dispatches.
			s.runtimeFailed = true
			s.cancel()
		}
	}()
	model, prepareErr := s.entry.PrepareModel(call)
	if errors.Is(prepareErr, configuredinference.ErrContextNoFit) {
		generation, e := s.entry.PrepareCompaction(call)
		if e != nil {
			return result, localPreparationError(ErrIncomplete, e)
		}
		op, e := generation.Operation()
		if e != nil {
			return result, ErrIncomplete
		}
		frozen, e := generation.FrozenRequest()
		if e != nil {
			return result, ErrIncomplete
		}
		compaction, e := s.invokeFrozen(ctx, call, op, frozen)
		result.Compaction = &compaction
		if e != nil {
			return result, e
		}
		published, e := generation.Publish(call)
		if e != nil || published == nil {
			return result, ErrIncomplete
		}
		model, e = generation.PrepareNextModel(call)
		if e != nil {
			return result, localPreparationError(ErrIncomplete, e)
		}
		next, e := model.FrozenRequest()
		if e != nil || next == nil || next.Digest() != published.Digest() {
			return result, ErrIncomplete
		}
		expected, actual := published.Body(), next.Body()
		equal := bytes.Equal(expected, actual)
		clear(expected)
		clear(actual)
		if !equal {
			return result, ErrIncomplete
		}
	} else if prepareErr != nil {
		return result, localPreparationError(ErrIncomplete, prepareErr)
	}
	op, e := model.Operation()
	if e != nil {
		return result, ErrIncomplete
	}
	frozen, e := model.FrozenRequest()
	if e != nil {
		return result, ErrIncomplete
	}
	result.Model, err = s.invokeFrozen(ctx, call, op, frozen)
	return result, err
}

// InvokeFirstModel preserves the legacy explicit first-operation contract.
// It cannot be mixed with entry-prepared steps or accept caller-made successors.
func (v *LocalV1) InvokeFirstModel(ctx context.Context, operation domain.AttemptOperationV1, limits domain.ConfiguredInferenceLimitsV1, request configuredinference.RequestV1) (result ModelResultV1, err error) {
	s, call, finish, err := v.begin(ctx)
	if err != nil {
		return result, err
	}
	defer finish()
	if s.entry == nil || s.attempted || operation.Sequence != 1 || operation.Kind != domain.AttemptOperationModelV1 {
		return result, ErrSession
	}
	s.attempted = true // failures cannot be retried through another wrapper copy
	op := operation.Clone()
	if op.ValidateForParent(s.parent.Authority, s.parent.Reservation) != nil || op.Scope.SessionID != s.source.Scope.SessionID ||
		op.Deadline.After(s.allocation.Window.ExpiresAt) || request.MaxOutputTokens > op.Debit.OutputTokens {
		return result, ErrSession
	}
	frozen, err := configuredinference.FreezeRequestV1(s.profile, limits, request)
	if err != nil {
		return result, ErrSession
	}
	return s.invokeFrozen(ctx, call, op, frozen)
}

// This is the existing physical transport sequence, not an injected sender or
// new approval path. Store Reserve and BeforeSend still require their own exact
// original prepared holder for every runtime operation.
func (s *localState) invokeFrozen(caller, call context.Context, operation domain.AttemptOperationV1, frozen *configuredinference.FrozenRequestV1) (result ModelResultV1, err error) {
	op := operation.Clone()
	if frozen == nil || op.ValidateForParent(s.parent.Authority, s.parent.Reservation) != nil ||
		op.Scope.SessionID != s.source.Scope.SessionID || op.Deadline.After(s.allocation.Window.ExpiresAt) ||
		(op.Kind != domain.AttemptOperationModelV1 && op.Kind != domain.AttemptOperationCompactionV1) {
		return result, ErrSession
	}
	payload := frozen.Body()
	defer clear(payload)
	sum := sha256.Sum256(payload)
	if op.RequestDigest != hex.EncodeToString(sum[:]) || op.RequestBytes != uint64(len(payload)) {
		return result, ErrSession
	}
	stored, err := s.bodies.ReadAttemptOperationBody(call, op.Scope, op.RequestRef, op.RequestBytes)
	if err != nil {
		return result, ErrSession
	}
	equal := bytes.Equal(stored, payload)
	clear(stored)
	if !equal {
		return result, ErrSession
	}
	// Do not silently shorten the immutable operation for a shorter caller.
	if d, ok := caller.Deadline(); ok && d.Before(op.Deadline) {
		return result, ErrSession
	}
	conn, err := s.keeper.Preflight(call, s.allocation, s.source, op, payload)
	if err != nil {
		return result, ErrSession
	}
	defer func() {
		if e := conn.Close(); e != nil {
			result.Observation = ports.AuthenticatedAttemptOperationObservationV1{}
			err = ErrIncomplete
		}
	}()
	proof, err := conn.Publication()
	if err != nil {
		return result, ErrSession
	}
	r := s.parent.Reservation
	parentRequest := ports.ReserveAttemptEffectRequestV1{TenantID: r.TenantID, RunID: r.RunID, AttemptID: r.AttemptID, LeaseID: r.LeaseID,
		FenceToken: r.FenceToken, PhysicalInvocationClaimID: r.PhysicalInvocationClaimID, UpstreamIdempotencyKeyDigest: r.UpstreamIdempotencyKeyDigest}
	registered, replayed, err := s.store.PublishOwnedProxy(call, parentRequest, proof)
	if err != nil || replayed {
		return result, ErrSession
	}
	reserved, err := s.store.ReserveAttemptOperation(call, ports.ReserveAttemptOperationRequestV1{Operation: op})
	if err != nil || reserved.Status != ports.AttemptOperationReservedV1 || reserved.Capability == nil || reserved.Validate() != nil {
		return result, ErrSession
	}
	transport, invokeErr := conn.Invoke(call, registered, reserved.Capability.Clone(), payload)
	result.SendAttempted = transport.SendAttempted
	defer clear(transport.Body)
	usage := domain.AttemptOperationUsageV1{State: domain.AttemptOperationUsageUnknownV1}
	if invokeErr != nil {
		outcome := domain.AttemptOperationRejectedBeforeSendV1
		if result.SendAttempted {
			outcome = domain.AttemptOperationAcceptedOutcomeUnknownV1
		}
		result.Observation, _ = s.record(reserved.ObservationGrant, outcome, domain.AttemptOperationReasonTransportUnknownV1, operationbody.ReferenceV1{}, usage)
		return result, ErrIncomplete
	}
	response, decodeErr := frozen.DecodeResponse(transport.Body)
	if u, e := response.Usage.OperationUsage(); e == nil {
		usage = u
	}
	if response.Call != nil {
		defer clear(response.Call.Arguments)
	}
	normalized, normalizeErr := frozen.NormalizeResponseBodyV1(transport.Body, op.Kind, min(op.ResultBytes, 64<<10))
	defer clear(normalized)
	if decodeErr != nil || normalizeErr != nil {
		result.Observation, _ = s.record(reserved.ObservationGrant, domain.AttemptOperationAcceptedOutcomeUnknownV1, domain.AttemptOperationReasonMalformedV1, operationbody.ReferenceV1{}, usage)
		return result, ErrIncomplete
	}
	ref, writeErr := s.bodies.Write(call, normalized)
	if writeErr != nil {
		result.Observation, _ = s.record(reserved.ObservationGrant, domain.AttemptOperationAcceptedOutcomeUnknownV1, domain.AttemptOperationReasonWorkerLostV1, operationbody.ReferenceV1{}, usage)
		return result, ErrIncomplete
	}
	result.Observation, err = s.record(reserved.ObservationGrant, domain.AttemptOperationCompletedV1, domain.AttemptOperationReasonNoneV1, ref, usage)
	if err != nil || call.Err() != nil {
		result.Observation = ports.AuthenticatedAttemptOperationObservationV1{}
		return result, ErrIncomplete
	}
	return result, nil
}

// Independent bounded reconciliation cannot send. It authenticates using the
// ORIGINAL grant only and never renews its expiry or remints a physical claim.
func (s *localState) record(grant ports.AttemptOperationObservationGrantV1, outcome domain.AttemptOperationOutcomeV1, reason domain.AttemptOperationReasonV1, ref operationbody.ReferenceV1, usage domain.AttemptOperationUsageV1) (ports.AuthenticatedAttemptOperationObservationV1, error) {
	ctx, cancel := context.WithTimeout(context.Background(), min(grant.Reservation.Operation.CleanupDuration, 30*time.Second))
	defer cancel()
	digest, err := grant.Reservation.Digest()
	if err != nil {
		return ports.AuthenticatedAttemptOperationObservationV1{}, ErrIncomplete
	}
	o := domain.AttemptOperationObservationV1{Version: 1, OperationDigest: grant.Reservation.OperationDigest, ReservationDigest: digest,
		Outcome: outcome, Reason: reason, ResponseRef: ref.Ref, ResponseDigest: ref.Digest, ResponseBytes: ref.Bytes,
		ObservedAt: time.Now().UTC().Truncate(time.Microsecond), Usage: usage.Clone()}
	authenticated, err := s.issuer.AuthenticateAttemptOperationObservation(grant.Clone(), o)
	if err != nil {
		return ports.AuthenticatedAttemptOperationObservationV1{}, ErrIncomplete
	}
	if s.store.RecordAttemptOperationObservation(ctx, ports.RecordAttemptOperationObservationRequestV1{ObservationGrant: grant.Clone(), Observation: authenticated}) != nil {
		return ports.AuthenticatedAttemptOperationObservationV1{}, ErrIncomplete
	}
	read, err := s.store.ReadAttemptOperationObservation(ctx, grant.Clone())
	ad, _ := authenticated.Observation.Digest()
	rd, re := read.Observation.Digest()
	if err != nil || re != nil || ad != rd || !hmac.Equal(read.Authenticator, authenticated.Authenticator) || s.issuer.VerifyAttemptOperationObservation(read) != nil || ctx.Err() != nil {
		return ports.AuthenticatedAttemptOperationObservationV1{}, ErrIncomplete
	}
	return read.Clone(), nil
}

// Close owns only the factory-created keeper/allocation and joins synchronous
// body/Store work. It never closes the borrowed Source/DB or the model server.
// A timed-out join is an error, not proof of retirement, erasure or disposal.
func (v *LocalV1) Close() error {
	if v == nil || v.state == nil {
		return nil
	}
	s := v.state
	s.closeOnce.Do(func() {
		// One original close window covers both the owned keeper and gate join;
		// waiting on the keeper must not restart the session cleanup allowance.
		deadline := time.Now().Add(min(s.profile.Limits.CleanupTimeout, 30*time.Second))
		s.mu.Lock()
		if !s.terminalDeadline.IsZero() && s.terminalDeadline.Before(deadline) {
			deadline = s.terminalDeadline
		}
		s.closed = true
		s.cancel()
		entryCancel, stopEntry, executionCancel := s.entryCancel, s.stopEntryCaller, s.executionCancel
		s.mu.Unlock()
		timer := time.NewTimer(max(time.Until(deadline), 0))
		defer timer.Stop()
		if entryCancel != nil {
			entryCancel()
		}
		if stopEntry != nil {
			stopEntry()
		}
		if executionCancel != nil {
			executionCancel()
		}
		s.stopSource()
		if s.keeper.Close() != nil {
			s.closeErr = ErrSession
		}
		if s.bridge.Close() != nil {
			s.closeErr = ErrSession
		}
		joined := false
		select {
		case s.gate <- struct{}{}:
			<-s.gate
			joined = true
		case <-timer.C:
			select {
			case s.gate <- struct{}{}:
				<-s.gate
				joined = true
			default:
				s.closeErr = ErrSession
			}
		}
		if joined {
			s.mu.Lock()
			terminal := s.terminalOutput
			s.mu.Unlock()
			if terminal != nil && terminal.source != nil {
				if terminal.output != nil {
					// A direct call on the original prepared output need not own
					// s.gate. Join its actual writer as well, under the SAME close
					// deadline, before releasing any Source-owned buffers.
					joinCtx, cancel := context.WithDeadline(context.Background(), deadline)
					joinErr := terminal.output.StopAndJoin(joinCtx)
					cancel()
					terminal.output.Clear()
					if joinErr != nil {
						s.closeErr = ErrSession
						return
					}
				}
				terminal.source.Clear()
			}
		}
	})
	return s.closeErr
}

func (*LocalV1) String() string              { return "LocalProviderSessionV1{private:[redacted]}" }
func (v *LocalV1) GoString() string          { return v.String() }
func (ModelResultV1) String() string         { return "LocalModelResultV1{private:[redacted]}" }
func (v ModelResultV1) GoString() string     { return v.String() }
func (ModelStepResultV1) String() string     { return "LocalModelStepResultV1{private:[redacted]}" }
func (v ModelStepResultV1) GoString() string { return v.String() }
