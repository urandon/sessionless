// Package opssignal defines content-free, best-effort operational observations.
// An observation is neither canonical state nor authority to retry an effect.
// The only proposed derived metric dimension is signal_id, never other fields.
package opssignal

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
)

const (
	SchemaVersion  = "ops_signal_v1"
	MaxRecordBytes = 768 // Includes the terminating newline.
)

type SignalID string

// The accepted 22 named slots are closed; the remaining two slots are inactive.
// Managed/attached constants reserve the #227 integration contract. Their
// existence here does not qualify a live profile or authorize emitting success.
const (
	WebAuthFailure             SignalID = "web.auth_failure"
	WebSessionFailure          SignalID = "web.session_failure"
	WebUploadFailure           SignalID = "web.upload_failure"
	WebArtifactAccessFailure   SignalID = "web.artifact_access_failure"
	WebAuthSuccess             SignalID = "web.auth_success"
	ControlWakeDeferred        SignalID = "control.wake_deferred"
	ControlDispatchFailure     SignalID = "control.dispatch_failure"
	ControlQueueFailure        SignalID = "control.queue_failure"
	ManagedExecutionFailure    SignalID = "managed.execution_failure"
	ManagedPublicationFailure  SignalID = "managed.publication_failure"
	ManagedCleanupFailure      SignalID = "managed.cleanup_failure"
	ManagedDeadlineExceeded    SignalID = "managed.deadline_exceeded"
	ManagedCanonicalUnknown    SignalID = "managed.canonical_unknown"
	ManagedCanonicalSuccess    SignalID = "managed.canonical_success"
	ManagedCleanupSuccess      SignalID = "managed.cleanup_success"
	AttachedTransportFailure   SignalID = "attached.transport_failure"
	AttachedExecutionFailure   SignalID = "attached.execution_failure"
	AttachedPublicationFailure SignalID = "attached.publication_failure"
	AttachedCleanupFailure     SignalID = "attached.cleanup_failure"
	AttachedDeadlineExceeded   SignalID = "attached.deadline_exceeded"
	AttachedCanonicalUnknown   SignalID = "attached.canonical_unknown"
	AttachedCanonicalSuccess   SignalID = "attached.canonical_success"
)

type Stage string

const (
	AuthStart            Stage = "auth_start"
	AuthChallenge        Stage = "auth_challenge"
	AuthProvider         Stage = "auth_provider"
	AuthIdentity         Stage = "auth_identity"
	AuthMembership       Stage = "auth_membership"
	AuthSession          Stage = "auth_session"
	SessionRead          Stage = "session_read"
	SessionWrite         Stage = "session_write"
	UploadIntent         Stage = "upload_intent"
	UploadPresign        Stage = "upload_presign"
	UploadStat           Stage = "upload_stat"
	UploadCommit         Stage = "upload_commit"
	MessageCommit        Stage = "message_commit"
	AttachmentCapability Stage = "attachment_capability"
	ArtifactCapability   Stage = "artifact_capability"
	WakePublish          Stage = "wake_publish"
	Dispatch             Stage = "dispatch"
	QueuePublish         Stage = "queue_publish"
	QueueReceive         Stage = "queue_receive"
	QueueAck             Stage = "queue_ack"
	QueueRetry           Stage = "queue_retry"
	QueueDeadLetter      Stage = "queue_dead_letter"
	Ingress              Stage = "ingress"
	// #227 stages require actual owning typed results/readback, not dispatch,
	// broker acknowledgement, process-memory counters or an unrelated success.
	Execution         Stage = "execution"
	Publication       Stage = "publication"
	Cleanup           Stage = "cleanup"
	CanonicalReadback Stage = "canonical_readback"
	Transport         Stage = "transport"
	OriginalDeadline  Stage = "original_deadline"
)

type ErrorClass string

// Expected denials have no class: callers must omit them, not classify them as
// failures. None is allowed only at the explicitly listed successful/unknown
// result boundaries; it cannot turn an unexpected failure into a success.
const (
	Unexpected       ErrorClass = "unexpected"
	Store            ErrorClass = "store"
	Provider         ErrorClass = "provider"
	ObjectStore      ErrorClass = "object_store"
	Queue            ErrorClass = "queue"
	Canceled         ErrorClass = "canceled"
	DeadlineExceeded ErrorClass = "deadline_exceeded"
	None             ErrorClass = "none"
)

type Component string
type Route string
type Outcome string
type DeadlineState string

const (
	Web            Component     = "web"
	Control        Component     = "control"
	Managed        Component     = "managed"
	Attached       Component     = "attached"
	WebRoute       Route         = "web"
	NoRoute        Route         = "none"
	ManagedRoute   Route         = "managed"
	AttachedRoute  Route         = "attached"
	Failure        Outcome       = "failure"
	Deferred       Outcome       = "deferred"
	UnknownOutcome Outcome       = "unknown"
	Success        Outcome       = "success"
	Within         DeadlineState = "within"
	Exceeded       DeadlineState = "exceeded"
	Unknown        DeadlineState = "unknown"
	NotApplicable  DeadlineState = "not_applicable"
)

// Record deliberately has no arbitrary strings, maps, payloads, timestamps or
// IDs. Named string types still require validation: manual conversions must not
// bypass the compiled allowlist. Observe validates all fields before encoding.
type Record struct {
	SchemaVersion string        `json:"schema_version"`
	SignalID      SignalID      `json:"signal_id"`
	Component     Component     `json:"component"`
	Route         Route         `json:"route"`
	Stage         Stage         `json:"stage"`
	Outcome       Outcome       `json:"outcome"`
	ErrorClass    ErrorClass    `json:"error_class"`
	DeadlineState DeadlineState `json:"deadline_state"`
}

// Sink must not change business results. Implementations can fail, drop or
// duplicate delivery. Emit ignores returned errors but does not recover panics
// from arbitrary caller-provided implementations.
type Sink interface {
	Observe(context.Context, Record) error
}

var (
	ErrInvalidRecord     = errors.New("operational observation rejected")
	ErrUnavailableWriter = errors.New("operational observation writer unavailable")
	ErrWrite             = errors.New("operational observation write failed")
)

type signalMetadata struct {
	component Component
	route     Route
	outcome   Outcome
}

func metadata(signal SignalID) (signalMetadata, bool) {
	switch signal {
	case WebAuthFailure, WebSessionFailure, WebUploadFailure, WebArtifactAccessFailure:
		return signalMetadata{Web, WebRoute, Failure}, true
	case WebAuthSuccess:
		return signalMetadata{Web, WebRoute, Success}, true
	case ControlWakeDeferred:
		return signalMetadata{Control, NoRoute, Deferred}, true
	case ControlDispatchFailure, ControlQueueFailure:
		return signalMetadata{Control, NoRoute, Failure}, true
	case ManagedExecutionFailure, ManagedPublicationFailure, ManagedCleanupFailure, ManagedDeadlineExceeded:
		return signalMetadata{Managed, ManagedRoute, Failure}, true
	case ManagedCanonicalUnknown:
		return signalMetadata{Managed, ManagedRoute, UnknownOutcome}, true
	case ManagedCanonicalSuccess, ManagedCleanupSuccess:
		return signalMetadata{Managed, ManagedRoute, Success}, true
	case AttachedTransportFailure, AttachedExecutionFailure, AttachedPublicationFailure, AttachedCleanupFailure, AttachedDeadlineExceeded:
		return signalMetadata{Attached, AttachedRoute, Failure}, true
	case AttachedCanonicalUnknown:
		return signalMetadata{Attached, AttachedRoute, UnknownOutcome}, true
	case AttachedCanonicalSuccess:
		return signalMetadata{Attached, AttachedRoute, Success}, true
	default:
		return signalMetadata{}, false
	}
}

func classIn(class ErrorClass, allowed ...ErrorClass) bool {
	for _, v := range allowed {
		if class == v {
			return true
		}
	}
	return false
}
func stageIn(stage Stage, allowed ...Stage) bool {
	for _, v := range allowed {
		if stage == v {
			return true
		}
	}
	return false
}
func failureClass(class ErrorClass, allowed ...ErrorClass) bool {
	return classIn(class, Canceled, DeadlineExceeded, Unexpected) || classIn(class, allowed...)
}

// allowed is a compiled signal/stage/class relation, not a Cartesian product.
// No raw errors or identifiers are inspected or converted into log fields.
func allowed(signal SignalID, stage Stage, class ErrorClass) bool {
	switch signal {
	case WebAuthFailure:
		switch stage {
		case AuthStart, AuthChallenge, AuthIdentity, AuthMembership, AuthSession:
			return failureClass(class, Store)
		case AuthProvider:
			return failureClass(class, Provider)
		}
	case WebAuthSuccess:
		return stage == AuthSession && class == None
	case WebSessionFailure:
		if stageIn(stage, SessionRead, SessionWrite) {
			return failureClass(class, Store)
		}
		if stage == MessageCommit {
			return failureClass(class, Store, ObjectStore)
		}
	case WebUploadFailure:
		switch stage {
		case UploadIntent, UploadCommit:
			return failureClass(class, Store)
		case UploadPresign, UploadStat:
			return failureClass(class, ObjectStore)
		}
	case WebArtifactAccessFailure:
		return stageIn(stage, AttachmentCapability, ArtifactCapability) && failureClass(class, Store, ObjectStore)
	case ControlWakeDeferred:
		return stage == WakePublish && failureClass(class, Queue)
	case ControlDispatchFailure:
		return stage == Dispatch && failureClass(class, Store, Queue, Provider)
	case ControlQueueFailure:
		if stageIn(stage, QueuePublish, QueueReceive, QueueAck, QueueRetry, QueueDeadLetter) {
			return failureClass(class, Queue)
		}
		return stage == Ingress && failureClass(class, Queue, Store)
	case ManagedExecutionFailure, AttachedExecutionFailure:
		return stage == Execution && failureClass(class, Provider)
	case ManagedPublicationFailure, AttachedPublicationFailure:
		return stage == Publication && failureClass(class, Store, ObjectStore)
	case ManagedCleanupFailure, AttachedCleanupFailure:
		return stage == Cleanup && failureClass(class, Store, ObjectStore, Provider)
	case ManagedDeadlineExceeded, AttachedDeadlineExceeded:
		return stage == OriginalDeadline && class == DeadlineExceeded
	case ManagedCanonicalUnknown, AttachedCanonicalUnknown:
		return stage == CanonicalReadback && classIn(class, None, Store, Unexpected)
	case ManagedCanonicalSuccess, AttachedCanonicalSuccess:
		return stage == CanonicalReadback && class == None
	case ManagedCleanupSuccess:
		return stage == Cleanup && class == None
	case AttachedTransportFailure:
		return stage == Transport && failureClass(class, Provider, Queue)
	}
	return false
}

// Validate rejects unknown values and contradictory fixed metadata, using only
// fixed error text. It cannot establish the truth of the caller's observation.
func (r Record) Validate() error {
	m, ok := metadata(r.SignalID)
	if !ok || r.SchemaVersion != SchemaVersion || r.Component != m.component || r.Route != m.route || r.Outcome != m.outcome || !allowed(r.SignalID, r.Stage, r.ErrorClass) {
		return ErrInvalidRecord
	}
	switch r.DeadlineState {
	case Within, Exceeded, Unknown, NotApplicable:
		return nil
	default:
		return ErrInvalidRecord
	}
}

func deadlineState(ctx context.Context) DeadlineState {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return Exceeded
	}
	if ctx.Err() != nil {
		return Unknown
	}
	if _, ok := ctx.Deadline(); ok {
		return Within
	}
	return NotApplicable
}

// Emit produces at most one observation for this call. It neither deduplicates
// repeated actual boundary calls nor retries a dropped/failed observation. Use
// the original operation context; do not detach or extend it for logging.
func Emit(ctx context.Context, sink Sink, signal SignalID, stage Stage, class ErrorClass) {
	if sink == nil || ctx == nil || ctx.Err() != nil {
		return
	}
	m, ok := metadata(signal)
	if !ok {
		return
	}
	r := Record{SchemaVersion: SchemaVersion, SignalID: signal, Component: m.component, Route: m.route, Stage: stage, Outcome: m.outcome, ErrorClass: class, DeadlineState: deadlineState(ctx)}
	if r.Validate() != nil {
		return
	}
	_ = sink.Observe(ctx, r)
}

type writerSink struct {
	mu     sync.Mutex
	writer io.Writer
}

// NewWriter serializes its own records, each with exactly one Write call. It
// does not serialize unrelated writers or guarantee atomic platform ingestion.
// There is no goroutine, spool, retry, periodic export or after-response flush.
// A context cannot cancel a mutex wait or an in-progress OS Write. Record size
// and number of writes are bounded; completion time is not. Logging can still
// delay an invocation, so existing downstream expiry checks remain mandatory.
func NewWriter(writer io.Writer) Sink { return &writerSink{writer: writer} }

func (s *writerSink) Observe(ctx context.Context, r Record) error {
	if ctx == nil {
		return ErrInvalidRecord
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := r.Validate(); err != nil {
		return err
	}
	if r.DeadlineState != deadlineState(ctx) {
		return ErrInvalidRecord
	}
	data, err := json.Marshal(r)
	if err != nil {
		return ErrInvalidRecord
	}
	data = append(data, '\n')
	if len(data) > MaxRecordBytes {
		return ErrInvalidRecord
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.writer == nil {
		return ErrUnavailableWriter
	}
	n, err := s.writer.Write(data)
	if n != len(data) || err != nil {
		return ErrWrite
	}
	return nil
}
