// Package attachedworkersession owns one worker-side bootstrap and immediate
// exchange session. It composes existing local, HTTP, transport, and protocol
// authority without enabling the product foreground path or a timer poller.
package attachedworkersession

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"sync"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerhttp"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
	"gitcode.com/urandon/sessionless/internal/domain"
)

const (
	SnapshotVersionV1          = uint32(1)
	connectionKeyBytes         = 32
	defaultRetryInitialBackoff = 100 * time.Millisecond
	defaultRetryMaxBackoff     = 2 * time.Second
	maxExchangeAttempts        = uint32(8)
)

var (
	ErrInvalidConfiguration    = errors.New("attached worker connection session configuration is invalid")
	ErrInvalidAuthority        = errors.New("attached worker connection session authority is invalid")
	ErrAlreadyOwned            = errors.New("attached worker connection session already has an owner")
	ErrSessionClosed           = errors.New("attached worker connection session is closed")
	ErrSessionFenced           = errors.New("attached worker connection session is fenced")
	ErrReconciliationRequired  = errors.New("attached worker connection session requires reconciliation")
	ErrConnectionSessionFailed = errors.New("attached worker connection session failed")
)

type State string

const (
	StateIdle                   State = "idle"
	StateConnecting             State = "connecting"
	StateReady                  State = "ready"
	StateFenced                 State = "fenced"
	StateReconciliationRequired State = "reconciliation_required"
	StateClosed                 State = "closed"
)

// BootstrapPort deliberately matches the existing outbound-only HTTP client.
type BootstrapPort interface {
	IssueChallenge(context.Context, attachedworkerhttp.ChallengeRequestV1) (*attachedworkerhttp.ChallengeResponseV1, error)
	Activate(context.Context, attachedworkerhttp.ActivateClientInputV1) (*attachedworkerhttp.ActivateResponseV1, error)
}

// ExchangePort deliberately matches the existing immediate HTTP exchange
// client. A concrete adapter may additionally implement Close() error.
type ExchangePort interface {
	Exchange(context.Context, attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error)
}

// ExchangeFactory receives the exact safe binding plus one temporary copy of
// the bearer. It must copy any credential material it retains. The caller
// clears its copy immediately after Open returns.
type ExchangeFactory interface {
	Open(ConnectionBindingV1, []byte) (ExchangePort, error)
}

type localState interface {
	AcquireRuntime(context.Context) (localRuntimeLease, error)
}

type localRuntimeLease interface {
	LoadSnapshot(context.Context) (attachedworkerlocal.SnapshotV1, error)
	LoadSecret(context.Context) (attachedworkerlocal.SecretRecordV1, error)
	Update(context.Context, uint64, attachedworkerlocal.ManifestV1, attachedworkerlocal.SecretRecordV1) error
	Close() error
}

type storeAdapter struct{ store *attachedworkerlocal.Store }

func (adapter storeAdapter) AcquireRuntime(ctx context.Context) (localRuntimeLease, error) {
	return adapter.store.AcquireRuntime(ctx)
}

type Config struct {
	Audience            string
	WorkerOffer         attachedworkerprotocol.VersionOfferV1
	ImplementedVersions []attachedworkerprotocol.ProtocolVersion
	OperationTimeout    time.Duration
	Random              io.Reader
	// MaxExchangeAttempts includes the first exchange. The default is one, so
	// exact replay remains disabled until a reviewed composition opts in.
	MaxExchangeAttempts uint32
	RetryInitialBackoff time.Duration
	RetryMaxBackoff     time.Duration
	RetryRandom         io.Reader
	Now                 func() time.Time
	retrySeed           uint64
	retryWait           func(context.Context, time.Duration) error
}

type ConnectInputV1 struct {
	ExpectedWorkerRevision uint64
	CapabilityManifest     attachedworkerprotocol.CapabilityManifestV1
}

// ConnectionBindingV1 is the safe, immutable identity of one live session.
// It intentionally excludes the bearer, connection secret, private key,
// nonces, proof, channel-binding bytes, endpoint, and payloads.
type ConnectionBindingV1 struct {
	TenantID              domain.TenantID
	OwnerUserID           domain.UserID
	WorkerID              domain.AttachedWorkerID
	EnrollmentGeneration  uint64
	ConnectionGeneration  uint64
	ProtocolVersion       attachedworkerprotocol.ProtocolVersion
	ConnectionID          domain.AttachedWorkerConnectionID
	CapabilityDigest      domain.AttachedWorkerCapabilityDigest
	AuthenticationExpires time.Time
}

type SnapshotV1 struct {
	Version               uint32                                 `json:"version"`
	State                 State                                  `json:"state"`
	TenantID              domain.TenantID                        `json:"tenant_id,omitempty"`
	OwnerUserID           domain.UserID                          `json:"owner_user_id,omitempty"`
	WorkerID              domain.AttachedWorkerID                `json:"worker_id,omitempty"`
	EnrollmentGeneration  uint64                                 `json:"enrollment_generation,omitempty"`
	ConnectionGeneration  uint64                                 `json:"connection_generation,omitempty"`
	ProtocolVersion       attachedworkerprotocol.ProtocolVersion `json:"protocol_version,omitempty"`
	ConnectionID          domain.AttachedWorkerConnectionID      `json:"connection_id,omitempty"`
	CapabilityDigest      domain.AttachedWorkerCapabilityDigest  `json:"capability_digest,omitempty"`
	AuthenticationExpires *time.Time                             `json:"authentication_expires,omitempty"`
	FailureCode           string                                 `json:"failure_code,omitempty"`
}

type Connector struct {
	mu        sync.Mutex
	state     State
	store     localState
	bootstrap BootstrapPort
	factory   ExchangeFactory
	config    Config
}

func New(store *attachedworkerlocal.Store, bootstrap BootstrapPort, factory ExchangeFactory, config Config) (*Connector, error) {
	if store == nil {
		return nil, ErrInvalidConfiguration
	}
	return newConnector(storeAdapter{store: store}, bootstrap, factory, config)
}

func newConnector(store localState, bootstrap BootstrapPort, factory ExchangeFactory, config Config) (*Connector, error) {
	if store == nil || bootstrap == nil || factory == nil || !validAudience(config.Audience) ||
		config.WorkerOffer.Validate() != nil || len(config.ImplementedVersions) != 1 ||
		config.ImplementedVersions[0] != attachedworkerprotocol.ProtocolVersionV1 ||
		config.OperationTimeout < 0 || config.OperationTimeout > time.Minute ||
		config.MaxExchangeAttempts > maxExchangeAttempts || config.RetryInitialBackoff < 0 ||
		config.RetryMaxBackoff < 0 {
		return nil, ErrInvalidConfiguration
	}
	if selected, err := attachedworkerprotocol.NegotiateOffers(
		config.WorkerOffer, config.WorkerOffer, config.ImplementedVersions,
	); err != nil || selected != attachedworkerprotocol.ProtocolVersionV1 {
		return nil, ErrInvalidConfiguration
	}
	if config.OperationTimeout == 0 {
		config.OperationTimeout = 15 * time.Second
	}
	if config.MaxExchangeAttempts == 0 {
		config.MaxExchangeAttempts = 1
	}
	if config.RetryInitialBackoff == 0 {
		config.RetryInitialBackoff = defaultRetryInitialBackoff
	}
	if config.RetryMaxBackoff == 0 {
		config.RetryMaxBackoff = defaultRetryMaxBackoff
	}
	if config.RetryInitialBackoff > config.RetryMaxBackoff ||
		(config.MaxExchangeAttempts > 1 && config.RetryMaxBackoff > config.OperationTimeout) {
		return nil, ErrInvalidConfiguration
	}
	if config.Random == nil {
		config.Random = rand.Reader
	}
	if config.MaxExchangeAttempts > 1 {
		if config.RetryRandom == nil {
			config.RetryRandom = rand.Reader
		}
		var seedBytes [8]byte
		if _, err := io.ReadFull(config.RetryRandom, seedBytes[:]); err != nil {
			return nil, ErrInvalidConfiguration
		}
		config.retrySeed = binary.BigEndian.Uint64(seedBytes[:])
		if config.retrySeed == 0 {
			config.retrySeed = 0x9e3779b97f4a7c15
		}
	}
	if config.retryWait == nil {
		config.retryWait = waitForExchangeRetry
	}
	if config.Now == nil {
		config.Now = func() time.Time { return time.Now().UTC() }
	}
	config.ImplementedVersions = append([]attachedworkerprotocol.ProtocolVersion(nil), config.ImplementedVersions...)
	config.WorkerOffer.Supported = append([]attachedworkerprotocol.ProtocolVersion(nil), config.WorkerOffer.Supported...)
	return &Connector{state: StateIdle, store: store, bootstrap: bootstrap, factory: factory, config: config}, nil
}

func (connector *Connector) Connect(ctx context.Context, input ConnectInputV1) (*Session, error) {
	if connector == nil || ctx == nil {
		return nil, ErrInvalidConfiguration
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ownedInput, err := cloneConnectInput(input)
	if err != nil {
		return nil, err
	}
	connector.mu.Lock()
	switch connector.state {
	case StateIdle:
		connector.state = StateConnecting
	case StateConnecting, StateReady:
		connector.mu.Unlock()
		return nil, ErrAlreadyOwned
	case StateReconciliationRequired:
		connector.mu.Unlock()
		return nil, ErrReconciliationRequired
	case StateFenced:
		connector.mu.Unlock()
		return nil, ErrSessionFenced
	default:
		connector.mu.Unlock()
		return nil, ErrSessionClosed
	}
	connector.mu.Unlock()

	opCtx, cancel := context.WithTimeout(ctx, connector.config.OperationTimeout)
	result := make(chan connectResult, 1)
	go func() { result <- connector.connect(opCtx, ownedInput) }()

	select {
	case outcome := <-result:
		operationErr := opCtx.Err()
		cancel()
		if operationErr != nil {
			connector.mark(StateReconciliationRequired)
			if outcome.session != nil {
				_ = outcome.session.abandon()
			}
			return nil, errors.Join(ErrReconciliationRequired, operationErr)
		}
		if outcome.err != nil {
			if outcome.reconciliation {
				connector.mark(StateReconciliationRequired)
			} else {
				connector.mark(StateIdle)
			}
			return nil, outcome.err
		}
		connector.mark(StateReady)
		return outcome.session, nil
	case <-opCtx.Done():
		cancel()
		connector.mark(StateReconciliationRequired)
		go func() {
			outcome := <-result
			if outcome.session != nil {
				_ = outcome.session.abandon()
			}
		}()
		return nil, errors.Join(ErrReconciliationRequired, opCtx.Err())
	}
}

type connectResult struct {
	session        *Session
	err            error
	reconciliation bool
}

func (connector *Connector) connect(ctx context.Context, input ConnectInputV1) (result connectResult) {
	if input.ExpectedWorkerRevision == 0 || input.CapabilityManifest.Validate() != nil {
		result.err = ErrInvalidAuthority
		return result
	}
	lease, err := connector.store.AcquireRuntime(ctx)
	if err != nil {
		result.err = sanitizeLocal(err)
		return result
	}
	owned := true
	defer func() {
		if owned {
			if closeErr := sanitizeClose(lease.Close()); closeErr != nil {
				result.err = errors.Join(result.err, closeErr)
				result.reconciliation = true
			}
		}
	}()

	snapshot, err := lease.LoadSnapshot(ctx)
	if err != nil {
		result.err = sanitizeLocal(err)
		return result
	}
	secret, err := lease.LoadSecret(ctx)
	if err != nil {
		result.err = sanitizeLocal(err)
		return result
	}
	defer clearLocalSecret(&secret)
	manifest := snapshot.Manifest
	if manifest.Lifecycle != attachedworkerlocal.LifecycleActive {
		result.err = ErrSessionFenced
		return result
	}
	if manifest.ConnectionGeneration != 0 || len(secret.ConnectionSecret) != 0 {
		result.err = ErrReconciliationRequired
		result.reconciliation = true
		return result
	}
	if err := validateCapabilityAuthority(manifest, input.CapabilityManifest, connector.config.WorkerOffer); err != nil {
		result.err = err
		return result
	}
	if manifest.Revision == math.MaxUint64 || manifest.ConnectionGeneration == math.MaxUint64 {
		result.err = ErrInvalidAuthority
		return result
	}

	rawConnectionSecret := make([]byte, connectionKeyBytes)
	defer clearBytes(rawConnectionSecret)
	workerNonce := make([]byte, connectionKeyBytes)
	defer clearBytes(workerNonce)
	if _, err := io.ReadFull(connector.config.Random, rawConnectionSecret); err != nil {
		result.err = ErrConnectionSessionFailed
		return result
	}
	connectionSecret, err := attachedworkertransport.ParseConnectionSecret(rawConnectionSecret)
	if err != nil {
		result.err = ErrConnectionSessionFailed
		return result
	}
	if _, err := io.ReadFull(connector.config.Random, workerNonce); err != nil || allZero(workerNonce) {
		result.err = ErrConnectionSessionFailed
		return result
	}

	// Advance local generation and persist the secret before the first network
	// effect. A crash at any later point therefore cannot silently start a
	// second attach: restart sees a non-zero generation and requires explicit
	// reconciliation while reconnect authority remains unavailable.
	nextManifest := manifest
	nextManifest.Revision++
	nextManifest.ConnectionGeneration++
	nextManifest.UpdatedAt = nextTime(connector.config.Now().UTC(), manifest.UpdatedAt)
	nextSecret := secret
	nextSecret.IdentityPrivateKey = append([]byte(nil), secret.IdentityPrivateKey...)
	nextSecret.ManifestRevision = nextManifest.Revision
	nextSecret.ConnectionGeneration = nextManifest.ConnectionGeneration
	nextSecret.ConnectionSecret = append([]byte(nil), rawConnectionSecret...)
	if err := lease.Update(ctx, manifest.Revision, nextManifest, nextSecret); err != nil {
		result.err = errors.Join(ErrReconciliationRequired, sanitizeLocal(err))
		result.reconciliation = true
		clearLocalSecret(&nextSecret)
		return result
	}
	clearLocalSecret(&nextSecret)
	result.reconciliation = true
	if err := ctx.Err(); err != nil {
		result.err = errors.Join(ErrReconciliationRequired, err)
		return result
	}

	digest, err := attachedworkerprotocol.ManifestDigestV1(input.CapabilityManifest)
	if err != nil {
		result.err = ErrInvalidAuthority
		return result
	}
	hello := attachedworkerprotocol.FrameV1{
		Version:   attachedworkerprotocol.ProtocolVersionV1,
		MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, 1),
		WorkerID:  string(manifest.WorkerID), EnrollmentGeneration: manifest.EnrollmentGeneration,
		ConnectionGeneration: nextManifest.ConnectionGeneration, Sequence: 1, Ack: 0,
		Kind: attachedworkerprotocol.MessageHello,
		Hello: &attachedworkerprotocol.HelloV1{
			Offer: cloneOffer(connector.config.WorkerOffer), WorkerNonce: append([]byte(nil), workerNonce...),
		},
	}
	proofRequest := attachedworkertransport.IssueChallengeRequest{
		WorkerID: manifest.WorkerID, ExpectedAudience: connector.config.Audience,
		ExpectedWorkerRevision: input.ExpectedWorkerRevision, Purpose: domain.AttachedWorkerAttachInitial,
		Hello: hello,
	}
	privateKey := ed25519.PrivateKey(secret.IdentityPrivateKey)
	publicKey := privateKey.Public().(ed25519.PublicKey)
	proof, err := attachedworkertransport.SignChallengeRequestV1(
		privateKey, publicKey, manifest.TenantID, manifest.OwnerUserID, manifest.WorkerID,
		manifest.EnrollmentGeneration, manifest.ConnectionGeneration, proofRequest,
	)
	if err != nil {
		result.err = ErrInvalidAuthority
		return result
	}
	challengeRequest := attachedworkerhttp.ChallengeRequestV1{
		TenantLocator: manifest.TenantID, OwnerLocator: manifest.OwnerUserID,
		ExpectedAudience: connector.config.Audience, ExpectedWorkerRevision: input.ExpectedWorkerRevision,
		Purpose: domain.AttachedWorkerAttachInitial, Hello: hello, Proof: proof,
	}
	challengeResponse, err := connector.bootstrap.IssueChallenge(ctx, challengeRequest)
	clearBytes(proof)
	if contextErr := ctx.Err(); contextErr != nil {
		result.err = errors.Join(ErrReconciliationRequired, contextErr)
		return result
	}
	if err != nil || challengeResponse == nil || !validChallenge(challengeRequest, *challengeResponse) {
		result.err = errors.Join(ErrReconciliationRequired, sanitizeExchange(err))
		return result
	}

	selected := challengeResponse.Frame.Version
	channelBinding := attachedworkertransport.ConnectionChannelBinding(
		challengeResponse.Challenge.ID,
		domain.DigestAttachedWorkerChallenge(workerNonce),
		domain.DigestAttachedWorkerChallenge(challengeResponse.Frame.Challenge.PlatformNonce),
		connectionSecret.Digest(),
	)
	channelBytes, err := hex.DecodeString(string(channelBinding))
	if err != nil {
		result.err = ErrConnectionSessionFailed
		return result
	}
	defer clearBytes(channelBytes)
	auth := attachedworkerprotocol.AuthContextV1{
		TenantID: string(manifest.TenantID), OwnerUserID: string(manifest.OwnerUserID), WorkerID: string(manifest.WorkerID),
		IdentityPublicKey: append(ed25519.PublicKey(nil), publicKey...), EnrollmentGeneration: manifest.EnrollmentGeneration,
		ConnectionGeneration: nextManifest.ConnectionGeneration, Version: selected,
		ChannelBinding: append([]byte(nil), channelBytes...),
	}
	attach := attachedworkerprotocol.FrameV1{
		Version: selected, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, 2),
		WorkerID: string(manifest.WorkerID), EnrollmentGeneration: manifest.EnrollmentGeneration,
		ConnectionGeneration: nextManifest.ConnectionGeneration, Sequence: 2, Ack: 1,
		Kind: attachedworkerprotocol.MessageAttach,
		Attach: &attachedworkerprotocol.AttachV1{
			WorkerOffer: cloneOffer(connector.config.WorkerOffer), PlatformOffer: cloneOffer(challengeResponse.Frame.Challenge.PlatformOffer),
			SelectedVersion: selected, WorkerNonce: append([]byte(nil), workerNonce...),
			PlatformNonce:    append([]byte(nil), challengeResponse.Frame.Challenge.PlatformNonce...),
			CapabilityDigest: append([]byte(nil), digest...),
		},
	}
	if err := attachedworkerprotocol.SignAttachV1(privateKey, auth, &attach); err != nil {
		result.err = ErrConnectionSessionFailed
		return result
	}
	defer clearBytes(attach.Attach.Signature)
	activation, err := connector.bootstrap.Activate(ctx, attachedworkerhttp.ActivateClientInputV1{
		TenantLocator: manifest.TenantID, OwnerLocator: manifest.OwnerUserID,
		ChallengeID: challengeResponse.Challenge.ID, ExpectedConnectionID: challengeResponse.Challenge.ConnectionID,
		ConnectionSecret: connectionSecret, Attach: attach,
	})
	if contextErr := ctx.Err(); contextErr != nil {
		result.err = errors.Join(ErrReconciliationRequired, contextErr)
		return result
	}
	if err != nil || activation == nil || !validActivation(manifest, challengeResponse.Challenge, connectionSecret, digest, attach, *activation) ||
		!activation.Connection.AuthExpiresAt.After(connector.config.Now().UTC()) {
		result.err = errors.Join(ErrReconciliationRequired, sanitizeExchange(err))
		return result
	}

	machineConfig := attachedworkerprotocol.MachineConfig{
		Auth: auth, WorkerOffer: cloneOffer(connector.config.WorkerOffer),
		PlatformOffer:       cloneOffer(challengeResponse.Frame.Challenge.PlatformOffer),
		ImplementedVersions: append([]attachedworkerprotocol.ProtocolVersion(nil), connector.config.ImplementedVersions...),
	}
	machineSnapshot, err := attachedworkerprotocol.BuildInitialAttachSnapshotV1(machineConfig, attach, activation.Accepted)
	if err != nil {
		result.err = ErrReconciliationRequired
		return result
	}
	machine, err := attachedworkerprotocol.RestoreConformanceMachine(machineConfig, machineSnapshot)
	if err != nil {
		result.err = ErrReconciliationRequired
		return result
	}
	bearer, err := attachedworkertransport.NewConnectionBearer(
		manifest.TenantID, manifest.OwnerUserID, manifest.WorkerID, activation.Connection.ID, connectionSecret,
	)
	if err != nil {
		result.err = ErrReconciliationRequired
		return result
	}
	rawBearer := bearer.Bytes()
	if err := ctx.Err(); err != nil {
		clearBytes(rawBearer)
		result.err = errors.Join(ErrReconciliationRequired, err)
		return result
	}
	exchange, err := connector.factory.Open(ConnectionBindingV1{
		TenantID: manifest.TenantID, OwnerUserID: manifest.OwnerUserID, WorkerID: manifest.WorkerID,
		EnrollmentGeneration: manifest.EnrollmentGeneration, ConnectionGeneration: nextManifest.ConnectionGeneration,
		ProtocolVersion: selected, ConnectionID: activation.Connection.ID,
		CapabilityDigest:      domain.AttachedWorkerCapabilityDigest(hex.EncodeToString(digest)),
		AuthenticationExpires: activation.Connection.AuthExpiresAt,
	}, rawBearer)
	clearBytes(rawBearer)
	if err != nil || exchange == nil {
		result.err = errors.Join(ErrReconciliationRequired, sanitizeExchange(err))
		if closer, ok := exchange.(interface{ Close() error }); ok {
			result.err = errors.Join(result.err, sanitizeClose(closer.Close()))
		}
		return result
	}

	session := &Session{
		state: StateConnecting, binding: ConnectionBindingV1{
			TenantID: manifest.TenantID, OwnerUserID: manifest.OwnerUserID, WorkerID: manifest.WorkerID,
			EnrollmentGeneration: manifest.EnrollmentGeneration, ConnectionGeneration: nextManifest.ConnectionGeneration,
			ProtocolVersion: selected, ConnectionID: activation.Connection.ID,
			CapabilityDigest:      domain.AttachedWorkerCapabilityDigest(hex.EncodeToString(digest)),
			AuthenticationExpires: activation.Connection.AuthExpiresAt,
		},
		machineConfig: machineConfig, machine: machine, exchange: exchange, lease: lease,
		operationTimeout: connector.config.OperationTimeout, operationGate: make(chan struct{}, 1), now: connector.config.Now,
		maxExchangeAttempts: connector.config.MaxExchangeAttempts,
		retryInitialBackoff: connector.config.RetryInitialBackoff, retryMaxBackoff: connector.config.RetryMaxBackoff,
		retryJitter: &exchangeRetryJitter{state: connector.config.retrySeed}, retryWait: connector.config.retryWait,
	}
	session.operationGate <- struct{}{}
	if err := ctx.Err(); err != nil {
		_ = session.abandon()
		owned = false
		result.err = errors.Join(ErrReconciliationRequired, err)
		return result
	}
	manifestFrame := attachedworkerprotocol.FrameV1{
		Version: selected, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, 3),
		WorkerID: string(manifest.WorkerID), EnrollmentGeneration: manifest.EnrollmentGeneration,
		ConnectionGeneration: nextManifest.ConnectionGeneration, Sequence: 3, Ack: 2,
		Kind:     attachedworkerprotocol.MessageManifest,
		Manifest: &attachedworkerprotocol.ManifestV1{Manifest: input.CapabilityManifest, Digest: append([]byte(nil), digest...)},
	}
	if err := attachedworkerprotocol.SignManifestV1(privateKey, auth, &manifestFrame); err != nil {
		_ = session.abandon()
		owned = false
		result.err = ErrReconciliationRequired
		return result
	}
	defer clearBytes(manifestFrame.Manifest.Signature)
	if _, err := session.exchangeWithReplay(ctx, attachedworkerprotocol.BatchV1{Version: selected, Frames: []attachedworkerprotocol.FrameV1{manifestFrame}}); err != nil {
		_ = session.abandon()
		owned = false
		result.err = errors.Join(ErrReconciliationRequired, err)
		return result
	}
	session.mu.Lock()
	session.state = StateReady
	session.mu.Unlock()
	owned = false
	result.session = session
	result.reconciliation = false
	return result
}

type Session struct {
	mu                  sync.Mutex
	state               State
	failureCode         string
	binding             ConnectionBindingV1
	machineConfig       attachedworkerprotocol.MachineConfig
	machine             *attachedworkerprotocol.ConformanceMachine
	exchange            ExchangePort
	lease               localRuntimeLease
	operationTimeout    time.Duration
	operationGate       chan struct{}
	now                 func() time.Time
	maxExchangeAttempts uint32
	retryInitialBackoff time.Duration
	retryMaxBackoff     time.Duration
	retryJitter         *exchangeRetryJitter
	retryWait           func(context.Context, time.Duration) error
}

type exchangeRetryJitter struct {
	state uint64
}

// ActionV1 is the bounded worker-to-platform vocabulary that may be emitted
// through a connected Session after bootstrap. The Session owns the connection
// envelope sequence and acknowledgement; callers own only the typed semantic
// payload. Raw FrameV1 construction remains inside this package so a daemon
// adapter cannot forge scope, generations, or connection watermarks.
type ActionV1 struct {
	Heartbeat  *attachedworkerprotocol.HeartbeatV1
	LeaseClaim *attachedworkerprotocol.LeaseClaimV1
	Progress   *attachedworkerprotocol.ProgressV1
	CancelAck  *attachedworkerprotocol.CancelAckV1
	Terminal   *attachedworkerprotocol.TerminalV1
	Drained    *attachedworkerprotocol.DrainedV1
	Revoked    *attachedworkerprotocol.RevokedV1
}

func (action ActionV1) String() string {
	kind, count := actionKind(action)
	return fmt.Sprintf("ActionV1{kind:%s payload:[redacted] count:%d}", kind, count)
}

func (action ActionV1) GoString() string { return action.String() }

func (session *Session) Exchange(ctx context.Context, batch attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error) {
	if session == nil || ctx == nil {
		return nil, ErrInvalidConfiguration
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ownedBatch, err := cloneBatch(batch)
	if err != nil {
		return nil, ErrInvalidAuthority
	}
	return session.exchangePrepared(ctx, func() (attachedworkerprotocol.BatchV1, error) {
		return ownedBatch, nil
	})
}

// ExchangeAction creates the next exact worker envelope under the Session's
// operation ownership and returns at most one validated platform frame. It is
// intentionally narrower than Exchange and never exposes connection sequence
// or acknowledgement construction to transport/daemon adapters.
func (session *Session) ExchangeAction(ctx context.Context, action ActionV1) (*attachedworkerprotocol.FrameV1, error) {
	if session == nil || ctx == nil {
		return nil, ErrInvalidConfiguration
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	response, err := session.exchangePrepared(ctx, func() (attachedworkerprotocol.BatchV1, error) {
		return session.buildActionBatch(action)
	})
	if err != nil || response == nil {
		return nil, err
	}
	if len(response.Frames) != 1 {
		return nil, ErrReconciliationRequired
	}
	frame := response.Frames[0]
	return &frame, nil
}

func (session *Session) exchangePrepared(
	ctx context.Context,
	prepare func() (attachedworkerprotocol.BatchV1, error),
) (*attachedworkerprotocol.BatchV1, error) {
	if session == nil || ctx == nil || prepare == nil {
		return nil, ErrInvalidConfiguration
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	session.mu.Lock()
	state := session.state
	session.mu.Unlock()
	if state != StateReady {
		return nil, stateError(state)
	}
	if err := session.acquire(ctx); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		session.release()
		return nil, err
	}
	session.mu.Lock()
	state = session.state
	expires := session.binding.AuthenticationExpires
	now := session.now
	session.mu.Unlock()
	if state != StateReady {
		session.release()
		return nil, stateError(state)
	}
	if now == nil || !now().UTC().Before(expires) {
		session.fail(StateFenced, "authentication_expired")
		session.release()
		return nil, ErrSessionFenced
	}
	ownedBatch, err := prepare()
	if err != nil {
		session.release()
		return nil, err
	}
	opCtx, cancel := context.WithTimeout(ctx, session.operationTimeout)
	result := make(chan exchangeResult, 1)
	go func() {
		response, err := session.exchangeWithReplay(opCtx, ownedBatch)
		result <- exchangeResult{response: response, err: err}
		session.release()
	}()
	select {
	case outcome := <-result:
		operationErr := opCtx.Err()
		cancel()
		if operationErr != nil {
			session.fail(StateReconciliationRequired, "exchange_cancelled")
			return nil, errors.Join(ErrReconciliationRequired, operationErr)
		}
		if outcome.err != nil && !errors.Is(outcome.err, ErrInvalidAuthority) {
			session.classifyFailure(outcome.err)
		}
		return outcome.response, outcome.err
	case <-opCtx.Done():
		cancel()
		session.fail(StateReconciliationRequired, "exchange_cancelled")
		return nil, errors.Join(ErrReconciliationRequired, opCtx.Err())
	}
}

// exchangeWithReplay retains one canonical encoded batch and, when explicitly
// enabled, replays only that exact content after a sanitized retryable
// transport error. It never calls prepare again, so sequence, acknowledgement,
// attempt authority, signature, and evidence cannot be regenerated between
// attempts.
func (session *Session) exchangeWithReplay(
	ctx context.Context,
	batch attachedworkerprotocol.BatchV1,
) (*attachedworkerprotocol.BatchV1, error) {
	if session == nil || ctx == nil || session.maxExchangeAttempts == 0 || session.retryJitter == nil || session.retryWait == nil {
		return nil, ErrInvalidConfiguration
	}
	encoded, err := attachedworkerprotocol.EncodeBatchV1(batch)
	if err != nil {
		return nil, ErrInvalidAuthority
	}
	defer clearBytes(encoded)
	session.mu.Lock()
	now := session.now
	session.mu.Unlock()
	if now == nil {
		return nil, ErrInvalidAuthority
	}
	acceptanceNowUnixMicro := now().UTC().UnixMicro()
	if acceptanceNowUnixMicro <= 0 {
		return nil, ErrInvalidAuthority
	}
	backoff := session.retryInitialBackoff
	for attempt := uint32(1); attempt <= session.maxExchangeAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		exact, err := attachedworkerprotocol.DecodeBatchV1(encoded)
		if err != nil {
			return nil, ErrInvalidAuthority
		}
		response, exchangeErr := session.exchangeOnce(ctx, exact, acceptanceNowUnixMicro)
		if exchangeErr == nil || !retryableExchange(exchangeErr) {
			return response, exchangeErr
		}
		if attempt == session.maxExchangeAttempts {
			return nil, errors.Join(ErrReconciliationRequired, exchangeErr)
		}
		delay := session.retryJitter.duration(backoff)
		if err := session.retryWait(ctx, delay); err != nil {
			return nil, err
		}
		backoff = growExchangeBackoff(backoff, session.retryMaxBackoff)
	}
	return nil, ErrReconciliationRequired
}

func retryableExchange(err error) bool {
	var classified *attachedworkerhttp.ExchangeError
	return errors.As(err, &classified) && classified.Retryable()
}

func (source *exchangeRetryJitter) duration(cap time.Duration) time.Duration {
	if source == nil || cap <= 0 {
		return 0
	}
	source.state += 0x9e3779b97f4a7c15
	value := source.state
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	value ^= value >> 31
	bound := uint64(cap)
	if bound == math.MaxUint64 {
		return time.Duration(value)
	}
	return time.Duration(value % (bound + 1))
}

func growExchangeBackoff(current, maximum time.Duration) time.Duration {
	if current >= maximum || current > maximum/2 {
		return maximum
	}
	return current * 2
}

func waitForExchangeRetry(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (session *Session) buildActionBatch(action ActionV1) (attachedworkerprotocol.BatchV1, error) {
	kind, count := actionKind(action)
	if count != 1 {
		return attachedworkerprotocol.BatchV1{}, ErrInvalidAuthority
	}
	session.mu.Lock()
	machine := session.machine
	binding := session.binding
	session.mu.Unlock()
	if machine == nil || !validBinding(binding) {
		return attachedworkerprotocol.BatchV1{}, ErrInvalidAuthority
	}
	snapshot, err := machine.Snapshot()
	if err != nil || snapshot.Worker.Sequence == math.MaxUint64 {
		return attachedworkerprotocol.BatchV1{}, ErrReconciliationRequired
	}
	sequence := snapshot.Worker.Sequence + 1
	frame := attachedworkerprotocol.FrameV1{
		Version: binding.ProtocolVersion, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionWorkerToPlatform, sequence),
		WorkerID: string(binding.WorkerID), EnrollmentGeneration: binding.EnrollmentGeneration,
		ConnectionGeneration: binding.ConnectionGeneration, Sequence: sequence, Ack: snapshot.Platform.Sequence, Kind: kind,
		Heartbeat: action.Heartbeat, LeaseClaim: action.LeaseClaim, Progress: action.Progress,
		CancelAck: action.CancelAck, Terminal: action.Terminal, Drained: action.Drained, Revoked: action.Revoked,
	}
	return cloneBatch(attachedworkerprotocol.BatchV1{Version: binding.ProtocolVersion, Frames: []attachedworkerprotocol.FrameV1{frame}})
}

func actionKind(action ActionV1) (attachedworkerprotocol.MessageKind, int) {
	var kind attachedworkerprotocol.MessageKind
	count := 0
	check := func(candidate attachedworkerprotocol.MessageKind, present bool) {
		if present {
			kind, count = candidate, count+1
		}
	}
	check(attachedworkerprotocol.MessageHeartbeat, action.Heartbeat != nil)
	check(attachedworkerprotocol.MessageLeaseClaim, action.LeaseClaim != nil)
	check(attachedworkerprotocol.MessageProgress, action.Progress != nil)
	check(attachedworkerprotocol.MessageCancelAck, action.CancelAck != nil)
	check(attachedworkerprotocol.MessageTerminal, action.Terminal != nil)
	check(attachedworkerprotocol.MessageDrained, action.Drained != nil)
	check(attachedworkerprotocol.MessageRevoked, action.Revoked != nil)
	return kind, count
}

type exchangeResult struct {
	response *attachedworkerprotocol.BatchV1
	err      error
}

func (session *Session) exchangeOnce(
	ctx context.Context,
	batch attachedworkerprotocol.BatchV1,
	acceptanceNowUnixMicro int64,
) (*attachedworkerprotocol.BatchV1, error) {
	session.mu.Lock()
	machine := session.machine
	config := session.machineConfig
	exchange := session.exchange
	binding := session.binding
	session.mu.Unlock()
	if machine == nil || exchange == nil || !validBinding(binding) || batch.Version != binding.ProtocolVersion || len(batch.Frames) == 0 {
		return nil, ErrInvalidAuthority
	}
	snapshot, err := machine.Snapshot()
	if err != nil {
		return nil, ErrReconciliationRequired
	}
	working, err := attachedworkerprotocol.RestoreConformanceMachine(config, snapshot)
	if err != nil {
		return nil, ErrReconciliationRequired
	}
	if acceptanceNowUnixMicro <= 0 {
		return nil, ErrInvalidAuthority
	}
	acceptance := attachedworkerprotocol.AcceptanceContextV1{
		ChannelBinding: append([]byte(nil), config.Auth.ChannelBinding...),
		NowUnixMicro:   acceptanceNowUnixMicro,
	}
	for _, frame := range batch.Frames {
		if !frameMatchesBinding(frame, binding) || working.Accept(attachedworkerprotocol.DirectionWorkerToPlatform, frame, acceptance) != nil {
			return nil, ErrInvalidAuthority
		}
	}
	// Receiving TerminalAck commits the attempt, but the worker must first
	// acknowledge that platform frame in a later connection envelope. Retire
	// only after that proof so a reconnect cannot regress committed authority
	// to idle. The connection watermarks and replay fingerprints are preserved.
	if working.AttemptState() == attachedworkerprotocol.AttemptTerminalCommitted {
		snapshot, snapshotErr := working.Snapshot()
		if snapshotErr != nil {
			return nil, ErrReconciliationRequired
		}
		if snapshot.Worker.Ack >= snapshot.Platform.Sequence {
			retired, retireErr := attachedworkerprotocol.RetireCommittedAttemptV1(config, snapshot)
			if retireErr != nil {
				return nil, ErrReconciliationRequired
			}
			working, retireErr = attachedworkerprotocol.RestoreConformanceMachine(config, retired)
			if retireErr != nil {
				return nil, ErrReconciliationRequired
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	response, err := exchange.Exchange(ctx, batch)
	if err != nil {
		return nil, sanitizeExchange(err)
	}
	if response != nil {
		if response.Version != binding.ProtocolVersion || len(response.Frames) == 0 || len(response.Frames) > 1 {
			return nil, ErrReconciliationRequired
		}
		for _, frame := range response.Frames {
			if !frameMatchesBinding(frame, binding) || working.Accept(attachedworkerprotocol.DirectionPlatformToWorker, frame, acceptance) != nil {
				return nil, ErrReconciliationRequired
			}
		}
	}
	session.mu.Lock()
	if session.state == StateConnecting || session.state == StateReady {
		session.machine = working
		if working.ConnectionState() == attachedworkerprotocol.ConnectionRevoked {
			session.state = StateFenced
			session.failureCode = "revoked"
		}
	}
	session.mu.Unlock()
	return response, nil
}

func (session *Session) Close(ctx context.Context) error {
	if session == nil || ctx == nil {
		return ErrInvalidConfiguration
	}
	if err := session.acquire(ctx); err != nil {
		return err
	}
	defer session.release()
	return session.abandon()
}

func (session *Session) abandon() error {
	if session == nil {
		return nil
	}
	session.mu.Lock()
	if session.state == StateClosed {
		session.mu.Unlock()
		return nil
	}
	exchange := session.exchange
	lease := session.lease
	session.exchange = nil
	session.lease = nil
	session.machine = nil
	clearBytes(session.machineConfig.Auth.ChannelBinding)
	clearBytes(session.machineConfig.Auth.IdentityPublicKey)
	session.machineConfig = attachedworkerprotocol.MachineConfig{}
	session.now = nil
	session.state = StateClosed
	session.mu.Unlock()
	var closeErr error
	if closer, ok := exchange.(interface{ Close() error }); ok {
		closeErr = errors.Join(closeErr, sanitizeClose(closer.Close()))
	}
	if lease != nil {
		closeErr = errors.Join(closeErr, sanitizeClose(lease.Close()))
	}
	return closeErr
}

func (session *Session) Snapshot() SnapshotV1 {
	if session == nil {
		return SnapshotV1{Version: SnapshotVersionV1, State: StateClosed, FailureCode: "invalid_session"}
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	result := SnapshotV1{
		Version: SnapshotVersionV1, State: session.state,
		TenantID: session.binding.TenantID, OwnerUserID: session.binding.OwnerUserID, WorkerID: session.binding.WorkerID,
		EnrollmentGeneration: session.binding.EnrollmentGeneration, ConnectionGeneration: session.binding.ConnectionGeneration,
		ProtocolVersion: session.binding.ProtocolVersion, ConnectionID: session.binding.ConnectionID,
		CapabilityDigest: session.binding.CapabilityDigest, FailureCode: session.failureCode,
	}
	if !session.binding.AuthenticationExpires.IsZero() {
		expires := session.binding.AuthenticationExpires
		result.AuthenticationExpires = &expires
	}
	return result
}

func (connector *Connector) String() string {
	if connector == nil {
		return "Connector{closed}"
	}
	connector.mu.Lock()
	defer connector.mu.Unlock()
	return fmt.Sprintf("Connector{state:%s}", connector.state)
}

func (connector *Connector) GoString() string { return connector.String() }

func (session *Session) String() string {
	if session == nil {
		return "Session{closed}"
	}
	snapshot := session.Snapshot()
	return fmt.Sprintf("Session{state:%s,worker:%s,enrollment_generation:%d,connection_generation:%d,protocol:%d}",
		snapshot.State, snapshot.WorkerID, snapshot.EnrollmentGeneration, snapshot.ConnectionGeneration, snapshot.ProtocolVersion)
}

func (session *Session) GoString() string { return session.String() }

func (connector *Connector) mark(state State) {
	connector.mu.Lock()
	connector.state = state
	connector.mu.Unlock()
}

func (session *Session) acquire(ctx context.Context) error {
	select {
	case <-session.operationGate:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (session *Session) release() { session.operationGate <- struct{}{} }

func (session *Session) fail(state State, code string) {
	session.mu.Lock()
	if session.state != StateClosed {
		session.state = state
		session.failureCode = code
	}
	session.mu.Unlock()
}

func (session *Session) classifyFailure(err error) {
	var exchangeErr *attachedworkerhttp.ExchangeError
	if errors.As(err, &exchangeErr) && exchangeErr.Kind == attachedworkerhttp.ErrorUnauthorized {
		session.fail(StateFenced, "unauthorized")
		return
	}
	session.fail(StateReconciliationRequired, "exchange_ambiguous")
}

func stateError(state State) error {
	switch state {
	case StateFenced:
		return ErrSessionFenced
	case StateReconciliationRequired:
		return ErrReconciliationRequired
	default:
		return ErrSessionClosed
	}
}

func validChallenge(input attachedworkerhttp.ChallengeRequestV1, response attachedworkerhttp.ChallengeResponseV1) bool {
	challenge := response.Challenge
	frame := response.Frame
	return challenge.Validate() == nil && frame.Validate() == nil && frame.Kind == attachedworkerprotocol.MessageChallenge && frame.Challenge != nil &&
		challenge.TenantID == input.TenantLocator && challenge.OwnerUserID == input.OwnerLocator &&
		challenge.WorkerID == domain.AttachedWorkerID(input.Hello.WorkerID) && challenge.Purpose == input.Purpose &&
		challenge.Audience == input.ExpectedAudience && challenge.ExpectedWorkerRevision == input.ExpectedWorkerRevision &&
		challenge.ExpectedEnrollmentGeneration == input.Hello.EnrollmentGeneration &&
		challenge.ExpectedConnectionGeneration+1 == input.Hello.ConnectionGeneration &&
		challenge.TargetConnectionGeneration == input.Hello.ConnectionGeneration &&
		challenge.ConnectionID.Validate() == nil && challenge.SelectedProtocolVersion == uint32(frame.Version) &&
		challenge.WorkerNonceDigest == domain.DigestAttachedWorkerChallenge(input.Hello.Hello.WorkerNonce) &&
		challenge.PlatformNonceDigest == domain.DigestAttachedWorkerChallenge(frame.Challenge.PlatformNonce) &&
		sameOffer(frame.Challenge.WorkerOffer, input.Hello.Hello.Offer) && challengeOfferMatches(challenge, frame.Challenge) &&
		frame.MessageID == attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionPlatformToWorker, 1) &&
		frame.WorkerID == input.Hello.WorkerID && frame.EnrollmentGeneration == input.Hello.EnrollmentGeneration &&
		frame.ConnectionGeneration == input.Hello.ConnectionGeneration && frame.Sequence == 1 && frame.Ack == 1 &&
		frame.Challenge.SelectedVersion == frame.Version && bytes.Equal(frame.Challenge.WorkerNonce, input.Hello.Hello.WorkerNonce)
}

func challengeOfferMatches(challenge domain.AttachedWorkerAttachChallenge, frame *attachedworkerprotocol.ChallengeV1) bool {
	if frame == nil || frame.WorkerOffer.Window.Minimum != attachedworkerprotocol.ProtocolVersion(challenge.WorkerProtocolMinimum) ||
		frame.WorkerOffer.Window.Maximum != attachedworkerprotocol.ProtocolVersion(challenge.WorkerProtocolMaximum) ||
		frame.PlatformOffer.Window.Minimum != attachedworkerprotocol.ProtocolVersion(challenge.PlatformProtocolMinimum) ||
		frame.PlatformOffer.Window.Maximum != attachedworkerprotocol.ProtocolVersion(challenge.PlatformProtocolMaximum) ||
		len(frame.WorkerOffer.Supported) != len(challenge.WorkerProtocolVersions) ||
		len(frame.PlatformOffer.Supported) != len(challenge.PlatformProtocolVersions) {
		return false
	}
	for index, version := range challenge.WorkerProtocolVersions {
		if frame.WorkerOffer.Supported[index] != attachedworkerprotocol.ProtocolVersion(version) {
			return false
		}
	}
	for index, version := range challenge.PlatformProtocolVersions {
		if frame.PlatformOffer.Supported[index] != attachedworkerprotocol.ProtocolVersion(version) {
			return false
		}
	}
	return true
}

func validActivation(
	manifest attachedworkerlocal.ManifestV1,
	challenge domain.AttachedWorkerAttachChallenge,
	secret attachedworkertransport.ConnectionSecret,
	capabilityDigest []byte,
	attach attachedworkerprotocol.FrameV1,
	response attachedworkerhttp.ActivateResponseV1,
) bool {
	connection := response.Connection
	accepted := response.Accepted
	expectedCapability := domain.AttachedWorkerCapabilityDigest(hex.EncodeToString(capabilityDigest))
	expectedBinding := attachedworkertransport.ConnectionChannelBinding(
		challenge.ID, domain.DigestAttachedWorkerChallenge(attach.Attach.WorkerNonce),
		domain.DigestAttachedWorkerChallenge(attach.Attach.PlatformNonce), secret.Digest(),
	)
	return accepted.Validate() == nil && accepted.Kind == attachedworkerprotocol.MessageAttachAccepted && accepted.AttachAccepted != nil &&
		connection.TenantID == manifest.TenantID && connection.OwnerUserID == manifest.OwnerUserID && connection.WorkerID == manifest.WorkerID &&
		connection.ID == challenge.ConnectionID && connection.ActivationChallengeID == challenge.ID &&
		connection.EnrollmentGeneration == manifest.EnrollmentGeneration && connection.ConnectionGeneration == challenge.TargetConnectionGeneration &&
		connection.ProtocolVersion == uint32(attach.Version) && connection.CapabilityDigest == expectedCapability &&
		connection.SecretDigest == secret.Digest() && connection.ChannelBinding == expectedBinding &&
		connection.State == domain.AttachedWorkerConnectionAttaching && connection.PlatformSequence == 2 && connection.WorkerSequence == 2 &&
		connection.PlatformAck == 2 && connection.WorkerAck == 1 && !connection.ConnectedAt.IsZero() &&
		connection.AuthExpiresAt.After(connection.ConnectedAt) && connection.Revision != 0 &&
		accepted.Version == attach.Version && accepted.WorkerID == attach.WorkerID &&
		accepted.EnrollmentGeneration == attach.EnrollmentGeneration && accepted.ConnectionGeneration == attach.ConnectionGeneration &&
		accepted.Sequence == 2 && accepted.Ack == 2 && bytes.Equal(accepted.AttachAccepted.CapabilityDigest, capabilityDigest)
}

func validateCapabilityAuthority(local attachedworkerlocal.ManifestV1, capability attachedworkerprotocol.CapabilityManifestV1, offer attachedworkerprotocol.VersionOfferV1) error {
	if capability.Validate() != nil || capability.WorkerID != string(local.WorkerID) ||
		capability.EnrollmentGeneration != local.EnrollmentGeneration || !sameOffer(capability.ProtocolOffer, offer) {
		return ErrInvalidAuthority
	}
	harnessDigest, err := hex.DecodeString(local.Harness.SHA256)
	if err != nil || !bytes.Equal(harnessDigest, capability.HarnessExecutableDigest) {
		return ErrInvalidAuthority
	}
	return nil
}

func frameMatchesBinding(frame attachedworkerprotocol.FrameV1, binding ConnectionBindingV1) bool {
	return frame.WorkerID == string(binding.WorkerID) && frame.EnrollmentGeneration == binding.EnrollmentGeneration &&
		frame.ConnectionGeneration == binding.ConnectionGeneration && frame.Version == binding.ProtocolVersion
}

func validBinding(binding ConnectionBindingV1) bool {
	return binding.TenantID.Validate() == nil && binding.OwnerUserID.Validate() == nil && binding.WorkerID.Validate() == nil &&
		binding.EnrollmentGeneration != 0 && binding.ConnectionGeneration != 0 &&
		binding.ProtocolVersion == attachedworkerprotocol.ProtocolVersionV1 && binding.ConnectionID.Validate() == nil &&
		binding.CapabilityDigest.Validate() == nil && !binding.AuthenticationExpires.IsZero()
}

func sanitizeExchange(err error) error {
	if err == nil {
		return ErrConnectionSessionFailed
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var exchangeErr *attachedworkerhttp.ExchangeError
	if errors.As(err, &exchangeErr) {
		return exchangeErr.SanitizedCopy()
	}
	return ErrConnectionSessionFailed
}

func sanitizeLocal(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	for _, candidate := range []error{
		attachedworkerlocal.ErrInvalidState, attachedworkerlocal.ErrStateMissing, attachedworkerlocal.ErrStateIncomplete,
		attachedworkerlocal.ErrStateConflict, attachedworkerlocal.ErrStateAmbiguous, attachedworkerlocal.ErrStateBusy,
		attachedworkerlocal.ErrStateUnsupported, attachedworkerlocal.ErrSecretRetired, attachedworkerlocal.ErrLocalIO,
	} {
		if errors.Is(err, candidate) {
			return candidate
		}
	}
	return ErrConnectionSessionFailed
}

func sanitizeClose(err error) error {
	if err == nil {
		return nil
	}
	return ErrConnectionSessionFailed
}

func validAudience(value string) bool {
	return value != "" && len(value) <= 256 && strings.TrimSpace(value) == value && !strings.ContainsRune(value, '\x00')
}

func cloneOffer(offer attachedworkerprotocol.VersionOfferV1) attachedworkerprotocol.VersionOfferV1 {
	offer.Supported = append([]attachedworkerprotocol.ProtocolVersion(nil), offer.Supported...)
	return offer
}

func cloneConnectInput(input ConnectInputV1) (ConnectInputV1, error) {
	if input.ExpectedWorkerRevision == 0 || input.CapabilityManifest.Validate() != nil {
		return ConnectInputV1{}, ErrInvalidAuthority
	}
	manifest := input.CapabilityManifest
	manifest.ProtocolOffer = cloneOffer(manifest.ProtocolOffer)
	manifest.HarnessExecutableDigest = append([]byte(nil), manifest.HarnessExecutableDigest...)
	manifest.IsolationEvidence = append([]attachedworkerprotocol.IsolationEvidenceV1(nil), manifest.IsolationEvidence...)
	manifest.Features = append([]attachedworkerprotocol.ProtocolFeatureV1(nil), manifest.Features...)
	return ConnectInputV1{ExpectedWorkerRevision: input.ExpectedWorkerRevision, CapabilityManifest: manifest}, nil
}

func cloneBatch(batch attachedworkerprotocol.BatchV1) (attachedworkerprotocol.BatchV1, error) {
	encoded, err := attachedworkerprotocol.EncodeBatchV1(batch)
	if err != nil {
		return attachedworkerprotocol.BatchV1{}, err
	}
	return attachedworkerprotocol.DecodeBatchV1(encoded)
}

func sameOffer(left, right attachedworkerprotocol.VersionOfferV1) bool {
	if left.Window != right.Window || len(left.Supported) != len(right.Supported) {
		return false
	}
	for index := range left.Supported {
		if left.Supported[index] != right.Supported[index] {
			return false
		}
	}
	return true
}

func nextTime(now, previous time.Time) time.Time {
	if now.After(previous) {
		return now
	}
	return previous.Add(time.Microsecond)
}

func allZero(value []byte) bool {
	var nonzero byte
	for _, item := range value {
		nonzero |= item
	}
	return nonzero == 0
}

func clearLocalSecret(secret *attachedworkerlocal.SecretRecordV1) {
	if secret == nil {
		return
	}
	clearBytes(secret.IdentityPrivateKey)
	clearBytes(secret.ConnectionSecret)
	*secret = attachedworkerlocal.SecretRecordV1{}
}

func clearBytes(value []byte) {
	for index := range value {
		value[index] = 0
	}
}
