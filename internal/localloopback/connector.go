// Package localloopback owns one retained, literal-loopback HTTP connection per
// original local operation. Observation and object identity never grant a send:
// the same concrete Store must still authorize its current canonical joins.
package localloopback

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"gitcode.com/urandon/sessionless/internal/boundedhttp"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localoperator"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// ResultV1 reports transport only. SendAttempted means application bytes may
// have escaped, including a failed/partial write; it is NOT model acceptance.
// RequestBytes counts actual HTTP wire bytes, including headers. ResponseBytes
// counts only a complete returned body; a failed partial read is unknown, not
// measured zero usage. Usage/model interpretation belongs to the separate codec.
type ResultV1 struct {
	Body                        []byte
	RequestBytes, ResponseBytes uint64
	ObservedAt                  time.Time
	SendAttempted               bool
}

type KeeperV1 struct {
	mu                         sync.Mutex
	self                       *KeeperV1
	source                     *localoperator.Source
	sourceLifetime             context.Context
	issuer                     *serverlessharness.OperationCapabilityIssuer
	bootstrap                  *ydbstore.LocalOwnedStoreBootstrapV1
	config                     domain.ConfiguredInferenceProfileV1
	policy                     ports.LocalLoopbackEgressPolicyV1
	configDigest, policyDigest string
	key                        [32]byte
	active                     *ConnectionV1
	closed                     bool
	terminalDeadline           time.Time
	terminalEligible           bool
	terminalJoined             bool
	terminalTainted            bool
}

type ConnectionV1 struct {
	mu                                     sync.Mutex
	self                                   *ConnectionV1
	keeper                                 *KeeperV1
	store                                  *ydbstore.Store
	lease                                  *serverlessharness.LocalModelOperationLeaseV1
	allocation                             domain.RegisteredAllocationV2
	source                                 domain.AttemptExecutionSourceV1
	operation                              domain.AttemptOperationV1
	publication                            ports.OwnedProxyPublicationV2
	payload, request                       []byte
	conn                                   *net.TCPConn
	ctx                                    context.Context
	cancel                                 context.CancelFunc
	stopCaller                             func() bool
	preflightDone, invokeDone, cleanupDone chan struct{}
	ready, used, closed                    bool
	cleanupTainted, cleanupCommitted       bool
	cleanupTimeout                         time.Duration
}

func NewKeeperV1(source *localoperator.Source, issuer *serverlessharness.OperationCapabilityIssuer, bootstrap *ydbstore.LocalOwnedStoreBootstrapV1) (*KeeperV1, error) {
	if !bootstrap.HasLocalConnectorIdentity(source, issuer) {
		return nil, domain.ErrMembershipDenied
	}
	c, err := source.Configuration()
	if err != nil || c.Auth != domain.ConfiguredInferenceAuthNoneV1 {
		return nil, domain.ErrMembershipDenied
	}
	p, err := ports.BuildLocalLoopbackEgressPolicyV1(c)
	if err != nil {
		return nil, domain.ErrMembershipDenied
	}
	cd, err := c.Digest()
	if err != nil {
		return nil, domain.ErrMembershipDenied
	}
	pd, _ := p.Digest()
	lifetime, err := source.ExecutionLifetime()
	if err != nil || lifetime.Err() != nil {
		return nil, domain.ErrMembershipDenied
	}
	k := &KeeperV1{source: source, sourceLifetime: lifetime, issuer: issuer, bootstrap: bootstrap, config: c.Clone(), policy: p, configDigest: cd, policyDigest: pd}
	if _, err = rand.Read(k.key[:]); err != nil {
		clear(k.key[:])
		return nil, domain.ErrMembershipDenied
	}
	k.self = k
	return k, nil
}

// payloadCopy bounds before copying and binds the exact caller-supplied wire
// body. It makes no JSON/model/token-fit or current-authority claim.
func payloadCopy(op domain.AttemptOperationV1, payload []byte, limits domain.ConfiguredInferenceLimitsV1) ([]byte, error) {
	if limits.Validate() != nil || len(payload) == 0 || uint64(len(payload)) != op.RequestBytes || op.RequestBytes > limits.RequestBytes || op.ResultBytes == 0 || op.ResultBytes > limits.ResponseBytes || op.CleanupDuration <= 0 || op.CleanupDuration > limits.CleanupTimeout {
		return nil, domain.ErrMembershipDenied
	}
	owned := bytes.Clone(payload)
	h := sha256.Sum256(owned)
	if hex.EncodeToString(h[:]) != op.RequestDigest {
		clear(owned)
		return nil, domain.ErrMembershipDenied
	}
	return owned, nil
}

func callerFits(ctx context.Context, deadline time.Time) bool {
	if ctx == nil || ctx.Err() != nil {
		return false
	}
	d, ok := ctx.Deadline()
	return !ok || !d.Before(deadline)
}

func literalAddress(baseURL string) (network, address string, ip net.IP, port int, err error) {
	if domain.ValidateConfiguredInferenceEndpointV1(baseURL) != nil {
		err = domain.ErrMembershipDenied
		return
	}
	address = strings.TrimSuffix(strings.TrimPrefix(baseURL, "http://"), "/v1")
	h, p, e := net.SplitHostPort(address)
	if e != nil {
		err = domain.ErrMembershipDenied
		return
	}
	port, e = strconv.Atoi(p)
	ip = net.ParseIP(h)
	if e != nil || ip == nil || (h != "127.0.0.1" && h != "::1") {
		err = domain.ErrMembershipDenied
		return
	}
	network = "tcp4"
	if h == "::1" {
		network = "tcp6"
	}
	return
}

func operationMatches(op domain.AttemptOperationV1, a domain.RegisteredAllocationV2, s domain.AttemptExecutionSourceV1) bool {
	x := op.Scope
	return (op.Kind == domain.AttemptOperationModelV1 || op.Kind == domain.AttemptOperationCompactionV1) && x.TenantID == s.Scope.TenantID && x.OwnerUserID == s.Scope.OwnerUserID && x.SessionID == s.Scope.SessionID && x.RunID == s.Scope.RunID && x.AttemptID == s.Scope.AttemptID && x.LeaseID == a.Lease.ID && x.FenceToken == a.Lease.FenceToken && x.PhysicalInvocationClaimID == a.ParentReservation.PhysicalInvocationClaimID && x.EffectReservationDigest == a.ParentReservationDigest && x.InvocationAuthorityDigest == a.ParentReservation.InvocationAuthorityDigest && x.HarnessBindingDigest == s.HarnessBindingDigest && x.AgentLoopPolicyDigest == s.AgentLoopPolicyDigest
}

func (k *KeeperV1) Preflight(ctx context.Context, allocation domain.RegisteredAllocationV2, source domain.AttemptExecutionSourceV1, operation domain.AttemptOperationV1, payload []byte) (_ *ConnectionV1, err error) {
	if k == nil || k.self != k || k.sourceLifetime == nil || k.sourceLifetime.Err() != nil || !callerFits(ctx, operation.Deadline) {
		return nil, domain.ErrMembershipDenied
	}
	// Freeze every graph and request fact before consuming the original slot or
	// opening a socket. The bridge separately authenticates its actual source.
	a, s, op := allocation.Clone(), source.Clone(), operation.Clone()
	now := time.Now().UTC().Truncate(time.Microsecond)
	ad, e := a.Digest()
	sd, se := s.Digest()
	od, oe := op.Digest()
	current, ce := k.source.Configuration()
	cd, cde := current.Digest()
	if e != nil || se != nil || oe != nil || ce != nil || cde != nil || cd != k.configDigest || a.ValidateAt(now) != nil || a.LocalRuntime == nil || a.SourceDigest != sd || a.Scope != s.Scope || a.Backend.BackendProfileDigest != k.configDigest || a.Substrate.EgressPolicyDigest != k.policyDigest || !operationMatches(op, a, s) || !now.Before(op.Deadline) || op.Deadline.After(now.Add(k.policy.Limits.OperationTimeout)) || op.Deadline.After(a.Window.ExpiresAt) {
		return nil, domain.ErrMembershipDenied
	}
	owned, e := payloadCopy(op, payload, k.policy.Limits)
	if e != nil {
		return nil, e
	}
	network, address, ip, port, e := literalAddress(k.policy.BaseURL)
	if e != nil {
		clear(owned)
		return nil, e
	}
	request := append([]byte("POST /v1/chat/completions HTTP/1.1\r\nHost: "+address+"\r\nContent-Type: application/json\r\nAccept: application/json\r\nConnection: close\r\nContent-Length: "+strconv.Itoa(len(owned))+"\r\n\r\n"), owned...)
	store, bridge, e := k.bootstrap.RuntimeFor(k)
	if e != nil {
		clear(owned)
		clear(request)
		return nil, domain.ErrMembershipDenied
	}
	k.mu.Lock()
	if k.closed || k.active != nil || k.sourceLifetime.Err() != nil {
		k.mu.Unlock()
		clear(owned)
		clear(request)
		return nil, domain.ErrMembershipDenied
	}
	lease, e := bridge.BeginLocalModelOperation(k.source, a, op, now)
	if e != nil {
		k.mu.Unlock()
		clear(owned)
		clear(request)
		return nil, domain.ErrMembershipDenied
	}
	ownedCtx, cancel := context.WithCancel(lease.Context())
	c := &ConnectionV1{keeper: k, store: store, lease: lease, allocation: a, source: s, operation: op, payload: owned, request: request, ctx: ownedCtx, cancel: cancel, preflightDone: make(chan struct{}), invokeDone: make(chan struct{}), cleanupDone: make(chan struct{}), cleanupTimeout: op.CleanupDuration}
	c.self = c
	c.stopCaller = context.AfterFunc(ctx, cancel)
	k.active = c
	k.mu.Unlock()
	go c.monitor()
	defer func() {
		close(c.preflightDone)
		if err != nil {
			_ = c.Close()
		}
	}()
	raw, e := (&net.Dialer{}).DialContext(c.ctx, network, address)
	if e != nil {
		return nil, domain.ErrMembershipDenied
	}
	tcp, ok := raw.(*net.TCPConn)
	if !ok {
		_ = raw.Close()
		return nil, domain.ErrMembershipDenied
	}
	remote, ok := tcp.RemoteAddr().(*net.TCPAddr)
	if !ok || remote.Zone != "" || remote.Port != port || !remote.IP.Equal(ip) {
		_ = tcp.Close()
		return nil, domain.ErrMembershipDenied
	}
	if tcp.SetDeadline(op.Deadline) != nil {
		_ = tcp.Close()
		return nil, domain.ErrMembershipDenied
	}
	observed := time.Now().UTC().Truncate(time.Microsecond)
	peer, _ := k.policy.LiteralPeerDigest()
	r := a.LocalRuntime
	observation := domain.RegisteredLocalLoopbackProxyV2{ConfiguredProfileDigest: k.configDigest, EndpointPolicy: k.policy.EndpointPolicy, ConnectorArtifactDigest: k.policy.ConnectorArtifactDigest, InstallationID: r.InstallationID, InstallationRevision: r.InstallationRevision, HostIdentityDigest: k.policy.HostIdentityDigest, NetworkNamespaceIdentityDigest: k.policy.NetworkNamespaceIdentityDigest, LiteralPeerDigest: peer}
	att := ports.LocalLoopbackEgressAttestationV1{Version: 1, PolicyDigest: k.policyDigest, Observation: observation, ObservedAt: observed, ExpiresAt: op.Deadline}
	pb, pe := json.Marshal(k.policy)
	ab, ae := json.Marshal(att)
	pub := ports.OwnedProxyPublicationV2{TransportKind: domain.RegisteredProxyTransportLocalLoopbackHTTPV2, OperationSequence: op.Sequence, OperationDigest: od, OperationDeadline: op.Deadline, ServiceKind: domain.RegisteredProxyProviderV1, Scope: s.Scope, LeaseID: a.Lease.ID, FenceToken: a.Lease.FenceToken, PhysicalInvocationClaimID: a.ParentReservation.PhysicalInvocationClaimID, RegisteredAllocationDigest: ad, SourceDigest: sd, ObservedAt: observed, PolicyBody: pb, AttestationBody: ab, Authenticator: make([]byte, sha256.Size)}
	transport, _ := k.policy.TransportDigest()
	_, be := domain.BuildRegisteredLocalLoopbackProxyAttestationV2(a, s, transport, k.policyDigest, observation, ports.OwnedProxyBodyReferenceV1("policy", pb), ports.OwnedProxyBodyReferenceV1("attestation", ab), domain.OwnedAgentAuthorityWindowV1{ObservedAt: observed, PublishedAt: observed, ExpiresAt: op.Deadline}, op.Sequence, od)
	if pe != nil || ae != nil || be != nil || pub.Validate() != nil || c.ctx.Err() != nil || !observed.Before(op.Deadline) {
		_ = tcp.Close()
		return nil, domain.ErrMembershipDenied
	}
	intent, _ := pub.IntentBytes()
	pub.Authenticator = k.authenticate(intent)
	k.mu.Lock()
	c.mu.Lock()
	if k.closed || k.active != c || c.closed || c.ctx.Err() != nil || k.sourceLifetime.Err() != nil {
		c.mu.Unlock()
		k.mu.Unlock()
		_ = tcp.Close()
		return nil, domain.ErrMembershipDenied
	}
	c.conn = tcp
	c.publication = pub
	c.ready = true
	c.mu.Unlock()
	k.mu.Unlock()
	return c, nil
}

func (k *KeeperV1) authenticate(intent []byte) []byte {
	h := hmac.New(sha256.New, k.key[:])
	_, _ = h.Write([]byte("sessionless.actual-local-loopback-connection.v1\x00"))
	_, _ = h.Write([]byte(k.configDigest))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(k.policyDigest))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write(intent)
	return h.Sum(nil)
}

// Historical authentication deliberately survives Close and later operations.
// It never samples a clock, touches a socket, or claims current permission.
func (k *KeeperV1) VerifyOwnedProxyPublication(pub ports.OwnedProxyPublicationV2, at time.Time) error {
	if k == nil || k.self != k || pub.Validate() != nil || pub.TransportKind != domain.RegisteredProxyTransportLocalLoopbackHTTPV2 || at.Before(pub.ObservedAt) {
		return domain.ErrMembershipDenied
	}
	b, e := pub.Bodies()
	if e != nil || b.LocalLoopback == nil {
		return domain.ErrMembershipDenied
	}
	pd, _ := b.LocalLoopback.Policy.Digest()
	if pd != k.policyDigest || b.LocalLoopback.Attestation.Observation.ConfiguredProfileDigest != k.configDigest {
		return domain.ErrMembershipDenied
	}
	intent, e := pub.IntentBytes()
	if e != nil || !hmac.Equal(pub.Authenticator, k.authenticate(intent)) {
		return domain.ErrMembershipDenied
	}
	return nil
}
func (k *KeeperV1) VerifyOwnedProxyPublicationCurrent(pub ports.OwnedProxyPublicationV2, at time.Time) error {
	if k.VerifyOwnedProxyPublication(pub, at) != nil {
		return domain.ErrMembershipDenied
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.closed || k.active == nil || k.sourceLifetime == nil || k.sourceLifetime.Err() != nil {
		return domain.ErrMembershipDenied
	}
	c := k.active
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.self != c || c.closed || c.used || !c.ready || c.conn == nil || c.ctx.Err() != nil || !at.Before(c.operation.Deadline) || !samePublication(pub, c.publication) {
		return domain.ErrMembershipDenied
	}
	return nil
}
func samePublication(a, b ports.OwnedProxyPublicationV2) bool {
	x, xe := a.IntentBytes()
	y, ye := b.IntentBytes()
	return xe == nil && ye == nil && bytes.Equal(x, y) && hmac.Equal(a.Authenticator, b.Authenticator)
}
func (c *ConnectionV1) Publication() (ports.OwnedProxyPublicationV2, error) {
	if c == nil || c.self != c {
		return ports.OwnedProxyPublicationV2{}, domain.ErrMembershipDenied
	}
	c.keeper.mu.Lock()
	defer c.keeper.mu.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.keeper.closed || c.keeper.active != c || c.closed || c.used || !c.ready || c.ctx.Err() != nil || c.keeper.sourceLifetime.Err() != nil {
		return ports.OwnedProxyPublicationV2{}, domain.ErrMembershipDenied
	}
	return c.publication.Clone(), nil
}

func (c *ConnectionV1) Invoke(ctx context.Context, registered domain.RegisteredProxyAttestationV2, capability ports.AttemptOperationCapabilityV1, payload []byte) (result ResultV1, err error) {
	if c == nil || c.self != c {
		return result, domain.ErrMembershipDenied
	}
	c.keeper.mu.Lock()
	c.mu.Lock()
	if c.keeper.closed || c.keeper.active != c || c.closed || c.used || !c.ready || c.conn == nil || c.ctx.Err() != nil || c.keeper.sourceLifetime.Err() != nil {
		c.mu.Unlock()
		c.keeper.mu.Unlock()
		return result, domain.ErrMembershipDenied
	}
	// Even failed first invocation cannot be retried. A denied concurrent copy
	// does not close or interfere with the already-owning invocation.
	c.used = true
	c.mu.Unlock()
	c.keeper.mu.Unlock()
	defer func() {
		close(c.invokeDone)
		if cleanupErr := c.Close(); cleanupErr != nil && err == nil {
			clear(result.Body)
			result.Body = nil
			result.ResponseBytes = 0
			err = domain.ErrMembershipDenied
		}
	}()
	if !callerFits(ctx, c.operation.Deadline) {
		return result, domain.ErrMembershipDenied
	}
	stop := context.AfterFunc(ctx, c.cancel)
	defer stop()
	r, cap := registered.Clone(), capability.Clone()
	now := time.Now().UTC().Truncate(time.Microsecond)
	if cap.Validate() != nil || r.ValidateInitial() != nil || r.ValidateAt(now) != nil || r.ValidateForAllocation(c.allocation, c.source) != nil || r.TransportKind != domain.RegisteredProxyTransportLocalLoopbackHTTPV2 || r.LocalLoopback == nil || r.OperationDigest != c.publication.OperationDigest || r.OperationSequence != c.operation.Sequence || r.PolicyDigest != c.keeper.policyDigest || r.PolicyBody != ports.OwnedProxyBodyReferenceV1("policy", c.publication.PolicyBody) || r.AttestationBody != ports.OwnedProxyBodyReferenceV1("attestation", c.publication.AttestationBody) || !r.Window.ObservedAt.Equal(c.publication.ObservedAt) || !r.Window.ExpiresAt.Equal(c.operation.Deadline) {
		return result, domain.ErrMembershipDenied
	}
	pd, _ := r.Digest()
	od, _ := cap.Reservation.Operation.Digest()
	bodies, bodyErr := c.publication.Bodies()
	transport, _ := c.keeper.policy.TransportDigest()
	if bodyErr != nil || bodies.LocalLoopback == nil || *r.LocalLoopback != bodies.LocalLoopback.Attestation.Observation || r.TransportDigest != transport {
		return result, domain.ErrMembershipDenied
	}
	if pd != cap.Reservation.RegisteredProxyDigest || cap.Reservation.RegisteredAllocationDigest != c.publication.RegisteredAllocationDigest || od != c.publication.OperationDigest || cap.Reservation.OperationDigest != od {
		return result, domain.ErrMembershipDenied
	}
	owned, e := payloadCopy(c.operation, payload, c.keeper.policy.Limits)
	if e != nil {
		return result, e
	}
	defer clear(owned)
	if !bytes.Equal(owned, c.payload) || c.ctx.Err() != nil || c.keeper.sourceLifetime.Err() != nil {
		return result, domain.ErrMembershipDenied
	}
	store, _, e := c.keeper.bootstrap.RuntimeFor(c.keeper)
	if e != nil || store != c.store || c.lease.Consume(c.operation, c.publication.RegisteredAllocationDigest, now) != nil {
		return result, domain.ErrMembershipDenied
	}
	// The socket/lease has been irreversibly consumed before the sole concrete
	// Store's current joins and one-use capability consumption. Never bypass it.
	if c.store.AuthorizeAttemptOperationBeforeSend(c.ctx, cap) != nil || c.ctx.Err() != nil || c.keeper.sourceLifetime.Err() != nil || ctx.Err() != nil || !time.Now().Before(c.operation.Deadline) {
		return result, domain.ErrMembershipDenied
	}
	// Admission closure is not an I/O join: an invocation already past this
	// check may still be in flight until the original monitor joins it.
	c.keeper.mu.Lock()
	sealed := c.keeper.closed
	c.keeper.mu.Unlock()
	if sealed {
		return result, domain.ErrMembershipDenied
	}
	result.SendAttempted = true
	n, e := c.conn.Write(c.request)
	result.RequestBytes = uint64(n)
	if e != nil || n != len(c.request) {
		result.ObservedAt = time.Now().UTC().Truncate(time.Microsecond)
		return result, domain.ErrMembershipDenied
	}
	body, e := boundedhttp.ReadOKResponse(c.conn, c.operation.ResultBytes, c.keeper.policy.Limits.ResponseHeaderBytes)
	result.ObservedAt = time.Now().UTC().Truncate(time.Microsecond)
	if e != nil || c.ctx.Err() != nil || ctx.Err() != nil || !result.ObservedAt.Before(c.operation.Deadline) {
		clear(body)
		return result, domain.ErrMembershipDenied
	}
	result.Body = body
	result.ResponseBytes = uint64(len(body))
	return result, nil
}

// monitor joins every owned dial/read/write before releasing the original
// operation slot. A timed-out caller Close does not release a tainted slot;
// this monitor alone can release an untainted slot after all I/O and buffers
// have joined. A cleanup timeout permanently taints the original lease slot.
func (c *ConnectionV1) monitor() {
	select {
	case <-c.ctx.Done():
	case <-c.keeper.sourceLifetime.Done():
		c.cancel()
	}
	c.mu.Lock()
	c.closed = true
	conn := c.conn
	c.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
	<-c.preflightDone
	c.mu.Lock()
	conn = c.conn
	invoked := c.used
	c.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
	if invoked {
		<-c.invokeDone
	}
	if c.stopCaller != nil {
		c.stopCaller()
	}
	clear(c.payload)
	clear(c.request)
	c.mu.Lock()
	tainted := c.cleanupTainted
	if !tainted {
		_ = c.lease.Close()
	}
	c.cleanupCommitted = true
	c.mu.Unlock()
	if !tainted {
		c.keeper.mu.Lock()
		if c.keeper.active == c {
			c.keeper.active = nil
		}
		c.keeper.mu.Unlock()
	}
	close(c.cleanupDone)
}
func (c *ConnectionV1) Close() error {
	if c == nil {
		return nil
	}
	if c.self != c || c.cancel == nil || c.cleanupDone == nil || c.cleanupTimeout <= 0 {
		return domain.ErrMembershipDenied
	}
	deadline := time.Now().Add(c.cleanupTimeout)
	if c.keeper != nil {
		c.keeper.mu.Lock()
		if d := c.keeper.terminalDeadline; !d.IsZero() && d.Before(deadline) {
			deadline = d
		}
		c.keeper.mu.Unlock()
	}
	return c.closeUntil(context.Background(), deadline)
}

func (c *ConnectionV1) closeUntil(ctx context.Context, deadline time.Time) error {
	c.cancel()
	timer := time.NewTimer(max(time.Until(deadline), 0))
	defer timer.Stop()
	select {
	case <-c.cleanupDone:
		return c.closeAfterWait(false)
	case <-timer.C:
		return c.closeAfterWait(true)
	case <-ctx.Done():
		return c.closeAfterWait(true)
	}
}

// A timeout is a failure bound, not evidence that cleanup is still pending:
// both select cases may be ready. Resolve that race before recording a verdict.
func (c *ConnectionV1) closeAfterWait(timedOut bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if timedOut {
		select {
		case <-c.cleanupDone:
			timedOut = false
		default:
		}
	}
	if timedOut {
		if !c.cleanupCommitted {
			c.cleanupTainted = true
		}
		return domain.ErrMembershipDenied
	}
	if c.cleanupTainted {
		return domain.ErrMembershipDenied
	}
	return nil
}
func (k *KeeperV1) Close() error {
	if k == nil {
		return nil
	}
	if k.self != k {
		return domain.ErrMembershipDenied
	}
	k.mu.Lock()
	terminal := !k.terminalDeadline.IsZero()
	if terminal {
		k.mu.Unlock()
		return k.StopAndJoinTerminal(context.Background())
	}
	k.closed = true
	c := k.active
	k.mu.Unlock()
	if c != nil {
		return c.Close()
	}
	return nil
}

// SealTerminal closes original transport admission irreversibly. A successful
// idle seal is not a stop, retirement, custody or Store-mutation receipt.
// An active/previously closed owner remains ineligible even after it drains.
func (k *KeeperV1) SealTerminal(deadline time.Time) error {
	if k == nil || k.self != k || k.source == nil || k.bootstrap == nil || k.sourceLifetime == nil {
		return domain.ErrMembershipDenied
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if !k.terminalDeadline.IsZero() {
		if !deadline.Equal(k.terminalDeadline) || !k.terminalEligible || k.terminalTainted {
			return domain.ErrMembershipDenied
		}
		return nil
	}
	now := time.Now()
	limit := min(k.config.Limits.CleanupTimeout, k.policy.Limits.CleanupTimeout, 30*time.Second)
	if k.active != nil {
		limit = min(limit, k.active.cleanupTimeout)
	}
	if limit <= 0 || deadline.IsZero() || !deadline.After(now) || deadline.After(now.Add(limit)) {
		return domain.ErrMembershipDenied
	}
	k.terminalDeadline = deadline
	k.terminalEligible = !k.closed && k.active == nil && k.sourceLifetime.Err() == nil
	k.closed = true
	if !k.terminalEligible {
		return domain.ErrMembershipDenied
	}
	return nil
}

// StopAndJoinTerminal cancels only owned transport work and observes its real
// monitor join. It never renews the original absolute terminal deadline, and
// successful failure-cleanup cannot repair an ineligible seal.
func (k *KeeperV1) StopAndJoinTerminal(ctx context.Context) error {
	if k == nil || k.self != k || ctx == nil {
		return domain.ErrMembershipDenied
	}
	k.mu.Lock()
	deadline, c := k.terminalDeadline, k.active
	if deadline.IsZero() || k.terminalTainted {
		k.mu.Unlock()
		return domain.ErrMembershipDenied
	}
	if k.terminalJoined {
		k.mu.Unlock()
		return nil
	}
	k.mu.Unlock()
	var err error
	if c != nil {
		deadline = minTime(deadline, time.Now().Add(c.cleanupTimeout))
		err = c.closeUntil(ctx, deadline)
	} else if ctx.Err() != nil || !time.Now().Before(deadline) {
		err = domain.ErrMembershipDenied
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if err != nil {
		k.terminalTainted = true
		return domain.ErrMembershipDenied
	}
	if k.terminalTainted {
		return domain.ErrMembershipDenied
	}
	k.terminalJoined = true
	return nil
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
func (*KeeperV1) String() string         { return "KeeperV1{private:[redacted]}" }
func (k *KeeperV1) GoString() string     { return k.String() }
func (*ConnectionV1) String() string     { return "ConnectionV1{private:[redacted]}" }
func (c *ConnectionV1) GoString() string { return c.String() }

func (ResultV1) String() string     { return "ResultV1{body:[redacted]}" }
func (r ResultV1) GoString() string { return r.String() }
