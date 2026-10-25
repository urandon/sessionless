package localloopback

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"net"
	"net/netip"
	"strconv"
	"time"

	"gitcode.com/urandon/sessionless/internal/boundedhttp"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/mcpwire"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/serverlessegress"
)

// Private tests may substitute the external DNS/TLS observation. Public
// construction has no URL, root-CA, resolver, socket, credential or SDK override.
type mcpHTTPSDependencies struct {
	lookup func(context.Context, string) ([]netip.Addr, error)
	dial   func(context.Context, string, string) (net.Conn, error)
	roots  *x509.CertPool
}

type mcpConnectionV1 struct {
	policy                    ports.LocalPublicMCPHTTPSProxyPolicyV1
	request                   mcpwire.RequestV1
	descriptor                mcpwire.DescriptorV1
	toolSendAttempted         bool
	messages, frames, inbound uint64
}

// Pure finite-counter DATA calculations, not current/send permission. Counters
// are retained only by the original keeper and never supplied by a caller.
type mcpExchangeUsageV1 struct{ messages, frames, inbound uint64 }

func mcpExchangeReadLimits(limits mcpwire.LimitsV1, debit domain.AttemptOperationDebitV1, used, total mcpExchangeUsageV1, notification bool) (uint64, uint64, error) {
	if limits.Validate() != nil || debit.MCPOutboundMessages == 0 || debit.MCPOutboundMessages > 12 || debit.MCPInboundBytes == 0 || debit.MCPInboundBytes > 256<<10 || debit.MCPInboundFrames == 0 || debit.MCPInboundFrames > 128 {
		return 0, 0, domain.ErrMembershipDenied
	}
	byteCeiling := min(limits.AggregateBytes, debit.MCPInboundBytes, debit.ResponseBytes, uint64(256<<10))
	frameCeiling := min(limits.SSEEvents, debit.MCPInboundFrames, uint64(128))
	if total.messages >= 12 || used.messages >= debit.MCPOutboundMessages || used.inbound > byteCeiling || used.frames > frameCeiling || total.inbound > 256<<10 || total.frames > 128 {
		return 0, 0, domain.ErrMembershipDenied
	}
	remainingBytes := min(byteCeiling-used.inbound, uint64(256<<10)-total.inbound)
	remainingFrames := min(frameCeiling-used.frames, uint64(128)-total.frames)
	if !notification && (remainingBytes == 0 || remainingFrames == 0) {
		return 0, 0, domain.ErrMembershipDenied
	}
	return min(limits.MessageBytes, remainingBytes), remainingFrames, nil
}

// There is deliberately no credential/header/URL parameter in this protocol
// encoder. Public MCP can never borrow the model's Authorization buffer.
func mcpRequestWire(body []byte) []byte {
	prefix := "POST /mcp HTTP/1.1\r\nHost: developers.openai.com\r\nContent-Type: application/json\r\nAccept: application/json, text/event-stream\r\nMCP-Protocol-Version: " + mcpwire.ProtocolVersionV1 + "\r\nContent-Length: " + strconv.Itoa(len(body)) + "\r\n\r\n"
	wire := make([]byte, 0, len(prefix)+len(body))
	wire = append(wire, prefix...)
	return append(wire, body...)
}

// PreflightMCP retains an actual public-address-pinned TLS connection under the
// SAME original allocation/Store/cleanup owner, but a distinct MCP proxy arm.
// It performs no HTTP or tools/call and never reads the provider credential.
// This first stateless subset owns one catalog per invocation. Stateful
// sessions/GET/DELETE/resumption are unavailable, not silently emulated.
func (k *KeeperV1) PreflightMCP(ctx context.Context, a domain.RegisteredAllocationV2, s domain.AttemptExecutionSourceV1, op domain.AttemptOperationV1, request mcpwire.RequestV1) (_ *ConnectionV1, err error) {
	if k == nil || k.self != k || !callerFits(ctx, op.Deadline) || k.sourceLifetime == nil || k.sourceLifetime.Err() != nil {
		return nil, domain.ErrMembershipDenied
	}
	a, s, op = a.Clone(), s.Clone(), op.Clone()
	now := time.Now().UTC().Truncate(time.Microsecond)
	grant, e := k.source.ReadMCPToolGrant(ctx, s.Profile, now)
	policy, pe := ports.BuildLocalPublicMCPHTTPSProxyPolicyV1(k.config, grant)
	pd, pde := policy.Digest()
	ad, ae := a.Digest()
	sd, se := s.Digest()
	od, oe := op.Digest()
	if e != nil || pe != nil || pde != nil || ae != nil || se != nil || oe != nil || a.ValidateAt(now) != nil || a.LocalRuntime == nil || a.SourceDigest != sd || !operationScopeMatches(op, a, s) || op.Kind != domain.AttemptOperationMCPToolV1 || op.ToolGrantDigest != policy.GrantDigest || s.Spending[2].ScopedGrantDigest != policy.GrantDigest || !now.Before(op.Deadline) || op.Deadline.After(now.Add(k.config.Limits.OperationTimeout)) || op.Deadline.After(a.Window.ExpiresAt) || op.Deadline.After(grant.ExpiresAt) || request.Kind() != mcpwire.RequestCallToolV1 || request.ID() != op.CallID || request.DescriptorDigest() != policy.DescriptorDigest {
		return nil, domain.ErrMembershipDenied
	}
	payload := request.Body()
	defer func() {
		if err != nil {
			clear(payload)
		}
	}()
	h := sha256.Sum256(payload)
	if len(payload) == 0 || uint64(len(payload)) != op.RequestBytes || hex.EncodeToString(h[:]) != op.RequestDigest || op.RequestBytes > 4096 || op.ResultBytes > 64<<10 || op.CleanupDuration <= 0 || op.CleanupDuration > k.config.Limits.CleanupTimeout {
		return nil, domain.ErrMembershipDenied
	}
	descriptor, e := mcpwire.ParseDescriptorV1(grant.Descriptor, grant.Limits)
	if e != nil {
		return nil, domain.ErrMembershipDenied
	}
	store, bridge, e := k.bootstrap.RuntimeFor(k)
	if e != nil {
		return nil, domain.ErrMembershipDenied
	}
	k.mu.Lock()
	if k.closed || k.active != nil || k.mcpCatalogConsumed || !k.credentialAdmittedLocked(k.credentialScope, a, s, op, now) || (k.credentialed && len(k.credentialScope.joins) >= 12) {
		k.mu.Unlock()
		return nil, domain.ErrMembershipDenied
	}
	lease, e := bridge.BeginLocalMCPProtocolOperation(k.source, a, op, now)
	if e != nil {
		k.mu.Unlock()
		return nil, domain.ErrMembershipDenied
	}
	k.mcpCatalogConsumed = true // preparation failure never restores this slot
	call, cancel := context.WithCancel(lease.Context())
	c := &ConnectionV1{keeper: k, store: store, lease: lease, allocation: a, source: s, operation: op, payload: payload, ctx: call, cancel: cancel, preflightDone: make(chan struct{}), invokeDone: make(chan struct{}), cleanupDone: make(chan struct{}), cleanupTimeout: op.CleanupDuration, credentialScope: k.credentialScope, mcp: &mcpConnectionV1{policy: policy, request: request, descriptor: descriptor}}
	c.self = c
	c.stopCaller = context.AfterFunc(ctx, cancel)
	if k.credentialed {
		c.stopCredential = context.AfterFunc(k.credentialScope.ctx, cancel)
		k.credentialScope.joins = append(k.credentialScope.joins, c.cleanupDone)
	}
	k.active = c
	deps := k.mcpDeps
	k.mu.Unlock()
	go c.monitor()
	defer func() {
		close(c.preflightDone)
		if err != nil {
			_ = c.Close()
		}
	}()
	if deps.lookup == nil {
		deps.lookup = func(ctx context.Context, host string) ([]netip.Addr, error) {
			return net.DefaultResolver.LookupNetIP(ctx, "ip", host)
		}
	}
	if deps.dial == nil {
		deps.dial = (&net.Dialer{}).DialContext
	}
	addresses, e := deps.lookup(call, "developers.openai.com")
	if e != nil {
		return nil, domain.ErrMembershipDenied
	}
	literals, resolution, e := serverlessegress.PublicResolutionDataV1(addresses)
	if e != nil {
		return nil, domain.ErrMembershipDenied
	}
	raw, e := deps.dial(call, "tcp", net.JoinHostPort(literals[0], "443"))
	if e != nil {
		return nil, domain.ErrMembershipDenied
	}
	remote, ok := raw.RemoteAddr().(*net.TCPAddr)
	if !ok || remote.Zone != "" || remote.Port != 443 || !remote.IP.Equal(net.ParseIP(literals[0])) || raw.SetDeadline(op.Deadline) != nil {
		_ = raw.Close()
		return nil, domain.ErrMembershipDenied
	}
	c.mu.Lock()
	if c.closed || call.Err() != nil {
		c.mu.Unlock()
		_ = raw.Close()
		return nil, domain.ErrMembershipDenied
	}
	c.conn = raw
	c.mu.Unlock()
	secure := tls.Client(raw, &tls.Config{ServerName: "developers.openai.com", RootCAs: deps.roots, MinVersion: tls.VersionTLS12, NextProtos: []string{"http/1.1"}})
	if secure.HandshakeContext(call) != nil || secure.ConnectionState().NegotiatedProtocol != "http/1.1" || len(secure.ConnectionState().VerifiedChains) == 0 {
		return nil, domain.ErrMembershipDenied
	}
	observed := time.Now().UTC().Truncate(time.Microsecond)
	r := a.LocalRuntime
	o := domain.RegisteredLocalPublicMCPHTTPSProxyV1{ConfiguredProfileDigest: k.configDigest, EndpointPolicy: policy.EndpointPolicy, ConnectorArtifactDigest: k.config.WorkerExecutableDigest, InstallationID: r.InstallationID, InstallationRevision: r.InstallationRevision, HostIdentityDigest: k.config.LocalHostIdentityDigest, NetworkNamespaceIdentityDigest: k.config.LocalNetworkNamespaceIdentityDigest, ResolutionSetDigest: resolution, GrantDigest: policy.GrantDigest, DescriptorDigest: policy.DescriptorDigest, MCPProfileDigest: policy.MCPProfileDigest}
	att := ports.LocalPublicMCPHTTPSProxyAttestationV1{Version: 1, PolicyDigest: pd, Observation: o, ConnectionAddressPinned: true, DNSRebindingDenied: true, PublicUnicastOnly: true, RedirectsDenied: true, AmbientProxyDenied: true, CertificateValidationRequired: true, TLSHandshakeVerified: true, VerifiedSNI: "developers.openai.com", ResolvedHost: "developers.openai.com", ResolvedPort: 443, ObservedAt: observed, ExpiresAt: op.Deadline}
	pb, pbe := json.Marshal(policy)
	ab, abe := json.Marshal(att)
	pub := ports.OwnedProxyPublicationV2{TransportKind: domain.RegisteredProxyTransportLocalPublicMCPHTTPSV1, OperationSequence: op.Sequence, OperationDigest: od, OperationDeadline: op.Deadline, ServiceKind: domain.RegisteredProxyMCPToolV1, Scope: s.Scope, LeaseID: a.Lease.ID, FenceToken: a.Lease.FenceToken, PhysicalInvocationClaimID: a.ParentReservation.PhysicalInvocationClaimID, RegisteredAllocationDigest: ad, SourceDigest: sd, ObservedAt: observed, PolicyBody: pb, AttestationBody: ab, Authenticator: make([]byte, sha256.Size)}
	if pbe != nil || abe != nil || pub.Validate() != nil || att.ValidateForPolicy(policy, observed, op.Deadline) != nil {
		return nil, domain.ErrMembershipDenied
	}
	intent, _ := pub.IntentBytes()
	pub.Authenticator = k.authenticateMCP(intent)
	k.mu.Lock()
	c.mu.Lock()
	defer k.mu.Unlock()
	defer c.mu.Unlock()
	if k.closed || k.active != c || c.closed || call.Err() != nil || !k.credentialAdmittedLocked(c.credentialScope, a, s, op, time.Now()) {
		return nil, domain.ErrMembershipDenied
	}
	c.conn = secure
	c.publication = pub
	c.ready = true
	return c, nil
}

func (k *KeeperV1) authenticateMCP(intent []byte) []byte {
	h := hmac.New(sha256.New, k.key[:])
	_, _ = h.Write([]byte("sessionless.actual-local-public-mcp-https.v1\x00"))
	_, _ = h.Write([]byte(k.configDigest))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write(intent)
	return h.Sum(nil)
}
func (k *KeeperV1) verifyMCPPublication(pub ports.OwnedProxyPublicationV2, at time.Time) error {
	if k == nil || k.self != k || pub.Validate() != nil || pub.TransportKind != domain.RegisteredProxyTransportLocalPublicMCPHTTPSV1 || pub.ServiceKind != domain.RegisteredProxyMCPToolV1 || at.Before(pub.ObservedAt) {
		return domain.ErrMembershipDenied
	}
	b, e := pub.Bodies()
	if e != nil || b.LocalPublicMCPHTTPS == nil || b.LocalPublicMCPHTTPS.Policy.ConfiguredProfileDigest != k.configDigest {
		return domain.ErrMembershipDenied
	}
	intent, e := pub.IntentBytes()
	if e != nil || !hmac.Equal(pub.Authenticator, k.authenticateMCP(intent)) {
		return domain.ErrMembershipDenied
	}
	return nil
}

// A one-byte wrapper prevents http.ReadResponse's private buffer from swallowing
// bytes belonging to a later/unsolicited response on the retained TLS socket.
// The existing bounded reader still caps headers, framing and body allocation.
type mcpExactReader struct{ net.Conn }

func (r mcpExactReader) Read(p []byte) (int, error) {
	if len(p) > 1 {
		p = p[:1]
	}
	return r.Conn.Read(p)
}

func (c *ConnectionV1) mcpExchange(body []byte, notification, toolCall bool) ([]byte, uint64, error) {
	if c.ctx.Err() != nil || len(body) == 0 || uint64(len(body)) > c.mcp.policy.Limits.MessageBytes {
		return nil, 0, domain.ErrMembershipDenied
	}
	// Original current authority before every setup/notification/catalog frame.
	// This does NOT consume or replace the tools/call one-use Store gate.
	if c.store.CheckLocalMCPProtocolAuthority(c.ctx, c.operation) != nil {
		return nil, 0, domain.ErrMembershipDenied
	}
	k := c.keeper
	k.mu.Lock()
	remainingBytes, remainingFrames, budgetErr := mcpExchangeReadLimits(c.mcp.policy.Limits, c.operation.Debit, mcpExchangeUsageV1{c.mcp.messages, c.mcp.frames, c.mcp.inbound}, mcpExchangeUsageV1{k.mcpMessages, k.mcpFrames, k.mcpBytes}, notification)
	if budgetErr != nil || k.closed || k.active != c || !k.credentialAdmittedLocked(c.credentialScope, c.allocation, c.source, c.operation, time.Now()) {
		k.mu.Unlock()
		return nil, 0, domain.ErrMembershipDenied
	}
	k.mcpMessages++
	c.mcp.messages++
	k.mu.Unlock()
	wire := mcpRequestWire(body)
	defer clear(wire)
	if toolCall {
		c.mcp.toolSendAttempted = true
	}
	n, e := c.conn.Write(wire)
	if e != nil || n != len(wire) {
		return nil, uint64(n), domain.ErrMembershipDenied
	}
	readCeiling := min(c.mcp.policy.Limits.MessageBytes, remainingBytes)
	if notification {
		readCeiling = 1
	} // only an empty 202 is admitted
	response, e := boundedhttp.ReadMCPResponseV1(mcpExactReader{c.conn}, readCeiling, c.keeper.config.Limits.ResponseHeaderBytes, notification)
	if e != nil {
		return nil, uint64(n), domain.ErrMembershipDenied
	}
	defer clear(response.Body)
	var frame []byte
	var frames uint64
	if !notification {
		limits := c.mcp.policy.Limits
		limits.SSEEvents = remainingFrames
		frame, frames, e = mcpwire.DecodeResponseFrameV1(response.Body, response.ContentType, limits)
	}
	k.mu.Lock()
	valid := e == nil && uint64(len(response.Body)) <= remainingBytes && frames <= remainingFrames
	if valid {
		k.mcpBytes += uint64(len(response.Body))
		k.mcpFrames += frames
		c.mcp.inbound += uint64(len(response.Body))
		c.mcp.frames += frames
	}
	k.mu.Unlock()
	if !valid || c.ctx.Err() != nil {
		clear(frame)
		return nil, uint64(n), domain.ErrMembershipDenied
	}
	return frame, uint64(n), nil
}

func (c *ConnectionV1) invokeMCP(ctx context.Context, registered domain.RegisteredProxyAttestationV2, capability ports.AttemptOperationCapabilityV1, payload []byte) (result ResultV1, err error) {
	if c == nil || c.self != c || c.mcp == nil {
		return result, domain.ErrMembershipDenied
	}
	k := c.keeper
	k.mu.Lock()
	c.mu.Lock()
	if k.closed || k.active != c || c.closed || c.used || !c.ready || c.conn == nil || c.ctx.Err() != nil || !k.credentialAdmittedLocked(c.credentialScope, c.allocation, c.source, c.operation, time.Now()) {
		c.mu.Unlock()
		k.mu.Unlock()
		return result, domain.ErrMembershipDenied
	}
	c.used = true
	c.mu.Unlock()
	k.mu.Unlock()
	defer func() {
		close(c.invokeDone)
		if e := c.Close(); e != nil {
			clear(result.Body)
			result.Body = nil
			err = domain.ErrMembershipDenied
		}
	}()
	if !callerFits(ctx, c.operation.Deadline) {
		return result, domain.ErrMembershipDenied
	}
	stop := context.AfterFunc(ctx, c.cancel)
	defer stop()
	r, cap := registered.Clone(), capability.Clone()
	rd, re := r.Digest()
	od, oe := cap.Reservation.Operation.Digest()
	if re != nil || oe != nil || cap.Validate() != nil || r.ValidateInitial() != nil || r.ValidateAt(time.Now().UTC().Truncate(time.Microsecond)) != nil || r.ValidateForAllocation(c.allocation, c.source) != nil || r.TransportKind != domain.RegisteredProxyTransportLocalPublicMCPHTTPSV1 || r.LocalPublicMCPHTTPS == nil || r.OperationDigest != c.publication.OperationDigest || r.OperationSequence != c.operation.Sequence || rd != cap.Reservation.RegisteredProxyDigest || cap.Reservation.RegisteredAllocationDigest != c.publication.RegisteredAllocationDigest || od != c.publication.OperationDigest || r.PolicyBody != ports.OwnedProxyBodyReferenceV1("policy", c.publication.PolicyBody) || r.AttestationBody != ports.OwnedProxyBodyReferenceV1("attestation", c.publication.AttestationBody) || !r.Window.ObservedAt.Equal(c.publication.ObservedAt) || !r.Window.ExpiresAt.Equal(c.operation.Deadline) || !bytes.Equal(payload, c.payload) {
		return result, domain.ErrMembershipDenied
	}
	if c.lease.Consume(c.operation, c.publication.RegisteredAllocationDigest, time.Now().UTC().Truncate(time.Microsecond)) != nil {
		return result, domain.ErrMembershipDenied
	}
	initialize, e := mcpwire.NewInitializeRequestV1("sessionless-init", c.mcp.policy.Limits)
	if e != nil {
		return result, e
	}
	frame, n, e := c.mcpExchange(initialize.Body(), false, false)
	result.RequestBytes += n
	if e != nil {
		return result, e
	}
	e = initialize.ValidateInitializationV1(frame)
	clear(frame)
	if e != nil {
		return result, e
	}
	notification, e := mcpwire.InitializedNotificationV1(c.mcp.policy.Limits)
	if e != nil {
		return result, e
	}
	_, n, e = c.mcpExchange(notification, true, false)
	clear(notification)
	result.RequestBytes += n
	if e != nil {
		return result, e
	}
	catalog, e := mcpwire.NewListToolsRequestV1("sessionless-catalog", c.mcp.policy.Limits)
	if e != nil {
		return result, e
	}
	frame, n, e = c.mcpExchange(catalog.Body(), false, false)
	result.RequestBytes += n
	if e != nil {
		return result, e
	}
	e = catalog.ValidateCatalogV1(frame, c.mcp.descriptor)
	clear(frame)
	if e != nil {
		return result, e
	}
	// One irreversible canonical tools/call gate, after exact setup/catalog,
	// immediately before the only request capable of disclosing tool arguments.
	if c.store.AuthorizeAttemptOperationBeforeSend(c.ctx, cap) != nil || c.ctx.Err() != nil {
		return result, domain.ErrMembershipDenied
	}
	frame, n, e = c.mcpExchange(c.payload, false, true)
	result.SendAttempted = c.mcp.toolSendAttempted
	result.RequestBytes += n
	result.ObservedAt = time.Now().UTC().Truncate(time.Microsecond)
	if e != nil || !result.ObservedAt.Before(c.operation.Deadline) {
		clear(frame)
		return result, domain.ErrMembershipDenied
	}
	result.Body = frame
	result.ResponseBytes = uint64(len(frame))
	return result, nil
}
