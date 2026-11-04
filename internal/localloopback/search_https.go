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
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/serverlessegress"
	"gitcode.com/urandon/sessionless/internal/websearch"
)

// These private test dependencies observe external DNS/TLS only. Normal
// construction exposes no URL, CA, resolver, dialer or ambient proxy override.
type searchHTTPSDependencies struct {
	lookup func(context.Context, string) ([]netip.Addr, error)
	dial   func(context.Context, string, string) (net.Conn, error)
	roots  *x509.CertPool
}

type searchConnectionV1 struct {
	policy         ports.LocalSearchHTTPSProxyPolicyV1
	request        *websearch.FrozenRequestV2
	handle         ports.SearchInvocationCredentialV1
	cleanupCeiling time.Time
}

var _ ports.LocalSearchTransportV1 = (*KeeperV1)(nil)
var _ ports.LocalSearchConnectionV1 = (*ConnectionV1)(nil)

func validSearchKeyBytes(value []byte) bool {
	if len(value) == 0 || len(value) > 8<<10 {
		return false
	}
	for _, b := range value {
		if b <= ' ' || b >= 0x7f {
			return false
		}
	}
	return true
}

func searchCredentialMatches(h ports.SearchInvocationCredentialV1, a domain.RegisteredAllocationV2, s domain.AttemptExecutionSourceV1, op domain.AttemptOperationV1, at time.Time) bool {
	if h.Validate() != nil || h.Correlation.Scope != op.Scope || string(h.Correlation.ConsentRequestDigest) != op.ConsentRequestDigest || string(h.Correlation.ConsentDecisionDigest) != op.ConsentDecisionDigest {
		return false
	}
	digest, err := op.Digest()
	c := h.Credential
	return err == nil && digest == h.Correlation.OperationDigest && c.TenantID == s.Scope.TenantID && c.OwnerUserID == s.Scope.OwnerUserID && c.WorkerID == a.Lease.WorkerID && c.LeaseID == a.Lease.ID && c.LeaseFence == a.Lease.FenceToken && at.Before(c.ExpiresAt) && !op.Deadline.After(c.ExpiresAt) && !c.ExpiresAt.After(a.Window.ExpiresAt) && !c.ExpiresAt.After(a.Lease.ExpiresAt) && !c.ExpiresAt.After(a.InvocationDeadline) && !c.ExpiresAt.After(s.ExpiresAt)
}

// One exact mutable allocation owns the only SEARCH key-bearing HTTP copy.
// It is registered on the same original keeper before unlocking admission.
func searchRequestWire(payload, key []byte) []byte {
	prefix := "POST /search HTTP/1.1\r\nHost: api.tavily.com\r\nContent-Type: application/json\r\nAccept: application/json\r\nConnection: close\r\nAuthorization: Bearer "
	suffix := "\r\nContent-Length: " + strconv.Itoa(len(payload)) + "\r\n\r\n"
	wire := make([]byte, 0, len(prefix)+len(key)+len(suffix)+len(payload))
	wire = append(wire, prefix...)
	wire = append(wire, key...)
	wire = append(wire, suffix...)
	return append(wire, payload...)
}

// PreflightSearch performs only the original owned fixed-endpoint DNS/TLS
// preflight. Neither TLS nor this publication sends arguments or a key. Only
// InvokeSearch may do so, after exact reservation and current one-use SQL gate.
// The caller must remain in the original synchronous distinct materializer.
func (k *KeeperV1) PreflightSearch(ctx context.Context, a domain.RegisteredAllocationV2, s domain.AttemptExecutionSourceV1, op domain.AttemptOperationV1, request *websearch.FrozenRequestV2, handle ports.SearchInvocationCredentialV1, borrowed []byte) (_ ports.LocalSearchConnectionV1, err error) {
	if k == nil || k.self != k || !callerFits(ctx, op.Deadline) || k.sourceLifetime == nil || k.sourceLifetime.Err() != nil || op.Kind != domain.AttemptOperationWebSearchV1 || op.SearchWire == nil || request == nil || request.ValidateOperation(op) != nil || !validSearchKeyBytes(borrowed) || s.Validate() != nil || len(s.Spending) != 4 {
		return nil, domain.ErrMembershipDenied
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	if a.ValidateAt(now) != nil || a.LocalRuntime == nil || !operationScopeMatches(op, a, s) || !searchCredentialMatches(handle, a, s, op, now) || op.RequestBytes > domain.WebSearchWireBytesV1 || op.Debit.ResponseBytes > domain.WebSearchResponseBytesV1 || op.CleanupDuration <= 0 || op.CleanupDuration > min(k.config.Limits.CleanupTimeout, 30*time.Second) || k.source.VerifyOwnedAttemptEffectSource(s) != nil {
		return nil, domain.ErrMembershipDenied
	}
	a, s, op = a.Clone(), s.Clone(), op.Clone()
	rawGrant, e := k.source.ReadWebSearchResourceGrant(ctx, s.Profile, domain.WebSearchResourceGrantMaxBytesV2)
	defer clear(rawGrant)
	grant, ge := domain.DecodeWebSearchResourceGrantV2(rawGrant)
	gd, gde := grant.Digest()
	declaration, de := k.source.ReadSearchCredentialDeclaration(ctx, s.Profile)
	policy, pe := ports.BuildLocalSearchHTTPSProxyPolicyV1(k.config, grant)
	pd, pde := policy.Digest()
	ad, ae := a.Digest()
	sd, se := s.Digest()
	od, oe := op.Digest()
	if e != nil || ge != nil || gde != nil || de != nil || pe != nil || pde != nil || ae != nil || se != nil || oe != nil || declaration != handle.Declaration || declaration.Resource.ResourceID != grant.CredentialRef || gd != op.ToolGrantDigest || gd != s.Spending[3].ScopedGrantDigest || a.SourceDigest != sd || !now.Before(op.Deadline) || op.Deadline.After(now.Add(policy.MaxOperationTimeout)) || op.Deadline.After(grant.ExpiresAt) || op.Deadline.After(a.Window.ExpiresAt) || ctx.Err() != nil {
		return nil, domain.ErrMembershipDenied
	}
	payload := request.Body()
	owned := false
	defer func() {
		if !owned {
			clear(payload)
		}
	}()
	if uint64(len(payload)) != op.RequestBytes || rawSearchDigest(payload) != op.RequestDigest {
		return nil, domain.ErrMembershipDenied
	}
	store, bridge, e := k.bootstrap.RuntimeFor(k)
	if e != nil {
		return nil, domain.ErrMembershipDenied
	}
	k.mu.Lock()
	if k.closed || k.active != nil || k.searchSlots >= 2 || k.credentialScope == nil || len(k.credentialScope.joins) >= 12 || !k.credentialAdmittedLocked(k.credentialScope, a, s, op, now) {
		k.mu.Unlock()
		return nil, domain.ErrMembershipDenied
	}
	lease, e := bridge.BeginLocalSearchOperation(k.source, a, op, now)
	if e != nil {
		k.mu.Unlock()
		return nil, domain.ErrMembershipDenied
	}
	call, cancel := context.WithCancel(lease.Context())
	c := &ConnectionV1{keeper: k, store: store, lease: lease, allocation: a, source: s, operation: op, payload: payload, request: searchRequestWire(payload, borrowed), ctx: call, cancel: cancel, preflightDone: make(chan struct{}), invokeDone: make(chan struct{}), cleanupDone: make(chan struct{}), cleanupTimeout: op.CleanupDuration, credentialScope: k.credentialScope, search: &searchConnectionV1{policy: policy, request: request, handle: handle, cleanupCeiling: op.Deadline.Add(op.CleanupDuration)}}
	c.self = c
	c.stopCaller = context.AfterFunc(ctx, cancel)
	c.stopCredential = context.AfterFunc(k.credentialScope.ctx, cancel)
	k.credentialScope.joins = append(k.credentialScope.joins, c.cleanupDone)
	k.searchSlots++ // no retry/refund after any failed DNS/TLS preflight
	k.active = c
	deps := k.searchDeps
	owned = true
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
	addresses, e := deps.lookup(call, "api.tavily.com")
	if e != nil {
		return nil, domain.ErrMembershipDenied
	}
	literals, resolution, e := serverlessegress.PublicResolutionDataV1(addresses)
	if e != nil {
		return nil, domain.ErrMembershipDenied
	}
	raw, e := deps.dial(call, "tcp", net.JoinHostPort(literals[0], "443"))
	if e != nil || raw == nil {
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
	secure := tls.Client(raw, &tls.Config{ServerName: "api.tavily.com", RootCAs: deps.roots, MinVersion: tls.VersionTLS12, NextProtos: []string{"http/1.1"}})
	if secure.HandshakeContext(call) != nil || secure.ConnectionState().NegotiatedProtocol != "http/1.1" || len(secure.ConnectionState().VerifiedChains) == 0 {
		return nil, domain.ErrMembershipDenied
	}
	observed := time.Now().UTC().Truncate(time.Microsecond)
	r := a.LocalRuntime
	o := domain.RegisteredLocalSearchHTTPSProxyV1{ConfiguredProfileDigest: k.configDigest, EndpointPolicy: string(domain.RegisteredProxyTransportLocalSearchHTTPSV1), ConnectorArtifactDigest: k.config.WorkerExecutableDigest, InstallationID: r.InstallationID, InstallationRevision: r.InstallationRevision, HostIdentityDigest: k.config.LocalHostIdentityDigest, NetworkNamespaceIdentityDigest: k.config.LocalNetworkNamespaceIdentityDigest, ResolutionSetDigest: resolution}
	attestation := ports.LocalSearchHTTPSProxyAttestationV1{Version: 1, PolicyDigest: pd, Observation: o, ConnectionAddressPinned: true, DNSRebindingDenied: true, PublicUnicastOnly: true, RedirectsDenied: true, AmbientProxyDenied: true, CertificateValidationRequired: true, TLSHandshakeVerified: true, VerifiedSNI: "api.tavily.com", ResolvedHost: "api.tavily.com", ResolvedPort: 443, ObservedAt: observed, ExpiresAt: op.Deadline}
	pb, pbe := json.Marshal(policy)
	ab, abe := json.Marshal(attestation)
	pub := ports.OwnedProxyPublicationV2{TransportKind: domain.RegisteredProxyTransportLocalSearchHTTPSV1, OperationSequence: op.Sequence, OperationDigest: od, OperationDeadline: op.Deadline, ServiceKind: domain.RegisteredProxyWebSearchV1, Scope: s.Scope, LeaseID: a.Lease.ID, FenceToken: a.Lease.FenceToken, PhysicalInvocationClaimID: a.ParentReservation.PhysicalInvocationClaimID, RegisteredAllocationDigest: ad, SourceDigest: sd, ObservedAt: observed, PolicyBody: pb, AttestationBody: ab, Authenticator: make([]byte, sha256.Size)}
	if pbe != nil || abe != nil || pub.Validate() != nil || attestation.ValidateForPolicy(policy) != nil {
		return nil, domain.ErrMembershipDenied
	}
	intent, e := pub.IntentBytes()
	if e != nil {
		return nil, domain.ErrMembershipDenied
	}
	pub.Authenticator = k.authenticateSearch(intent)
	k.mu.Lock()
	c.mu.Lock()
	defer k.mu.Unlock()
	defer c.mu.Unlock()
	if k.closed || k.active != c || c.closed || call.Err() != nil || !searchCredentialMatches(handle, a, s, op, time.Now()) || !k.credentialAdmittedLocked(c.credentialScope, a, s, op, time.Now()) {
		return nil, domain.ErrMembershipDenied
	}
	c.conn, c.publication, c.ready = secure, pub, true
	return c, nil
}

func rawSearchDigest(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func (k *KeeperV1) authenticateSearch(intent []byte) []byte {
	h := hmac.New(sha256.New, k.key[:])
	_, _ = h.Write([]byte("sessionless.actual-local-search-https.v1\x00"))
	_, _ = h.Write([]byte(k.configDigest))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write(intent)
	return h.Sum(nil)
}

func (k *KeeperV1) verifySearchPublication(pub ports.OwnedProxyPublicationV2, at time.Time) error {
	if k == nil || k.self != k || pub.Validate() != nil || pub.TransportKind != domain.RegisteredProxyTransportLocalSearchHTTPSV1 || pub.ServiceKind != domain.RegisteredProxyWebSearchV1 || at.Before(pub.ObservedAt) {
		return domain.ErrMembershipDenied
	}
	bodies, err := pub.Bodies()
	if err != nil || bodies.LocalSearchHTTPS == nil || bodies.LocalSearchHTTPS.Policy.ConfiguredProfileDigest != k.configDigest {
		return domain.ErrMembershipDenied
	}
	intent, err := pub.IntentBytes()
	if err != nil || !hmac.Equal(pub.Authenticator, k.authenticateSearch(intent)) {
		return domain.ErrMembershipDenied
	}
	return nil
}

// InvokeSearch is one exact POST, with no redirects/retries/resumption or
// alternate endpoint. It leaves interpretation to the original bound codec.
func (c *ConnectionV1) InvokeSearch(ctx context.Context, registered domain.RegisteredProxyAttestationV2, capability ports.AttemptOperationCapabilityV1, payload []byte) (result ports.LocalSearchTransportResultV1, err error) {
	if c == nil || c.self != c || c.search == nil || c.keeper == nil {
		return result, domain.ErrMembershipDenied
	}
	k := c.keeper
	k.mu.Lock()
	c.mu.Lock()
	if k.closed || k.active != c || c.closed || c.used || !c.ready || c.conn == nil || c.ctx.Err() != nil || k.sourceLifetime.Err() != nil || !searchCredentialMatches(c.search.handle, c.allocation, c.source, c.operation, time.Now()) || !k.credentialAdmittedLocked(c.credentialScope, c.allocation, c.source, c.operation, time.Now()) {
		c.mu.Unlock()
		k.mu.Unlock()
		return result, domain.ErrMembershipDenied
	}
	c.used = true
	c.mu.Unlock()
	k.mu.Unlock()
	defer func() {
		close(c.invokeDone)
		if cleanupErr := c.Close(); cleanupErr != nil {
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
	rd, re := r.Digest()
	od, oe := cap.Reservation.Operation.Digest()
	policyDigest, _ := c.search.policy.Digest()
	transportDigest, _ := c.search.policy.TransportDigest()
	bodies, be := c.publication.Bodies()
	if re != nil || oe != nil || be != nil || bodies.LocalSearchHTTPS == nil || cap.Validate() != nil || r.ValidateInitial() != nil || r.ValidateAt(now) != nil || r.ValidateForAllocation(c.allocation, c.source) != nil || r.TransportKind != domain.RegisteredProxyTransportLocalSearchHTTPSV1 || r.ServiceKind != domain.RegisteredProxyWebSearchV1 || r.LocalSearchHTTPS == nil || *r.LocalSearchHTTPS != bodies.LocalSearchHTTPS.Attestation.Observation || r.GrantDigest != c.operation.ToolGrantDigest || r.PolicyDigest != policyDigest || r.TransportDigest != transportDigest || r.OperationDigest != c.publication.OperationDigest || r.OperationSequence != c.operation.Sequence || rd != cap.Reservation.RegisteredProxyDigest || cap.Reservation.RegisteredAllocationDigest != c.publication.RegisteredAllocationDigest || od != c.publication.OperationDigest || cap.Reservation.OperationDigest != od || r.PolicyBody != ports.OwnedProxyBodyReferenceV1("policy", c.publication.PolicyBody) || r.AttestationBody != ports.OwnedProxyBodyReferenceV1("attestation", c.publication.AttestationBody) || !r.Window.ObservedAt.Equal(c.publication.ObservedAt) || !r.Window.ExpiresAt.Equal(c.operation.Deadline) || len(payload) != len(c.payload) || !bytes.Equal(payload, c.payload) {
		return result, domain.ErrMembershipDenied
	}
	store, _, e := k.bootstrap.RuntimeFor(k)
	if e != nil || store != c.store || c.lease.Consume(c.operation, c.publication.RegisteredAllocationDigest, now) != nil {
		return result, domain.ErrMembershipDenied
	}
	if c.store.AuthorizeAttemptOperationBeforeSend(c.ctx, cap) != nil || c.ctx.Err() != nil || ctx.Err() != nil || k.sourceLifetime.Err() != nil || !time.Now().Before(c.operation.Deadline) {
		return result, domain.ErrMembershipDenied
	}
	k.mu.Lock()
	sealed := k.closed || !k.credentialAdmittedLocked(c.credentialScope, c.allocation, c.source, c.operation, time.Now()) || !searchCredentialMatches(c.search.handle, c.allocation, c.source, c.operation, time.Now())
	k.mu.Unlock()
	if sealed {
		return result, domain.ErrMembershipDenied
	}
	result.SendAttempted = true // includes failed/partial request writes
	n, e := c.conn.Write(c.request)
	result.RequestBytes = uint64(n)
	result.ObservedAt = time.Now().UTC().Truncate(time.Microsecond)
	if e != nil || n != len(c.request) {
		return result, domain.ErrMembershipDenied
	}
	response, e := boundedhttp.ReadSearchResponseV1(c.conn, min(c.operation.Debit.ResponseBytes, domain.WebSearchResponseBytesV1), min(k.config.Limits.ResponseHeaderBytes, uint64(16<<10)))
	result.ObservedAt = time.Now().UTC().Truncate(time.Microsecond)
	if e != nil || c.ctx.Err() != nil || ctx.Err() != nil || !result.ObservedAt.Before(c.operation.Deadline) {
		clear(response.Body)
		return result, domain.ErrMembershipDenied
	}
	result.Body, result.ResponseBytes = response.Body, uint64(len(response.Body))
	result.Metadata = websearch.HTTPMetadataV1{StatusCode: response.StatusCode, ContentType: response.ContentType, ContentEncoding: response.ContentEncoding}
	return result, nil
}
