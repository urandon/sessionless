//go:build ydbintegration

package serverlessegress

// Fixed local concrete original fixture custody only. Release builds have no
// factory, filesystem injection, generic close callback or signing primitive.
import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type ownedCustodyPeer struct {
	proxy *PinnedHTTPSProxyV1
	proof ports.OwnedProxyPublicationV2
}
type OwnedFixtureCustodyV1 struct {
	self             *OwnedFixtureCustodyV1
	mu               sync.Mutex
	scope            domain.AttemptOperationScopeV1
	parentDigest     string
	sourceDigest     string
	allocationDigest domain.RegisteredAllocationDigestV2
	clock            ports.Clock
	directory        string
	key              [32]byte
	closed           bool
	pendingDigest    string // original frozen cleanup only; bounded retry after partial deletion
	bodies           map[string]ports.OwnedOperationResidualBodyV1
	peers            map[uint64]ownedCustodyPeer
	receipt          *ports.OwnedOperationResidualReceiptV1
}

func NewOwnedFixtureCustodyV1(parent domain.OwnedAttemptEffectParentV1, source domain.AttemptExecutionSourceV1, allocation domain.RegisteredAllocationV2, clock ports.Clock) (*OwnedFixtureCustodyV1, error) {
	pd, pe := parent.Digest()
	sd, se := source.Digest()
	ad, ae := allocation.Digest()
	rd, re := parent.Reservation.DigestForAuthority(parent.Authority)
	if pe != nil || se != nil || ae != nil || re != nil || clock == nil || allocation.Validate() != nil || allocation.ParentReservationDigest != rd || allocation.Scope != source.Scope || allocation.SourceDigest != sd || parent.Authority.HarnessBinding.Backend.BackendKind != domain.HarnessBackendDeterministicFixtureV1 {
		return nil, ErrConfig
	}
	policyDigest, _ := parent.Authority.HarnessBinding.AgentLoopPolicy.Digest()
	r := parent.Reservation
	c := &OwnedFixtureCustodyV1{scope: domain.AttemptOperationScopeV1{TenantID: r.TenantID, SessionID: parent.Scope.SessionID, OwnerUserID: parent.Scope.OwnerUserID, RunID: r.RunID, AttemptID: r.AttemptID, LeaseID: r.LeaseID, FenceToken: r.FenceToken, PhysicalInvocationClaimID: r.PhysicalInvocationClaimID, EffectReservationDigest: rd, InvocationAuthorityDigest: r.InvocationAuthorityDigest, HarnessBindingDigest: r.HarnessBindingDigest, AgentLoopPolicyDigest: policyDigest}, parentDigest: pd, sourceDigest: sd, allocationDigest: ad, clock: clock, bodies: make(map[string]ports.OwnedOperationResidualBodyV1), peers: make(map[uint64]ownedCustodyPeer)}
	if _, err := rand.Read(c.key[:]); err != nil {
		return nil, ErrConfig
	}
	directory, err := os.MkdirTemp("", "sessionless-owned-custody-")
	if err != nil {
		return nil, ErrConfig
	}
	c.directory, c.self = directory, c
	return c, nil
}
func (c *OwnedFixtureCustodyV1) path(ref string) string {
	return filepath.Join(c.directory, ports.OwnedFixtureBytesDigestV1([]byte(ref))+".blob")
}
func (c *OwnedFixtureCustodyV1) PutOwnedBody(ctx context.Context, scope domain.AttemptOperationScopeV1, ref string, body []byte) error {
	if c == nil || c.self != c || ctx == nil || ctx.Err() != nil || scope != c.scope || ref == "" || len(ref) > 2048 || len(body) == 0 || len(body) > 1<<20 {
		return domain.ErrMembershipDenied
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return domain.ErrMembershipDenied
	}
	b := ports.OwnedOperationResidualBodyV1{Ref: ref, Digest: ports.OwnedFixtureBytesDigestV1(body), Bytes: uint64(len(body))}
	if old, found := c.bodies[ref]; found {
		if old != b {
			return domain.ErrMembershipDenied
		}
		return nil
	}
	total := b.Bytes
	for _, old := range c.bodies {
		total += old.Bytes
	}
	if len(c.bodies) >= 28 || total > 1<<20 {
		return domain.ErrMembershipDenied
	}
	file, err := os.OpenFile(c.path(ref), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return domain.ErrMembershipDenied
	}
	n, writeErr := file.Write(body)
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil || n != len(body) || ctx.Err() != nil {
		c.closed = true
		return domain.ErrMembershipDenied
	}
	c.bodies[ref] = b
	return nil
}
func (c *OwnedFixtureCustodyV1) ReadAttemptOperationBody(ctx context.Context, scope domain.AttemptOperationScopeV1, ref string, cap uint64) ([]byte, error) {
	if c == nil || c.self != c || ctx == nil || ctx.Err() != nil || scope != c.scope || cap == 0 || cap > 1<<20 {
		return nil, domain.ErrMembershipDenied
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	b, found := c.bodies[ref]
	if c.closed || !found || b.Bytes > cap {
		return nil, domain.ErrMembershipDenied
	}
	info, err := os.Lstat(c.path(ref))
	if err != nil || !info.Mode().IsRegular() || info.Size() != int64(b.Bytes) {
		return nil, domain.ErrMembershipDenied
	}
	body, err := os.ReadFile(c.path(ref))
	if err != nil || ctx.Err() != nil || uint64(len(body)) != b.Bytes || ports.OwnedFixtureBytesDigestV1(body) != b.Digest {
		clear(body)
		return nil, domain.ErrMembershipDenied
	}
	return body, nil
}
func (c *OwnedFixtureCustodyV1) AttachOriginalProxy(proxy *PinnedHTTPSProxyV1, proof ports.OwnedProxyPublicationV2) error {
	if c == nil || c.self != c || proxy == nil || proof.Validate() != nil || proxy.VerifyOwnedProxyPublication(proof, proof.ObservedAt) != nil || proof.Scope.TenantID != c.scope.TenantID || proof.Scope.SessionID != c.scope.SessionID || proof.Scope.OwnerUserID != c.scope.OwnerUserID || proof.Scope.RunID != c.scope.RunID || proof.Scope.AttemptID != c.scope.AttemptID || proof.LeaseID != c.scope.LeaseID || proof.FenceToken != c.scope.FenceToken || proof.PhysicalInvocationClaimID != c.scope.PhysicalInvocationClaimID || proof.RegisteredAllocationDigest != c.allocationDigest || proof.SourceDigest != c.sourceDigest {
		return domain.ErrMembershipDenied
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return domain.ErrMembershipDenied
	}
	if old, found := c.peers[proof.OperationSequence]; found {
		if old.proxy != proxy {
			return domain.ErrMembershipDenied
		}
		return nil
	}
	if len(c.peers) >= 12 {
		return domain.ErrMembershipDenied
	}
	c.peers[proof.OperationSequence] = ownedCustodyPeer{proxy, proof.Clone()}
	return nil
}
func (c *OwnedFixtureCustodyV1) CloseOwnedOperationNamespace(ctx context.Context, inventory ports.OwnedOperationResidualInventoryV1) (ports.OwnedOperationResidualReceiptV1, error) {
	deny := func() (ports.OwnedOperationResidualReceiptV1, error) {
		return ports.OwnedOperationResidualReceiptV1{}, domain.ErrMembershipDenied
	}
	if c == nil || c.self != c || ctx == nil || ctx.Err() != nil {
		return deny()
	}
	digest, err := inventory.Digest()
	if err != nil {
		return deny()
	}
	pd, _ := inventory.Parent.Digest()
	sd, _ := inventory.Source.Digest()
	ad, _ := inventory.OriginalAllocation.Digest()
	c.mu.Lock()
	defer c.mu.Unlock()
	if pd != c.parentDigest || sd != c.sourceDigest || ad != c.allocationDigest {
		return deny()
	}
	if c.receipt != nil {
		if c.receipt.InventoryDigest != digest {
			return deny()
		}
		return c.receipt.Clone(), nil
	}
	if c.closed && c.pendingDigest != digest || len(inventory.Bodies) != len(c.bodies) || len(inventory.Proxies) != len(c.peers) {
		return deny()
	}
	for _, body := range inventory.Bodies {
		if c.bodies[body.Ref] != body {
			return deny()
		}
	}
	for _, peer := range inventory.Proxies {
		original, found := c.peers[peer.Sequence]
		if !found || peer.Original.TransportKind != original.proof.TransportKind || peer.Original.OperationSequence != peer.Sequence || peer.Original.OperationDigest != original.proof.OperationDigest || peer.Original.ServiceKind != original.proof.ServiceKind || peer.Original.RegisteredAllocationDigest != c.allocationDigest || peer.Original.SourceDigest != c.sourceDigest || peer.Original.PolicyBody != ports.OwnedProxyBodyReferenceV1("policy", original.proof.PolicyBody) || peer.Original.AttestationBody != ports.OwnedProxyBodyReferenceV1("attestation", original.proof.AttestationBody) || original.proxy.VerifyOwnedProxyPublication(original.proof, original.proof.ObservedAt) != nil {
			return deny()
		}
	}
	// Verify every actual remaining object before deleting any. Exact frozen
	// retry may encounter objects already deleted by this same close attempt.
	for _, body := range inventory.Bodies {
		info, err := os.Lstat(c.path(body.Ref))
		if os.IsNotExist(err) && c.pendingDigest == digest {
			continue
		}
		if err != nil || !info.Mode().IsRegular() || info.Size() != int64(body.Bytes) || ctx.Err() != nil {
			return deny()
		}
		actual, err := os.ReadFile(c.path(body.Ref))
		valid := err == nil && uint64(len(actual)) == body.Bytes && ports.OwnedFixtureBytesDigestV1(actual) == body.Digest
		clear(actual)
		if !valid {
			return deny()
		}
	}
	// All checks precede any destructive action. Irreversible original namespace
	// fence precedes connection close and file deletion, including error paths.
	c.closed = true
	c.pendingDigest = digest
	for _, peer := range c.peers {
		if peer.proxy.Close() != nil || ctx.Err() != nil {
			return deny()
		}
	}
	var total uint64
	for _, body := range inventory.Bodies {
		if ctx.Err() != nil {
			return deny()
		}
		if err := os.Remove(c.path(body.Ref)); err != nil && !os.IsNotExist(err) {
			return deny()
		}
		if _, err := os.Lstat(c.path(body.Ref)); !os.IsNotExist(err) {
			return deny()
		}
		total += body.Bytes
	}
	if err := os.Remove(c.directory); err != nil && !os.IsNotExist(err) {
		return deny()
	}
	if _, err := os.Lstat(c.directory); !os.IsNotExist(err) || ctx.Err() != nil {
		return deny()
	}
	receipt := ports.OwnedOperationResidualReceiptV1{Version: 1, Scope: c.scope, ParentDigest: c.parentDigest, InventoryDigest: digest, NamespaceDigest: ports.OwnedFixtureBytesDigestV1([]byte(c.directory)), ClosedAt: c.clock.Now().UTC().Truncate(time.Microsecond), DeletedBodies: uint64(len(c.bodies)), DeletedBytes: total, Authenticator: make([]byte, 32)}
	raw, err := receipt.IntentBytes()
	if err != nil {
		return deny()
	}
	mac := hmac.New(sha256.New, c.key[:])
	_, _ = mac.Write(raw)
	receipt.Authenticator = mac.Sum(nil)
	c.receipt = &receipt
	return receipt.Clone(), nil
}
func (c *OwnedFixtureCustodyV1) VerifyOwnedOperationResidualReceipt(v ports.OwnedOperationResidualReceiptV1) error {
	if c == nil || c.self != c {
		return domain.ErrMembershipDenied
	}
	raw, err := v.IntentBytes()
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed || c.receipt == nil || v.Scope != c.scope || v.ParentDigest != c.parentDigest {
		return domain.ErrMembershipDenied
	}
	saved, _ := json.Marshal(c.receipt)
	supplied, _ := json.Marshal(v)
	if !bytes.Equal(saved, supplied) {
		return domain.ErrMembershipDenied
	}
	mac := hmac.New(sha256.New, c.key[:])
	_, _ = mac.Write(raw)
	if subtle.ConstantTimeCompare(mac.Sum(nil), v.Authenticator) != 1 {
		return domain.ErrMembershipDenied
	}
	return nil
}
