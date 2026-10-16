//go:build pi_sdk_functional && (darwin || linux)

package providercredential

// This bridge exists ONLY in providercredential's test binary, so its external
// tests can import pisdkmedia without a production import cycle or public trust
// override. It installs test-owned CA/DNS/loopback on the genuine file recipe;
// it neither returns/replaces a driver nor exposes the private send borrow.

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	daemon "gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type PiHTTPConstructorTestFixture struct {
	Service  *InvocationService
	Root     string
	bindings *fakeBindingStore
	secrets  *fakeSecretStore
}

func NewPiHTTPConstructorTestFixture(t *testing.T, issue ports.ProviderCredentialIssueRequestV1, now time.Time) *PiHTTPConstructorTestFixture {
	t.Helper()
	if err := issue.Validate(); err != nil {
		t.Fatal("synthetic issue:", err)
	}
	service, bindings, secrets, _, clock := immutableAPIInvocationFixture(t, domain.ProviderCredentialDeliveryFileV1)
	clock.Set(now)
	service.maxBytes = 128
	ref, err := domain.NewCredentialSecretRef("test/pi-constructor/generation")
	if err != nil {
		t.Fatal(err)
	}
	marker := []byte("synthetic-key") // public test literal, never a configured key
	resource := issue.HarnessBinding.Resource
	binding := domain.ProviderCredentialBindingV1{Version: domain.ProviderCredentialBindingVersionV1,
		TenantID: issue.HarnessBinding.TenantID, OwnerUserID: issue.HarnessBinding.OwnerUserID,
		ResourceKind: resource.Kind, ResourceID: resource.ResourceID, ResourceRevision: resource.Revision,
		CredentialGeneration: resource.CredentialGeneration, CandidateMutationID: "synthetic-http-mutation",
		State: domain.ProviderCredentialActiveV1, SecretRef: ref, SecretFingerprint: domain.FingerprintCredential(marker), UpdatedAt: now}
	if err := binding.Validate(); err != nil {
		t.Fatal("synthetic binding:", err)
	}
	bindings.bindings = map[ports.ProviderCredentialLocatorV1]domain.ProviderCredentialBindingV1{locatorFor(binding): binding}
	secrets.values = map[domain.CredentialSecretRef][]byte{ref: marker}
	secrets.committed = map[domain.CredentialSecretRef]struct{}{ref: {}}
	return &PiHTTPConstructorTestFixture{Service: service, Root: service.root, bindings: bindings, secrets: secrets}
}

func (f *PiHTTPConstructorTestFixture) Counts() (bindings, reads, recoveries, writes int) {
	f.bindings.mu.Lock()
	bindings = f.bindings.loadCalls
	f.bindings.mu.Unlock()
	f.secrets.mu.Lock()
	defer f.secrets.mu.Unlock()
	return bindings, f.secrets.readCalls, f.secrets.recoverCalls, f.secrets.putCalls
}

type PiHTTPConstructorTestPeer struct{ peer *piTLSPeer }

func (f *PiHTTPConstructorTestFixture) PrepareLoopback(t *testing.T, use *PreparedPiFileUseV1, request daemon.PreparedAPIRequestV1, entity []byte) (*PiHTTPConstructorTestPeer, error) {
	t.Helper()
	if err := use.PrepareRequestAuthenticationV1(request); err != nil {
		return nil, fmt.Errorf("actual request authenticator: %w", err)
	}
	limits := request.Profile.Limits
	if err := use.PrepareHTTPReceiveV1(limits.ResponseHeaderBytes, limits.ResponseBodyBytes); err != nil {
		return nil, fmt.Errorf("actual receiver: %w", err)
	}
	if err := use.prepareResponseScan(); err != nil {
		return nil, fmt.Errorf("actual response scanner: %w", err)
	}
	bundle, cert := piTLSFixture(t)
	if err := use.prepareHTTPS(bundle); err != nil {
		return nil, fmt.Errorf("actual HTTPS sink: %w", err)
	}
	// The fixed production factory is intentionally NOT called here. The external
	// test must call actual pisdkmedia.PreparePiHTTPExecutionV1, which owns that call.
	response := fmt.Sprintf("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s", len(entity), entity)
	peer := piTLSPeerStart(t, use.state.network.Load(), cert, []string{"http/1.1"}, response)
	return &PiHTTPConstructorTestPeer{peer: peer}, nil
}

func (p *PiHTTPConstructorTestPeer) Dials() int { return int(p.peer.dials.Load()) }

func (p *PiHTTPConstructorTestPeer) Request(t *testing.T) []byte {
	t.Helper()
	piTLSWait(t, p.peer.done)
	if p.peer.err != nil {
		t.Fatal("loopback TLS peer:", p.peer.err)
	}
	return bytes.Clone(p.peer.request)
}
