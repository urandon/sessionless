package localloopback

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localoperator"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// These private synthetic owners test scope/wire/join bookkeeping only, not
// authentic Source, SQL currentness, socket observation or accepted sends.
func credentialFixture(t *testing.T) (*KeeperV1, ports.ProviderInvocationCredentialV1, domain.RegisteredAllocationV2, domain.AttemptExecutionSourceV1, domain.AttemptOperationV1) {
	t.Helper()
	resource := domain.ProviderResourceBindingV1{Kind: domain.ProviderResourceLocalEndpointV1, ResourceID: "synthetic-resource-190", OwnerUserID: "owner-190", Revision: 1, CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 1}
	expires := time.Now().Add(time.Hour)
	h := ports.ProviderInvocationCredentialV1{HandleID: "synthetic-handle-190", TenantID: "tenant-190", OwnerUserID: resource.OwnerUserID, RunID: "run-190", AttemptID: "attempt-190", WorkerID: "worker-190", LeaseID: "lease-190", LeaseFence: 1, ProviderResource: resource, ExpiresAt: expires}
	if err := h.Validate(); err != nil {
		t.Fatalf("synthetic credential scope: %v", err)
	}
	k := &KeeperV1{credentialed: true, sourceLifetime: t.Context(), source: &localoperator.Source{}, bootstrap: &ydbstore.LocalOwnedStoreBootstrapV1{}, terminalSealed: make(chan struct{}), credentialSealed: make(chan struct{}), config: domain.ConfiguredInferenceProfileV1{TenantID: h.TenantID, OwnerUserID: h.OwnerUserID, Resource: resource, Auth: domain.ConfiguredInferenceAuthBearerV1, Limits: testLimits()}, policy: ports.LocalLoopbackEgressPolicyV1{Limits: testLimits()}}
	k.self = k
	s := domain.AttemptExecutionSourceV1{Scope: domain.AttemptExecutionSourceScopeV1{TenantID: h.TenantID, OwnerUserID: h.OwnerUserID, RunID: h.RunID, AttemptID: h.AttemptID, SessionID: "session-190"}, ExpiresAt: expires}
	a := domain.RegisteredAllocationV2{Scope: s.Scope, Lease: domain.Lease{ID: h.LeaseID, WorkerID: h.WorkerID, FenceToken: h.LeaseFence, ExpiresAt: expires}, Window: domain.OwnedAgentAuthorityWindowV1{ExpiresAt: expires}, InvocationDeadline: expires, Backend: domain.HarnessBackendDescriptorV1{CredentialDeliveryKind: domain.ProviderCredentialDeliveryDirectV1}}
	op := domain.AttemptOperationV1{Deadline: time.Now().Add(time.Minute)}
	return k, h, a, s, op
}

func TestLocalCredentialWireMutableExactAndNoneUnchanged(t *testing.T) {
	key := []byte("synthetic-marker-not-a-key")
	body := []byte(`{"x":1}`)
	wire := requestWire("127.0.0.1:12345", body, key)
	if len(wire) != cap(wire) || bytes.Count(wire, []byte("Authorization: Bearer ")) != 1 || !bytes.Contains(wire, append(append([]byte("Authorization: Bearer "), key...), '\r', '\n')) {
		t.Fatal("credential wire is not one exact owned allocation/header")
	}
	clear(wire)
	if !bytes.Equal(key, []byte("synthetic-marker-not-a-key")) || !bytes.Equal(body, []byte(`{"x":1}`)) {
		t.Fatal("clearing owned wire changed borrowed key/body")
	}
	none := requestWire("127.0.0.1:12345", body, nil)
	want := "POST /v1/chat/completions HTTP/1.1\r\nHost: 127.0.0.1:12345\r\nContent-Type: application/json\r\nAccept: application/json\r\nConnection: close\r\nContent-Length: 7\r\n\r\n" + string(body)
	if string(none) != want || bytes.Contains(none, []byte("Authorization")) {
		t.Fatal("none wire changed or contains a credential header")
	}
	for _, key := range [][]byte{nil, []byte(" "), []byte("x\ty"), []byte("x\r\nInjected:y"), []byte{0x7f}, []byte{0xc2, 0xa0}, bytes.Repeat([]byte{'a'}, 4097)} {
		if validBearerBytes(key) {
			t.Fatalf("unsafe header bytes accepted: length=%d", len(key))
		}
	}
	if !validBearerBytes(bytes.Repeat([]byte{'a'}, 4096)) {
		t.Fatal("exact 4096-byte bound denied")
	}
}

func TestLocalCredentialScopeExactJoinsAndOneEntry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		k, handle, a, s, op := credentialFixture(t)
		key := []byte("synthetic-marker")
		if err := k.BeginCredentialScope(t.Context(), handle, key); err != nil {
			t.Fatalf("begin synthetic scope: %v", err)
		}
		defer func() { _ = k.EndCredentialScope(context.Background(), time.Now().Add(time.Second)) }()
		if err := k.BeginCredentialScope(t.Context(), handle, key); !errors.Is(err, domain.ErrMembershipDenied) {
			t.Fatal("second scope entry admitted")
		}
		if !k.credentialAdmittedLocked(k.credentialScope, a, s, op, time.Now()) {
			t.Fatal("exact synthetic scope denied")
		}
		for _, mutate := range []func(*ports.ProviderInvocationCredentialV1){func(h *ports.ProviderInvocationCredentialV1) { h.TenantID = "foreign" }, func(h *ports.ProviderInvocationCredentialV1) { h.OwnerUserID = "foreign" }, func(h *ports.ProviderInvocationCredentialV1) { h.RunID = "foreign" }, func(h *ports.ProviderInvocationCredentialV1) { h.AttemptID = "foreign" }, func(h *ports.ProviderInvocationCredentialV1) { h.WorkerID = "foreign" }, func(h *ports.ProviderInvocationCredentialV1) { h.LeaseID = "foreign" }, func(h *ports.ProviderInvocationCredentialV1) { h.LeaseFence++ }, func(h *ports.ProviderInvocationCredentialV1) { h.ProviderResource.CredentialGeneration++ }, func(h *ports.ProviderInvocationCredentialV1) { h.ExpiresAt = time.Now() }, func(h *ports.ProviderInvocationCredentialV1) { h.ExpiresAt = a.Window.ExpiresAt.Add(time.Second) }} {
			foreign := handle
			mutate(&foreign)
			k.credentialScope.handle = foreign
			if k.credentialAdmittedLocked(k.credentialScope, a, s, op, time.Now()) {
				t.Fatal("foreign credential scope joined original operation")
			}
		}
		k.credentialScope.handle = handle
		a.Backend.CredentialDeliveryKind = domain.ProviderCredentialDeliveryEnvironmentV1
		if k.credentialAdmittedLocked(k.credentialScope, a, s, op, time.Now()) {
			t.Fatal("non-direct delivery admitted")
		}
	})
}

func TestLocalCredentialEndDisposesReferenceAndPreservesTerminalSeal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		k, h, _, _, _ := credentialFixture(t)
		key := []byte("synthetic-marker")
		if err := k.BeginCredentialScope(t.Context(), h, key); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(time.Second)
		if err := k.EndCredentialScope(t.Context(), deadline); err != nil {
			t.Fatalf("idle scope end: %v", err)
		}
		if k.closed || !k.credentialEnded || k.credentialScope.borrowed != nil || k.credentialScope.ctx != nil || !bytes.Equal(key, []byte("synthetic-marker")) {
			t.Fatal("scope end closed keeper, retained borrowed reference or cleared caller bytes")
		}
		if k.BeginCredentialScope(t.Context(), h, key) == nil || k.SealTerminal(deadline) != nil || k.StopAndJoinTerminal(t.Context()) != nil {
			t.Fatal("scope was reusable or positive disposal destroyed original terminal sealability")
		}
	})
}

func TestLocalCredentialExpiredEndStillWaitsActualDisposal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		k, h, _, _, _ := credentialFixture(t)
		key := []byte("synthetic-marker")
		if err := k.BeginCredentialScope(t.Context(), h, key); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		wire := requestWire("127.0.0.1:12345", []byte(`{}`), key)
		c := &ConnectionV1{keeper: k, ctx: ctx, cancel: cancel, request: wire, cleanupDone: make(chan struct{}), cleanupTimeout: time.Second}
		c.self = c
		finished := make(chan error, 1)
		var disposeOnce sync.Once
		dispose := func() {
			disposeOnce.Do(func() {
				clear(wire)
				k.mu.Lock()
				k.active = nil
				k.mu.Unlock()
				close(c.cleanupDone)
			})
		}
		joined := false
		t.Cleanup(func() {
			dispose()
			synctest.Wait()
			if !joined {
				<-finished
			}
		})
		k.active = c
		k.credentialScope.joins = []<-chan struct{}{c.cleanupDone}
		go func() { finished <- k.EndCredentialScope(t.Context(), time.Now()) }()
		synctest.Wait()
		select {
		case <-finished:
			joined = true
			t.Fatal("expired End returned before actual transport disposal")
		default:
		}
		if ctx.Err() == nil || !bytes.Contains(wire, key) || k.credentialScope.borrowed != nil {
			t.Fatal("End did not cancel, or cleared wire before its actual join")
		}
		// Synthetic actual monitor completion, explicitly not a network proof.
		dispose()
		synctest.Wait()
		err := <-finished
		joined = true
		if !errors.Is(err, domain.ErrMembershipDenied) || !k.credentialTainted || !bytes.Equal(key, []byte("synthetic-marker")) || strings.Trim(string(wire), "\x00") != "" {
			t.Fatal("late disposal became success, retained wire, or altered caller key")
		}
		if k.SealTerminal(time.Now().Add(time.Second)) == nil {
			t.Fatal("late disposal silently repaired terminal eligibility")
		}
	})
}
