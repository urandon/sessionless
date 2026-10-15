package providercredential

import (
	"bytes"
	"errors"
	"testing"

	"gitcode.com/urandon/sessionless/internal/pisdkhttpwire"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func TestPiHTTPSOriginalFilePreparationAndNoStructuralSource(t *testing.T) {
	for _, mode := range []string{"missing genuine sender", "bad MAC", "held use gate", "post-auth ceiling"} {
		t.Run(mode, func(t *testing.T) {
			s, secrets, issue, p, ledger, request := piAuthOwnerFixture(t, 64)
			bundle, _ := piTLSFixture(t)
			if err := p.PrepareRequestAuthenticationV1(request); err != nil {
				t.Fatal(err)
			}
			if err := p.PrepareHTTPReceiveV1(request.Profile.Limits.ResponseHeaderBytes, request.Profile.Limits.ResponseBodyBytes); err != nil {
				t.Fatal(err)
			}
			if err := p.prepareResponseScan(); err != nil {
				t.Fatal(err)
			}
			if err := p.prepareHTTPS(bundle); err != nil {
				t.Fatal(err)
			}
			network := p.state.network.Load()
			if network == nil || secrets.readCalls != 0 {
				t.Fatal("preparation read a credential or lost private socket owner")
			}
			copyBefore, liveBefore, nodesBefore := ledger.copy, ledger.live, ledger.nodes
			h := issueImmutableAPI(t, s, issue)
			err := s.MaterializeProviderCredential(t.Context(), h, func(_ ports.ProviderCredentialMaterializationV1, secret []byte) error {
				if secret != nil || p.state.use.Load() == nil {
					t.Fatal("file materialization exposed key or lost genuine use")
				}
				use, a := p.state.use.Load(), p.state.auth
				frame := pisdkhttpwire.Frame{Kind: pisdkhttpwire.FetchRequest, Profile: a.profile, Headers: a.headers, Body: request.Body}
				frame.MAC = piRequestMAC([]byte("synthetic-immutable-api-marker"), p.Nonce(), a.binding, a.profile, a.endpoint, frame.Headers, frame.Body)
				if mode == "bad MAC" {
					frame.MAC[0] ^= 1
				}
				if mode == "post-auth ceiling" {
					network.requestHeaders = network.sdkHeaderBytes + uint64(len("authorization")+len("Bearer ")+4+len(use.secret)) - 1
				}
				if mode == "held use gate" {
					<-use.gate
					defer func() { use.gate <- struct{}{} }()
				}
				borrow, err := p.state.beginHTTPS(frame)
				if err == nil || borrow != nil || network.spent.Load() {
					t.Fatal("missing genuine Source/invalid request reached network", err)
				}
				if mode == "post-auth ceiling" && !errors.Is(err, ErrDeliveryMismatch) {
					t.Fatal("complete post-auth limit was not checked before Source lookup", err)
				}
				if borrow, err := p.state.beginHTTPS(frame); err == nil || borrow != nil {
					t.Fatal("failed owner use replenished", err)
				}
				if err := p.Close(); !errors.Is(err, ErrConsumed) || ledger.live != liveBefore {
					t.Fatal("active callback refunded", err, ledger.live)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := p.Close(); err != nil || ledger.live != 0 || ledger.copy != copyBefore || ledger.nodes != nodesBefore || p.state.network.Load() != nil || network.wire != nil || network.roots != nil {
				t.Fatal("post-callback root/wire/ledger retirement", err)
			}
		})
	}
}

func TestPiHTTPSResponseBorrowSameCallbackCloseNeverWaitsOnItself(t *testing.T) {
	s, _, issue, p, ledger, request := piAuthOwnerFixture(t)
	if err := p.PrepareRequestAuthenticationV1(request); err != nil {
		t.Fatal(err)
	}
	if err := p.PrepareHTTPReceiveV1(request.Profile.Limits.ResponseHeaderBytes, request.Profile.Limits.ResponseBodyBytes); err != nil {
		t.Fatal(err)
	}
	if err := p.prepareResponseScan(); err != nil {
		t.Fatal(err)
	}
	h := issueImmutableAPI(t, s, issue)
	// Genuine Issue/Materialize and gate; response-borrow mechanics are synthetic,
	// NOT a claim of Source/TLS/quarantine composition or a serving producer.
	if err := s.MaterializeProviderCredential(t.Context(), h, func(ports.ProviderCredentialMaterializationV1, []byte) error {
		use := p.state.use.Load()
		alias := use.secret
		before := ledger.live
		p.state.mu.Lock()
		<-use.gate
		b := &piHTTPSResponseBorrow{s: p.state, use: use}
		p.state.httpBorrow.Store(true)
		defer b.close()
		// SAME synchronous callback used to deadlock on its own retained mutex.
		if err := p.Close(); !errors.Is(err, ErrConsumed) || ledger.live != before || len(alias) == 0 || alias[0] == 0 || p.state.http == nil {
			t.Fatal("Close waited/refunded/zeroed beneath callback borrow", err)
		}
		b.close()
		b.close()
		if p.state.httpBorrow.Load() || ledger.live != before || !bytes.Equal(alias, []byte("synthetic-immutable-api-marker")) {
			t.Fatal("borrow close refunded or zeroed original owner's live secret")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := p.Close(); err != nil || ledger.live != 0 {
		t.Fatal("post-join cleanup", err)
	}
}

func TestPiHTTPSOriginalMaximumKeyMustFitBeforeIssue(t *testing.T) {
	s, secrets, _, p, ledger, request := piAuthOwnerFixture(t)
	if err := p.PrepareRequestAuthenticationV1(request); err != nil {
		t.Fatal(err)
	}
	if err := p.PrepareHTTPReceiveV1(request.Profile.Limits.ResponseHeaderBytes, request.Profile.Limits.ResponseBodyBytes); err != nil {
		t.Fatal(err)
	}
	if err := p.prepareResponseScan(); err != nil {
		t.Fatal(err)
	}
	bundle, _ := piTLSFixture(t)
	before := ledger.live
	if uint64(p.state.file.maxBytes) < request.Profile.Limits.RequestHeaderBytes {
		t.Fatal("fixture no longer exceeds complete post-auth maximum")
	}
	if err := p.prepareHTTPS(bundle); err == nil || !p.state.stopped.Load() || p.state.network.Load() != nil || ledger.live != before || len(s.handles) != 0 || secrets.readCalls != 0 {
		t.Fatal("unknown maximum fit reached Issue/secret read or retained partial network", err)
	}
}
