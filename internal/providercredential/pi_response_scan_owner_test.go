package providercredential

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/ports"
)

func TestPiResponseScanSealedOriginalLimitsAndFileBorrowRetirement(t *testing.T) {
	s, secrets, issue, p, l, request := piAuthOwnerFixture(t, 64)
	original := request.Profile.Limits
	if err := p.PrepareRequestAuthenticationV1(request); err != nil {
		t.Fatal(err)
	}
	if err := p.PrepareHTTPReceiveV1(original.ResponseHeaderBytes, original.ResponseBodyBytes); err != nil {
		t.Fatal(err)
	}
	// Caller DATA mutation after the original auth seal cannot replace limits.
	request.Profile.Limits.JSONNodes = 1
	request.Profile.Limits.ResponseBodyBytes = 1
	beforeCopy := l.copy
	if err := p.prepareResponseScan(); err != nil {
		t.Fatal(err)
	}
	q := p.state.responseScan
	if q.limits != original || l.copy != beforeCopy+original.ResponseCopyWorkBytes || secrets.readCalls != 0 || len(s.handles) != 0 {
		t.Fatal("preparation replaced original limits/ledger or touched credential lifecycle")
	}
	reserved := l.live
	h := issueImmutableAPI(t, s, issue)
	var originalKey []byte
	// Actual Issue/Materialize/file gate/receiver/scan. Source, network and the
	// response borrow are synthetic here; this is NOT a complete Pi producer.
	if err := s.MaterializeProviderCredential(t.Context(), h, func(ports.ProviderCredentialMaterializationV1, []byte) error {
		use := p.state.use.Load()
		originalKey = use.secret
		p.state.mu.Lock()
		<-use.gate
		b := &piHTTPSResponseBorrow{s: p.state, use: use, ctx: use.ctx}
		p.state.httpBorrow.Store(true)
		defer b.close()
		view, err := p.state.http.receive(use.ctx, strings.NewReader(piHTTPFixture("data: {\"a\":\"ordinary\"}\n\n", false)))
		if err != nil {
			t.Fatal(err)
		}
		if err := q.scan(use.ctx, view, use.secret); err != nil {
			t.Fatal(err)
		}
		if q.textN == 0 || len(q.key) == 0 || &q.key[0] != &use.secret[0] {
			t.Fatal("scanner lost same original secret borrow or copied it")
		}
		if err := p.Close(); !errors.Is(err, ErrConsumed) || l.live != reserved || q.erased.Load() || q.ctx.Err() == nil {
			t.Fatal("active response retirement refunded/cleared instead of revoking", err)
		}
		b.close()
		if !piScanZero(q) || l.live != reserved || len(use.secret) == 0 || use.secret[0] == 0 {
			t.Fatal("scanner retirement failed or stole original secret/debit ownership")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(originalKey, make([]byte, len(originalKey))) || secrets.readCalls != 1 {
		t.Fatal("original file owner did not zero/join its sole secret read")
	}
	if err := p.Close(); err != nil || l.live != 0 || p.state.responseScan != nil {
		t.Fatal("post-callback scanner retirement", err)
	}
}

func TestPiResponseScanOriginalOwnerPreparationDenials(t *testing.T) {
	for _, mode := range []string{"before auth", "before HTTP", "work short", "repeat"} {
		t.Run(mode, func(t *testing.T) {
			_, _, _, p, l, request := piAuthOwnerFixture(t, 64)
			if mode != "before auth" {
				if err := p.PrepareRequestAuthenticationV1(request); err != nil {
					t.Fatal(err)
				}
			}
			if mode != "before auth" && mode != "before HTTP" {
				if err := p.PrepareHTTPReceiveV1(request.Profile.Limits.ResponseHeaderBytes, request.Profile.Limits.ResponseBodyBytes); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "repeat" {
				if err := p.prepareResponseScan(); err != nil {
					t.Fatal(err)
				}
			}
			beforeCopy, beforeLive, beforeNodes := l.copy, l.live, l.nodes
			if mode == "work short" {
				l.maxCopy = beforeCopy + request.Profile.Limits.ResponseCopyWorkBytes - 1
			}
			if err := p.prepareResponseScan(); err == nil || !p.state.stopped.Load() || l.copy != beforeCopy || l.live != beforeLive || l.nodes != beforeNodes {
				t.Fatal("invalid/partial/repeated scanner preparation allocated or replenished", err)
			}
			if err := p.Close(); err != nil || l.live != 0 {
				t.Fatal("denied setup retirement", err)
			}
		})
	}
}
