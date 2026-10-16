//go:build darwin || linux

package providercredential

import (
	"bytes"
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	daemon "gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/pisdkhttpwire"
	"gitcode.com/urandon/sessionless/internal/ports"
)

func piDriverFactoryFixture(t *testing.T, roots bool) (*PreparedPiFileUseV1, *piFileLedger, *daemon.Supervisor, daemon.PreparedAPIRequestV1, *fakeSecretStore) {
	t.Helper()
	_, secrets, _, p, ledger, request := piAuthOwnerFixture(t, 64)
	request.Model = []byte(`{"api":"openai-responses"}`)
	request.Schema = []byte(`{"type":"object"}`)
	if err := p.PrepareRequestAuthenticationV1(request); err != nil {
		t.Fatal(err)
	}
	if err := p.PrepareHTTPReceiveV1(request.Profile.Limits.ResponseHeaderBytes, request.Profile.Limits.ResponseBodyBytes); err != nil {
		t.Fatal(err)
	}
	if err := p.prepareResponseScan(); err != nil {
		t.Fatal(err)
	}
	if roots {
		bundle, _ := piTLSFixture(t)
		if err := p.prepareHTTPS(bundle); err != nil {
			t.Fatal(err)
		}
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	s, err := daemon.NewSupervisor(daemon.SupervisorConfig{ScratchRoot: root, Launcher: piDriverLauncher{}, Timeout: time.Hour, TerminationGrace: 5 * time.Second, MaxStdoutBytes: 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	return p, ledger, s, request, secrets
}

func TestPiHTTPDriverFactoryOriginalCommitmentsAndNoOverrides(t *testing.T) {
	for _, mode := range []string{"exact", "body", "model", "schema", "headers", "profile", "missing private roots", "copied second factory"} {
		t.Run(mode, func(t *testing.T) {
			p, l, s, r, secrets := piDriverFactoryFixture(t, mode != "missing private roots")
			switch mode {
			case "body":
				r.Body = bytes.Clone(r.Body)
				r.Body[0] ^= 1
			case "model":
				r.Model = bytes.Clone(r.Model)
				r.Model[0] ^= 1
			case "schema":
				r.Schema = bytes.Clone(r.Schema)
				r.Schema[0] ^= 1
			case "headers":
				r.Headers = bytes.Clone(r.Headers)
				r.Headers[0] ^= 1
			case "profile":
				r.Profile.Model.ModelID = "foreign"
			}
			plan, err := p.PrepareHTTPIOV1(s, r)
			valid := mode == "exact" || mode == "copied second factory"
			if (err == nil) != valid || (plan != nil) != valid {
				t.Fatal("factory commitment mismatch", mode, err)
			}
			if secrets.readCalls != 0 || len(p.state.file.service.handles) != 0 {
				t.Fatal("factory touched Issue/key")
			}
			if valid && (p.state.ioPlan != plan || p.state.ioChunk != r.Profile.Limits.ResponseChunkBytes) {
				t.Fatal("factory did not retain exact fixed plan/limits")
			}
			if mode == "copied second factory" {
				copyRecipe := *p
				if second, err := copyRecipe.PrepareHTTPIOV1(s, r); err == nil || second != nil || !p.state.stopped.Load() {
					t.Fatal("copied factory revived original scope", err)
				}
			}
			work, nodes := l.copy, l.nodes
			if err := p.Close(); err != nil || l.live != 0 || l.copy != work || l.nodes != nodes || p.state.ioPlan != nil {
				t.Fatal("factory retirement", err, l.live)
			}
		})
	}
}

func TestPiHTTPDriverFactoryExactGlobalBudgetAndOneShort(t *testing.T) {
	p, l, s, r, _ := piDriverFactoryFixture(t, true)
	c, v, n := l.copy, l.live, l.nodes
	if _, err := p.PrepareHTTPIOV1(s, r); err != nil {
		t.Fatal(err)
	}
	dc, dv, dn := l.copy-c, l.live-v, l.nodes-n
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"exact", "work one short", "live one short", "nodes one short"} {
		t.Run(mode, func(t *testing.T) {
			p, l, s, r, secrets := piDriverFactoryFixture(t, true)
			l.maxCopy = l.copy + dc
			l.maxLive = l.live + dv
			l.maxNodes = l.nodes + dn
			switch mode {
			case "work one short":
				l.maxCopy--
			case "live one short":
				l.maxLive--
			case "nodes one short":
				l.maxNodes--
			}
			plan, err := p.PrepareHTTPIOV1(s, r)
			if (err == nil) != (mode == "exact") || (plan != nil) != (mode == "exact") {
				t.Fatal("global budget admission", mode, err)
			}
			if secrets.readCalls != 0 || len(p.state.file.service.handles) != 0 {
				t.Fatal("funding failure reached Issue")
			}
			work, nodes := l.copy, l.nodes
			if err := p.Close(); err != nil || l.live != 0 || l.copy != work || l.nodes != nodes {
				t.Fatal("denied preparation refund", err, l.live)
			}
		})
	}
}

func TestPiHTTPDriverReplayIndependentSchedule(t *testing.T) {
	// Independent integer oracle: C=1, N=(4096-6)/2=2045 is the
	// maximum canonical frame-count cohort, not an observed successful debit.
	l := domain.AttachedPiSDKNativeLimitsV1{ProcessFrameBytes: 114, ResponseBodyBytes: 2045, ResponseHeaderBytes: 2, ResponseChunkBytes: 1, ProcessEvents: 4096}
	lengths := [4]int{17, 19, 23, 29}
	want := uint64(2047)*(4*uint64(reflect.TypeOf(pisdkhttpwire.Frame{}).Size())+256) +
		8*(2045+2+17+19+23+29) + 8*uint64(domain.AttachedPiSDKCanonicalMaxBytesV1) +
		4*uint64(reflect.TypeOf(domain.AttachedPiSDKNativeProfileV1{}).Size()) + 16384
	work, chunk, err := measurePiHTTPReplay(l, lengths)
	if err != nil || work != want || chunk != 1 {
		t.Fatal("minimum-chunk maximum-count schedule", work, want, chunk, err)
	}
	for _, oneShort := range []bool{false, true} {
		ledger := newPiFileLedger()
		ledger.maxCopy = want
		if oneShort {
			ledger.maxCopy--
		}
		if err := ledger.Reserve(work, 256, 1); (err == nil) == oneShort {
			t.Fatal("independent exact/one-short admission", oneShort, err)
		}
	}
	l.ResponseBodyBytes++
	if _, _, err := measurePiHTTPReplay(l, lengths); err == nil {
		t.Fatal("first-over canonical frame count accepted")
	}
	l.ResponseBodyBytes = 2045
	l.ResponseChunkBytes = 2045
	work, chunk, err = measurePiHTTPReplay(l, lengths)
	if err != nil || chunk != 93 { // frame114 minus part-header21
		t.Fatal("frame-limited canonical chunk", chunk, err)
	}
	if work >= want {
		t.Fatal("per-frame cost did not track canonical N", work, want)
	}
}

func TestPiHTTPDriverReplayPartitionFundedBeforeComparison(t *testing.T) {
	for _, oneShort := range []bool{false, true} {
		p, l, s, r, secrets := piDriverFactoryFixture(t, true)
		work, _, err := measurePiHTTPReplay(p.state.auth.responseLimits, p.state.auth.sourceLengths)
		if err != nil {
			t.Fatal(err)
		}
		before := l.copy
		l.maxCopy = before + work
		if oneShort {
			l.maxCopy--
		}
		// With exactly the driver partition the subsequent daemon partition must
		// fail; one byte short must fail before any driver work is admitted.
		if plan, err := p.PrepareHTTPIOV1(s, r); err == nil || plan != nil {
			t.Fatal("unfunded subsequent daemon partition accepted", err)
		}
		want := before + work
		if oneShort {
			want = before
		}
		if l.copy != want || secrets.readCalls != 0 {
			t.Fatal("prospective driver partition", l.copy, want, secrets.readCalls)
		}
		if err := p.Close(); err != nil || l.live != 0 {
			t.Fatal("failed partition retirement", err, l.live)
		}
	}
}

type piReplaySinkFunc func(pisdkhttpwire.Frame) error

func (f piReplaySinkFunc) Write(v pisdkhttpwire.Frame) error { return f(v) }

func TestPiHTTPDriverFailedWriteAbortsBeforeOriginalKeyRetirement(t *testing.T) {
	for _, failure := range []pisdkhttpwire.Kind{pisdkhttpwire.ResponseStart, pisdkhttpwire.ResponsePart, pisdkhttpwire.ResponseEnd} {
		t.Run(string(rune('0'+failure)), func(t *testing.T) {
			s, _, issue, p, l, r := piAuthOwnerFixture(t, 64)
			if err := p.PrepareRequestAuthenticationV1(r); err != nil {
				t.Fatal(err)
			}
			if err := p.PrepareHTTPReceiveV1(r.Profile.Limits.ResponseHeaderBytes, r.Profile.Limits.ResponseBodyBytes); err != nil {
				t.Fatal(err)
			}
			if err := p.prepareResponseScan(); err != nil {
				t.Fatal(err)
			}
			h := issueImmutableAPI(t, s, issue)
			var keyAlias []byte
			err := s.MaterializeProviderCredential(t.Context(), h, func(ports.ProviderCredentialMaterializationV1, []byte) error {
				use := p.state.use.Load()
				keyAlias = use.secret
				p.state.mu.Lock()
				<-use.gate
				b := &piHTTPSResponseBorrow{s: p.state, use: use, ctx: use.ctx}
				p.state.httpBorrow.Store(true)
				defer b.close()
				view, err := p.state.http.receive(use.ctx, bytes.NewReader([]byte(piHTTPFixture(piPolicyBody(t, piPolicyTerminal()), false))))
				if err != nil {
					t.Fatal(err)
				}
				if err := p.state.responseScan.scan(use.ctx, view, use.secret); err != nil {
					t.Fatal(err)
				}
				if err := p.state.responseScan.checkResponsePolicy(p.state.expected); err != nil {
					t.Fatal(err)
				}
				if err := p.state.responseScan.scanPiSemantics(); err != nil {
					t.Fatal(err)
				}
				b.view = view
				live := l.live
				failed := false
				err = b.replay(context.Background(), piReplaySinkFunc(func(f pisdkhttpwire.Frame) error {
					if b.closed.Load() || l.live != live || !bytes.Equal(keyAlias, []byte("synthetic-immutable-api-marker")) {
						t.Error("borrow cleared beneath Write")
					}
					if f.Kind == failure {
						failed = true
						return errors.New("synthetic short/failed write")
					}
					return nil
				}), r.Profile.Limits.ResponseChunkBytes)
				if err == nil || !failed {
					t.Fatal("failed frame became successful replay", failure, err)
				}
				b.close()
				if !p.state.stopped.Load() || !p.state.responseScan.erased.Load() || l.live != live || !bytes.Equal(keyAlias, []byte("synthetic-immutable-api-marker")) {
					t.Fatal("failure did not abort/clear response while retaining original key/debit")
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(keyAlias, make([]byte, len(keyAlias))) {
				t.Fatal("original owner did not zero key after callback join")
			}
			if err := p.Close(); err != nil || l.live != 0 {
				t.Fatal("postcallback retirement", err, l.live)
			}
		})
	}
}
