package providercredential

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"testing/synctest"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/pisdkhttpwire"
	"gitcode.com/urandon/sessionless/internal/ports"
)

const piAuthFixtureHeaders = `{"accept":"application/json","content-type":"application/json","user-agent":"fixture/1","x-stainless-arch":"arm64","x-stainless-lang":"js","x-stainless-os":"Linux","x-stainless-package-version":"6.40.0","x-stainless-retry-count":"0","x-stainless-runtime":"node","x-stainless-runtime-version":"v24.19.0","x-stainless-timeout":"30"}`

func TestPiRequestAuthenticationLiteralVector(t *testing.T) {
	headers, err := piCanonicalRequestHeaders([]byte(piAuthFixtureHeaders), 331)
	if err != nil || len(headers) != 331 || pisdkhttpwire.ValidateHeaders(headers, 331, true) != nil {
		t.Fatalf("canonical fixture headers: bytes=%d err=%v", len(headers), err)
	}
	if got := sha256.Sum256(headers); hex.EncodeToString(got[:]) != "4eb66216ea83097b10b2fd2ad62e11739725fdb164324c97fd8a7fcd89cd04b8" {
		t.Fatal("header vector differs")
	}
	var nonce, binding, profile [32]byte
	for i := range nonce {
		nonce[i], binding[i], profile[i] = byte(i), 0x11, 0x22
	}
	endpoint, body, key := "https://api.example.invalid/v1/responses", []byte(`{"fixture":true}`), []byte("fixture-only-api-key")
	want := "39dea11e34ea383bc5a3299aeba6577bc397e2737d0792df93ad0f67912a0e04"
	mac := piRequestMAC(key, nonce, binding, profile, endpoint, headers, body)
	if hex.EncodeToString(mac[:]) != want {
		t.Fatalf("literal HMAC got %x want %s", mac, want)
	}
	for _, field := range []string{"nonce", "binding", "profile", "endpoint", "headers", "body", "key"} {
		t.Run(field, func(t *testing.T) {
			n, b, p, e, h, d, k := nonce, binding, profile, endpoint, bytes.Clone(headers), bytes.Clone(body), bytes.Clone(key)
			switch field {
			case "nonce":
				n[0] ^= 1
			case "binding":
				b[0] ^= 1
			case "profile":
				p[0] ^= 1
			case "endpoint":
				e += "x"
			case "headers":
				h[len(h)-1] ^= 1
			case "body":
				d = []byte(`{ "fixture": true }`)
			case "key":
				k[0] ^= 1
			}
			if got := piRequestMAC(k, n, b, p, e, h, d); got == mac {
				t.Error("transcript field/key not committed")
			}
		})
	}
}

func TestPiRequestAuthenticationHeaderRejections(t *testing.T) {
	for _, raw := range []string{
		strings.Replace(piAuthFixtureHeaders, `"accept":`, `"authorization":`, 1),
		strings.Replace(piAuthFixtureHeaders, `"user-agent":`, `"accept":`, 1),
		strings.Replace(piAuthFixtureHeaders, `"fixture/1"`, `" fixture/1"`, 1),
		strings.Replace(piAuthFixtureHeaders, `"fixture/1"`, `"fixture/1 "`, 1),
		strings.Replace(piAuthFixtureHeaders, `"fixture/1"`, `"fixture\r\n1"`, 1),
		strings.Replace(piAuthFixtureHeaders, `"fixture/1"`, `"é"`, 1),
		strings.Replace(piAuthFixtureHeaders, `"fixture/1"`, `[]`, 1),
		piAuthFixtureHeaders + `{}`, `{}`, `[]`,
	} {
		if out, err := piCanonicalRequestHeaders([]byte(raw), len(raw)+128); err == nil || out != nil {
			t.Fatalf("invalid headers accepted: %q", raw)
		}
	}
	if out, err := piCanonicalRequestHeaders([]byte(piAuthFixtureHeaders), 330); err == nil || out != nil {
		t.Fatal("first-over header block accepted")
	}
	// Original projection key order and JSON escapes do not change exact
	// decoded header values. Actual wire order remains strictly canonical.
	escaped := strings.Replace(piAuthFixtureHeaders, `fixture/1`, `fixture\/1`, 1)
	a, err := piCanonicalRequestHeaders([]byte(escaped), 331)
	b, _ := piCanonicalRequestHeaders([]byte(piAuthFixtureHeaders), 331)
	if err != nil || !bytes.Equal(a, b) {
		t.Fatal("decoded value changed", err)
	}
}

// Structural profile only; no installed SDK/native/platform/model proof.
func piAuthProfileFixture(t *testing.T) domain.AttachedPiSDKNativeProfileV1 {
	t.Helper()
	p := domain.AttachedPiSDKNativeProfileV1{
		Version: 1, ProfileID: domain.AttachedPiSDKProfileIDV1, NativeProtocolVersion: domain.AttachedPiSDKNativeProtocolV1,
		Sources:  domain.AttachedPiSDKNativeSourcesV1{PiSourceRevision: "6c4f360264397c59801f6da2bdac13e3b1fcbe91", OpenAISDKVersion: "6.40.0", OpenAISDKIntegrity: "sha512-MWtTjd/gQt4jpbji61NTgFWJLoY/PdRJ6wG9/ZDRMYNMlBKrCrSlkLI+KgHP1vR1qT6LKSAyAqIxno6lcK9JiA==", NodeVersion: "v24.19.0"},
		Model:    domain.AttachedPiSDKNativeModelV1{ModelVendorID: "openrouter", ModelID: "stealth-ox-alpha", Endpoint: "https://fixture.invalid/v1/responses", ModelContextTokens: 8192, MaxBilledInputTokens: 2048, MaxOutputTokens: 1024},
		Platform: domain.AttachedPiSDKNativePlatformV1{OS: "Linux", Architecture: "arm64"},
		Limits:   domain.AttachedPiSDKNativeLimitsV1{CanonicalInputBytes: 512 << 10, NativeRequestBytes: 1 << 20, PreSendAllocationBytes: 8 << 20, RequestHeaderBytes: 4096, ResponseHeaderBytes: 4096, ResponseBodyBytes: 1 << 20, ResponseChunkBytes: 32 << 10, SSELineBytes: 32 << 10, SSEEventBytes: 128 << 10, SSEEvents: 512, JSONNodes: 4096, ResponseCopyWorkBytes: 16 << 20, ResponseAllocationBytes: 8 << 20, ProcessMemoryBytes: 256 << 20, ProcessDiskBytes: 1 << 20, ProcessFrameBytes: 512 << 10, ProcessStdoutBytes: 8 << 20, ProcessEvents: 2048, JSONDepth: 16, RawFinalBytes: 192 << 10, StructuredFinalBytes: 96 << 10, AnswerBytes: 16 << 10, ArtifactBytes: 16 << 10, ArtifactTotalBytes: 48 << 10},
		Behavior: domain.AttachedPiSDKNativeBehaviorV1{ProviderAPI: "openai-responses", ImageDerivation: "NoTransformOriginal", ImageDetail: "auto", Transport: "http-sse", LocalToolRegistry: "empty", MaxPhysicalSends: 1},
	}
	for _, target := range []any{&p.Sources, &p.Model, &p.Platform} {
		v := reflect.ValueOf(target).Elem()
		for i := range v.NumField() {
			if strings.HasSuffix(v.Type().Field(i).Name, "Digest") {
				v.Field(i).SetString(strings.Repeat("a", 64))
			}
		}
	}
	if err := p.Validate(); err != nil {
		t.Fatal(err)
	}
	return p
}

const piPolicyFixtureRequest = `{"model":"stealth-ox-alpha","input":[{"role":"user","content":[{"type":"input_text","text":"fixture"}]}],"stream":true,"store":false,"max_output_tokens":1024,"tool_choice":"none","truncation":"disabled","text":{"format":{"type":"json_schema","name":"fixture","schema":{"type":"object","properties":{},"additionalProperties":false},"strict":true}}}`

func piAuthOwnerFixture(t *testing.T, keyLimit ...int64) (*InvocationService, *fakeSecretStore, ports.ProviderCredentialIssueRequestV1, *PreparedPiFileUseV1, *piFileLedger, attachedworkerdaemon.PreparedAPIRequestV1) {
	t.Helper()
	s, _, secrets, issue, _ := immutableAPIInvocationFixture(t, domain.ProviderCredentialDeliveryFileV1)
	if len(keyLimit) != 0 {
		s.maxBytes = keyLimit[0] // test-owned source limit, BEFORE immutable preparation
	}
	p := piAuthProfileFixture(t)
	d, err := p.Descriptor()
	if err != nil {
		t.Fatal(err)
	}
	issue.HarnessBinding.Backend = d
	issue.HarnessBinding.AttachedPiAPIInvocationPolicyDigest = domain.AttachedPiAPIInvocationPolicyMappingDigestV1(strings.Repeat("b", 64))
	f := prepareFileFixture(t, s, issue)
	l := newPiFileLedger()
	r := attachedworkerdaemon.PreparedAPIRequestV1{Profile: p, Body: []byte(piPolicyFixtureRequest), Headers: []byte(piAuthFixtureHeaders)}
	recipe, err := f.PreparePiFileUseV1(l, r.Body, d.BackendProfileDigest)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := recipe.Close(); err != nil {
			t.Error(err)
		}
	})
	return s, secrets, issue, recipe, l, r
}

func TestPiRequestAuthenticationOriginalFileOwner(t *testing.T) {
	for _, mode := range []string{"valid", "MAC", "body bytes", "headers", "nonce", "profile", "wrong kind", "closed", "repeat"} {
		t.Run(mode, func(t *testing.T) {
			s, secrets, issue, p, l, request := piAuthOwnerFixture(t)
			if err := p.PrepareRequestAuthenticationV1(request); err != nil {
				t.Fatal("pre-Issue auth seal:", err)
			}
			beforeCopy, beforeLive := l.copy, l.live
			h := issueImmutableAPI(t, s, issue)
			err := s.MaterializeProviderCredential(t.Context(), h, func(_ ports.ProviderCredentialMaterializationV1, plaintext []byte) error {
				if plaintext != nil {
					t.Fatal("file callback exported secret")
				}
				a := p.state.auth
				// Test marker only. Production obtains the key ONLY from active use.
				marker := []byte("synthetic-immutable-api-marker")
				frame := pisdkhttpwire.Frame{Kind: pisdkhttpwire.FetchRequest, Profile: a.profile, Headers: bytes.Clone(a.headers), Body: bytes.Clone(request.Body)}
				frame.MAC = piRequestMAC(marker, p.Nonce(), a.binding, a.profile, a.endpoint, frame.Headers, frame.Body)
				limits := pisdkhttpwire.Limits{FrameBytes: 4096, InitializeBytes: 128, RequestBytes: uint64(len(request.Body)), FinalBytes: 128, RequestHeaderBytes: 512, ResponseHeaderBytes: 128, ResponseBytes: 64, ResponseChunkBytes: 7, ParentBytes: 4 << 20, ChildBytes: 16 << 20, Frames: 4096}
				wire := make([]byte, 4096)
				n, err := pisdkhttpwire.EncodeInto(wire, frame, limits)
				if err != nil {
					t.Fatal("encode genuine FetchRequest:", err)
				}
				frame, err = pisdkhttpwire.DecodeView(wire[:n], pisdkhttpwire.ChildToParent, limits)
				if err != nil {
					t.Fatal("decode genuine FetchRequest:", err)
				}
				switch mode {
				case "MAC":
					frame.MAC[0] ^= 1
				case "body bytes":
					frame.Body = []byte(`{ "fixture": true }`)
					frame.MAC = piRequestMAC(marker, p.Nonce(), a.binding, a.profile, a.endpoint, frame.Headers, frame.Body)
				case "headers":
					frame.Headers[len(frame.Headers)-1] ^= 1
					frame.MAC = piRequestMAC(marker, p.Nonce(), a.binding, a.profile, a.endpoint, frame.Headers, frame.Body)
				case "nonce":
					p.state.nonce[0] ^= 1
				case "profile":
					frame.Profile[0] ^= 1
				case "wrong kind":
					frame.Kind = pisdkhttpwire.Initialize
				case "closed":
					if err := p.Close(); !errors.Is(err, ErrConsumed) {
						t.Fatal(err)
					}
				}
				wantGood := mode == "valid" || mode == "repeat"
				if err := p.state.compareRequestMAC(frame); (err == nil) != wantGood {
					t.Errorf("compare mode %s err=%v", mode, err)
				}
				if err := p.state.compareRequestMAC(frame); !errors.Is(err, ErrConsumed) {
					t.Error("comparison replenished", err)
				}
				if l.copy != beforeCopy || l.live != beforeLive {
					t.Error("post-Issue debit/refund")
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if secrets.readCalls != 1 {
				t.Fatalf("secret backend reads=%d want1", secrets.readCalls)
			}
			if err := p.Close(); err != nil || l.live != 0 {
				t.Fatalf("joined cleanup err=%v live=%d", err, l.live)
			}
			if err := p.state.compareRequestMAC(pisdkhttpwire.Frame{}); err == nil {
				t.Error("retired recipe reusable")
			}
		})
	}
}

func TestPiRequestAuthenticationPreparationDenials(t *testing.T) {
	for _, mode := range []string{"copy short", "live short", "nodes short", "profile", "endpoint", "body", "headers", "after Issue", "repeat"} {
		t.Run(mode, func(t *testing.T) {
			s, secrets, issue, p, l, r := piAuthOwnerFixture(t)
			beforeCopy, beforeLive, beforeNodes := l.copy, l.live, l.nodes
			switch mode {
			case "copy short":
				l.maxCopy = l.copy
			case "live short":
				l.maxLive = l.live
			case "nodes short":
				l.maxNodes = l.nodes
			case "profile":
				r.Profile.Platform.OS = "Other"
			case "endpoint":
				r.Profile.Model.Endpoint += "x"
			case "body":
				r.Body = []byte(`{"fixture":null}`)
			case "headers":
				r.Headers = []byte(`{}`)
			case "after Issue":
				_ = issueImmutableAPI(t, s, issue)
			case "repeat":
				if err := p.PrepareRequestAuthenticationV1(r); err != nil {
					t.Fatal(err)
				}
			}
			if err := p.PrepareRequestAuthenticationV1(r); err == nil {
				t.Error("invalid/late/repeated seal accepted")
			}
			if !p.state.stopped.Load() {
				t.Error("failed seal did not burn recipe")
			}
			if strings.HasSuffix(mode, "short") && (l.copy != beforeCopy || l.live != beforeLive || l.nodes != beforeNodes) {
				t.Error("unfunded allocation changed ledger")
			}
			if secrets.readCalls != 0 {
				t.Error("preparation read secret")
			}
			if err := p.Close(); err != nil || l.live != 0 {
				t.Fatal("cleanup", err, l.live)
			}
		})
	}
}

func TestPiRequestAuthenticationCanonicalHeaderOrder(t *testing.T) {
	// Reverse every JSON pair; this differs from the alphabetical fixture.
	pairs := strings.Split(strings.Trim(piAuthFixtureHeaders, "{}"), ",")
	for i, j := 0, len(pairs)-1; i < j; i, j = i+1, j-1 {
		pairs[i], pairs[j] = pairs[j], pairs[i]
	}
	raw := []byte("{" + strings.Join(pairs, ",") + "}")
	if bytes.Equal(raw, []byte(piAuthFixtureHeaders)) {
		t.Fatal("fixture did not reorder keys")
	}
	a, err := piCanonicalRequestHeaders(raw, 331)
	if err != nil {
		t.Fatal(err)
	}
	b, err := piCanonicalRequestHeaders([]byte(piAuthFixtureHeaders), 331)
	if err != nil || !bytes.Equal(a, b) {
		t.Fatal("JSON map order changed wire identity", err)
	}
}

func TestPiRequestAuthenticationExactFunding(t *testing.T) {
	for _, mode := range []string{"exact", "copy one short", "live one short", "nodes one short"} {
		t.Run(mode, func(t *testing.T) {
			_, _, _, p, l, r := piAuthOwnerFixture(t)
			beforeCopy, beforeLive, beforeNodes := l.copy, l.live, l.nodes
			// Independently account the specified fixed-shape metadata, header
			// buffer capacity and retained hash/HMAC/file scratch. Parser/profile
			// scratch must disappear immediately after the pre-Issue seal.
			retained := uint64(len(r.Headers)+128) + uint64(reflect.TypeOf(piRequestAuthentication{}).Size()) + 20480
			temporary := uint64(1<<20) + 4*domain.AttachedPiSDKCanonicalMaxBytesV1 + 16*uint64(len(r.Headers)) + 16384
			work := 8*temporary + 4*uint64(len(r.Body)) + 8*uint64(p.state.file.maxBytes)
			l.maxCopy = beforeCopy + work
			l.maxLive = beforeLive + retained + temporary
			l.maxNodes = beforeNodes + 1
			switch mode {
			case "copy one short":
				l.maxCopy--
			case "live one short":
				l.maxLive--
			case "nodes one short":
				l.maxNodes--
			}
			err := p.PrepareRequestAuthenticationV1(r)
			if mode == "exact" {
				if err != nil || l.copy != beforeCopy+work || l.live != beforeLive+retained || l.nodes != beforeNodes+1 {
					t.Fatalf("exact budget err=%v got=%+v work=%d retained=%d temporary=%d", err, l, work, retained, temporary)
				}
			} else if err == nil || l.copy != beforeCopy || l.live != beforeLive || l.nodes != beforeNodes || p.state.auth != nil {
				t.Fatalf("one-short admitted/allocated: err=%v ledger=%+v", err, l)
			}
			if err := p.Close(); err != nil || l.live != 0 {
				t.Fatal("retirement", err, l.live)
			}
		})
	}
}

func TestPiRequestAuthenticationUnsealedDenied(t *testing.T) {
	s, _, issue, p, _, _ := piAuthOwnerFixture(t)
	h := issueImmutableAPI(t, s, issue)
	if err := s.MaterializeProviderCredential(t.Context(), h, func(ports.ProviderCredentialMaterializationV1, []byte) error {
		if err := p.state.compareRequestMAC(pisdkhttpwire.Frame{}); err == nil || !p.state.stopped.Load() {
			t.Error("unsealed original file admitted")
		}
		if err := p.state.compareRequestMAC(pisdkhttpwire.Frame{}); !errors.Is(err, ErrConsumed) {
			t.Error("unsealed attempt replenished", err)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestPiRequestAuthenticationConcurrentCloseCancelsBeforeJoin(t *testing.T) {
	s, _, issue, p, l, r := piAuthOwnerFixture(t)
	if err := p.PrepareRequestAuthenticationV1(r); err != nil {
		t.Fatal(err)
	}
	h := issueImmutableAPI(t, s, issue)
	synctest.Test(t, func(t *testing.T) {
		if err := s.MaterializeProviderCredential(context.Background(), h, func(ports.ProviderCredentialMaterializationV1, []byte) error {
			use := p.state.use.Load()
			alias := use.secret
			before := l.live
			paused := &piBindPauseContext{Context: context.Background(), entered: make(chan struct{}), resume: make(chan struct{})}
			p.state.file.ctx = paused // installed before concurrent consumers start
			compared, closed := make(chan error, 1), make(chan error, 1)
			release := sync.OnceFunc(func() { close(paused.resume) })
			joinedCompare, joinedClose := false, false
			defer func() {
				release()
				if !joinedCompare {
					<-compared
				}
				if !joinedClose {
					<-closed
				}
			}()
			go func() { compared <- p.state.compareRequestMAC(pisdkhttpwire.Frame{}) }()
			<-paused.entered // comparison holds recipe.mu, before file use gate
			go func() { closed <- p.Close() }()
			<-use.ctx.Done() // Close must cancel without acquiring recipe.mu
			// Mutex waits are explicitly NOT durably blocked in synctest.
			// The Err barrier holds recipe.mu; cancellation is the observable
			// checkpoint, not a request for global quiescence with a mutex waiter.
			if p.state.mu.TryLock() {
				p.state.mu.Unlock()
				t.Fatal("comparison did not hold the owner join mutex")
			}
			select {
			case err := <-closed:
				joinedClose = true
				t.Fatalf("Close did not join held comparison: %v", err)
			default:
			}
			if l.live != before || len(alias) == 0 || alias[0] == 0 || p.state.auth == nil {
				t.Error("early refund/secret zero/auth drop before join")
			}
			release()
			compareErr := <-compared
			joinedCompare = true
			if compareErr == nil {
				t.Error("cancelled comparison succeeded")
			}
			closeErr := <-closed
			joinedClose = true
			if !errors.Is(closeErr, ErrConsumed) {
				t.Error("callback-active Close", closeErr)
			}
			if l.live != before {
				t.Error("callback-active Close refunded")
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	})
	if err := p.Close(); err != nil || l.live != 0 {
		t.Fatal("post-callback joined cleanup", err, l.live)
	}
}
