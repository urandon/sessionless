//go:build darwin || linux

package attachedworkersealedinput

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	"image/png"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemontransport"
	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/codexexec"
	"gitcode.com/urandon/sessionless/internal/credentiallifecycle"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/pisdkmedia"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/providercredential"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

// These are deliberately oversized SYNTHETIC logical budgets for four graphs,
// not installed native/model/heap qualification or permission to serve a model.
type piOwnerFixture struct {
	owner        *PiPreparedOwnerV1
	inv          attachedworkerdaemon.Invocation
	ctx          context.Context
	cancel       context.CancelFunc
	ledger       *codexexec.CanonicalMediaLedgerV1
	files        *credentiallifecycle.PinnedSecretFileSystem
	backend      *piOwnerForbiddenBackend
	launcher     *piOwnerNoLaunch
	phase        *atomic.Int32
	phaseHandler *atomic.Pointer[http.HandlerFunc]
	newSource    func() *SessionSourceFactory
}

type piOwnerForbiddenBackend struct {
	ports.ProviderCredentialBindingStore
	ports.ProviderCredentialSecretStore
	now                                time.Time
	plans, ids, loads, reads, recovers int
}

func (b *piOwnerForbiddenBackend) Now() time.Time { return b.now }
func (b *piOwnerForbiddenBackend) NewID(context.Context, ports.IDKind) (string, error) {
	b.ids++
	return "", ErrUnavailable
}
func (b *piOwnerForbiddenBackend) PlanProviderCredentialDelivery(context.Context, domain.HarnessBindingV2) (ports.ProviderCredentialDeliveryTemplateV1, error) {
	b.plans++
	return ports.ProviderCredentialDeliveryTemplateV1{}, ErrUnavailable
}
func (b *piOwnerForbiddenBackend) LoadProviderCredential(context.Context, ports.ProviderCredentialLocatorV1) (domain.ProviderCredentialBindingV1, bool, error) {
	b.loads++
	return domain.ProviderCredentialBindingV1{}, false, ErrUnavailable
}
func (b *piOwnerForbiddenBackend) ReadProviderCredentialSecret(context.Context, domain.ProviderCredentialBindingV1, int64) ([]byte, error) {
	b.reads++
	return nil, ErrUnavailable
}
func (b *piOwnerForbiddenBackend) RecoverProviderCredentialCandidate(context.Context, domain.ProviderCredentialBindingV1) (bool, error) {
	b.recovers++
	return false, ErrUnavailable
}

type piOwnerNoLaunch struct{ calls int }

func (*piOwnerNoLaunch) Profile() attachedworkerdaemon.IsolationProfile {
	return attachedworkerdaemon.IsolationProfile{Name: "SYNTHETIC-never-launched", FilesystemReadBoundary: true, FilesystemWriteBoundary: true, NetworkDenied: true, ProcessBoundary: true, DiskBytesBounded: true}
}
func (l *piOwnerNoLaunch) Prepare(context.Context, attachedworkerdaemon.LaunchSpec) (attachedworkerdaemon.IsolationBoundary, error) {
	l.calls++
	return nil, ErrUnavailable
}

func newPiOwnerFixture(t *testing.T) *piOwnerFixture {
	t.Helper()
	p := piDecodeProfile(t)
	p.Model.ModelVendorID, p.Model.ModelID, p.Model.Endpoint = "openai", "synthetic", "https://synthetic.invalid/responses"
	p.Model.ModelContextTokens, p.Model.MaxBilledInputTokens, p.Model.MaxOutputTokens = 65536, 8192, 100
	p.Limits.PreSendAllocationBytes, p.Limits.ProcessMemoryBytes, p.Limits.JSONNodes = 512<<20, 1<<30, 32<<20
	schema := []byte(attachedworkeroutput.StructuredFinalSchemaV1)
	sum := sha256.Sum256(schema)
	p.Sources.FinalSchemaDigest = hex.EncodeToString(sum[:])
	if err := p.Validate(); err != nil {
		t.Fatal("synthetic profile:", err)
	}
	service, _, jobs, blobs, request := canonicalFixture(t)
	// Replace both original canonical event/blob references before Service.Load.
	for i := range jobs.input.Events {
		e := &jobs.input.Events[i]
		size := image.Rect(0, 0, 64, 32)
		if i == 0 {
			size = image.Rect(0, 0, 3000, 1000)
		}
		var encoded bytes.Buffer
		if err := (&png.Encoder{CompressionLevel: png.BestSpeed}).Encode(&encoded, image.NewGray(size)); err != nil {
			t.Fatal(err)
		}
		ref := testBlob(e.TenantID, "image", encoded.Bytes())
		ref.Key = domain.SessionEventObjectPrefix(e.TenantID, e.SessionID, e.ID) + "image.png"
		body, err := json.Marshal(struct {
			Version     uint32                         `json:"version"`
			Text        string                         `json:"text"`
			Attachments []sessioncontext.AttachmentRef `json:"attachments"`
		}{1, fmt.Sprintf("turn %d", i+1), []sessioncontext.AttachmentRef{{Name: "image.png", MediaType: "image/png", Blob: ref}}})
		if err != nil {
			t.Fatal(err)
		}
		key := e.Payload.Key
		e.Payload = testBlob(e.TenantID, "payload", body)
		e.Payload.Key = key
		blobs.contents[key], blobs.contents[ref.Key] = body, bytes.Clone(encoded.Bytes())
	}
	// Match the real canonical ingress envelope, not canonicalFixture's
	// unrelated legacy alpha artifact: the root message is the exact trigger.
	trigger := &jobs.input.Events[len(jobs.input.Events)-1]
	trigger.RunID = &jobs.state.Job.RunID
	jobs.state.InputManifest.Artifacts = []domain.Artifact{{Name: "message.json", MediaType: "application/json", Blob: trigger.Payload}}
	contextDigest, err := domain.AttachedWorkerJobContextDigestV1(jobs.state.Job, jobs.state.InputManifest)
	if err != nil {
		t.Fatal(err)
	}
	request.Attempt.ContextDigest = decodeDigest(t, string(contextDigest))
	now := time.Now()
	request.Attempt.ExpiresAtUnixMicro = now.Add(time.Hour).UnixMicro()
	sealed, err := service.Load(t.Context(), []byte("test-connection-bearer"), request)
	if err != nil {
		t.Fatal("canonical service:", err)
	}
	t.Cleanup(func() { clearResult(&sealed) })
	b := &sealed.Job.HarnessBinding
	b.Profile, b.AgentLoopPolicy = domain.HarnessExecutionSingleTurnV2, nil
	b.Backend, err = p.Descriptor()
	if err != nil {
		t.Fatal(err)
	}
	b.Resource = domain.ProviderResourceBindingV1{Kind: domain.ProviderResourceAPIAccountV1, ResourceID: "synthetic-api", OwnerUserID: request.OwnerUserID, Revision: 1, CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 1}
	b.ModelVendorID, b.ModelID = p.Model.ModelVendorID, p.Model.ModelID
	b.AttachedPiAPIInvocationPolicyDigest = domain.AttachedPiAPIInvocationPolicyMappingDigestV1(fmt.Sprintf("%064x", 7))
	expires := now.Add(2 * time.Hour).UTC()
	b.EvidenceExpiresAt = &expires
	digest, err := domain.AttachedWorkerJobContextDigestV1(sealed.Job, sealed.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	sealed.AcceptedAttempt.ContextDigest, request.Attempt.ContextDigest = digest, decodeDigest(t, string(digest))
	payload, err := json.Marshal(sealed)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { clear(payload) })
	if len(payload) > 1<<20 {
		t.Fatal("fixture exceeds original full-seal limit")
	}
	phase := new(atomic.Int32)
	phaseHandler := new(atomic.Pointer[http.HandlerFunc])
	server, hc := piLoopback(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != PathV1 {
			phase.Add(1)
			if handler := phaseHandler.Load(); handler != nil {
				(*handler)(w, r)
				return
			}
			w.WriteHeader(http.StatusForbidden)
			return
		}
		piReplyHeaders(w)
		_, _ = w.Write(payload)
	}))
	// net/http.Transport.Clone initializes its source's next-protocol defaults.
	// Pin H1 BEFORE either factory clones this test client so constructing a
	// second same-origin source does not inherit a lazily installed h2 hook.
	transport := hc.Transport.(*http.Transport)
	transport.ForceAttemptHTTP2 = false
	transport.Protocols = &http.Protocols{}
	transport.Protocols.SetHTTP1(true)
	binding, bearer, _, _ := piClientFixture(t)
	binding.CapabilityDigest = sealed.AcceptedAttempt.CapabilityDigest
	binding.AuthenticationExpires = now.Add(2 * time.Hour).UTC()
	newSource := func() *SessionSourceFactory {
		t.Helper()
		f := piFactory(t, server.URL, hc, &exchangeFixture{port: &exchangePortFixture{}})
		if _, err := f.Open(binding, bearer); err != nil {
			t.Fatal("accepted session:", err)
		}
		return f
	}
	source := newSource()
	ctx, cancel := context.WithTimeout(t.Context(), time.Hour)
	t.Cleanup(cancel)
	a, request := piDecodeAcquisition(t, ctx, request, p)
	m, err := attachedworkerdaemontransport.NewPiBoundMaterializer(source, p, 1<<20, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	input, err := m.Materialize(ctx, request)
	if err != nil {
		t.Fatal("original materialization:", err)
	}
	t.Cleanup(input.VerifiedInput.Clear)
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	files, err := credentiallifecycle.NewPinnedSecretFileSystem(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := files.Close(); err != nil {
			t.Error(err)
		}
	})
	backend := &piOwnerForbiddenBackend{now: now}
	credentials, err := providercredential.NewInvocationService(providercredential.InvocationConfig{Clock: backend, IDs: backend, FileSystem: files, MaxSecretBytes: 128}, backend, backend, backend)
	if err != nil {
		t.Fatal(err)
	}
	launcher := &piOwnerNoLaunch{}
	supervisor, err := attachedworkerdaemon.NewSupervisor(attachedworkerdaemon.SupervisorConfig{ScratchRoot: root, Launcher: launcher, Timeout: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	model := []byte(`{"id":"synthetic","name":"structural fixture","api":"openai-responses","provider":"openai","baseUrl":"https://synthetic.invalid","reasoning":true,"input":["text","image"],"cost":{"input":0.125,"output":1.5,"cacheRead":0.0125,"cacheWrite":0},"contextWindow":65536,"maxTokens":100,"compat":{"supportsDeveloperRole":true,"supportsExplicitPromptCacheMode":true}}`)
	cfg := PiPreparedOwnerConfigV1{Request: pisdkmedia.RequestDataV1{Profile: &p, Model: &pisdkmedia.ModelDataV1{API: "openai-responses", Provider: "openai", ID: p.Model.ModelID, Endpoint: p.Model.Endpoint, Vision: true, Reasoning: true, SupportsDeveloperRole: true, SupportsExplicitPromptCacheMode: true}, SystemPrompt: "bounded synthetic test", MaxOutputTokens: 16, FinalSchema: schema, FinalSchemaSourceDigest: p.Sources.FinalSchemaDigest, FinalSchemaName: "result", CredentialMaxBytes: 128, Headers: []pisdkmedia.HeaderDataV1{{Name: "accept", Value: "application/json"}, {Name: "content-type", Value: "application/json"}, {Name: "user-agent", Value: "Pi/synthetic"}, {Name: "x-stainless-lang", Value: "js"}, {Name: "x-stainless-package-version", Value: "6.40.0"}, {Name: "x-stainless-os", Value: "MacOS"}, {Name: "x-stainless-arch", Value: "arm64"}, {Name: "x-stainless-runtime", Value: "node"}, {Name: "x-stainless-runtime-version", Value: "v24.19.0"}, {Name: "x-stainless-retry-count", Value: "0"}, {Name: "x-stainless-timeout", Value: "1"}}}, ModelJSON: model, TimeoutMilliseconds: 1000, CredentialFileName: "api.key", RetirementGrace: time.Minute / 2}
	owner, err := NewPiPreparedOwnerV1(source, supervisor, credentials, files, cfg)
	if err != nil {
		t.Fatal("original owner:", err)
	}
	t.Cleanup(func() {
		if r := owner.current; r != nil {
			c, _ := r.Context()
			if err := owner.RetireOriginalAPIStaging(c, r.identity, &r.data); err != nil {
				t.Error(err)
			}
		}
	})
	h := input.VerifiedInput.AcceptedAttempt
	id := attachedworkerdaemon.InvocationIdentity{TenantID: h.TenantID, OwnerUserID: h.OwnerUserID, WorkerID: h.WorkerID, RunID: h.RunID, AttemptID: h.AttemptID, LeaseID: h.LeaseID, FenceToken: h.LeaseGeneration}
	proc := attachedworkerdaemon.AttemptSpec{Executable: "/synthetic-never-executed", ExecutableDigest: sha256.Sum256([]byte("synthetic executable")), Arguments: []string{"fixed-entry"}}
	copy(proc.ExecutableDigest[:], decodeDigest(t, p.Sources.NativeArtifactDigest)) // structural match only, never an installed executable proof
	return &piOwnerFixture{owner: owner, inv: attachedworkerdaemon.Invocation{Identity: id, Process: proc, VerifiedInput: input.VerifiedInput, InputAcquisition: a}, ctx: ctx, cancel: cancel, ledger: a.OriginalLedger().(*codexexec.CanonicalMediaLedgerV1), files: files, backend: backend, launcher: launcher, phase: phase, phaseHandler: phaseHandler, newSource: newSource}
}

func (f *piOwnerFixture) noEffects(t *testing.T) {
	t.Helper()
	f.noCredentialEffects(t)
	if f.phase.Load() != 0 {
		t.Fatalf("forbidden phase effects: %d", f.phase.Load())
	}
}

func (f *piOwnerFixture) noCredentialEffects(t *testing.T) {
	t.Helper()
	b := f.backend
	if b.plans+b.ids+b.loads+b.reads+b.recovers+f.launcher.calls != 0 {
		t.Fatalf("forbidden effects: backend=%d/%d/%d/%d/%d launch=%d", b.plans, b.ids, b.loads, b.reads, b.recovers, f.launcher.calls)
	}
}

func TestPiPreparedOwnerOriginalPipelineMissingProducerAndRetirement(t *testing.T) {
	f := newPiOwnerFixture(t)
	before := f.ledger.Accounting()
	original := bytes.Clone(f.inv.VerifiedInput.Context)
	d, err := f.owner.PrepareOriginalAPIInvocation(f.ctx, f.inv.Identity, f.inv.VerifiedInput, f.inv.Process, f.inv.InputAcquisition)
	r := f.owner.current
	if !errors.Is(err, pisdkmedia.ErrMissingProducer) || d == nil || r == nil || r.projection == nil || r.file == nil || r.init == nil || r.execution != nil {
		if r == nil {
			t.Fatalf("incomplete actual pipeline: data=%t owned=false err=%v", d != nil, err)
		}
		t.Fatalf("incomplete actual pipeline: data=%t projection=%t file=%t init=%t execution=%t err=%v", d != nil, r.projection != nil, r.file != nil, r.init != nil, r.execution != nil, err)
	}
	if d.Ledger != f.ledger || d.Entry != f.inv.InputAcquisition.OriginalEntry() || r.original != f.ctx || !reflect.DeepEqual(d.ParentInput, f.inv.VerifiedInput) || !reflect.DeepEqual(d.ChildInput, f.inv.VerifiedInput) {
		t.Fatal("original ledger/entry/full input association lost")
	}
	if len(d.ParentInput.CanonicalContext.Attachments) != 2 || len(d.ParentInput.Artifacts) != 1 {
		t.Fatal("original image/artifact cohort changed")
	}
	for i, a := range d.ParentInput.CanonicalContext.Attachments {
		cfg, e := png.DecodeConfig(bytes.NewReader(a.Body))
		w, h := 64, 32
		if i == 0 {
			w, h = 3000, 1000
		}
		if e != nil || cfg.Width != w || cfg.Height != h || !bytes.Equal(a.Body, f.inv.VerifiedInput.CanonicalContext.Attachments[i].Body) {
			t.Fatalf("historical image %d: %dx%d err=%v", i, cfg.Width, cfg.Height, e)
		}
	}
	if !bytes.Contains(d.ParentInput.Context, []byte("turn 1")) || !bytes.Contains(d.ParentInput.Context, []byte("turn 2")) {
		t.Fatal("complete two-turn history lost")
	}
	args := r.init.EntryArguments()
	want := append([]string{"fixed-entry"}, args[:]...)
	if d.InitArguments != [10]string{} || d.HTTPInitArguments != args || !reflect.DeepEqual(d.ParentProcess.Arguments, want) || !reflect.DeepEqual(d.ChildProcess.Arguments, want) || len(d.ChildProcess.AdditionalReadRoots) != 1 {
		t.Fatal("exact original argv prefix/separate seventeen funded HTTP arguments lost")
	}
	d.ParentInput.Context[0] ^= 1
	d.ParentInput.Artifacts[0].Body[0] ^= 1
	d.ParentInput.CanonicalContext.Attachments[0].Body[0] ^= 1
	d.ParentRequest.Body[0] ^= 1
	d.ParentRequest.Model[0] ^= 1
	d.ParentRequest.Schema[0] ^= 1
	d.ParentRequest.Headers[0] ^= 1
	d.ParentProcess.Arguments[0] = "parent mutation"
	if !bytes.Equal(d.ChildInput.Context, original) || !bytes.Equal(f.inv.VerifiedInput.Context, original) || bytes.Equal(d.ParentRequest.Body, d.ChildRequest.Body) || bytes.Equal(d.ParentRequest.Model, d.ChildRequest.Model) || bytes.Equal(d.ParentRequest.Schema, d.ChildRequest.Schema) || bytes.Equal(d.ParentRequest.Headers, d.ChildRequest.Headers) || bytes.Equal(d.ParentInput.Artifacts[0].Body, d.ChildInput.Artifacts[0].Body) || bytes.Equal(d.ParentInput.CanonicalContext.Attachments[0].Body, d.ChildInput.CanonicalContext.Attachments[0].Body) || !bytes.Equal(f.inv.VerifiedInput.Artifacts[0].Body, d.ChildInput.Artifacts[0].Body) || !bytes.Equal(f.inv.VerifiedInput.CanonicalContext.Attachments[0].Body, d.ChildInput.CanonicalContext.Attachments[0].Body) || f.inv.Process.Arguments[0] != "fixed-entry" || d.ChildProcess.Arguments[0] != "fixed-entry" {
		t.Fatal("caller/parent/child payloads alias")
	}
	ownedBody := d.ParentRequest.Body
	if again, e := f.owner.PrepareOriginalAPIInvocation(f.ctx, f.inv.Identity, f.inv.VerifiedInput, f.inv.Process, f.inv.InputAcquisition); e == nil || again != nil {
		t.Fatal("active preparation reentered")
	}
	if err := f.ledger.Reserve(0, 123, 0); err != nil {
		t.Fatal(err)
	}
	c, _ := r.Context()
	if err := f.owner.RetireOriginalAPIStaging(c, f.inv.Identity, d); err != nil {
		t.Fatal("actual retirement:", err)
	}
	after := f.ledger.Accounting()
	if f.owner.current != nil || after.LiveBytes != before.LiveBytes+123 || after.CopyBytes <= before.CopyBytes || after.Nodes <= before.Nodes || !bytes.Equal(f.inv.VerifiedInput.Context, original) {
		t.Fatalf("wrong retirement accounting: before=%+v after=%+v", before, after)
	}
	if !bytes.Equal(ownedBody, make([]byte, len(ownedBody))) || r.projection != nil || r.init != nil || r.execution != nil || d.ParentInput != nil || len(d.ParentRequest.Body) != 0 {
		t.Fatal("retirement retained private buffers/DTO aliases")
	}
	if err := f.ledger.EndLive(123); err != nil {
		t.Fatal(err)
	}
	if _, err := f.files.ImmutableFilePathBound(f.ctx, "api.key"); err != nil {
		t.Fatal("attempt retirement closed shared namespace:", err)
	}
	if again, e := f.owner.PrepareOriginalAPIInvocation(f.ctx, f.inv.Identity, f.inv.VerifiedInput, f.inv.Process, f.inv.InputAcquisition); e == nil || again != nil {
		t.Fatal("retirement replenished original preparation slot")
	}
	f.noEffects(t)
}

func TestPiPreparedOwnerOriginalRunnerRetiresCompleteAndPartialPreparation(t *testing.T) {
	for _, mode := range []string{"complete", "invalid-model", "exhausted"} {
		t.Run(mode, func(t *testing.T) {
			f := newPiOwnerFixture(t)
			if mode == "invalid-model" {
				f.owner.config.ModelJSON = []byte(`{}`)
			}
			if mode == "exhausted" {
				remaining := f.ledger.Remaining().CopyBytes
				if remaining <= 10<<20 {
					t.Fatal("fixture lacks exhaustion control")
				}
				if err := f.ledger.Reserve(remaining-(10<<20), 0, 0); err != nil {
					t.Fatal(err)
				}
			}
			before := f.ledger.Accounting()
			runner, err := attachedworkerdaemon.NewPreparedAPIInvocationRunner(f.owner, f.owner.credentials, f.owner)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := runner.Close(t.Context()); err != nil {
					t.Error(err)
				}
			})
			result, err := runner.Run(f.ctx, f.inv)
			if !errors.Is(err, attachedworkerdaemon.ErrCredentialUnavailable) || result.FailureCode != "api_preparation_failed" || f.owner.current != nil {
				t.Fatalf("runner did not retire rejected preparation: result=%+v err=%v owned=%t", result, err, f.owner.current != nil)
			}
			after := f.ledger.Accounting()
			if after.LiveBytes != before.LiveBytes || after.CopyBytes <= before.CopyBytes || len(f.inv.VerifiedInput.Context) == 0 {
				t.Fatalf("runner retired caller or refunded/leaked stage: before=%+v after=%+v", before, after)
			}
			if _, err := f.files.ImmutableFilePathBound(f.ctx, "api.key"); err != nil {
				t.Fatal("runner closed runtime namespace:", err)
			}
			f.noEffects(t)
		})
	}
}

func TestPiPreparedOwnerRejectsForeignClosedAndCancelledSource(t *testing.T) {
	for _, mode := range []string{"foreign-session", "source-close", "cancel", "runner-close"} {
		t.Run(mode, func(t *testing.T) {
			f := newPiOwnerFixture(t)
			before := f.ledger.Accounting()
			switch mode {
			case "foreign-session":
				f.owner.source = f.newSource()
			case "source-close":
				if err := f.owner.source.Close(); err != nil {
					t.Fatal(err)
				}
			case "cancel":
				f.cancel()
			}
			if mode == "runner-close" {
				r, err := attachedworkerdaemon.NewPreparedAPIInvocationRunner(f.owner, f.owner.credentials, f.owner)
				if err != nil {
					t.Fatal(err)
				}
				if err := r.Close(t.Context()); err != nil {
					t.Fatal(err)
				}
				if _, err := r.Run(f.ctx, f.inv); err == nil {
					t.Fatal("closed runner prepared")
				}
			} else if data, err := f.owner.PrepareOriginalAPIInvocation(f.ctx, f.inv.Identity, f.inv.VerifiedInput, f.inv.Process, f.inv.InputAcquisition); err == nil || data != nil {
				t.Fatal("foreign/closed/cancelled source prepared")
			}
			if f.owner.current != nil || f.ledger.Accounting() != before || len(f.inv.VerifiedInput.Context) == 0 {
				t.Fatal("pre-acquisition denial allocated/refunded/cleared caller")
			}
			f.noEffects(t)
		})
	}
}

func TestPiPreparedOwnerRejectsAcquiredFirstOverCountsBeforeClone(t *testing.T) {
	for _, mode := range []string{"event-bodies", "attachments", "events", "artifacts", "snapshot-count"} {
		t.Run(mode, func(t *testing.T) {
			f := newPiOwnerFixture(t)
			input := f.inv.VerifiedInput
			proof := input.CanonicalContext
			// Retain the genuine acquired containers for cleanup. These mutations
			// are post-acquisition hostile DATA, not another materialization/ledger.
			bodies, attachments, events, artifacts, snapshot := proof.EventBodies, proof.Attachments, proof.Input.Events, input.Artifacts, proof.Input.Snapshot
			t.Cleanup(func() {
				proof.EventBodies, proof.Attachments, proof.Input.Events, input.Artifacts, proof.Input.Snapshot = bodies, attachments, events, artifacts, snapshot
			})
			switch mode {
			case "event-bodies":
				proof.EventBodies = make([][]byte, 4097)
			case "attachments":
				proof.Attachments = make([]sessioncontext.AttachmentPayload, 65)
			case "events":
				proof.Input.Events = make([]domain.SessionEvent, 4097)
			case "artifacts":
				input.Artifacts = make([]attachedworkerdaemon.VerifiedAttachedArtifactV1, 65)
			case "snapshot-count":
				proof.Input.Snapshot = &domain.SessionSnapshot{EventCount: 4097}
			}
			original := bytes.Clone(input.Context)
			before := f.ledger.Accounting()
			data, err := f.owner.PrepareOriginalAPIInvocation(f.ctx, f.inv.Identity, input, f.inv.Process, f.inv.InputAcquisition)
			r := f.owner.current
			if err == nil || data == nil || r == nil || r.inputs[0] != nil || r.inputs[1] != nil || r.projection != nil || r.file != nil || r.init != nil || r.execution != nil {
				t.Fatalf("first-over %s entered clone/compiler: data=%t owned=%t err=%v", mode, data != nil, r != nil, err)
			}
			after := f.ledger.Accounting()
			metadata := uint64(reflect.TypeOf(piOwnedPreparation{}).Size())
			if after.CopyBytes-before.CopyBytes != metadata || after.LiveBytes-before.LiveBytes != metadata || after.Nodes != before.Nodes {
				t.Fatalf("first-over %s traversed/funded hostile collection: before=%+v after=%+v metadata=%d", mode, before, after, metadata)
			}
			ctx, err := r.Context()
			if err != nil {
				t.Fatal(err)
			}
			if err := f.owner.RetireOriginalAPIStaging(ctx, f.inv.Identity, data); err != nil {
				t.Fatal(err)
			}
			retired := f.ledger.Accounting()
			if f.owner.current != nil || retired.LiveBytes != before.LiveBytes || retired.CopyBytes != after.CopyBytes || retired.Nodes != after.Nodes || !bytes.Equal(input.Context, original) || input.CanonicalContext != proof {
				t.Fatalf("first-over %s retirement cleared caller/refunded work/leaked stage: before=%+v retired=%+v", mode, before, retired)
			}
			f.noEffects(t)
		})
	}
}
