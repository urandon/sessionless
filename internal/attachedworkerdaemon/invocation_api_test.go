package attachedworkerdaemon

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/credentiallifecycle"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/pisdkphasewire"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/providercredential"
)

// All positive parent/phase data here is SYNTHETIC local orchestration, not a
// genuine Source/P/model/native-fit/S/hard-JS profile or serving permission.
type apiTestLedger struct {
	copy, live, nodes uint64
	deny              bool
}

func (l *apiTestLedger) Reserve(c, b, n uint64) error {
	if l.deny {
		return ErrInvocationInvalid
	}
	l.copy += c
	l.live += b
	l.nodes += n
	return nil
}
func (l *apiTestLedger) EndLive(n uint64) error {
	if n > l.live {
		return ErrInvocationInvalid
	}
	l.live -= n
	return nil
}

type apiTestRetirement struct {
	selected context.Context
	cancel   context.CancelFunc
	calls    int
	invalid  bool
}

func (r *apiTestRetirement) Context() (context.Context, error) {
	r.calls++
	if r.invalid {
		return nil, ErrInvocationInvalid
	}
	if r.selected == nil {
		r.selected, r.cancel = context.WithTimeout(context.Background(), time.Minute)
	}
	return r.selected, nil
}

type apiTestParent struct {
	profile      domain.AttachedPiSDKNativeProfileV1
	data         *PreparedAPIDataV1
	ledger       apiTestLedger
	retirement   apiTestRetirement
	order        []string
	prepareErr   error
	mutate       func(*PreparedAPIDataV1)
	phaseErr     pisdkphasewire.Phase
	phaseWindow  time.Duration
	phaseMutate  func(pisdkphasewire.Request, *pisdkphasewire.Reply)
	cleared      bool
	ownedInputs  []*VerifiedAttachedInputV1
	ownedBuffers [][]byte
}

func (p *apiTestParent) PrepareOriginalAPIInvocation(ctx context.Context, id InvocationIdentity, input *VerifiedAttachedInputV1, process AttemptSpec) (*PreparedAPIDataV1, error) {
	p.order = append(p.order, "prepare")
	if p.prepareErr != nil {
		return nil, p.prepareErr
	}
	// Explicit fixture reservation BEFORE either owned Clone or byte/slice
	// copy. It is not the production StageA algorithm/native-fit certificate.
	if err := p.ledger.Reserve(8*maxVerifiedAttachedInputBytes, 8*maxVerifiedAttachedInputBytes, 1000); err != nil {
		return nil, err
	}
	a, err := input.Clone()
	if err != nil {
		return nil, err
	}
	b, err := input.Clone()
	if err != nil {
		a.Clear()
		return nil, err
	}
	schema := []byte(`{"type":"object","properties":{"answer":{"type":"string"},"artifacts":{"type":"array","items":{"type":"object","properties":{"name":{"type":"string"},"content":{"type":"string"}},"required":["name","content"],"additionalProperties":false},"maxItems":2}},"required":["answer"],"additionalProperties":false}`)
	body := []byte(`{"model":"fixture","input":[{"role":"user","content":[{"type":"input_text","text":"complete synthetic task"}]}],"stream":true,"store":false,"tools":[],"text":{"format":{"type":"json_schema","strict":true}}}`)
	model := []byte(`{"api":"openai-responses","provider":"fixture-provider","id":"fixture","endpoint":"https://fixture.invalid/v1/responses","reasoning":true,"vision":true}`)
	headers := []byte(`{"accept":"text/event-stream","content-type":"application/json","user-agent":"fixture","x-stainless-lang":"js","x-stainless-package-version":"6.40.0","x-stainless-os":"fixture","x-stainless-arch":"fixture","x-stainless-runtime":"node","x-stainless-runtime-version":"fixture","x-stainless-retry-count":"0","x-stainless-timeout":"fixture"}`)
	request := PreparedAPIRequestV1{Profile: p.profile, Body: body, Model: model, Schema: schema, Headers: headers}
	copyRequest := request
	copyRequest.Body = bytes.Clone(body)
	copyRequest.Model = bytes.Clone(model)
	copyRequest.Schema = bytes.Clone(schema)
	copyRequest.Headers = bytes.Clone(headers)
	p.ownedInputs = []*VerifiedAttachedInputV1{a, b}
	p.ownedBuffers = [][]byte{body, model, schema, headers, copyRequest.Body, copyRequest.Model, copyRequest.Schema, copyRequest.Headers}
	parentProcess := cloneAttemptSpec(process)
	childProcess := cloneAttemptSpec(process)
	childProcess.AdditionalReadRoots = append(childProcess.AdditionalReadRoots, "")
	head := input.AcceptedAttempt
	deadline, _ := ctx.Deadline()
	entry := time.Now()
	p.data = &PreparedAPIDataV1{Issue: ports.ProviderCredentialIssueRequestV1{HarnessBinding: a.Job.HarnessBinding, WorkerID: string(id.WorkerID), LeaseID: id.LeaseID, LeaseFence: id.FenceToken, ExpiresAt: head.LeaseExpiresAt.Add(-time.Minute)},
		ParentInput: a, ChildInput: b, ParentRequest: request, ChildRequest: copyRequest, ParentProcess: parentProcess, ChildProcess: childProcess,
		Entry: entry, CallerDeadline: deadline, InvocationDeadline: entry.Add(a.Job.Limits.MaxRuntime), ProcessDeadline: entry.Add(a.Job.Limits.MaxRuntime), SafetySlack: time.Nanosecond, Ledger: &p.ledger, Retirement: &p.retirement, Custody: p,
		Authorization: ports.AttachedWorkerSealedInputAuthorization{TenantID: head.TenantID, OwnerUserID: head.OwnerUserID, WorkerID: head.WorkerID, ConnectionID: head.ConnectionID, EnrollmentGeneration: head.EnrollmentGeneration, ConnectionGeneration: head.ConnectionGeneration, RunID: head.RunID, AttemptID: head.AttemptID, AttemptSequence: 1, LeaseID: head.LeaseID, LeaseGeneration: head.LeaseGeneration, FenceToken: head.FenceToken, LeaseExpiresAtUnixMicro: head.LeaseExpiresAt.UnixMicro(), ContextDigest: head.ContextDigest, CapabilityDigest: head.CapabilityDigest, PolicyDigest: head.PolicyDigest, ExpectedAttemptRevision: head.Revision}}
	if p.mutate != nil {
		p.mutate(p.data)
	}
	return p.data, nil
}
func (p *apiTestParent) ObserveOriginalAPIPhase(_ context.Context, request pisdkphasewire.Request) (pisdkphasewire.Reply, error) {
	p.order = append(p.order, string(request.Phase))
	if p.phaseErr == request.Phase {
		return pisdkphasewire.Reply{Status: ports.AttachedWorkerExecutionDenied}, nil
	}
	checked := time.Now().UTC().Truncate(time.Microsecond)
	window := time.Duration(request.CallerDeadline.RemainingNanoseconds)
	cap := p.phaseWindow
	if cap == 0 {
		cap = 30 * time.Second // unchanged original fixture default
	}
	if window > cap {
		window = cap
	}
	reply := pisdkphasewire.Reply{Status: ports.AttachedWorkerExecutionApplied, AcceptedAttempt: p.data.ParentInput.AcceptedAttempt, AcceptedAttemptRevision: p.data.ParentInput.AcceptedAttempt.Revision, HarnessBindingDigest: request.ExpectedHarnessBindingDigest, AttachedPiAPIInvocationPolicyDigest: request.ExpectedAttachedPiAPIInvocationPolicyDigest, CheckedAt: checked, UseExpiresAt: checked.Add(window)}
	if p.phaseMutate != nil {
		p.phaseMutate(request, &reply)
	}
	return reply, nil
}
func (p *apiTestParent) Clear() {
	p.cleared = true
	if p.data == nil {
		return
	}
	for _, input := range p.ownedInputs {
		input.Clear()
	}
	for _, body := range p.ownedBuffers {
		clear(body)
	}
	clear(p.data.ParentProcess.Stdin)
	clear(p.data.ChildProcess.Stdin)
	_ = p.ledger.EndLive(p.ledger.live) // live-only relinquishment; cumulative stays
}

type apiTestLifecycle struct {
	parent                                            *apiTestParent
	fs                                                *credentiallifecycle.PinnedSecretFileSystem
	file                                              *credentiallifecycle.PinnedSecretFile
	handle                                            ports.ProviderInvocationCredentialV1
	plan                                              ports.ProviderCredentialMaterializationV1
	issueErr, materializeErr, finalizeErr, releaseErr error
	returnedHandleOnError                             bool
	repeatConsumer                                    bool
	mutateHandle                                      func(*ports.ProviderInvocationCredentialV1)
	mutatePlan                                        func(*ports.ProviderCredentialMaterializationV1)
	secret                                            bool
	active, consumed                                  bool
	callbackCount, finalizeCount, releaseCount        int
	releaseHandle                                     ports.ProviderInvocationCredentialV1
	releaseCtx                                        context.Context
}

func (l *apiTestLifecycle) IssueProviderCredential(_ context.Context, request ports.ProviderCredentialIssueRequestV1) (ports.ProviderInvocationCredentialV1, error) {
	l.parent.order = append(l.parent.order, "issue")
	if l.issueErr != nil && !l.returnedHandleOnError {
		return ports.ProviderInvocationCredentialV1{}, l.issueErr
	}
	b := request.HarnessBinding
	l.handle = ports.ProviderInvocationCredentialV1{HandleID: "synthetic-api-handle", TenantID: b.TenantID, OwnerUserID: b.OwnerUserID, RunID: b.RunID, AttemptID: b.AttemptID, WorkerID: request.WorkerID, LeaseID: request.LeaseID, LeaseFence: request.LeaseFence, ProviderResource: b.Resource, ExpiresAt: request.ExpiresAt}
	if l.mutateHandle != nil {
		l.mutateHandle(&l.handle)
	}
	return l.handle, l.issueErr
}
func (l *apiTestLifecycle) MaterializeProviderCredential(_ context.Context, _ ports.ProviderInvocationCredentialV1, consume ports.ProviderCredentialConsumerV1) error {
	l.parent.order = append(l.parent.order, "materialize")
	if l.materializeErr != nil {
		return l.materializeErr
	}
	if l.consumed {
		return ErrCredentialUnavailable
	}
	l.consumed = true
	file, err := l.fs.CreateImmutable("api.key", []byte("synthetic-public-api-marker"), 1024)
	if err != nil {
		return err
	}
	l.file = file
	l.plan = ports.ProviderCredentialMaterializationV1{Kind: domain.ProviderCredentialDeliveryFileV1, RootDir: file.RootDir, FilePath: file.FilePath}
	plan := l.plan
	if l.mutatePlan != nil {
		l.mutatePlan(&plan)
	}
	l.active = true
	defer func() { l.active = false; l.parent.order = append(l.parent.order, "unlocked") }()
	l.callbackCount++
	var secret []byte
	if l.secret {
		secret = []byte("synthetic-public-api-marker")
	}
	err = consume(plan, secret)
	if err == nil && l.repeatConsumer {
		l.callbackCount++
		err = consume(plan, secret)
	}
	return err
}
func (l *apiTestLifecycle) FinalizeProviderCredentialFile(ctx context.Context, handle ports.ProviderInvocationCredentialV1, plan ports.ProviderCredentialMaterializationV1) error {
	l.parent.order = append(l.parent.order, "finalize")
	l.finalizeCount++
	if l.active || handle != l.handle || plan != l.plan {
		return ErrInvocationInvalid
	}
	if l.finalizeErr != nil {
		return l.finalizeErr
	}
	return l.fs.VerifyImmutable(ctx, l.file, domain.FingerprintCredential([]byte("synthetic-public-api-marker")), 1024)
}
func (l *apiTestLifecycle) ReleaseProviderCredential(ctx context.Context, handle ports.ProviderInvocationCredentialV1) error {
	l.parent.order = append(l.parent.order, "release")
	l.releaseCount++
	l.releaseHandle = handle
	l.releaseCtx = ctx
	if ctx == nil {
		return ErrInvocationInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if l.active {
		return ErrInvocationInvalid
	}
	if l.releaseErr != nil {
		return l.releaseErr
	}
	if l.file != nil {
		return l.fs.Cleanup(l.file)
	}
	return nil
}

func apiOwnerProfile(t *testing.T, digest ExecutableDigest) domain.AttachedPiSDKNativeProfileV1 {
	t.Helper()
	p := domain.AttachedPiSDKNativeProfileV1{Version: 1, ProfileID: domain.AttachedPiSDKProfileIDV1, NativeProtocolVersion: domain.AttachedPiSDKNativeProtocolV1,
		Sources: domain.AttachedPiSDKNativeSourcesV1{PiSourceRevision: "6c4f360264397c59801f6da2bdac13e3b1fcbe91", OpenAISDKVersion: "6.40.0", OpenAISDKIntegrity: "sha512-MWtTjd/gQt4jpbji61NTgFWJLoY/PdRJ6wG9/ZDRMYNMlBKrCrSlkLI+KgHP1vR1qT6LKSAyAqIxno6lcK9JiA==", NodeVersion: "fixture"},
		Model:   domain.AttachedPiSDKNativeModelV1{ModelVendorID: "fixture-vendor", ModelID: "fixture", Endpoint: "https://fixture.invalid/v1/responses", ModelContextTokens: 8192, MaxBilledInputTokens: 2048, MaxOutputTokens: 1024}, Platform: domain.AttachedPiSDKNativePlatformV1{OS: "fixture-os", Architecture: "fixture-arch"},
		Limits:   domain.AttachedPiSDKNativeLimitsV1{CanonicalInputBytes: 1 << 20, NativeRequestBytes: 1 << 20, PreSendAllocationBytes: 32 << 20, RequestHeaderBytes: 4096, ResponseHeaderBytes: 4096, ResponseBodyBytes: 1 << 20, ResponseChunkBytes: 32 << 10, SSELineBytes: 32 << 10, SSEEventBytes: 128 << 10, SSEEvents: 512, JSONNodes: 4096, ResponseCopyWorkBytes: 16 << 20, ResponseAllocationBytes: 8 << 20, ProcessMemoryBytes: 256 << 20, ProcessDiskBytes: 1 << 20, ProcessFrameBytes: 1 << 20, ProcessStdoutBytes: 8 << 20, ProcessEvents: 2048, JSONDepth: 16, RawFinalBytes: 192 << 10, StructuredFinalBytes: 96 << 10, AnswerBytes: 16 << 10, ArtifactBytes: 16 << 10, ArtifactTotalBytes: 48 << 10},
		Behavior: domain.AttachedPiSDKNativeBehaviorV1{ProviderAPI: "openai-responses", ImageDerivation: "NoTransformOriginal", ImageDetail: "auto", Transport: "http-sse", LocalToolRegistry: "empty", MaxPhysicalSends: 1}}
	for _, target := range []any{&p.Sources, &p.Model, &p.Platform} {
		v := reflect.ValueOf(target).Elem()
		for i := 0; i < v.NumField(); i++ {
			if strings.HasSuffix(v.Type().Field(i).Name, "Digest") {
				h := sha256.Sum256([]byte(v.Type().Name() + v.Type().Field(i).Name))
				v.Field(i).SetString(hex.EncodeToString(h[:]))
			}
		}
	}
	p.Sources.NativeArtifactDigest = hex.EncodeToString(digest[:])
	schema := []byte(`{"type":"object","properties":{"answer":{"type":"string"},"artifacts":{"type":"array","items":{"type":"object","properties":{"name":{"type":"string"},"content":{"type":"string"}},"required":["name","content"],"additionalProperties":false},"maxItems":2}},"required":["answer"],"additionalProperties":false}`)
	h := sha256.Sum256(schema)
	p.Sources.FinalSchemaDigest = hex.EncodeToString(h[:])
	if err := p.Validate(); err != nil {
		t.Fatalf("synthetic profile: %v", err)
	}
	return p
}
func apiOwnerFixture(t *testing.T) (Invocation, *apiTestParent, *apiTestLifecycle) {
	t.Helper()
	inv := validPreparedCredentialInvocation(t)
	inv.Credential = nil
	inv.Process.Arguments = []string{"original-fixture-argument"}
	inv.Process.Stdin = []byte("original fixture stdin")
	p := apiOwnerProfile(t, inv.Process.ExecutableDigest)
	v := inv.VerifiedInput
	head := &v.AcceptedAttempt
	head.CreatedAt = head.CreatedAt.Truncate(time.Microsecond)
	head.UpdatedAt = head.UpdatedAt.Truncate(time.Microsecond)
	head.LeaseExpiresAt = head.LeaseExpiresAt.Truncate(time.Microsecond)
	d, err := p.Descriptor()
	if err != nil {
		t.Fatal(err)
	}
	v.Job.HarnessBinding.Backend = d
	v.Job.HarnessBinding.ModelVendorID = p.Model.ModelVendorID
	v.Job.HarnessBinding.ModelID = p.Model.ModelID
	v.Job.HarnessBinding.Resource = domain.ProviderResourceBindingV1{Kind: domain.ProviderResourceAPIAccountV1, ResourceID: "fixture-api", OwnerUserID: inv.Identity.OwnerUserID, Revision: 1, CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 1}
	v.Job.HarnessBinding.AttachedPiAPIInvocationPolicyDigest = domain.AttachedPiAPIInvocationPolicyMappingDigestV1(strings.Repeat("a", 64))
	expiry := head.LeaseExpiresAt
	v.Job.HarnessBinding.EvidenceExpiresAt = &expiry
	contextDigest, err := domain.AttachedWorkerJobContextDigestV1(v.Job, v.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	head.ContextDigest = contextDigest
	if err := v.ValidateFor(inv.Identity); err != nil {
		t.Fatalf("API input: %v", err)
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	fs, err := credentiallifecycle.NewPinnedSecretFileSystem(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := fs.Close(); err != nil {
			t.Errorf("owned descriptor Close: %v", err)
		}
	})
	parent := &apiTestParent{profile: p}
	life := &apiTestLifecycle{parent: parent, fs: fs}
	t.Cleanup(func() {
		if life.file != nil {
			_ = fs.Cleanup(life.file)
		}
		if parent.retirement.cancel != nil {
			parent.retirement.cancel()
		}
	})
	return inv, parent, life
}

func TestAPIOwnerSameRunnerOrderAndOwnedMutationIsolation(t *testing.T) {
	inv, parent, life := apiOwnerFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	var borrowed []byte
	runner, err := NewPreparedAPIInvocationRunner(preparedExecutorFunc(func(ctx context.Context, input PreparedInvocationV1) (AttemptResult, error) {
		parent.order = append(parent.order, "execute")
		if input.Credential != life.handle || input.Materialization != life.plan || input.Process.credentialWriteFile != "" || input.APIRequest == nil {
			t.Error("wrong API projection")
		}
		if !life.active || input.Process.AdditionalReadRoots[len(input.Process.AdditionalReadRoots)-1] != life.plan.RootDir {
			t.Error("not inside original read-only consumer")
		}
		if d, ok := ctx.Deadline(); !ok || !d.Before(parent.data.CallerDeadline) {
			t.Error("original F was not retained on executor context")
		}
		if err := authorizeSyntheticAPISend(input); err != nil {
			return AttemptResult{}, err
		}
		borrowed = input.APIRequest.Body
		input.APIRequest.Body[0] = '!'
		input.VerifiedInput.Context[0] = '!'
		input.Process.Arguments[0] = "child mutation"
		input.Materialization.FilePath = "/not-owned"
		input.Credential.OwnerUserID = "other"
		return AttemptResult{DescendantsReaped: true, BoundaryReleased: true, CleanupSucceeded: true}, nil
	}), life, parent)
	if err != nil {
		t.Fatal(err)
	}
	result, err := runner.Run(ctx, inv)
	want := []string{"prepare", "pre_issue", "issue", "materialize", "execute", "physical_send", "unlocked", "immutable_finalize", "finalize", "release"}
	if !reflect.DeepEqual(parent.order, want) {
		t.Errorf("order=%v want=%v", parent.order, want)
	}
	if !result.Succeeded(err) || !result.CredentialReleased || !parent.cleared || life.releaseHandle != life.handle {
		t.Errorf("local owner result=%+v err=%v", result, err)
	}
	if inv.Process.Arguments[0] != "original-fixture-argument" || string(inv.VerifiedInput.Context) != "private typed context" {
		t.Error("child changed original caller")
	}
	if !bytes.Equal(borrowed, make([]byte, len(borrowed))) {
		t.Error("borrowed body survived owner retirement")
	}
	if _, err := os.Lstat(life.plan.FilePath); !errors.Is(err, os.ErrNotExist) {
		t.Error("actual credential file survived successful Release")
	}
	if parent.ledger.copy == 0 || parent.ledger.nodes == 0 || parent.ledger.live != 0 {
		t.Error("shared cumulative debit/live ownership lost")
	}
}

func TestAPIOwnerReceiverCanonicalExpiry(t *testing.T) {
	inv, parent, life := apiOwnerFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	parent.mutate = func(d *PreparedAPIDataV1) {
		d.Issue.ExpiresAt = d.Issue.ExpiresAt.In(time.FixedZone("synthetic", 3600)).Add(time.Nanosecond)
	}
	// Exactly the unchanged real InvocationService Issue canonicalizer; this
	// fixture proves the orchestration boundary, not production Source authority.
	life.mutateHandle = func(h *ports.ProviderInvocationCredentialV1) {
		h.ExpiresAt = h.ExpiresAt.UTC().Truncate(time.Microsecond)
	}
	runner, err := NewPreparedAPIInvocationRunner(preparedExecutorFunc(func(_ context.Context, input PreparedInvocationV1) (AttemptResult, error) {
		if err := authorizeSyntheticAPISend(input); err != nil {
			return AttemptResult{}, err
		}
		return AttemptResult{DescendantsReaped: true, BoundaryReleased: true, CleanupSucceeded: true}, nil
	}), life, parent)
	if err != nil {
		t.Fatal(err)
	}
	result, err := runner.Run(ctx, inv)
	if !result.Succeeded(err) || life.finalizeCount != 1 || life.releaseHandle != life.handle {
		t.Errorf("canonical receiver expiry rejected: err=%v order=%v", err, parent.order)
	}
}

func TestAPIOwnerCompleteMutableOwnership(t *testing.T) {
	for _, name := range []string{"metadata pointer", "metadata slice", "empty metadata capacity", "cross request", "caller request", "empty process capacity"} {
		t.Run(name, func(t *testing.T) {
			inv, parent, life := apiOwnerFixture(t)
			if name == "empty process capacity" {
				inv.Process.Stdin = make([]byte, 0, 8)
			}
			if name == "metadata slice" {
				inv.VerifiedInput.Job.AllowedMCPServers = []string{"synthetic-opaque-mcp"}
			}
			if name == "empty metadata capacity" {
				inv.VerifiedInput.Job.AllowedMCPServers = make([]string, 0, 2)
			}
			digest, err := domain.AttachedWorkerJobContextDigestV1(inv.VerifiedInput.Job, inv.VerifiedInput.Manifest)
			if err != nil {
				t.Fatal(err)
			}
			inv.VerifiedInput.AcceptedAttempt.ContextDigest = digest
			parent.mutate = func(d *PreparedAPIDataV1) {
				switch name {
				case "metadata pointer":
					d.ChildInput.Job.HarnessBinding.EvidenceExpiresAt = inv.VerifiedInput.Job.HarnessBinding.EvidenceExpiresAt
				case "metadata slice":
					d.ChildInput.Job.AllowedMCPServers = inv.VerifiedInput.Job.AllowedMCPServers
				case "empty metadata capacity":
					d.ParentInput.Job.AllowedMCPServers = make([]string, 0, 2)
					d.ChildInput.Job.AllowedMCPServers = inv.VerifiedInput.Job.AllowedMCPServers
				case "cross request":
					d.ParentRequest.Model = bytes.Clone(d.ParentRequest.Body)
					d.ChildRequest.Model = d.ParentRequest.Body
				case "caller request":
					d.ParentRequest.Body = inv.VerifiedInput.Context
					d.ChildRequest.Body = bytes.Clone(inv.VerifiedInput.Context)
				case "empty process capacity":
					d.ParentProcess.Stdin = make([]byte, 0, 8)
					d.ChildProcess.Stdin = inv.Process.Stdin
				}
			}
			calls := 0
			runner, err := NewPreparedAPIInvocationRunner(preparedExecutorFunc(func(context.Context, PreparedInvocationV1) (AttemptResult, error) {
				calls++
				return AttemptResult{DescendantsReaped: true, BoundaryReleased: true, CleanupSucceeded: true}, nil
			}), life, parent)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
			defer cancel()
			_, err = runner.Run(ctx, inv)
			if err == nil || calls != 0 || life.handle.HandleID != "" {
				t.Errorf("mutable alias crossed Issue: err=%v order=%v", err, parent.order)
			}
			if name != "cross request" && parent.cleared {
				t.Error("caller-aliased custody was invoked")
			}
		})
	}
}

func TestAPIOwnerIndependentMetadataMutation(t *testing.T) {
	inv, parent, life := apiOwnerFixture(t)
	inv.VerifiedInput.Job.AllowedMCPServers = []string{"synthetic-opaque-mcp"}
	digest, err := domain.AttachedWorkerJobContextDigestV1(inv.VerifiedInput.Job, inv.VerifiedInput.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	inv.VerifiedInput.AcceptedAttempt.ContextDigest = digest
	originalExpiry := *inv.VerifiedInput.Job.HarnessBinding.EvidenceExpiresAt
	runner, err := NewPreparedAPIInvocationRunner(preparedExecutorFunc(func(_ context.Context, input PreparedInvocationV1) (AttemptResult, error) {
		if err := authorizeSyntheticAPISend(input); err != nil {
			return AttemptResult{}, err
		}
		*input.VerifiedInput.Job.HarnessBinding.EvidenceExpiresAt = time.Time{}
		input.VerifiedInput.Job.AllowedMCPServers[0] = "child-only"
		if *parent.data.ParentInput.Job.HarnessBinding.EvidenceExpiresAt != originalExpiry || parent.data.ParentInput.Job.AllowedMCPServers[0] != "synthetic-opaque-mcp" {
			t.Error("child mutated parent metadata")
		}
		return AttemptResult{DescendantsReaped: true, BoundaryReleased: true, CleanupSucceeded: true}, nil
	}), life, parent)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	result, err := runner.Run(ctx, inv)
	if !result.Succeeded(err) || *inv.VerifiedInput.Job.HarnessBinding.EvidenceExpiresAt != originalExpiry || inv.VerifiedInput.Job.AllowedMCPServers[0] != "synthetic-opaque-mcp" {
		t.Errorf("owned metadata control failed: %v", err)
	}
}

type apiReceiverClock struct{ now time.Time }

func (c apiReceiverClock) Now() time.Time { return c.now }

type apiReceiverIDs struct{}

func (apiReceiverIDs) NewID(context.Context, ports.IDKind) (string, error) {
	return "synthetic-real-receiver-handle", nil
}

type apiReceiverBindings struct {
	ports.ProviderCredentialBindingStore
	binding domain.ProviderCredentialBindingV1
}

func (b apiReceiverBindings) LoadProviderCredential(context.Context, ports.ProviderCredentialLocatorV1) (domain.ProviderCredentialBindingV1, bool, error) {
	return b.binding, true, nil
}

type apiReceiverSecrets struct {
	ports.ProviderCredentialSecretStore
}

func (apiReceiverSecrets) RecoverProviderCredentialCandidate(context.Context, domain.ProviderCredentialBindingV1) (bool, error) {
	return true, nil
}

type apiReceiverPlanner struct{}

func (apiReceiverPlanner) PlanProviderCredentialDelivery(context.Context, domain.HarnessBindingV2) (ports.ProviderCredentialDeliveryTemplateV1, error) {
	return ports.ProviderCredentialDeliveryTemplateV1{Kind: domain.ProviderCredentialDeliveryDirectV1}, nil
}

func TestAPIOwnerGenuineIssueCanonicalExpiry(t *testing.T) {
	// Genuine original Issue and Release, isolated direct delivery: no file
	// descriptor, secret read, external store/network or production Pi activation.
	now := time.Unix(100, 0).UTC()
	evidence := now.Add(time.Hour)
	resource := domain.ProviderResourceBindingV1{Kind: domain.ProviderResourceRouterAccountV1, ResourceID: "synthetic-router", OwnerUserID: "owner", Revision: 1, CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 1}
	binding := domain.HarnessBindingV2{Version: domain.HarnessBindingVersionV2, Profile: domain.HarnessExecutionSingleTurnV2, TenantID: "tenant", OwnerUserID: "owner", RunID: "run", AttemptID: "attempt", Resource: resource,
		Backend:       domain.HarnessBackendDescriptorV1{HarnessKind: domain.HarnessKindSessionlessV1, HarnessVersion: "1", BackendKind: domain.HarnessBackendDirectOpenRouterV1, ArtifactKind: domain.HarnessArtifactExecutableV1, ArtifactDigest: strings.Repeat("1", 64), NativeProtocolVersion: "openrouter-http.v1", BackendProfileDigest: strings.Repeat("2", 64), ProviderContractKind: domain.ProviderContractInvocationV1, CredentialDeliveryKind: domain.ProviderCredentialDeliveryDirectV1},
		ModelVendorID: "synthetic", ModelID: "synthetic", InputDataClass: domain.ProviderDataPublicV1, ProviderCatalogDigest: strings.Repeat("3", 64), ProviderRouteDigest: strings.Repeat("4", 64), PrivacyPolicyDigest: strings.Repeat("5", 64), CapabilityEvidenceDigest: strings.Repeat("6", 64), EffectivePolicyDigest: strings.Repeat("7", 64), ExecutionPlacementDigest: strings.Repeat("8", 64), EvidenceExpiresAt: &evidence}
	ref, err := domain.NewCredentialSecretRef("lockbox/synthetic/receiver")
	if err != nil {
		t.Fatal(err)
	}
	stored := domain.ProviderCredentialBindingV1{Version: domain.ProviderCredentialBindingVersionV1, TenantID: binding.TenantID, OwnerUserID: binding.OwnerUserID, ResourceKind: resource.Kind, ResourceID: resource.ResourceID, ResourceRevision: resource.Revision, CredentialGeneration: resource.CredentialGeneration, CandidateMutationID: "synthetic-mutation", State: domain.ProviderCredentialActiveV1, SecretRef: ref, SecretFingerprint: domain.FingerprintCredential([]byte("public-synthetic-marker")), UpdatedAt: now}
	service, err := providercredential.NewInvocationService(providercredential.InvocationConfig{Clock: apiReceiverClock{now}, IDs: apiReceiverIDs{}, MaxSecretBytes: 1024}, apiReceiverPlanner{}, apiReceiverBindings{binding: stored}, apiReceiverSecrets{})
	if err != nil {
		t.Fatal(err)
	}
	request := ports.ProviderCredentialIssueRequestV1{HarnessBinding: binding, WorkerID: "worker", LeaseID: "lease", LeaseFence: 1, ExpiresAt: now.Add(30*time.Minute + time.Nanosecond).In(time.FixedZone("synthetic", 3600))}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	t.Cleanup(cancel)
	handle, err := service.IssueProviderCredential(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := service.ReleaseProviderCredential(ctx, handle); err != nil {
			t.Error(err)
		}
	})
	if !apiHandleMatches(handle, request) {
		t.Error("genuine canonical receiver handle rejected")
	}
	for _, mutate := range []func(*ports.ProviderInvocationCredentialV1){func(h *ports.ProviderInvocationCredentialV1) { h.ExpiresAt = h.ExpiresAt.Add(time.Microsecond) }, func(h *ports.ProviderInvocationCredentialV1) { h.LeaseFence++ }, func(h *ports.ProviderInvocationCredentialV1) { h.ProviderResource.CredentialGeneration++ }} {
		foreign := handle
		mutate(&foreign)
		if apiHandleMatches(foreign, request) {
			t.Error("foreign canonical handle accepted")
		}
	}
}

func TestAPIOwnerBindsExecutableArtifact(t *testing.T) {
	inv, parent, life := apiOwnerFixture(t)
	inv.Process.ExecutableDigest[0] = 2 // alternate valid nonzero digest, not native evidence
	calls := 0
	runner, err := NewPreparedAPIInvocationRunner(preparedExecutorFunc(func(context.Context, PreparedInvocationV1) (AttemptResult, error) {
		calls++
		return AttemptResult{DescendantsReaped: true, BoundaryReleased: true, CleanupSucceeded: true}, nil
	}), life, parent)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	_, err = runner.Run(ctx, inv)
	if err == nil || calls != 0 || life.handle.HandleID != "" {
		t.Errorf("foreign artifact crossed Issue: err=%v order=%v", err, parent.order)
	}
}

func TestAPIOwnerPreIssueDenialsHaveZeroLifecycleEffects(t *testing.T) {
	for _, name := range []string{"both arms", "missing input", "preparation error", "unfunded", "missing ledger", "missing retirement", "wall entry", "wall caller", "original caller", "process bound", "foreign issue", "wrong expiry", "schema", "body alias", "child input alias", "process alias", "phase denied", "head revision"} {
		t.Run(name, func(t *testing.T) {
			inv, parent, life := apiOwnerFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
			defer cancel()
			if name == "wall caller" {
				cancel()
				ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(time.Hour).Round(0))
				defer cancel()
			}
			switch name {
			case "both arms":
				inv.Credential = validCredentialInvocation(t).Credential
			case "missing input":
				inv.VerifiedInput = nil
			case "preparation error":
				parent.prepareErr = ErrInvocationInvalid
			case "unfunded":
				parent.ledger.deny = true
			case "phase denied":
				parent.phaseErr = pisdkphasewire.PreIssue
			case "head revision":
				parent.phaseMutate = func(_ pisdkphasewire.Request, r *pisdkphasewire.Reply) { r.AcceptedAttemptRevision++ }
			default:
				parent.mutate = func(d *PreparedAPIDataV1) {
					switch name {
					case "missing ledger":
						d.Ledger = nil
					case "missing retirement":
						d.Retirement = nil
					case "wall entry":
						d.Entry = d.Entry.Round(0)
					case "original caller":
						d.CallerDeadline = d.CallerDeadline.Add(time.Second)
					case "process bound":
						d.ProcessDeadline = time.Time{}
					case "foreign issue":
						d.Issue.LeaseFence++
					case "wrong expiry":
						d.Issue.ExpiresAt = d.ParentInput.AcceptedAttempt.LeaseExpiresAt.Add(time.Second)
					case "schema":
						d.ChildRequest.Schema[0] = '!'
					case "body alias":
						d.ChildRequest.Body = d.ParentRequest.Body
					case "child input alias":
						d.ChildInput = d.ParentInput
					case "process alias":
						d.ChildProcess.Arguments = d.ParentProcess.Arguments
					}
				}
			}
			calls := 0
			runner, err := NewPreparedAPIInvocationRunner(preparedExecutorFunc(func(context.Context, PreparedInvocationV1) (AttemptResult, error) {
				calls++
				return AttemptResult{}, nil
			}), life, parent)
			if err != nil {
				t.Fatal(err)
			}
			result, runErr := runner.Run(ctx, inv)
			if runErr == nil || calls != 0 || life.handle.HandleID != "" || result.CredentialReleaseRequired || life.releaseCount != 0 {
				t.Errorf("denial %s crossed Issue/execution: result=%+v err=%v order=%v", name, result, runErr, parent.order)
			}
		})
	}
}

func TestAPIOwnerPostIssueFailuresAlwaysAttemptRelease(t *testing.T) {
	for _, name := range []string{"handle mismatch", "returned handle error", "materialize", "repeat consumer", "wrong plan", "plaintext", "delegate error", "nonquiescent released", "nonquiescent unreleased", "phase denied", "finalizer error", "release error", "model cancelled", "expired retirement", "invalid retirement"} {
		t.Run(name, func(t *testing.T) {
			inv, parent, life := apiOwnerFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
			defer cancel()
			process := AttemptResult{DescendantsReaped: true, BoundaryReleased: true, CleanupSucceeded: true}
			var delegateErr error
			switch name {
			case "handle mismatch":
				life.mutateHandle = func(h *ports.ProviderInvocationCredentialV1) { h.ProviderResource.CredentialGeneration++ }
			case "materialize":
				life.materializeErr = ErrInvocationInvalid
			case "returned handle error":
				life.issueErr = ErrInvocationInvalid
				life.returnedHandleOnError = true
			case "repeat consumer":
				life.repeatConsumer = true
			case "wrong plan":
				life.mutatePlan = func(p *ports.ProviderCredentialMaterializationV1) { p.Kind = domain.ProviderCredentialDeliveryDirectV1 }
			case "plaintext":
				life.secret = true
			case "delegate error":
				delegateErr = ErrInvocationInvalid
			case "nonquiescent released", "nonquiescent unreleased":
				process.DescendantsReaped = false
				if name == "nonquiescent unreleased" {
					life.releaseErr = ErrInvocationInvalid
				}
			case "phase denied":
				parent.phaseErr = pisdkphasewire.ImmutableFinalize
			case "finalizer error":
				life.finalizeErr = ErrInvocationInvalid
			case "release error":
				life.releaseErr = ErrInvocationInvalid
			case "expired retirement":
				parent.retirement.selected, parent.retirement.cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			case "invalid retirement":
				parent.retirement.invalid = true
			}
			runner, err := NewPreparedAPIInvocationRunner(preparedExecutorFunc(func(_ context.Context, input PreparedInvocationV1) (AttemptResult, error) {
				parent.order = append(parent.order, "execute")
				if err := authorizeSyntheticAPISend(input); err != nil {
					return AttemptResult{}, err
				}
				if name == "model cancelled" {
					cancel()
				}
				return process, delegateErr
			}), life, parent)
			if err != nil {
				t.Fatal(err)
			}
			result, runErr := runner.Run(ctx, inv)
			if runErr == nil || result.Succeeded(runErr) || life.releaseCount != 1 || life.releaseHandle != life.handle || !result.CredentialReleaseRequired {
				t.Errorf("post-Issue %s lost retirement: result=%+v err=%v order=%v", name, result, runErr, parent.order)
			}
			wantReleased := name != "nonquiescent unreleased" && name != "release error" && name != "expired retirement" && name != "invalid retirement"
			if result.CredentialReleased != wantReleased {
				t.Errorf("%s Release actual result=%t want=%t", name, result.CredentialReleased, wantReleased)
			}
			if strings.HasPrefix(name, "nonquiescent") && (life.finalizeCount != 0 || result.CleanupComplete()) {
				t.Error("nonquiescence became Finalize/cleanup")
			}
			if name == "model cancelled" && (life.releaseCtx == nil || life.releaseCtx.Err() != nil || !result.CredentialReleased) {
				t.Error("model cancellation skipped independent live cleanup")
			}
		})
	}
}

func TestAPIOwnerRetainsExistingRetirementCutoff(t *testing.T) {
	inv, parent, life := apiOwnerFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	var original context.Context
	runner, err := NewPreparedAPIInvocationRunner(preparedExecutorFunc(func(context.Context, PreparedInvocationV1) (AttemptResult, error) {
		original, _ = parent.retirement.Context() // actual fixture retirement starts here
		parent.retirement.cancel()                // applicable cleanup exhausts before Release
		return AttemptResult{DescendantsReaped: false}, nil
	}), life, parent)
	if err != nil {
		t.Fatal(err)
	}
	result, err := runner.Run(ctx, inv)
	if err == nil || result.CredentialReleased || life.releaseCtx == nil || life.releaseCtx != original || life.releaseCtx.Err() != context.Canceled || parent.retirement.calls != 2 || life.finalizeCount != 0 {
		t.Errorf("original retirement was renewed/lost: result=%+v err=%v", result, err)
	}
}

func TestAPIOwnerConstructorAndDataRedaction(t *testing.T) {
	var nilParent *apiTestParent
	var nilLife *apiTestLifecycle
	var nilExecutor preparedExecutorFunc
	for _, args := range []struct {
		execute PreparedProcessRunner
		life    ports.ProviderResourceCredentialLifecycleV1
		parent  PreparedAPIParentV1
	}{{nilExecutor, nilLife, nilParent}, {preparedExecutorFunc(nil), &apiTestLifecycle{}, &apiTestParent{}}} {
		if _, err := NewPreparedAPIInvocationRunner(args.execute, args.life, args.parent); err == nil {
			t.Error("typed-nil API dependency accepted")
		}
	}
	for _, v := range []any{PreparedAPIDataV1{ParentRequest: PreparedAPIRequestV1{Body: []byte("private request")}}, PreparedAPIRequestV1{Body: []byte("private request")}} {
		raw, err := json.Marshal(v)
		if err != nil || string(raw) != "{}" {
			t.Errorf("DATA wire exposure: %s err=%v", raw, err)
		}
		if strings.Contains(fmt.Sprintf("%#v", v), "private request") {
			t.Error("DATA formatter leaked body")
		}
	}
}

func TestAPIOwnerFailedIssueWithoutHandleDoesNotInventCleanup(t *testing.T) {
	inv, parent, life := apiOwnerFixture(t)
	life.issueErr = ErrCredentialUnavailable
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	runner, err := NewPreparedAPIInvocationRunner(preparedExecutorFunc(func(context.Context, PreparedInvocationV1) (AttemptResult, error) {
		t.Error("executor after failed Issue")
		return AttemptResult{}, nil
	}), life, parent)
	if err != nil {
		t.Fatal(err)
	}
	result, err := runner.Run(ctx, inv)
	if err == nil || result.CredentialReleased || result.CredentialReleaseRequired || life.releaseCount != 0 || !parent.cleared {
		t.Errorf("failed zero-handle Issue made cleanup evidence: %+v %v", result, err)
	}
}

func TestAPIOwnerFreshPhaseAfterExternalRevocation(t *testing.T) {
	inv, parent, life := apiOwnerFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	entered, resume, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(resume) }) }
	var result InvocationResult
	var runErr error
	runner, err := NewPreparedAPIInvocationRunner(preparedExecutorFunc(func(_ context.Context, input PreparedInvocationV1) (AttemptResult, error) {
		close(entered)
		<-resume
		if err := authorizeSyntheticAPISend(input); err != nil {
			return AttemptResult{}, err
		}
		return AttemptResult{DescendantsReaped: true, BoundaryReleased: true, CleanupSucceeded: true}, nil
	}), life, parent)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { unblock(); apiWait(t, done) })
	go func() { defer close(done); result, runErr = runner.Run(ctx, inv) }()
	apiWait(t, entered)
	// Synthetic external source G+1 denial can arrive while consume is held.
	// This is NOT a DB/currentness/Fence implementation or source proof.
	parent.phaseErr = pisdkphasewire.ImmutableFinalize
	if life.releaseCount != 0 || life.finalizeCount != 0 {
		t.Error("owner reentered held consumer")
	}
	unblock()
	apiWait(t, done)
	if runErr == nil || life.finalizeCount != 0 || life.releaseCount != 1 || !result.CredentialReleased {
		t.Errorf("fresh phase after external denial: %+v %v", result, runErr)
	}
}

func TestAPIOwnerWaitsForCallbackReturnBeforeFinalize(t *testing.T) {
	inv, parent, life := apiOwnerFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()
	entered, resume, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(resume) }) }
	var result InvocationResult
	var runErr error
	runner, err := NewPreparedAPIInvocationRunner(preparedExecutorFunc(func(_ context.Context, input PreparedInvocationV1) (AttemptResult, error) {
		close(entered)
		<-resume
		if err := authorizeSyntheticAPISend(input); err != nil {
			return AttemptResult{}, err
		}
		return AttemptResult{DescendantsReaped: true, BoundaryReleased: true, CleanupSucceeded: true}, nil
	}), life, parent)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { unblock(); apiWait(t, done) }) // register join before launch/TempDir cleanup
	go func() { defer close(done); result, runErr = runner.Run(ctx, inv) }()
	apiWait(t, entered)
	if life.finalizeCount != 0 || life.releaseCount != 0 {
		t.Error("lifecycle reentry while callback held")
	}
	unblock()
	apiWait(t, done)
	if !result.Succeeded(runErr) || life.finalizeCount != 1 {
		t.Errorf("after-unlock restored control: %+v %v", result, runErr)
	}
}
func apiWait(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case <-ch:
		return
	case <-timer.C:
		select {
		case <-ch:
			return
		default:
			t.Fatal("owned API fixture did not complete")
		}
	}
}

// Only synthetic SAME-parent orchestration, never a native physical send or
// genuine Source/fit proof. Real executors must recheck at the actual sink.
func authorizeSyntheticAPISend(input PreparedInvocationV1) error {
	if input.PhysicalSend == nil || input.APIRequest == nil || input.VerifiedInput == nil {
		return ErrInvocationInvalid
	}
	cutoff, err := input.PhysicalSend.Authorize(input.APIRequest.Body, input.VerifiedInput.Job.HarnessBinding.Backend.BackendProfileDigest)
	if err != nil || !time.Now().Before(cutoff) {
		return ErrCredentialUnavailable
	}
	return nil
}
