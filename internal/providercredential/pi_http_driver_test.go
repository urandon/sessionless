//go:build darwin || linux

package providercredential

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	daemon "gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/pisdkhttpwire"
	"gitcode.com/urandon/sessionless/internal/pisdkphasewire"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Test-only original parent/Source and Go child. The runner, credential service,
// immutable file/read/gate, fixed driver, Supervisor pipes and loopback TLS are
// real. This is NOT Pi SDK, actual Source authority, OCI/platform qualification,
// a full prospective native fit, a producer stamp or MVP demo evidence.
type piDriverParent struct {
	t           *testing.T
	service     *InvocationService
	request     daemon.PreparedAPIRequestV1
	issue       ports.ProviderCredentialIssueRequestV1
	ledger      *piFileLedger
	data        *daemon.PreparedAPIDataV1
	recipe      *PreparedPiFileUseV1
	plan        *daemon.PreparedPiHTTPStdioV1
	supervisor  *daemon.Supervisor
	retireCtx   context.Context
	bundle      piHTTPSRootArtifact
	preparePeer func(*piHTTPS)
	phases      []pisdkphasewire.Phase
	clearErr    error
	stage       string
	ownedLive   uint64
}

func (p *piDriverParent) Context() (context.Context, error) { return p.retireCtx, nil }
func (p *piDriverParent) RetireOriginalAPIStaging(ctx context.Context, _ daemon.InvocationIdentity, d *daemon.PreparedAPIDataV1) error {
	if ctx != p.retireCtx || d != p.data {
		return ErrInvalid
	}
	return nil
}
func (p *piDriverParent) Clear() {
	if p.recipe != nil {
		p.clearErr = p.recipe.Close()
	}
	if p.data != nil {
		p.data.ParentInput.Clear()
		p.data.ChildInput.Clear()
		for _, r := range []*daemon.PreparedAPIRequestV1{&p.data.ParentRequest, &p.data.ChildRequest} {
			clear(r.Body)
			clear(r.Model)
			clear(r.Schema)
			clear(r.Headers)
		}
	}
	if p.ownedLive != 0 {
		if err := p.ledger.EndLive(p.ownedLive); err != nil {
			p.clearErr = err
		} else {
			p.ownedLive = 0
		}
	}
}
func (p *piDriverParent) ObserveOriginalAPIPhase(ctx context.Context, r pisdkphasewire.Request) (pisdkphasewire.Reply, error) {
	p.phases = append(p.phases, r.Phase)
	checked := time.Now().UTC().Truncate(time.Microsecond)
	if ctx.Err() != nil {
		return pisdkphasewire.Reply{}, ctx.Err()
	}
	return pisdkphasewire.Reply{Status: ports.AttachedWorkerExecutionApplied, AcceptedAttempt: p.data.ParentInput.AcceptedAttempt,
		AcceptedAttemptRevision: p.data.ParentInput.AcceptedAttempt.Revision, HarnessBindingDigest: r.ExpectedHarnessBindingDigest,
		AttachedPiAPIInvocationPolicyDigest: r.ExpectedAttachedPiAPIInvocationPolicyDigest, CheckedAt: checked,
		UseExpiresAt: checked.Add(min(time.Hour, time.Duration(r.CallerDeadline.RemainingNanoseconds)))}, nil
}

func (p *piDriverParent) PrepareOriginalAPIInvocation(ctx context.Context, id daemon.InvocationIdentity, v *daemon.VerifiedAttachedInputV1, process daemon.AttemptSpec, _ daemon.PreparedInputAcquisitionV1) (*daemon.PreparedAPIDataV1, error) {
	// Synthetic fixture allowance only; not a full source/native fit certificate.
	if err := p.ledger.Reserve(32<<20, 16<<20, 4096); err != nil {
		return nil, err
	}
	p.ownedLive = 16 << 20
	a, err := v.Clone()
	if err != nil {
		return nil, err
	}
	b, err := v.Clone()
	if err != nil {
		return nil, err
	}
	clone := func(r daemon.PreparedAPIRequestV1) daemon.PreparedAPIRequestV1 {
		r.Body = bytes.Clone(r.Body)
		r.Model = bytes.Clone(r.Model)
		r.Schema = bytes.Clone(r.Schema)
		r.Headers = bytes.Clone(r.Headers)
		return r
	}
	parent, child := process, process
	parent.Arguments = append([]string(nil), process.Arguments...)
	child.Arguments = append([]string(nil), process.Arguments...)
	parent.Environment = append([]daemon.EnvironmentVariable(nil), process.Environment...)
	child.Environment = append([]daemon.EnvironmentVariable(nil), process.Environment...)
	child.AdditionalReadRoots = []string{""}
	l := p.request.Profile.Limits
	var args [10]string
	for i, n := range [10]uint64{l.ProcessFrameBytes - 17, min(l.NativeRequestBytes, l.ProcessFrameBytes-146), min(l.RawFinalBytes, l.StructuredFinalBytes, l.ProcessFrameBytes-17), l.ProcessFrameBytes, 700, 600, 500, l.JSONDepth, 300, uint64(len(p.request.Body))} {
		args[i] = strconv.FormatUint(n, 10)
	}
	parent.Arguments = append(parent.Arguments, args[:]...)
	child.Arguments = append(child.Arguments, args[:]...)
	head := a.AcceptedAttempt
	entry := time.Now()
	deadline, _ := ctx.Deadline()
	ownedIssue := p.issue
	ownedIssue.HarnessBinding = a.Job.HarnessBinding // original optional expiry pointer must not alias caller partition
	p.data = &daemon.PreparedAPIDataV1{Issue: ownedIssue, ParentInput: a, ChildInput: b, ParentRequest: clone(p.request), ChildRequest: clone(p.request), ParentProcess: parent, ChildProcess: child, InitArguments: args,
		Entry: entry, CallerDeadline: deadline, InvocationDeadline: entry.Add(v.Job.Limits.MaxRuntime), ProcessDeadline: entry.Add(v.Job.Limits.MaxRuntime), SafetySlack: time.Nanosecond, Ledger: p.ledger, Retirement: p, Custody: p,
		Authorization: ports.AttachedWorkerSealedInputAuthorization{TenantID: head.TenantID, OwnerUserID: head.OwnerUserID, WorkerID: head.WorkerID, ConnectionID: head.ConnectionID,
			EnrollmentGeneration: head.EnrollmentGeneration, ConnectionGeneration: head.ConnectionGeneration, RunID: head.RunID, AttemptID: head.AttemptID, AttemptSequence: 1, LeaseID: head.LeaseID,
			LeaseGeneration: head.LeaseGeneration, FenceToken: head.FenceToken, LeaseExpiresAtUnixMicro: head.LeaseExpiresAt.UnixMicro(), ContextDigest: head.ContextDigest, CapabilityDigest: head.CapabilityDigest, PolicyDigest: head.PolicyDigest, ExpectedAttemptRevision: head.Revision}}
	p.stage = "file"
	f, err := p.service.PrepareImmutableAPIFileV1(ctx, p.issue, "api-key", 64)
	if err != nil {
		return p.data, err
	}
	p.recipe, err = f.PreparePiFileUseV1(p.ledger, p.data.ParentRequest.Body, p.issue.HarnessBinding.Backend.BackendProfileDigest)
	if err != nil {
		return p.data, err
	}
	p.stage = "auth"
	if err = p.recipe.PrepareRequestAuthenticationV1(p.data.ParentRequest); err != nil {
		return p.data, err
	}
	p.stage = "receive"
	if err = p.recipe.PrepareHTTPReceiveV1(l.ResponseHeaderBytes, l.ResponseBodyBytes); err != nil {
		return p.data, err
	}
	p.stage = "scan"
	if err = p.recipe.prepareResponseScan(); err != nil {
		return p.data, err
	}
	p.stage = "https"
	if err = p.recipe.prepareHTTPS(p.bundle); err != nil {
		return p.data, err
	}
	p.preparePeer(p.recipe.state.network.Load()) // private fake DNS/local TLS ONLY
	p.stage = "plan"
	p.plan, err = p.recipe.PrepareHTTPIOV1(p.supervisor, p.data.ParentRequest)
	if err == nil {
		p.stage = "prepared"
	}
	return p.data, err
}

type piDriverLauncher struct{}

func (piDriverLauncher) Profile() daemon.IsolationProfile {
	return daemon.IsolationProfile{Name: "synthetic-driver-fixture", FilesystemReadBoundary: true, FilesystemWriteBoundary: true, NetworkDenied: true, ProcessBoundary: true, DiskBytesBounded: true}
}
func (piDriverLauncher) Prepare(_ context.Context, s daemon.LaunchSpec) (daemon.IsolationBoundary, error) {
	c := exec.Command(s.Executable, s.Arguments...)
	c.Dir = s.Directory
	c.Env = s.Environment
	return &piDriverBoundary{cmd: c, workload: daemon.WorkloadAttestation{Executable: s.Executable, ExecutableDigest: s.ExecutableDigest, Arguments: append([]string(nil), s.Arguments...)}}, nil
}

type piDriverBoundary struct {
	cmd      *exec.Cmd
	workload daemon.WorkloadAttestation
}

func (b *piDriverBoundary) Command() *exec.Cmd                           { return b.cmd }
func (b *piDriverBoundary) AttestedWorkload() daemon.WorkloadAttestation { return b.workload }
func (b *piDriverBoundary) GracefulStop(context.Context) error {
	if b.cmd.Process == nil {
		return nil
	}
	err := b.cmd.Process.Signal(syscall.SIGTERM)
	if errors.Is(err, os.ErrProcessDone) {
		return nil
	}
	return err
}
func (b *piDriverBoundary) ForceStop(context.Context) error {
	if b.cmd.Process == nil {
		return nil
	}
	err := b.cmd.Process.Kill()
	if errors.Is(err, os.ErrProcessDone) {
		return nil
	}
	return err
}
func (b *piDriverBoundary) Alive(context.Context) (bool, error) { return false, nil } // helper has no descendants; NOT real boundary proof
func (b *piDriverBoundary) Release(context.Context) error       { return nil }

type piDriverExecutor func(context.Context, daemon.PreparedInvocationV1) (daemon.AttemptResult, error)

func (f piDriverExecutor) RunPrepared(ctx context.Context, in daemon.PreparedInvocationV1) (daemon.AttemptResult, error) {
	return f(ctx, in)
}

type piDriverInit struct {
	File, Endpoint string
	Headers, Body  []byte
	Expected       string
}

func piDriverFixture(t *testing.T) (*piDriverParent, daemon.Invocation, *fakeSecretStore) {
	t.Helper()
	s, _, secrets, issue, clock := immutableAPIInvocationFixture(t, domain.ProviderCredentialDeliveryFileV1)
	s.maxBytes = 64
	now := time.Now().UTC().Truncate(time.Microsecond)
	clock.Set(now)
	expires := now.Add(2 * time.Hour)
	issue.ExpiresAt = now.Add(time.Hour)
	issue.HarnessBinding.EvidenceExpiresAt = &expires
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	executable, err = filepath.EvalSymlinks(executable)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := daemon.DigestExecutable(executable)
	if err != nil {
		t.Fatal(err)
	}
	profile := piAuthProfileFixture(t)
	profile.Sources.NativeArtifactDigest = hex.EncodeToString(digest[:])
	schema := []byte(`{"type":"object"}`)
	schemaHash := sha256.Sum256(schema)
	profile.Sources.FinalSchemaDigest = hex.EncodeToString(schemaHash[:])
	placement := domain.ExecutionPlacementV2{Version: 2, Kind: domain.ExecutionPlacementAttachedWorker, FallbackPolicy: domain.ExecutionFallbackDenied, OwnerUserID: issue.HarnessBinding.OwnerUserID, WorkerID: domain.AttachedWorkerID(issue.WorkerID), CapabilityDigest: domain.AttachedWorkerCapabilityDigest(strings.Repeat("8", 64)), PolicyDigest: domain.AttachedWorkerPolicyDigest(strings.Repeat("9", 64))}
	pd, err := domain.ExecutionPlacementDigest(placement)
	if err != nil {
		t.Fatal(err)
	}
	issue.HarnessBinding.ExecutionPlacementDigest = string(pd)
	issue.HarnessBinding.AttachedPiAPIInvocationPolicyDigest = domain.AttachedPiAPIInvocationPolicyMappingDigestV1(strings.Repeat("b", 64))
	desc, err := profile.Descriptor()
	if err != nil {
		t.Fatal(err)
	}
	issue.HarnessBinding.Backend = desc
	id := daemon.InvocationIdentity{TenantID: issue.HarnessBinding.TenantID, OwnerUserID: issue.HarnessBinding.OwnerUserID, WorkerID: domain.AttachedWorkerID(issue.WorkerID), RunID: issue.HarnessBinding.RunID, AttemptID: issue.HarnessBinding.AttemptID, LeaseID: issue.LeaseID, FenceToken: issue.LeaseFence}
	contextBody := []byte("synthetic full history")
	ch := sha256.Sum256(contextBody)
	job := domain.WorkerJob{TenantID: id.TenantID, RunID: id.RunID, AttemptID: id.AttemptID, SessionID: "session-a", TriggerEventID: "event-a", ReservationID: "reservation-a", InputManifestID: "manifest-a", CredentialOwnerUserID: id.OwnerUserID, ExecutionPlacementV2: placement, HarnessBinding: issue.HarnessBinding,
		ContextSnapshot: domain.BlobRef{TenantID: id.TenantID, Key: "tenants/tenant-a/context", Size: int64(len(contextBody)), SHA256: hex.EncodeToString(ch[:])}, Limits: domain.ProductLimits{MaxTenantQueueDepth: 1, MaxActiveRuns: 1, MaxRuntime: time.Hour, MaxTurns: 1, MaxInputBytes: 1 << 20, MaxContextBytes: 1 << 20, MaxContextEvents: 10, MaxArtifacts: 10, MaxToolEvents: 10, MaxToolEventBytes: 4096}}
	manifest := domain.ArtifactManifest{ID: job.InputManifestID, TenantID: id.TenantID, RunID: id.RunID, CreatedAt: now}
	cd, err := domain.AttachedWorkerJobContextDigestV1(job, manifest)
	if err != nil {
		t.Fatal(err)
	}
	fence, err := domain.NewAttachedWorkerFenceTokenV1(id.TenantID, id.OwnerUserID, id.WorkerID, id.RunID, id.AttemptID, id.LeaseID, id.FenceToken)
	if err != nil {
		t.Fatal(err)
	}
	v := &daemon.VerifiedAttachedInputV1{AcceptedAttempt: domain.AttachedWorkerAttemptV1{Version: 1, TenantID: id.TenantID, OwnerUserID: id.OwnerUserID, WorkerID: id.WorkerID, ConnectionID: "connection-a", RunID: id.RunID, AttemptID: id.AttemptID, ReservationID: job.ReservationID, LeaseID: id.LeaseID, LeaseGeneration: id.FenceToken, FenceToken: fence, EnrollmentGeneration: 1, ConnectionGeneration: 1, ContextDigest: cd, CapabilityDigest: placement.CapabilityDigest, PolicyDigest: placement.PolicyDigest, State: domain.AttachedWorkerAttemptClaimed, PlatformAttemptSequence: 1, WorkerAttemptSequence: 1, CreatedAt: now, UpdatedAt: now, LeaseExpiresAt: expires, Revision: 1}, Job: job, Manifest: manifest, Context: contextBody}
	if err := v.ValidateFor(id); err != nil {
		t.Fatal("admitted fixture", err)
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	sup, err := daemon.NewSupervisor(daemon.SupervisorConfig{ScratchRoot: root, Launcher: piDriverLauncher{}, Timeout: time.Hour, TerminationGrace: 5 * time.Second, MaxStdoutBytes: 8 << 20, AllowedEnvironmentNames: []string{"SESSIONLESS_PI_DRIVER_HELPER"}, AllowedReadRoots: []string{s.root}})
	if err != nil {
		t.Fatal(err)
	}
	retire, cancel := context.WithTimeout(t.Context(), time.Hour)
	t.Cleanup(cancel)
	p := &piDriverParent{t: t, service: s, issue: issue, ledger: newPiFileLedger(), supervisor: sup, retireCtx: retire,
		request: daemon.PreparedAPIRequestV1{Profile: profile, Body: []byte(piPolicyFixtureRequest), Model: []byte(`{"api":"openai-responses"}`), Schema: schema, Headers: []byte(piAuthFixtureHeaders)}}
	return p, daemon.Invocation{Identity: id, VerifiedInput: v, Process: daemon.AttemptSpec{Executable: executable, ExecutableDigest: digest, Arguments: []string{"-test.run=^TestPiHTTPDriverHelperProcess$"}, Environment: []daemon.EnvironmentVariable{{Name: "SESSIONLESS_PI_DRIVER_HELPER", Value: "1"}}}}, secrets
}

func TestPiHTTPDriverOriginalRunnerFileTLSAndSupervisor(t *testing.T) {
	for _, mode := range []string{"safe original bytes", "UTF8 split across parts", "normal exit before retirement", "close at Start", "close at Part", "close at End", "unsafe secret", "tool response", "split secret", "failed MAC"} {
		t.Run(mode, func(t *testing.T) {
			p, inv, secrets := piDriverFixture(t)
			bundle, cert := piTLSFixture(t)
			p.bundle = bundle
			body := ": Ω original comment\n" + piPolicyBody(t, piPolicyTerminal())
			if mode == "unsafe secret" {
				body = ": synthetic-immutable-api-marker\n" + piPolicyBody(t, piPolicyTerminal())
			}
			if mode == "UTF8 split across parts" {
				body = strings.Repeat(": x\n", 8191) + ":: Ω\n" + piPolicyBody(t, piPolicyTerminal())
				if body[32767] != 0xce || body[32768] != 0xa9 {
					t.Fatal("fixture did not split original UTF-8 bytes at canonical C")
				}
			}
			if mode == "tool response" {
				r := piPolicyResponse("completed")
				r["output"] = []any{map[string]any{"type": "function_call", "id": "tool", "call_id": "call", "name": "forbidden", "arguments": "{}", "status": "completed"}}
				body = piPolicyBody(t, piPolicyLifecycle("response.completed", r))
				q, x := piPolicyParsed(t, piPolicyFixtureRequest, body)
				if err := q.checkResponsePolicy(x); err == nil {
					t.Fatal("tool fixture did not fail closed policy")
				}
			}
			if mode == "split secret" {
				a := map[string]any{"type": "response.output_text.delta", "sequence_number": 0, "item_id": "same", "output_index": 0, "content_index": 0, "delta": "synthetic-immutable-"}
				b := map[string]any{"type": "response.refusal.delta", "sequence_number": 0, "item_id": "same", "output_index": 0, "content_index": 0, "delta": "api-marker"}
				body = piPolicyBody(t, piPolicyAdded("message", "same", 0), a, b, piPolicyTerminal())
				q, x := piPolicyParsed(t, piPolicyFixtureRequest, body)
				if strings.Contains(body, "synthetic-immutable-api-marker") || q.checkResponsePolicy(x) != nil {
					t.Fatal("split-secret fixture must pass raw and closed-shape policy")
				}
			}
			noStart := mode == "unsafe secret" || mode == "tool response" || mode == "split secret" || mode == "failed MAC"
			var peer *piTLSPeer
			hookReached := false
			p.preparePeer = func(network *piHTTPS) {
				peer = piTLSPeerStart(t, network, cert, []string{"http/1.1"}, piHTTPFixture(body, true))
				p.recipe.state.afterReplayWrite = func(kind pisdkhttpwire.Kind, source context.Context) {
					if mode == "normal exit before retirement" && kind == pisdkhttpwire.ResponseEnd {
						// This hook runs AFTER Write(End) returned. It proves Source
						// withdrawal before finishReplay, not a held End write return.
						piTLSWait(t, source.Done())
						hookReached = true
						return
					}
					want := pisdkhttpwire.Kind(0)
					switch mode {
					case "close at Start":
						want = pisdkhttpwire.ResponseStart
					case "close at Part":
						want = pisdkhttpwire.ResponsePart
					case "close at End":
						want = pisdkhttpwire.ResponseEnd
					}
					if kind != want || hookReached {
						return
					}
					hookReached = true
					before := p.ledger.live
					bodyAlias := p.recipe.state.http.entity
					joined := make(chan error, 1)
					go func() { joined <- p.recipe.Close() }()
					if err := <-joined; !errors.Is(err, ErrConsumed) || p.ledger.live != before || p.recipe.state.responseScan.erased.Load() || !bytes.Contains(bodyAlias, []byte("response.completed")) {
						t.Error("concurrent Close cleared/refunded borrowed response", err)
					}
				}
			}
			entered := false
			var actual daemon.AttemptResult
			var keyAlias []byte
			runner, err := daemon.NewPreparedAPIInvocationRunner(piDriverExecutor(func(ctx context.Context, in daemon.PreparedInvocationV1) (daemon.AttemptResult, error) {
				entered = true
				if err := p.recipe.BindMaterialized(ctx, in.Credential, in.Materialization, in.PhysicalSend); err != nil {
					return daemon.AttemptResult{}, err
				}
				use := p.recipe.state.use.Load()
				keyAlias = use.secret
				a := p.recipe.state.auth
				expected := body
				if noStart {
					expected = ""
				}
				payload, err := json.Marshal(piDriverInit{File: in.Materialization.FilePath, Endpoint: a.endpoint, Headers: a.headers, Body: in.APIRequest.Body, Expected: expected})
				if err != nil {
					return daemon.AttemptResult{}, err
				}
				in.Process.Stdin = payload
				if mode == "failed MAC" {
					in.Process.Environment[0].Value = "bad-mac"
				}
				beforeCopy, beforeNodes := p.ledger.copy, p.ledger.nodes
				actual, err = p.supervisor.RunPreparedPiHTTP(ctx, in, p.plan)
				if p.ledger.copy != beforeCopy || p.ledger.nodes != beforeNodes {
					t.Error("post-Issue replenished original ledger")
				}
				if !bytes.Equal(keyAlias, []byte("synthetic-immutable-api-marker")) {
					t.Error("driver zeroed original file owner's key before callback joined")
				}
				if _, ok := actual.PiStdioObservationV1(); ok {
					t.Error("mechanics minted producer stamp")
				}
				return actual, err
			}), p.service, p)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Hour)
			t.Cleanup(cancel)
			result, err := runner.Run(ctx, inv)
			if !entered || p.recipe == nil || p.plan == nil {
				t.Fatalf("original callback not reached: stage=%s phases=%v err=%v", p.stage, p.phases, err)
			}
			if mode == "safe original bytes" || mode == "UTF8 split across parts" || mode == "normal exit before retirement" {
				if err != nil || !result.Succeeded(err) || actual.ExitCode != 0 || !actual.CleanupSucceeded || actual.StderrBytes != len("START") {
					t.Fatalf("joined original replay failed result=%+v process=%+v err=%v", result, actual, err)
				}
				piTLSWait(t, peer.done)
				if peer.dials.Load() != 1 || peer.err != nil {
					t.Fatal("fixed one-connection receive", peer.dials.Load(), peer.err)
				}
			} else {
				if err == nil {
					t.Fatal("unsafe/cancelled run succeeded")
				}
				if noStart && actual.StderrBytes != 0 {
					t.Fatal("unsafe response/MAC emitted Start", err, actual.StderrBytes)
				}
			}
			if (strings.HasPrefix(mode, "close at ") || mode == "normal exit before retirement") && !hookReached {
				t.Fatal("causal replay/exit boundary not reached")
			}
			if mode == "failed MAC" && peer.dials.Load() != 0 {
				t.Fatal("bad MAC reached socket")
			}
			if p.clearErr != nil || secrets.readCalls != 1 || p.recipe.state.httpBorrow.Load() {
				t.Fatal("original owner did not retire", p.clearErr, secrets.readCalls)
			}
			if !bytes.Equal(keyAlias, make([]byte, len(keyAlias))) {
				t.Error("original callback did not zero its sole secret read")
			}
		})
	}
}

func TestPiHTTPDriverHelperProcess(t *testing.T) {
	mode := os.Getenv("SESSIONLESS_PI_DRIVER_HELPER")
	if mode == "" {
		return
	}
	l := pisdkhttpwire.Limits{FrameBytes: 512 << 10, InitializeBytes: (512 << 10) - 113, RequestBytes: (512 << 10) - 85 - 4096, FinalBytes: 96 << 10, RequestHeaderBytes: 4096, ResponseHeaderBytes: 4096, ResponseBytes: 1 << 20, ResponseChunkBytes: 32 << 10, ParentBytes: 4 << 20, ChildBytes: 8 << 20, Frames: 2048}
	read := func() pisdkhttpwire.Frame {
		var h [17]byte
		if _, err := io.ReadFull(os.Stdin, h[:]); err != nil {
			os.Exit(21)
		}
		header, err := pisdkhttpwire.InspectHeader(h[:], pisdkhttpwire.ParentToChild, l)
		if err != nil {
			os.Exit(22)
		}
		raw := make([]byte, int(header.TotalBytes))
		copy(raw, h[:])
		if _, err := io.ReadFull(os.Stdin, raw[17:]); err != nil {
			os.Exit(23)
		}
		f, err := pisdkhttpwire.DecodeView(raw, pisdkhttpwire.ParentToChild, l)
		if err != nil {
			os.Exit(24)
		}
		return f
	}
	write := func(f pisdkhttpwire.Frame) {
		raw := make([]byte, int(l.FrameBytes))
		n, err := pisdkhttpwire.EncodeInto(raw, f, l)
		if err != nil {
			os.Exit(25)
		}
		if _, err := os.Stdout.Write(raw[:n]); err != nil {
			os.Exit(26)
		}
	}
	init := read()
	var p piDriverInit
	if json.Unmarshal(init.Body, &p) != nil {
		os.Exit(27)
	}
	key, err := os.ReadFile(p.File)
	if err != nil {
		os.Exit(28)
	}
	f := pisdkhttpwire.Frame{Kind: pisdkhttpwire.FetchRequest, Profile: init.Profile, Headers: p.Headers, Body: p.Body}
	f.MAC = piRequestMAC(key, init.Nonce, init.Binding, init.Profile, p.Endpoint, f.Headers, f.Body)
	clear(key)
	if mode == "bad-mac" {
		f.MAC[0] ^= 1
	}
	write(f)
	start := read()
	if start.Kind != pisdkhttpwire.ResponseStart {
		os.Exit(29)
	}
	_, _ = io.WriteString(os.Stderr, "START")
	var received []byte
	for i := uint32(0); i < start.Count; i++ {
		write(pisdkhttpwire.Frame{Kind: pisdkhttpwire.Pull, Index: i})
		part := read()
		if part.Kind != pisdkhttpwire.ResponsePart || part.Index != i {
			os.Exit(30)
		}
		received = append(received, part.Body...)
	}
	end := read()
	if end.Kind != pisdkhttpwire.ResponseEnd || start.Digest != end.Digest || sha256.Sum256(received) != end.Digest || string(received) != p.Expected {
		os.Exit(31)
	}
	write(pisdkhttpwire.Frame{Kind: pisdkhttpwire.StructuredFinal, Body: []byte(`{"answer":"synthetic","artifacts":[]}`)})
	os.Exit(0)
}
