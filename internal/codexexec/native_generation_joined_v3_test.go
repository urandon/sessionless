//go:build darwin || linux

package codexexec

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedresourcegenerationsession"
	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkerhttp"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkersession"
	"gitcode.com/urandon/sessionless/internal/credentiallifecycle"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ownerlocalcredential"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/testkit"
)

// Real Connector/Session/reducer, local persistence/lifecycle, original runner
// and supervisor; the only executed workload is our owned shell fixture. No
// installed native binary, engine, provider/network request or real auth file.
type nativeGenerationFixtureV3 struct {
	profile             NativeProfileV3
	baseline            domain.AttachedResourceGenerationBaselineV1
	lifecycle           *ownerlocalcredential.Lifecycle
	owner               *attachedresourcegenerationsession.OwnerV1
	peer                *nativeGenerationPeerV3
	metadata            *nativeGenerationMetadataV3
	now                 time.Time
	materializationRoot string
	ids                 *nativeGenerationIDsV3
}

type nativeGenerationIDsV3 struct {
	ports.IDGenerator
	issues atomic.Int64
}

func (g *nativeGenerationIDsV3) NewID(ctx context.Context, kind ports.IDKind) (string, error) {
	if kind == ports.IDCredentialHandle {
		g.issues.Add(1)
	}
	return g.IDGenerator.NewID(ctx, kind)
}

type nativeGenerationPeerV3 struct {
	*nativeConstructorPeer
	retired bool
}

func (p *nativeGenerationPeerV3) Open(b attachedworkersession.ConnectionBindingV1, bearer []byte) (attachedworkersession.ExchangePort, error) {
	if p.retired {
		return nil, errors.New("fixture source retired")
	}
	if _, err := p.nativeConstructorPeer.Open(b, bearer); err != nil {
		return nil, err
	}
	return p, nil
}
func (p *nativeGenerationPeerV3) RetireConnectionSourcesV1() error { p.retired = true; return nil }
func (p *nativeGenerationPeerV3) Close() error                     { p.retired = true; return nil }
func (p *nativeGenerationPeerV3) Exchange(ctx context.Context, batch attachedworkerprotocol.BatchV1) (*attachedworkerprotocol.BatchV1, error) {
	if p.retired || ctx.Err() != nil || len(batch.Frames) != 1 {
		return nil, errors.New("fixture closed or invalid batch")
	}
	in := batch.Frames[0]
	if in.Kind == attachedworkerprotocol.MessageManifest {
		return p.nativeConstructorPeer.Exchange(ctx, batch)
	}
	p.step++
	out := attachedworkerprotocol.FrameV1{Version: 1, MessageID: attachedworkerprotocol.MessageIDV1(attachedworkerprotocol.DirectionPlatformToWorker, p.step+2), WorkerID: string(p.connection.WorkerID),
		EnrollmentGeneration: p.connection.EnrollmentGeneration, ConnectionGeneration: p.connection.ConnectionGeneration, Sequence: p.step + 2, Ack: in.Sequence}
	switch in.Kind {
	case attachedworkerprotocol.MessageHeartbeat:
		if !in.Heartbeat.Available {
			p.step--
			return nil, nil
		}
		out.Kind, out.LeaseOffer = attachedworkerprotocol.MessageLeaseOffer, &attachedworkerprotocol.LeaseOfferV1{Binding: p.binding, AttemptSequence: 1}
	case attachedworkerprotocol.MessageLeaseClaim:
		out.Kind, out.LeaseAccepted = attachedworkerprotocol.MessageLeaseAccepted, &attachedworkerprotocol.LeaseAcceptedV1{Binding: p.binding, AttemptSequence: 2}
	case attachedworkerprotocol.MessageTerminal:
		v := in.Terminal
		out.Kind, out.TerminalAck = attachedworkerprotocol.MessageTerminalAck, &attachedworkerprotocol.TerminalAckV1{Binding: v.Binding, AttemptSequence: v.AttemptSequence + 1, TerminalSequence: v.TerminalSequence, Status: v.Status, Result: v.Result, EvidenceDigest: bytes.Clone(v.EvidenceDigest)}
	default:
		return nil, fmt.Errorf("unexpected fixture action %s", in.Kind)
	}
	return &attachedworkerprotocol.BatchV1{Version: 1, Frames: []attachedworkerprotocol.FrameV1{out}}, nil
}

type nativeGenerationMetadataV3 struct {
	head  *domain.AttachedResourceGenerationHeadV1
	calls int
	fail  bool
}
type nativeGenerationRoundTripV3 func(*http.Request) (*http.Response, error)

func (f nativeGenerationRoundTripV3) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func (p *nativeGenerationMetadataV3) roundTrip(r *http.Request) (*http.Response, error) {
	p.calls++
	if p.fail {
		return nil, errors.New("controlled metadata failure")
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, 4097))
	if err != nil || len(raw) > 4096 {
		return nil, errors.New("fixture request bound")
	}
	defer clear(raw)
	if r.URL.Path == attachedworkerhttp.ResourceGenerationPublishPathV1 {
		var v attachedworkerhttp.ResourceGenerationPublishRequestV1
		if json.Unmarshal(raw, &v) != nil || domain.ValidateAttachedResourceGenerationTransitionV1(p.head, v.ExpectedCredentialGeneration, v.Target) != nil {
			return nil, errors.New("fixture CAS denied")
		}
		copy := v.Target
		p.head = &copy
	} else if r.URL.Path != attachedworkerhttp.ResourceGenerationReadPathV1 {
		return nil, errors.New("fixture unknown path")
	}
	if p.head == nil {
		return &http.Response{StatusCode: 404, Body: io.NopCloser(bytes.NewReader(nil))}, nil
	}
	body, err := json.Marshal(attachedworkerhttp.ResourceGenerationResponseV1{Version: 1, Status: ports.AttachedWorkerExecutionApplied, Head: *p.head})
	if err != nil {
		return nil, err
	}
	return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(bytes.NewReader(body)), ContentLength: int64(len(body))}, nil
}

func (g *nativeGenerationFixtureV3) install(t *testing.T, m attachedworkerlocal.ManifestV1, c attachedworkerprotocol.CapabilityManifestV1, peer *nativeConstructorPeer) attachedworkersession.ExchangeFactory {
	t.Helper()
	p, err := freezeNativeProfileV3(g.profile)
	if err != nil {
		t.Fatal(err)
	}
	_, e, b, _ := nativePolicyFixtureV3(t)
	b.TenantID, b.OwnerUserID, b.Backend, b.ModelID = m.TenantID, m.OwnerUserID, p.descriptor, g.profile.Backend.Model
	b.Resource = domain.ProviderResourceBindingV1{Kind: domain.ProviderResourceSubscriptionV1, ResourceID: "subscription-a", OwnerUserID: m.OwnerUserID, Revision: 1, CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 1}
	for _, s := range []*domain.ProviderEvidenceScopeV1{&e.Catalog.Scope, &e.Route.Scope, &e.Capability.Scope, &e.Privacy.Scope, &e.Price.Scope, &e.Policy.Scope} {
		*s = domain.ProviderEvidenceScopeV1{TenantID: m.TenantID, OwnerUserID: m.OwnerUserID, Resource: b.Resource, Backend: b.Backend}
	}
	e.Catalog.Models[0].ModelID = b.ModelID
	e.Capability.ModelID, e.Privacy.ModelID, e.Price.ModelID = b.ModelID, b.ModelID, b.ModelID
	observed, expires := g.now.Add(-time.Minute), g.now.Add(time.Hour)
	e.Catalog.ObservedAt, e.Route.ObservedAt, e.Capability.ObservedAt, e.Privacy.ObservedAt, e.Price.ObservedAt, e.Policy.ObservedAt = observed, observed, observed, observed, observed, observed
	e.Catalog.ExpiresAt, e.Route.ExpiresAt, e.Capability.ExpiresAt, e.Privacy.ExpiresAt, e.Price.ExpiresAt, e.Policy.ExpiresAt = expires, expires, expires, expires, expires, expires
	nativePolicyRebindV2(t, &e, &b)
	b.RunID, b.AttemptID, b.ExecutionPlacementDigest = "", "", ""
	capability, err := attachedworkerprotocol.ManifestDigestV1(c)
	if err != nil {
		t.Fatal(err)
	}
	g.baseline = domain.AttachedResourceGenerationBaselineV1{Version: 1, TenantID: m.TenantID, OwnerUserID: m.OwnerUserID, SubscriptionConnectionID: "subscription-a", WorkerID: m.WorkerID, EnrollmentGeneration: m.EnrollmentGeneration, BaselineRevision: 1, CapabilityDigest: domain.AttachedWorkerCapabilityDigest(hex.EncodeToString(capability)), GenerationOnlyDelegation: true, Binding: b, Evidence: e}
	if err := g.baseline.Validate(); err != nil {
		t.Fatal(err)
	}
	root := canonicalTempDir(t)
	g.materializationRoot = root
	credentials := filepath.Join(root, "encrypted")
	scratch := filepath.Join(root, "materialized")
	for _, path := range []string{credentials, scratch} {
		if err := os.Mkdir(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	store, err := ownerlocalcredential.New(t.Context(), ownerlocalcredential.Config{Root: credentials, Scope: ownerlocalcredential.Scope{TenantID: m.TenantID, OwnerUserID: m.OwnerUserID, SubscriptionConnectionID: "subscription-a"}, KeyEpoch: "synthetic"}, bytes.Repeat([]byte{0x37}, 32))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := store.Initialize(t.Context(), ownerlocalcredential.Registration{Provider: "openai", AuthMode: "chatgpt", Entitlement: domain.EntitlementActive, At: g.now}, []byte(`{"synthetic":"initial"}`)); err != nil {
		t.Fatal(err)
	}
	g.ids = &nativeGenerationIDsV3{IDGenerator: testkit.NewSequenceIDGenerator("generation-runner-")}
	g.lifecycle, err = ownerlocalcredential.NewLifecycle(store, credentiallifecycle.Config{ScratchRoot: scratch, Clock: testkit.NewFakeClock(g.now), IDs: g.ids, MaxAuthBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := g.lifecycle.Close(); err != nil {
			t.Error(err)
		}
	})
	g.peer = &nativeGenerationPeerV3{nativeConstructorPeer: peer}
	g.metadata = &nativeGenerationMetadataV3{}
	g.owner, err = attachedresourcegenerationsession.NewV1(m.ControlPlaneOrigin, &http.Client{Transport: nativeGenerationRoundTripV3(g.metadata.roundTrip)}, g.peer, g.lifecycle, g.baseline)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := g.owner.Close(); err != nil {
			t.Error(err)
		}
	})
	return g.owner
}
func (g *nativeGenerationFixtureV3) bind(t *testing.T, s *attachedworkersession.Session) {
	t.Helper()
	if err := g.owner.BindInitialSessionV1(t.Context(), s); err != nil {
		t.Fatal(err)
	}
	if err := g.owner.SynchronizeV1(t.Context()); err != nil {
		t.Fatal(err)
	}
}
func (g *nativeGenerationFixtureV3) sealRequest(t *testing.T, r *ports.ExecutionRequest, s attachedworkersession.SnapshotV1) {
	t.Helper()
	head, found, err := g.lifecycle.LoadCredentialBinding(t.Context(), g.baseline.TenantID, g.baseline.SubscriptionConnectionID)
	if err != nil || !found {
		t.Fatal(err)
	}
	b, _, err := domain.ProjectAttachedResourceGenerationV1(g.baseline, head.Generation)
	if err != nil {
		t.Fatal(err)
	}
	r.ExecutionPlacementV2 = domain.ExecutionPlacementV2{Version: 2, Kind: domain.ExecutionPlacementAttachedWorker, FallbackPolicy: domain.ExecutionFallbackDenied, OwnerUserID: s.OwnerUserID, WorkerID: s.WorkerID, CapabilityDigest: s.CapabilityDigest, PolicyDigest: domain.AttachedWorkerPolicyDigest(b.EffectivePolicyDigest)}
	d, err := domain.ExecutionPlacementDigest(r.ExecutionPlacementV2)
	if err != nil {
		t.Fatal(err)
	}
	b.RunID, b.AttemptID, b.ExecutionPlacementDigest = r.RunID, r.AttemptID, string(d)
	r.HarnessBinding = b
}

func nativeGenerationJoinedFixtureV3(t *testing.T, refresh bool) (*PreparedAttachedWorkerRunnerV1, nativeConstructorFixture, *nativeGenerationFixtureV3, *nativeJoinedLauncherV3, attachedworkerdaemon.Invocation) {
	t.Helper()
	final := `{"answer":"generation-aware answer","artifacts":[]}`
	text, _ := json.Marshal(final)
	protocol := "{\"type\":\"thread.started\",\"thread_id\":\"synthetic\"}\n{\"type\":\"turn.started\"}\n{\"type\":\"item.completed\",\"item\":{\"type\":\"agent_message\",\"text\":" + string(text) + "}}\n{\"type\":\"turn.completed\"}\n"
	writeback := ""
	if refresh {
		writeback = "printf '%s' '{\"synthetic\":\"refreshed\"}' >\"$CODEX_HOME/auth.json\"\n"
	}
	executable := writePreparedCodexFixture(t, "#!/bin/sh\nwhile IFS= read -r line; do :; done\n"+writeback+"printf '%s' '"+strings.ReplaceAll(protocol, "'", "'\\''")+"'\n")
	digest, err := attachedworkerdaemon.DigestExecutable(executable)
	if err != nil {
		t.Fatal(err)
	}
	p := NativeProfileV3(nativeProfileFixtureV2(t))
	now := time.Now().UTC()
	p.Backend.Enabled, p.Backend.Executable, p.Backend.ExecutableDigest, p.Backend.Now = true, executable, digest, func() time.Time { return now }
	p.Launch.Workload = attachedworkerdaemon.NativeArtifactV2{Executable: executable, Digest: digest}
	p.Launch.Companion = nativeHostArtifact(t)
	g := &nativeGenerationFixtureV3{profile: p, now: now}
	f := newNativeConstructorFixtureAtProfiles(t, executable, nil, now, nil, g, false)
	launcher := &nativeJoinedLauncherV3{}
	scratch, err := os.MkdirTemp("", "ng-")
	if err != nil {
		t.Fatal(err)
	}
	scratch, err = filepath.EvalSymlinks(scratch)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Remove(scratch); err != nil {
			t.Errorf("owned supervisor root residue: %v", err)
		}
	})
	sup, err := attachedworkerdaemon.NewSupervisorV3(attachedworkerdaemon.SupervisorConfigV3{Base: attachedworkerdaemon.SupervisorConfig{ScratchRoot: scratch, Timeout: 10 * time.Second, TerminationGrace: time.Second, MaxStdoutBytes: maxJSONLOutputBytes, MaxStderrBytes: maxProcessStderrBytesV1, AllowedEnvironmentNames: []string{CredentialHomeEnvironmentV1}, AllowedReadRoots: []string{g.materializationRoot}}, Launcher: launcher})
	if err != nil {
		t.Fatal(err)
	}
	r, err := NewNativePreparedAttachedWorkerRunnerV3(NativePreparedConfigV3{Profile: p, Baseline: g.baseline, GenerationOwner: g.owner, Lifecycle: g.lifecycle, InlineOutputV2: true}, privatePreparedScratch(t), sup, g.lifecycle, attachedworkerdaemon.InvocationRunnerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.BindNativeSessionV3(f.session); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := r.Close(ctx); err != nil && !errors.Is(err, ErrInlineReceiptPendingV2) {
			t.Error(err)
		}
	})
	inv := generationInvocationV3(t, f.input, executable, digest, g.profile)
	return r, f, g, launcher, inv
}
func generationInvocationV3(t *testing.T, input *attachedworkerdaemon.VerifiedAttachedInputV1, executable string, digest attachedworkerdaemon.ExecutableDigest, p NativeProfileV3) attachedworkerdaemon.Invocation {
	t.Helper()
	a := input.AcceptedAttempt
	started := a.CreatedAt
	expires := a.LeaseExpiresAt
	args, err := p.ArgumentsV3()
	if err != nil {
		t.Fatal(err)
	}
	return attachedworkerdaemon.Invocation{Identity: attachedworkerdaemon.InvocationIdentity{TenantID: a.TenantID, OwnerUserID: a.OwnerUserID, WorkerID: a.WorkerID, RunID: a.RunID, AttemptID: a.AttemptID, LeaseID: a.LeaseID, FenceToken: a.LeaseGeneration}, Process: attachedworkerdaemon.AttemptSpec{Executable: executable, ExecutableDigest: digest, Arguments: args}, VerifiedInput: input,
		Credential: &attachedworkerdaemon.CredentialInvocation{HomeEnvironment: CredentialHomeEnvironmentV1, ExpectedBindingGeneration: input.Job.HarnessBinding.Resource.CredentialGeneration, IssueRequest: ports.CredentialIssueRequest{OwnerUserID: a.OwnerUserID, ProviderResource: input.Job.HarnessBinding.Resource, ExpiresAt: expires,
			Run:     domain.Run{ID: a.RunID, TenantID: a.TenantID, SessionID: input.Job.SessionID, TriggerEventID: input.Job.TriggerEventID, SubscriptionConnectionID: domain.SubscriptionConnectionID(input.Job.HarnessBinding.Resource.ResourceID), Status: domain.RunRunning, IdempotencyKey: "generation-joined-run", StartedAt: &started, CreatedAt: started, UpdatedAt: started},
			Attempt: domain.Attempt{ID: a.AttemptID, TenantID: a.TenantID, RunID: a.RunID, Number: 1, Status: domain.AttemptRunning, WorkerID: string(a.WorkerID), CreatedAt: started, UpdatedAt: started},
			Lease:   domain.Lease{ID: a.LeaseID, TenantID: a.TenantID, RunID: a.RunID, AttemptID: a.AttemptID, WorkerID: string(a.WorkerID), FenceToken: a.LeaseGeneration, AcquiredAt: started, ExpiresAt: expires}}}}
}

func TestNativeGenerationRunnerV3OriginalLifecycleAndReceiptAfterOwnWriteBack(t *testing.T) {
	for _, refresh := range []bool{false, true} {
		t.Run(fmt.Sprint(refresh), func(t *testing.T) {
			r, f, g, launcher, inv := nativeGenerationJoinedFixtureV3(t, refresh)
			baseline := g.baseline.Clone()
			job := f.input.Job.HarnessBinding.Clone()
			result, err := r.Run(t.Context(), inv)
			if !result.Succeeded(err) || len(launcher.envelopes) != 1 || g.ids.issues.Load() != 1 {
				t.Fatalf("original generation run result=%+v err=%v launches=%d", result, err, len(launcher.envelopes))
			}
			if result.CredentialChanged != refresh || result.CredentialGeneration != 1+uint64(boolInt(refresh)) {
				t.Fatalf("own writeback wrong: changed=%t generation=%d", result.CredentialChanged, result.CredentialGeneration)
			}
			if r.slot.nativeEvidenceV3 == nil || r.slot.nativeEvidenceV3.Policy.Scope.Resource.CredentialGeneration != 1 || !reflect.DeepEqual(g.baseline, baseline) || !reflect.DeepEqual(f.input.Job.HarnessBinding, job) {
				t.Fatal("per-attempt evidence/source mutated")
			}
			calls := g.metadata.calls
			g.metadata.fail = true
			if refresh {
				if syncErr := g.owner.SynchronizeV1(t.Context()); syncErr == nil {
					t.Fatal("controlled successor ACK unexpectedly succeeded")
				}
			}
			inline, err := r.InlineReceiptCandidate(t.Context(), inv.Identity, result, nil)
			if err != nil {
				t.Fatalf("original N receipt lost after own WB/next ACK failure: %v", err)
			}
			if _, _, _, err := inline.Declaration(); err != nil {
				t.Fatal(err)
			}
			if !refresh && g.metadata.calls != calls {
				t.Fatal("receipt gate performed metadata HTTP")
			}
			if refresh && r.slot.nativeEvidenceV3.Policy.Scope.Resource.CredentialGeneration != 1 {
				t.Fatal("receipt upgraded old N evidence")
			}
		})
	}
}
func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func TestNativeGenerationRunnerV3MissingAckStopsBeforeIssue(t *testing.T) {
	r, _, g, launcher, inv := nativeGenerationJoinedFixtureV3(t, false)
	if err := g.owner.Close(); err != nil {
		t.Fatal(err)
	}
	before, found, err := g.lifecycle.LoadCredentialBinding(t.Context(), g.baseline.TenantID, g.baseline.SubscriptionConnectionID)
	if err != nil || !found {
		t.Fatal(err)
	}
	result, err := r.Run(t.Context(), inv)
	if err == nil || result.CredentialReleaseRequired || len(launcher.envelopes) != 0 || r.slot != nil || g.ids.issues.Load() != 0 {
		t.Fatalf("missing ACK reached sole Issue/launch: result=%+v err=%v launches=%d", result, err, len(launcher.envelopes))
	}
	after, found, err := g.lifecycle.LoadCredentialBinding(t.Context(), g.baseline.TenantID, g.baseline.SubscriptionConnectionID)
	if err != nil || !found || after != before {
		t.Fatal("denied generation mutated original binding", err)
	}
}

// The canonical receipt digest here belongs only to the controlled peer. This
// exercises the real Session terminal/ACK and runner retirement, NOT YDB receipt
// custody or the adapter's journal/READY integration (covered separately).
func retireGenerationRunnerFixtureV3(t *testing.T, r *PreparedAttachedWorkerRunnerV1, f nativeConstructorFixture, g *nativeGenerationFixtureV3, inv attachedworkerdaemon.Invocation, result attachedworkerdaemon.InvocationResult) {
	t.Helper()
	if _, err := r.InlineReceiptCandidate(t.Context(), inv.Identity, result, nil); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte("controlled canonical receipt"))
	ack, err := f.session.ExchangeAction(t.Context(), attachedworkersession.ActionV1{Terminal: &attachedworkerprotocol.TerminalV1{Binding: g.peer.binding, AttemptSequence: 2, TerminalSequence: 1, Status: attachedworkerprotocol.TerminalSucceeded, Result: attachedworkerprotocol.TerminalResultCompleted, EvidenceDigest: digest[:]}})
	if err != nil || ack == nil || ack.Kind != attachedworkerprotocol.MessageTerminalAck || !bytes.Equal(ack.TerminalAck.EvidenceDigest, digest[:]) {
		t.Fatalf("original Session exact terminal ACK: %v", err)
	}
	if err := r.RetireReceipt(inv.Identity); err != nil {
		t.Fatal(err)
	}
}

func TestNativeGenerationRunnerV3NewAcceptedSuccessorHasIndependentEvidence(t *testing.T) {
	r, f, g, launcher, first := nativeGenerationJoinedFixtureV3(t, true)
	result, err := r.Run(t.Context(), first)
	if !result.Succeeded(err) || !result.CredentialChanged || result.CredentialGeneration != 2 {
		t.Fatalf("original N changed WB: %+v %v", result, err)
	}
	oldEvidence := r.slot.nativeEvidenceV3
	oldBytes, err := json.Marshal(oldEvidence)
	if err != nil {
		t.Fatal(err)
	}
	retireGenerationRunnerFixtureV3(t, r, f, g, first, result)
	if err := g.owner.SynchronizeV1(t.Context()); err != nil {
		t.Fatal("successor ACK", err)
	}
	if g.metadata.calls != 2 || g.metadata.head == nil || g.metadata.head.CredentialGeneration != 2 {
		t.Fatal("successor was not acknowledged exactly once")
	}
	input, err := f.input.Clone()
	if err != nil {
		t.Fatal(err)
	}
	defer input.Clear()
	a := &input.AcceptedAttempt
	a.RunID, a.AttemptID, a.LeaseID, a.ReservationID = "run-generation-2", "attempt-generation-2", "lease-generation-2", "reservation-generation-2"
	a.LeaseGeneration++
	a.FenceToken, err = domain.NewAttachedWorkerFenceTokenV1(a.TenantID, a.OwnerUserID, a.WorkerID, a.RunID, a.AttemptID, a.LeaseID, a.LeaseGeneration)
	if err != nil {
		t.Fatal(err)
	}
	input.Job.RunID, input.Job.AttemptID, input.Job.ReservationID = a.RunID, a.AttemptID, a.ReservationID
	input.Manifest.RunID = a.RunID
	request := ports.ExecutionRequest{TenantID: a.TenantID, OwnerUserID: a.OwnerUserID, RunID: a.RunID, AttemptID: a.AttemptID}
	g.sealRequest(t, &request, f.session.Snapshot())
	input.Job.HarnessBinding, input.Job.ExecutionPlacementV2 = request.HarnessBinding, request.ExecutionPlacementV2
	a.PolicyDigest = input.Job.ExecutionPlacementV2.PolicyDigest
	a.ContextDigest, err = domain.AttachedWorkerJobContextDigestV1(input.Job, input.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	g.peer.binding = attachedworkerprotocol.AttemptBindingV1{RunID: string(a.RunID), AttemptID: string(a.AttemptID), LeaseID: string(a.LeaseID), LeaseGeneration: a.LeaseGeneration, FenceToken: string(a.FenceToken), ExpiresAtUnixMicro: a.LeaseExpiresAt.UnixMicro(), ContextDigest: nativeDecodeHex(t, string(a.ContextDigest)), CapabilityDigest: nativeDecodeHex(t, string(a.CapabilityDigest)), PolicyDigest: nativeDecodeHex(t, string(a.PolicyDigest))}
	if _, err := f.session.ExchangeAction(t.Context(), attachedworkersession.ActionV1{Heartbeat: &attachedworkerprotocol.HeartbeatV1{ObservedAtUnixMicro: g.now.UnixMicro(), Available: true}}); err != nil {
		t.Fatal("new successor offer", err)
	}
	if _, err := f.session.ExchangeAction(t.Context(), attachedworkersession.ActionV1{LeaseClaim: &attachedworkerprotocol.LeaseClaimV1{Binding: g.peer.binding, AttemptSequence: 1}}); err != nil {
		t.Fatal("new successor actual claim", err)
	}
	next := generationInvocationV3(t, input, first.Process.Executable, first.Process.ExecutableDigest, g.profile)
	second, err := r.Run(t.Context(), next)
	if !second.Succeeded(err) || second.CredentialChanged || second.CredentialGeneration != 2 || len(launcher.envelopes) != 2 || g.ids.issues.Load() != 2 {
		t.Fatalf("successor N+1 consumption: %+v err=%v launches=%d issues=%d", second, err, len(launcher.envelopes), g.ids.issues.Load())
	}
	currentBytes, err := json.Marshal(oldEvidence)
	if err != nil {
		t.Fatal(err)
	}
	if r.slot.nativeEvidenceV3 == oldEvidence || r.slot.nativeEvidenceV3.Policy.Scope.Resource.CredentialGeneration != 2 || !bytes.Equal(currentBytes, oldBytes) || f.input.Job.HarnessBinding.Resource.CredentialGeneration != 1 {
		t.Fatal("new job mutated old-N evidence/input or reused a shared bundle")
	}
	retireGenerationRunnerFixtureV3(t, r, f, g, next, second)
}
