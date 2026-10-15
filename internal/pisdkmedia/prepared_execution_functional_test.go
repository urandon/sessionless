//go:build pi_sdk_functional && (darwin || linux)

package pisdkmedia

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/pisdkphasewire"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// SAME original InvocationRunner and Supervisor, NEW concrete executor and
// genuine existing SDK. Only Source/OS isolation/HTTP collaborators are fake;
// no native isolation, current Source, model/profile or output-custody claim.
func TestPreparedPiSameOriginalSupervisor(t *testing.T) {
	const node = "/opt/homebrew/Cellar/node@24/24.19.0/bin/node"
	const sdk = "/Volumes/hubdisk/workspace/ai/sessionless/.gitcode/native-build/pi-sdk-6c4f36-catalog-9c40af/source"
	for _, mode := range []string{"history", "snapshot-tail", "close-during-send", "mutated-final", "foreign-model", "foreign-schema", "foreign-headers", "foreign-head", "foreign-ledger", "foreign-file", "forged-physical-cap", "altered-argv", "altered-env", "cancel-before-execute", "denied-physical", "release-failed"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
			defer cancel()
			nodeDigest, err := attachedworkerdaemon.DigestExecutable(node)
			if err != nil {
				t.Fatal(err)
			}
			root, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			for _, asset := range functionalNativeAssets(t, "entry_fake_transport.mjs") {
				path := filepath.Join(root, asset.path)
				if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
					t.Fatal(err)
				}
				if err = os.WriteFile(path, asset.body, 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err = os.Symlink(sdk, filepath.Join(root, "internal/pisdkmedia/pi")); err != nil {
				t.Fatal(err)
			}
			cfg := initFixtureConfig(t)
			cfg.FinalSchema = []byte(attachedworkeroutput.StructuredFinalSchemaV1)
			schema := sha256.Sum256(cfg.FinalSchema)
			cfg.FinalSchemaSourceDigest = hex.EncodeToString(schema[:])
			cfg.Profile.Sources.FinalSchemaDigest = cfg.FinalSchemaSourceDigest
			cfg.Profile.Sources.NativeArtifactDigest = hex.EncodeToString(nodeDigest[:])
			cfg.Model.SupportsExplicitPromptCacheMode = false
			cfg.Headers[2].Value = entryPlatformUserAgent(t)
			cfg.Headers[10].Value = "30"
			model := []byte(strings.Replace(initFixtureModel, `"supportsExplicitPromptCacheMode":true`, `"supportsExplicitPromptCacheMode":false`, 1))
			id, input := fixtureInputResource(t, mode == "snapshot-tail", true)
			now := time.Now().UTC().Truncate(time.Microsecond)
			expiry := now.Add(2 * time.Hour)
			input.Job.Limits.MaxRuntime = time.Hour
			input.Job.HarnessBinding.Backend, err = cfg.Profile.Descriptor()
			if err != nil {
				t.Fatal(err)
			}
			input.Job.HarnessBinding.AttachedPiAPIInvocationPolicyDigest = domain.AttachedPiAPIInvocationPolicyMappingDigestV1(strings.Repeat("a", 64)) // structural fake, never owner authorization
			input.Job.HarnessBinding.EvidenceExpiresAt = &expiry
			input.AcceptedAttempt.LeaseExpiresAt = expiry
			input.AcceptedAttempt.ContextDigest, err = domain.AttachedWorkerJobContextDigestV1(input.Job, input.Manifest)
			if err != nil {
				t.Fatal(err)
			}
			ledger := fixtureLedger(t, cfg.Profile.Limits)
			projection, err := CompileOriginalRequestV1(ctx, id, input, cfg, ledger)
			if err != nil {
				t.Fatal(err)
			}
			defer projection.Clear()
			credentials := initFixtureCredentialsAt(t, projection, now)
			init, err := PrepareInitV1(ctx, projection, model, 30000, credentials.source)
			if err != nil {
				t.Fatal(err)
			}
			defer init.Clear()
			measure := wireWriter{cap: cfg.Profile.Limits.RequestHeaderBytes, maxDepth: 1, maxNodes: cfg.Profile.Limits.JSONNodes}
			writeExecutionHeaders(&measure, projection.config.headers)
			headers := wireWriter{out: make([]byte, 0, int(measure.bytes)), cap: measure.bytes, maxDepth: 1, maxNodes: cfg.Profile.Limits.JSONNodes}
			writeExecutionHeaders(&headers, projection.config.headers)
			expected := attachedworkerdaemon.PreparedAPIRequestV1{Profile: *cfg.Profile, Body: projection.Body(), Model: model, Schema: cfg.FinalSchema, Headers: headers.out}
			args := []string{"--permission", "--allow-fs-read=" + root, "--allow-fs-read=" + sdk, "--allow-fs-read=" + credentials.root, "--no-addons", "--disable-proto=throw", filepath.Join(root, "internal/pisdkmedia/native/entry_fake_transport.mjs")}
			values := init.EntryArguments()
			args = append(args, values[:]...)
			process := attachedworkerdaemon.AttemptSpec{Executable: node, ExecutableDigest: nodeDigest, Arguments: args, Environment: []attachedworkerdaemon.EnvironmentVariable{{Name: "PI_OFFLINE", Value: "1"}}}
			launcher := &executionTestLauncher{}
			scratch := filepath.Join(root, "supervisor")
			if err = os.Mkdir(scratch, 0700); err != nil {
				t.Fatal(err)
			}
			supervisor, err := attachedworkerdaemon.NewSupervisor(attachedworkerdaemon.SupervisorConfig{ScratchRoot: scratch, Launcher: launcher, Timeout: time.Hour, TerminationGrace: 3 * time.Second,
				AllowedEnvironmentNames: []string{"PI_OFFLINE"}, AllowedReadRoots: []string{credentials.root}})
			if err != nil {
				t.Fatal(err)
			}
			execution, err := PreparePiExecutionV1(supervisor, projection, init, expected, process)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := execution.Close(); err != nil {
					t.Error(err)
				}
			}()
			retirement, stopRetirement := context.WithTimeout(context.Background(), 2*time.Hour)
			defer stopRetirement()
			parent := &executionTestParent{request: expected, process: process, issue: credentials.request, ledger: ledger, retirement: retirement, execution: execution, deny: mode == "denied-physical", closeDuring: mode == "close-during-send"}
			life := &executionTestLifecycle{initCredentials: credentials, order: &parent.order, releaseFailed: mode == "release-failed"}
			parent.life = life
			executor := &executionTestMutator{execution: execution, mode: mode, badModel: bytes.Clone(model), badSchema: bytes.Clone(cfg.FinalSchema), badHeaders: bytes.Clone(headers.out), badLedger: fixtureLedger(t, cfg.Profile.Limits), cancel: cancel}
			executor.badModel[0] = '['
			executor.badSchema[0] = '['
			executor.badHeaders[0] = '['
			runner, err := attachedworkerdaemon.NewPreparedAPIInvocationRunner(executor, life, parent)
			if err != nil {
				t.Fatal(err)
			}
			result, runErr := runner.Run(ctx, attachedworkerdaemon.Invocation{Identity: id, Process: process, VerifiedInput: input})
			if mode == "mutated-final" {
				observation, ok := result.Process.PiStdioObservationV1()
				if !ok || len(observation.Final) < 12 {
					t.Fatal("missing actual original final before mutation")
				}
				observation.Final[11] = 'X'
			}
			final, finalErr := execution.FinalData(ctx, result, runErr)
			positive := mode == "history" || mode == "snapshot-tail" || mode == "close-during-send"
			if positive {
				if !result.Succeeded(runErr) || finalErr != nil || final.Answer != "native entry answer" || len(final.Artifacts) != 0 {
					t.Fatalf("same owner result: run=%v final=%v failure=%s process=%v launches=%d order=%v prepared=%v accounting=%+v remaining=%+v", runErr, finalErr, result.FailureCode, result.Process, launcher.launches, parent.order, parent.data != nil, ledger.Accounting(), ledger.Remaining())
				}
				want := []string{"prepare", "pre_issue", "issue", "materialize", "physical_send", "unlocked", "immutable_finalize", "finalize", "release", "clear"}
				if !reflect.DeepEqual(parent.order, want) {
					t.Fatalf("owner order=%v want=%v", parent.order, want)
				}
			} else if mode == "mutated-final" {
				if runErr != nil || finalErr == nil {
					t.Fatal("borrowed final mutation accepted or original process failed")
				}
			} else if runErr == nil || finalErr == nil {
				t.Fatalf("mode=%s invalid original lifecycle made usable final", mode)
			}
			if life.issues != 1 || life.materializes != 1 || life.releases != 1 || life.active {
				t.Fatalf("lifecycle counters issue=%d materialize=%d release=%d active=%v", life.issues, life.materializes, life.releases, life.active)
			}
			if _, err = os.Lstat(life.path); !os.IsNotExist(err) {
				t.Fatal("public test file remained after actual Release")
			}
			wantLaunches := 1
			if strings.HasPrefix(mode, "foreign-") || mode == "altered-argv" || mode == "altered-env" || mode == "forged-physical-cap" || mode == "cancel-before-execute" {
				wantLaunches = 0
			}
			if launcher.launches != wantLaunches {
				t.Fatalf("launches=%d want=%d", launcher.launches, wantLaunches)
			}
			if executor.forgedCalls != 0 {
				t.Fatal("forged public Authorize implementation used")
			}
			if mode == "close-during-send" && !parent.closeBlocked {
				t.Fatal("Close not rejected during actual active physical phase")
			}
			if _, err = execution.RunPrepared(ctx, attachedworkerdaemon.PreparedInvocationV1{}); err == nil || launcher.launches != wantLaunches {
				t.Fatal("spent executor relaunched")
			}
			if _, err = execution.FinalData(ctx, result, runErr); err == nil {
				t.Fatal("final parsed twice")
			}
			copyWork := ledger.Accounting().CopyBytes
			if err = execution.Close(); err != nil {
				t.Fatal(err)
			}
			if err = execution.Close(); err != nil {
				t.Fatal(err)
			}
			if ledger.Accounting().CopyBytes != copyWork {
				t.Fatal("Close refunded cumulative work")
			}
		})
	}
}

type executionTestMutator struct {
	execution             *PreparedPiExecutionV1
	mode                  string
	badModel              []byte
	badSchema, badHeaders []byte
	badLedger             attachedworkerdaemon.PreparedAPILedgerV1
	cancel                context.CancelFunc
	forgedCalls           int
}

func (e *executionTestMutator) RunPrepared(ctx context.Context, v attachedworkerdaemon.PreparedInvocationV1) (attachedworkerdaemon.AttemptResult, error) {
	if e.mode == "foreign-model" {
		request := *v.APIRequest
		request.Model = e.badModel
		v.APIRequest = &request
	}
	if e.mode == "foreign-schema" || e.mode == "foreign-headers" {
		r := *v.APIRequest
		if e.mode == "foreign-schema" {
			r.Schema = e.badSchema
		} else {
			r.Headers = e.badHeaders
		}
		v.APIRequest = &r
	}
	if e.mode == "foreign-head" {
		input := *v.VerifiedInput
		input.AcceptedAttempt.RunID = "foreign-run"
		v.VerifiedInput = &input
	}
	if e.mode == "foreign-ledger" {
		v.Ledger = e.badLedger
	}
	if e.mode == "foreign-file" {
		v.Materialization.FilePath = filepath.Join(v.Materialization.RootDir, "not-the-original")
	}
	if e.mode == "forged-physical-cap" {
		v.PhysicalSend = e
	}
	if e.mode == "cancel-before-execute" {
		e.cancel()
	}
	if e.mode == "altered-argv" {
		v.Process.Arguments[0] = "--eval"
	}
	if e.mode == "altered-env" {
		v.Process.Environment[0].Value = "changed"
	}
	return e.execution.RunPrepared(ctx, v)
}
func (e *executionTestMutator) Authorize([]byte, string) (time.Time, error) {
	e.forgedCalls++
	return time.Now().Add(time.Hour), nil
}

type executionTestParent struct {
	request attachedworkerdaemon.PreparedAPIRequestV1
	process attachedworkerdaemon.AttemptSpec
	issue   ports.ProviderCredentialIssueRequestV1
	ledger  interface {
		Reserve(uint64, uint64, uint64) error
		EndLive(uint64) error
	}
	retirement                context.Context
	execution                 *PreparedPiExecutionV1
	life                      *executionTestLifecycle
	data                      *attachedworkerdaemon.PreparedAPIDataV1
	order                     []string
	deny                      bool
	closeDuring, closeBlocked bool
	clonePayload              uint64
}

func (p *executionTestParent) PrepareOriginalAPIInvocation(ctx context.Context, _ attachedworkerdaemon.InvocationIdentity, input *attachedworkerdaemon.VerifiedAttachedInputV1, process attachedworkerdaemon.AttemptSpec) (*attachedworkerdaemon.PreparedAPIDataV1, error) {
	p.order = append(p.order, "prepare")
	// This fixture has no legacy Artifacts copyShape slice. Both existing
	// Clone calls enforce their 1MiB encoded envelope before Marshal/Unmarshal:
	// two encoded + two decoded payload appearances <=4MiB. Add exact lengths
	// for independent parent/child request buffers and bounded process arrays.
	// This is logical payload only, NOT a full native/heap/Source fit producer.
	if len(input.Artifacts) != 0 {
		return nil, errors.New("unsupported fixture clone shape")
	}
	p.clonePayload = 4 << 20
	for _, v := range [][]byte{p.request.Body, p.request.Model, p.request.Schema, p.request.Headers} {
		p.clonePayload += 2 * uint64(len(v))
	}
	p.clonePayload += 64 * uint64(len(process.Arguments)+len(process.Environment)+len(process.AdditionalReadRoots)+1)
	if err := p.ledger.Reserve(p.clonePayload, p.clonePayload, 4096); err != nil {
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
	cloneReq := func() attachedworkerdaemon.PreparedAPIRequestV1 {
		v := p.request
		v.Body = bytes.Clone(v.Body)
		v.Model = bytes.Clone(v.Model)
		v.Schema = bytes.Clone(v.Schema)
		v.Headers = bytes.Clone(v.Headers)
		return v
	}
	cloneProcess := func() attachedworkerdaemon.AttemptSpec {
		v := process
		v.Arguments = append([]string(nil), v.Arguments...)
		v.Environment = append([]attachedworkerdaemon.EnvironmentVariable(nil), v.Environment...)
		v.AdditionalReadRoots = append([]string(nil), v.AdditionalReadRoots...)
		return v
	}
	child := cloneProcess()
	child.AdditionalReadRoots = append(child.AdditionalReadRoots, "")
	h := a.AcceptedAttempt
	entry := time.Now()
	deadline, _ := ctx.Deadline()
	p.data = &attachedworkerdaemon.PreparedAPIDataV1{Issue: p.issue, ParentInput: a, ChildInput: b, ParentRequest: cloneReq(), ChildRequest: cloneReq(), ParentProcess: cloneProcess(), ChildProcess: child, Entry: entry, CallerDeadline: deadline, InvocationDeadline: deadline, ProcessDeadline: deadline, SafetySlack: time.Nanosecond, Ledger: p.ledger, Retirement: p, Custody: p,
		Authorization: ports.AttachedWorkerSealedInputAuthorization{TenantID: h.TenantID, OwnerUserID: h.OwnerUserID, WorkerID: h.WorkerID, ConnectionID: h.ConnectionID, EnrollmentGeneration: h.EnrollmentGeneration, ConnectionGeneration: h.ConnectionGeneration, RunID: h.RunID, AttemptID: h.AttemptID, AttemptSequence: 1, LeaseID: h.LeaseID, LeaseGeneration: h.LeaseGeneration, FenceToken: h.FenceToken, LeaseExpiresAtUnixMicro: h.LeaseExpiresAt.UnixMicro(), ContextDigest: h.ContextDigest, CapabilityDigest: h.CapabilityDigest, PolicyDigest: h.PolicyDigest, ExpectedAttemptRevision: h.Revision}}
	return p.data, nil
}
func (p *executionTestParent) Context() (context.Context, error) { return p.retirement, nil }
func (p *executionTestParent) Clear() {
	p.order = append(p.order, "clear")
	if p.data == nil {
		return
	}
	p.data.ParentInput.Clear()
	p.data.ChildInput.Clear()
	for _, r := range []*attachedworkerdaemon.PreparedAPIRequestV1{&p.data.ParentRequest, &p.data.ChildRequest} {
		clear(r.Body)
		clear(r.Model)
		clear(r.Schema)
		clear(r.Headers)
	}
	_ = p.ledger.EndLive(p.clonePayload)
}
func (p *executionTestParent) ObserveOriginalAPIPhase(_ context.Context, r pisdkphasewire.Request) (pisdkphasewire.Reply, error) {
	p.order = append(p.order, string(r.Phase))
	if (r.Phase == pisdkphasewire.PhysicalSend) != p.life.active {
		return pisdkphasewire.Reply{}, errors.New("synthetic phase/callback order mismatch")
	}
	if r.Phase == pisdkphasewire.ImmutableFinalize && p.execution.state.Load() != 2 {
		return pisdkphasewire.Reply{}, errors.New("process not joined")
	}
	if p.deny && r.Phase == pisdkphasewire.PhysicalSend {
		return pisdkphasewire.Reply{Status: ports.AttachedWorkerExecutionDenied}, nil
	}
	if p.closeDuring && r.Phase == pisdkphasewire.PhysicalSend {
		if p.execution.Close() == nil {
			return pisdkphasewire.Reply{}, errors.New("Close accepted during running original process")
		}
		p.closeBlocked = true
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	window := min(time.Duration(r.CallerDeadline.RemainingNanoseconds), time.Minute)
	return pisdkphasewire.Reply{Status: ports.AttachedWorkerExecutionApplied, AcceptedAttempt: p.data.ParentInput.AcceptedAttempt, AcceptedAttemptRevision: p.data.ParentInput.AcceptedAttempt.Revision, HarnessBindingDigest: r.ExpectedHarnessBindingDigest, AttachedPiAPIInvocationPolicyDigest: r.ExpectedAttachedPiAPIInvocationPolicyDigest, CheckedAt: now, UseExpiresAt: now.Add(window)}, nil
}

type executionTestLifecycle struct {
	*initCredentials
	order                          *[]string
	active                         bool
	issues, materializes, releases int
	path                           string
	releaseFailed                  bool
}

func (l *executionTestLifecycle) IssueProviderCredential(ctx context.Context, r ports.ProviderCredentialIssueRequestV1) (ports.ProviderInvocationCredentialV1, error) {
	l.issues++
	*l.order = append(*l.order, "issue")
	return l.service.IssueProviderCredential(ctx, r)
}
func (l *executionTestLifecycle) MaterializeProviderCredential(ctx context.Context, h ports.ProviderInvocationCredentialV1, fn ports.ProviderCredentialConsumerV1) error {
	l.materializes++
	*l.order = append(*l.order, "materialize")
	err := l.service.MaterializeProviderCredential(ctx, h, func(v ports.ProviderCredentialMaterializationV1, b []byte) error {
		l.active = true
		l.path = v.FilePath
		defer func() { l.active = false }()
		return fn(v, b)
	})
	*l.order = append(*l.order, "unlocked")
	return err
}
func (l *executionTestLifecycle) FinalizeProviderCredentialFile(ctx context.Context, h ports.ProviderInvocationCredentialV1, v ports.ProviderCredentialMaterializationV1) error {
	if l.active {
		return errors.New("finalize inside callback")
	}
	*l.order = append(*l.order, "finalize")
	return l.service.FinalizeProviderCredentialFile(ctx, h, v)
}
func (l *executionTestLifecycle) ReleaseProviderCredential(ctx context.Context, h ports.ProviderInvocationCredentialV1) error {
	l.releases++
	*l.order = append(*l.order, "release")
	err := l.service.ReleaseProviderCredential(ctx, h)
	if l.releaseFailed {
		return errors.New("synthetic failed Release acknowledgement")
	}
	return err
}

type executionTestLauncher struct{ launches int }

func (l *executionTestLauncher) Profile() attachedworkerdaemon.IsolationProfile {
	return attachedworkerdaemon.IsolationProfile{Name: "SYNTHETIC-no-native-proof", FilesystemReadBoundary: true, FilesystemWriteBoundary: true, NetworkDenied: true, ProcessBoundary: true, DiskBytesBounded: true}
}
func (l *executionTestLauncher) Prepare(ctx context.Context, s attachedworkerdaemon.LaunchSpec) (attachedworkerdaemon.IsolationBoundary, error) {
	l.launches++
	// Supervisor owns execution cancellation, not this already-ending Prepare
	// context. Its attestation deliberately rejects a launcher Cancel hook.
	cmd := exec.Command(s.Executable, s.Arguments...)
	cmd.Dir = s.Directory
	cmd.Env = append([]string(nil), s.Environment...)
	return &executionTestBoundary{cmd: cmd, attestation: attachedworkerdaemon.WorkloadAttestation{Executable: s.Executable, ExecutableDigest: s.ExecutableDigest, Arguments: append([]string(nil), s.Arguments...)}}, nil
}

type executionTestBoundary struct {
	cmd         *exec.Cmd
	attestation attachedworkerdaemon.WorkloadAttestation
}

func (b *executionTestBoundary) Command() *exec.Cmd { return b.cmd }
func (b *executionTestBoundary) AttestedWorkload() attachedworkerdaemon.WorkloadAttestation {
	return b.attestation
}
func (b *executionTestBoundary) GracefulStop(context.Context) error {
	if b.cmd.Process == nil {
		return nil
	}
	err := b.cmd.Process.Signal(syscall.SIGTERM)
	if errors.Is(err, os.ErrProcessDone) {
		return nil
	}
	return err
}
func (b *executionTestBoundary) ForceStop(context.Context) error {
	if b.cmd.Process == nil {
		return nil
	}
	err := b.cmd.Process.Kill()
	if errors.Is(err, os.ErrProcessDone) {
		return nil
	}
	return err
}
func (b *executionTestBoundary) Alive(context.Context) (bool, error) {
	if b.cmd.Process == nil {
		return false, nil
	}
	err := b.cmd.Process.Signal(syscall.Signal(0))
	if errors.Is(err, os.ErrProcessDone) || errors.Is(err, syscall.ESRCH) {
		return false, nil
	}
	return err == nil, err
}
func (b *executionTestBoundary) Release(context.Context) error { return nil }
