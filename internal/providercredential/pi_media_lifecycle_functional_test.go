//go:build pi_sdk_functional && (darwin || linux)

package providercredential_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/codexexec"
	"gitcode.com/urandon/sessionless/internal/pisdkmedia"
	"gitcode.com/urandon/sessionless/internal/pisdkphasewire"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/providercredential"
	"golang.org/x/sys/unix"
)

type executionTestParent struct {
	t                                *testing.T
	request                          attachedworkerdaemon.PreparedAPIRequestV1
	process                          attachedworkerdaemon.AttemptSpec
	issue                            ports.ProviderCredentialIssueRequestV1
	ledger                           *codexexec.CanonicalMediaLedgerV1
	preparedLive, retainedOutputLive uint64
	retirement                       context.Context
	execution                        *pisdkmedia.PreparedPiHTTPExecutionV1
	arguments                        [17]string
	launcher                         *executionTestLauncher
	life                             *executionTestLifecycle
	data                             *attachedworkerdaemon.PreparedAPIDataV1
	order                            []string
	deny                             bool
	closeDuring, closeBlocked        bool
	cleanupFailed                    bool
	clonePayload                     uint64
}

func (p *executionTestParent) PrepareOriginalAPIInvocation(ctx context.Context, _ attachedworkerdaemon.InvocationIdentity, input *attachedworkerdaemon.VerifiedAttachedInputV1, process attachedworkerdaemon.AttemptSpec, acquisition attachedworkerdaemon.PreparedInputAcquisitionV1) (*attachedworkerdaemon.PreparedAPIDataV1, error) {
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
	p.clonePayload += 64 * uint64(len(p.process.Arguments)+len(p.process.Environment)+len(p.process.AdditionalReadRoots)+1)
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
		v := p.process
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
	p.data = &attachedworkerdaemon.PreparedAPIDataV1{Issue: p.issue, ParentInput: a, ChildInput: b, ParentRequest: cloneReq(), ChildRequest: cloneReq(), ParentProcess: cloneProcess(), ChildProcess: child, HTTPInitArguments: p.arguments, Entry: entry, CallerDeadline: deadline, InvocationDeadline: deadline, ProcessDeadline: deadline, SafetySlack: time.Nanosecond, Ledger: p.ledger, Retirement: p, Custody: p,
		Authorization: ports.AttachedWorkerSealedInputAuthorization{TenantID: h.TenantID, OwnerUserID: h.OwnerUserID, WorkerID: h.WorkerID, ConnectionID: h.ConnectionID, EnrollmentGeneration: h.EnrollmentGeneration, ConnectionGeneration: h.ConnectionGeneration, RunID: h.RunID, AttemptID: h.AttemptID, AttemptSequence: 1, LeaseID: h.LeaseID, LeaseGeneration: h.LeaseGeneration, FenceToken: h.FenceToken, LeaseExpiresAtUnixMicro: h.LeaseExpiresAt.UnixMicro(), ContextDigest: h.ContextDigest, CapabilityDigest: h.CapabilityDigest, PolicyDigest: h.PolicyDigest, ExpectedAttemptRevision: h.Revision}}
	p.data.Issue.HarnessBinding = a.Job.HarnessBinding // original caller's optional expiry pointer is not owned here
	p.preparedLive = p.ledger.Accounting().LiveBytes
	return p.data, nil
}
func (p *executionTestParent) Context() (context.Context, error) { return p.retirement, nil }
func (p *executionTestParent) RetireOriginalAPIStaging(ctx context.Context, id attachedworkerdaemon.InvocationIdentity, data *attachedworkerdaemon.PreparedAPIDataV1) error {
	// This closed synthetic parent creates no filesystem staging beyond the
	// actual InvocationService credential file. Verify that same owner has
	// released it; Clear below only relinquishes funded input buffers.
	if ctx == nil || ctx != p.retirement || ctx.Err() != nil || data == nil || data != p.data ||
		data.ParentInput == nil || p.life == nil || p.life.active {
		return errors.New("synthetic staging retirement owner mismatch")
	}
	h := data.ParentInput.AcceptedAttempt
	// InvocationIdentity carries the canonical numeric lease fence; the
	// protocol FenceToken is its separately derived opaque string, not a number.
	if id.TenantID != h.TenantID || id.OwnerUserID != h.OwnerUserID || id.WorkerID != h.WorkerID ||
		id.RunID != h.RunID || id.AttemptID != h.AttemptID || id.LeaseID != h.LeaseID || id.FenceToken != h.LeaseGeneration {
		return errors.New("synthetic staging retirement invocation mismatch")
	}
	if p.life.path != "" {
		if _, err := os.Lstat(p.life.path); !os.IsNotExist(err) || p.life.releases != 1 {
			return errors.New("actual credential file not retired before staging cleanup")
		}
	}
	p.order = append(p.order, "staging_cleanup")
	if p.cleanupFailed {
		return errors.New("synthetic staging retirement failure after actual Release")
	}
	return nil
}
func (p *executionTestParent) Clear() {
	if p.data == nil && p.clonePayload == 0 {
		return
	}
	p.order = append(p.order, "clear")
	if p.data != nil {
		p.data.ParentInput.Clear()
		p.data.ChildInput.Clear()
		for _, r := range []*attachedworkerdaemon.PreparedAPIRequestV1{&p.data.ParentRequest, &p.data.ChildRequest} {
			clear(r.Body)
			clear(r.Model)
			clear(r.Schema)
			clear(r.Headers)
		}
	}
	if err := p.ledger.EndLive(p.clonePayload); err != nil {
		p.t.Errorf("retire original parent partition: %v", err)
	}
	p.clonePayload = 0
	p.data = nil
}
func (p *executionTestParent) ObserveOriginalAPIPhase(_ context.Context, r pisdkphasewire.Request) (pisdkphasewire.Reply, error) {
	p.order = append(p.order, string(r.Phase))
	if r.Phase == pisdkphasewire.PreIssue {
		// The original runner alone allocates its output-custody slot between
		// Prepare return and PreIssue. Record that independent owned partition;
		// no test can acknowledge/discard it while HTTP has no producer stamp.
		p.retainedOutputLive = p.ledger.Accounting().LiveBytes - p.preparedLive
	}
	if (r.Phase == pisdkphasewire.PhysicalSend) != p.life.active {
		return pisdkphasewire.Reply{}, errors.New("synthetic phase/callback order mismatch")
	}
	if r.Phase == pisdkphasewire.ImmutableFinalize && (p.launcher.last == nil || p.launcher.last.ProcessState == nil) {
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
	service                        *providercredential.InvocationService
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

type executionTestLauncher struct {
	launches int
	last     *exec.Cmd
}

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
	l.last = cmd
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

func httpEntrySyntheticResponse(t *testing.T, body []byte, final string) []byte {
	t.Helper()
	var request map[string]any
	if e := json.Unmarshal(body, &request); e != nil {
		t.Fatal(e)
	}
	part := map[string]any{"type": "output_text", "text": final, "annotations": []any{}}
	item := map[string]any{"id": "msg-http", "type": "message", "role": "assistant", "status": "completed", "content": []any{part}}
	response := func(status string) map[string]any {
		r := map[string]any{"id": "resp-http", "object": "response", "created_at": 0, "status": status, "error": nil, "incomplete_details": nil, "instructions": nil, "metadata": map[string]any{}, "model": request["model"], "output": []any{}, "parallel_tool_calls": true, "temperature": 1, "tool_choice": "none", "tools": []any{}, "top_p": 1, "store": false, "max_output_tokens": request["max_output_tokens"], "truncation": "disabled", "text": request["text"], "usage": nil}
		if status == "completed" {
			r["output"] = []any{item}
			r["usage"] = map[string]any{"input_tokens": 1, "output_tokens": 1, "total_tokens": 2, "input_tokens_details": map[string]any{"cached_tokens": 0}, "output_tokens_details": map[string]any{"reasoning_tokens": 0}}
		}
		return r
	}
	events := []map[string]any{
		{"type": "response.created", "sequence_number": 0, "response": response("in_progress")},
		{"type": "response.output_item.added", "sequence_number": 1, "output_index": 0, "item": map[string]any{"id": "msg-http", "type": "message", "role": "assistant", "status": "in_progress", "content": []any{}}},
		{"type": "response.content_part.added", "sequence_number": 2, "output_index": 0, "content_index": 0, "item_id": "msg-http", "part": map[string]any{"type": "output_text", "text": "", "annotations": []any{}}},
		{"type": "response.output_text.delta", "sequence_number": 3, "output_index": 0, "content_index": 0, "item_id": "msg-http", "delta": final, "logprobs": []any{}, "obfuscation": "PUBLIC"},
		{"type": "response.output_text.done", "sequence_number": 4, "output_index": 0, "content_index": 0, "item_id": "msg-http", "text": final, "logprobs": []any{}},
		{"type": "response.content_part.done", "sequence_number": 5, "output_index": 0, "content_index": 0, "item_id": "msg-http", "part": part},
		{"type": "response.output_item.done", "sequence_number": 6, "output_index": 0, "item": item},
		{"type": "response.completed", "sequence_number": 7, "response": response("completed")},
	}
	var out bytes.Buffer
	for _, event := range events {
		b, e := json.Marshal(event)
		if e != nil {
			t.Fatal(e)
		}
		fmt.Fprintf(&out, "event: %s\ndata: %s\n\n", event["type"], b)
	}
	return out.Bytes()
}

func entryPlatformUserAgent(t *testing.T) string {
	t.Helper()
	var name unix.Utsname
	if err := unix.Uname(&name); err != nil {
		t.Fatal(err)
	}
	var release strings.Builder
	for _, c := range name.Release {
		if c == 0 {
			break
		}
		release.WriteByte(byte(c))
	}
	arch := runtime.GOARCH
	if arch == "amd64" {
		arch = "x64"
	}
	return fmt.Sprintf("pi (%s %s; %s)", runtime.GOOS, release.String(), arch)
}
