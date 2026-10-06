//go:build ydbintegration

package ydbintegration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemontransport"
	"gitcode.com/urandon/sessionless/internal/attachedworkerprotocol"
	"gitcode.com/urandon/sessionless/internal/attachedworkersealedinput"
	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
	"gitcode.com/urandon/sessionless/internal/testkit"
)

type aw07ProcessInput struct {
	URL     string                                                 `json:"url"`
	Trust   []byte                                                 `json:"trust"`
	Bearer  []byte                                                 `json:"bearer"`
	Request attachedworkerdaemontransport.MaterializationRequestV1 `json:"request"`
}

type aw07ProcessResult struct {
	Unavailable    bool   `json:"unavailable"`
	ContextSHA256  string `json:"context_sha256"`
	ArtifactSHA256 string `json:"artifact_sha256"`
	ArtifactCount  int    `json:"artifact_count"`
	Error          string `json:"error,omitempty"`
}

// The child receives no YDB or object-store handles or credentials. Test code
// uses the same owner-scoped HTTPS sealed-input API as the activated runtime;
// this is not an OS sandbox or a credential-path isolation proof.
func TestAW07SealedArtifactProcessChild(t *testing.T) {
	path := os.Getenv("SESSIONLESS_AW07_SEALED_CHILD")
	if path == "" {
		t.Skip("test-owned sealed-input client process")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var input aw07ProcessInput
	if err := json.Unmarshal(data, &input); err != nil {
		t.Fatal(err)
	}
	os.Clearenv()
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(input.Trust) {
		t.Fatal("test TLS root is invalid")
	}
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}}, Timeout: 45 * time.Second}
	defer client.CloseIdleConnections()
	source, err := attachedworkersealedinput.NewClientSource(input.URL+attachedworkersealedinput.PathV1, client, input.Bearer)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = source.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	sealed, err := source.Load(ctx, input.Request)
	result := aw07ProcessResult{Unavailable: errors.Is(err, attachedworkersealedinput.ErrUnavailable), ArtifactCount: len(sealed.Artifacts)}
	if err != nil && !result.Unavailable {
		result.Error = err.Error()
	}
	contextDigest := sha256.Sum256(sealed.Context)
	result.ContextSHA256 = hex.EncodeToString(contextDigest[:])
	if len(sealed.Artifacts) == 1 {
		artifactDigest := sha256.Sum256(sealed.Artifacts[0].Body)
		result.ArtifactSHA256 = hex.EncodeToString(artifactDigest[:])
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("AW07_PROCESS_RESULT=%s\n", encoded)
}

type aw07Process struct {
	cancel  context.CancelFunc
	done    chan error
	once    sync.Once
	waitErr error
	waited  bool
	output  bytes.Buffer
}

func (process *aw07Process) wait() (aw07ProcessResult, error) {
	process.once.Do(func() { process.waitErr = <-process.done })
	process.waited = true
	if process.waitErr != nil {
		return aw07ProcessResult{}, fmt.Errorf("sealed-input child: %w: %s", process.waitErr, process.output.String())
	}
	const marker = "AW07_PROCESS_RESULT="
	output := process.output.String()
	start := strings.Index(output, marker)
	if start < 0 {
		return aw07ProcessResult{}, fmt.Errorf("child result missing: %s", output)
	}
	line := strings.SplitN(output[start+len(marker):], "\n", 2)[0]
	var result aw07ProcessResult
	if err := json.Unmarshal([]byte(line), &result); err != nil {
		return aw07ProcessResult{}, fmt.Errorf("decode child result: %w: %s", err, output)
	}
	return result, nil
}

func aw07StartSealedProcess(t *testing.T, ctx context.Context, input aw07ProcessInput) *aw07Process {
	t.Helper()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "sealed-input.json")
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	childCtx, cancel := context.WithCancel(ctx)
	process := &aw07Process{cancel: cancel, done: make(chan error, 1)}
	command := exec.CommandContext(childCtx, binary, "-test.run=^TestAW07SealedArtifactProcessChild$")
	command.Env = []string{"SESSIONLESS_AW07_SEALED_CHILD=" + path}
	command.Stdout, command.Stderr = &process.output, &process.output
	if err := command.Start(); err != nil {
		cancel()
		t.Fatal(err)
	}
	go func() { process.done <- command.Wait() }()
	t.Cleanup(func() {
		if process.waited {
			process.cancel()
			return
		}
		select {
		case err := <-process.done:
			t.Errorf("unobserved sealed-input child exit: err=%v output=%s", err, process.output.String())
		default:
			process.cancel()
			select {
			case <-process.done:
			case <-time.After(5 * time.Second):
				t.Errorf("sealed-input child did not exit after cancellation")
			}
		}
	})
	return process
}

// The YDB authority boundary remains server-side, while A and B are separate
// OS processes with no shared Go memory, YDB handle, or inherited credentials.
func TestAW07TwoOwnerYDBSealedArtifactProcessRevocation(t *testing.T) {
	aStore, aDB, aWorker, aConnection, aDigest, _, _, aNow := readyAttachedWorkerForDrain(t, "aw07-process-a")
	bStore, bDB, bWorker, bConnection, bDigest, _, _, bNow := readyAttachedWorkerForDrainWithIdentity(t,
		"aw07-process-b", aWorker.TenantID, aWorker.ID)
	aContext, aArtifact := []byte("A private process context"), []byte("A private process artifact")
	bContext, bArtifact := []byte("B private process context"), []byte("B private process artifact")
	a := aw07ClaimedInputWithPayload(t, aStore, aDB, aWorker, aConnection, aDigest, aNow,
		attachedWorkerDrainTestSuffix(t, "aw07-process-claim-a"), aContext, aArtifact)
	b := aw07ClaimedInputWithPayload(t, bStore, bDB, bWorker, bConnection, bDigest, bNow,
		attachedWorkerDrainTestSuffix(t, "aw07-process-claim-b"), bContext, bArtifact)
	if a.request.TenantID != b.request.TenantID || a.request.OwnerUserID == b.request.OwnerUserID || a.request.WorkerID != b.request.WorkerID {
		t.Fatal("process fixture must have separate owners and a colliding worker locator")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	aJob, found, err := aStore.LoadWorkerJob(ctx, a.request.TenantID, a.request.RunID)
	if err != nil || !found || len(aJob.InputManifest.Artifacts) != 1 {
		t.Fatalf("load A YDB artifact: found=%t err=%v", found, err)
	}
	bJob, found, err := bStore.LoadWorkerJob(ctx, b.request.TenantID, b.request.RunID)
	if err != nil || !found || len(bJob.InputManifest.Artifacts) != 1 {
		t.Fatalf("load B YDB artifact: found=%t err=%v", found, err)
	}
	blobs := &aw07ArtifactBlobs{objects: map[string][]byte{
		aJob.Job.ContextSnapshot.Key: aContext, aJob.InputManifest.Artifacts[0].Blob.Key: aArtifact,
		bJob.Job.ContextSnapshot.Key: bContext, bJob.InputManifest.Artifacts[0].Blob.Key: bArtifact,
	}, opens: make(map[string]int), blockedKey: aJob.InputManifest.Artifacts[0].Blob.Key,
		blocked: make(chan struct{}), release: make(chan struct{})}
	release := func() { blobs.releaseOnce.Do(func() { close(blobs.release) }) }
	transport, err := attachedworkertransport.NewService(attachedworkertransport.ServiceConfig{
		IDs: testkit.NewSequenceIDGenerator("aw07-process-"), Audience: "sessionless:attached-worker:v1",
		PlatformOffer: attachedworkerprotocol.VersionOfferV1{
			Window:    attachedworkerprotocol.VersionWindow{Minimum: 1, Maximum: 1},
			Supported: []attachedworkerprotocol.ProtocolVersion{attachedworkerprotocol.ProtocolVersionV1},
		},
		ImplementedVersions: []attachedworkerprotocol.ProtocolVersion{attachedworkerprotocol.ProtocolVersionV1},
		ChallengeLifetime:   5 * time.Minute, ChallengeRetention: time.Hour,
		PresenceTTL: 20 * time.Minute, AuthTTL: time.Hour,
		CheckpointInterval: attachedworkertransport.MinimumHeartbeatInterval,
	}, aStore, aStore)
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := attachedworkersealedinput.NewService(transport, aStore, blobs)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewTLSServer(attachedworkersealedinput.Handler(sealed))
	t.Cleanup(func() { release(); server.Close() })
	trust := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})
	aInput := aw07ProcessInput{URL: server.URL, Trust: trust,
		Bearer: aw07JoinedBearer(t, aWorker, a.request.ConnectionID, aDigest), Request: aw07ArtifactRequest(a)}
	bInput := aw07ProcessInput{URL: server.URL, Trust: trust,
		Bearer: aw07JoinedBearer(t, bWorker, b.request.ConnectionID, bDigest), Request: aw07ArtifactRequest(b)}
	foreign := aInput
	foreign.Request = bInput.Request
	foreignResult, err := aw07StartSealedProcess(t, ctx, foreign).wait()
	if err != nil || !foreignResult.Unavailable || foreignResult.ArtifactCount != 0 ||
		foreignResult.ContextSHA256 != aw07HexSHA256(nil) || blobs.totalOpens() != 0 {
		t.Fatalf("cross-owner child reached material: result=%+v err=%v opens=%d", foreignResult, err, blobs.totalOpens())
	}
	aProcess := aw07StartSealedProcess(t, ctx, aInput)
	select {
	case <-blobs.blocked:
	case <-ctx.Done():
		t.Fatalf("A process did not reach held artifact EOF: %v", ctx.Err())
	}
	bResult, err := aw07StartSealedProcess(t, ctx, bInput).wait()
	if err != nil || bResult.Unavailable || bResult.Error != "" || bResult.ArtifactCount != 1 ||
		bResult.ContextSHA256 != aw07HexSHA256(bContext) || bResult.ArtifactSHA256 != aw07HexSHA256(bArtifact) {
		t.Fatalf("B process failed while A was held: result=%+v err=%v", bResult, err)
	}
	aw07Revoke(t, aStore, ctx, aWorker)
	release()
	aResult, err := aProcess.wait()
	if err != nil || !aResult.Unavailable || aResult.ArtifactCount != 0 ||
		aResult.ContextSHA256 != aw07HexSHA256(nil) || aResult.ArtifactSHA256 != "" {
		t.Fatalf("revoked A process received material: result=%+v err=%v", aResult, err)
	}
	if got := blobs.totalOpens(); got != 4 {
		t.Fatalf("object reads=%d, want A/B own context and artifact only", got)
	}
	bResult, err = aw07StartSealedProcess(t, ctx, bInput).wait()
	if err != nil || bResult.Unavailable || bResult.ContextSHA256 != aw07HexSHA256(bContext) ||
		bResult.ArtifactSHA256 != aw07HexSHA256(bArtifact) {
		t.Fatalf("A revocation changed B process authority: result=%+v err=%v", bResult, err)
	}
}

func aw07HexSHA256(body []byte) string {
	digest := sha256.Sum256(body)
	return hex.EncodeToString(digest[:])
}
