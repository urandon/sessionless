//go:build pi_sdk_functional && (darwin || linux)

package providercredential_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/pisdkbootstrap"
	"gitcode.com/urandon/sessionless/internal/pisdkmedia"
	"gitcode.com/urandon/sessionless/internal/pisdktransport"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/providercredential"
)

// Actual production constructor -> same original file/private HTTPS factory ->
// original runner/Supervisor -> genuine pinned Pi SDK/parser. ONLY Source,
// launcher isolation, CA/DNS/loopback and the model's SSE reply are synthetic.
// This proves composition, NOT selected-profile/global MIN/MAX fit, installed
// trust/Source, private #200 producer custody, model/provider or cloud/demo.
func TestPiHTTPConstructorOriginalSDKSyntheticResponseLimits(t *testing.T) {
	const node = "/opt/homebrew/Cellar/node@24/24.19.0/bin/node"
	const sdk = "/Volumes/hubdisk/workspace/ai/sessionless/.gitcode/native-build/pi-sdk-6c4f36-catalog-9c40af/source"
	for _, path := range []string{node, filepath.Join(sdk, "packages/coding-agent/dist/index.js"), filepath.Join(sdk, "node_modules/openai/index.mjs")} {
		if _, err := os.Stat(path); err != nil {
			t.Fatal("existing pinned prerequisite missing; no install:", err)
		}
	}
	for _, mode := range []string{"history", "snapshot-tail", "denied-physical", "unsafe-tool-response", "original-response-sizing-denied", "foreign-request", "foreign-argv"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
			t.Cleanup(cancel)
			digest, err := attachedworkerdaemon.DigestExecutable(node)
			if err != nil {
				t.Fatal(err)
			}
			root, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			installHTTPNativeAssets(t, root)
			if err := os.Symlink(sdk, filepath.Join(root, "internal/pisdkmedia/pi")); err != nil {
				t.Fatal(err)
			}
			cfg := initFixtureConfig(t)
			// Explicit structural orchestration allowance BEFORE descriptor, ledger and
			// compilation; no refill/reset, no inherited media/frame/envelope cap raised.
			// A positive run here cannot certify full production/native fit.
			cfg.Profile.Limits.PreSendAllocationBytes = 512 << 20
			cfg.Profile.Limits.ProcessMemoryBytes = 1 << 30
			// This positive transport-composition fixture needs only a <=4KiB SSE
			// entity. Narrow the RESPONSE envelope before creating the profile and
			// ledger; preserve the complete original input/images and all existing
			// input/frame/secret/response-allocation/work caps. The original broad
			// structural fixture's inconsistent response sizing is not qualified.
			if mode != "original-response-sizing-denied" {
				cfg.Profile.Limits.ResponseBodyBytes = 4096
				cfg.Profile.Limits.ResponseChunkBytes = 4096
				cfg.Profile.Limits.SSEEventBytes = 4096
				cfg.Profile.Limits.SSELineBytes = 4096
			}
			cfg.Profile.Model.Endpoint = "https://fixture.invalid/responses"
			cfg.Model.Endpoint = cfg.Profile.Model.Endpoint
			cfg.FinalSchema = []byte(attachedworkeroutput.StructuredFinalSchemaV1)
			schema := sha256.Sum256(cfg.FinalSchema)
			cfg.FinalSchemaSourceDigest = hex.EncodeToString(schema[:])
			cfg.Profile.Sources.FinalSchemaDigest = cfg.FinalSchemaSourceDigest
			cfg.Profile.Sources.NativeArtifactDigest = hex.EncodeToString(digest[:])
			cfg.Model.SupportsExplicitPromptCacheMode = false
			cfg.Headers[2].Value = entryPlatformUserAgent(t)
			cfg.Headers[10].Value = "30"
			model := []byte(strings.ReplaceAll(strings.Replace(initFixtureModel, `"supportsExplicitPromptCacheMode":true`, `"supportsExplicitPromptCacheMode":false`, 1), "https://synthetic.invalid", "https://fixture.invalid"))
			id, input := fixtureInputResource(t, mode == "snapshot-tail", true)
			now := time.Now().UTC().Truncate(time.Microsecond)
			expiry := now.Add(2 * time.Hour)
			input.Job.Limits.MaxRuntime = time.Hour
			input.Job.HarnessBinding.Backend, err = cfg.Profile.Descriptor()
			if err != nil {
				t.Fatal(err)
			}
			input.Job.HarnessBinding.AttachedPiAPIInvocationPolicyDigest = domain.AttachedPiAPIInvocationPolicyMappingDigestV1(strings.Repeat("a", 64))
			input.Job.HarnessBinding.EvidenceExpiresAt = &expiry
			input.AcceptedAttempt.LeaseExpiresAt = expiry
			input.AcceptedAttempt.ContextDigest, err = domain.AttachedWorkerJobContextDigestV1(input.Job, input.Manifest)
			if err != nil {
				t.Fatal(err)
			}
			ledger := fixtureLedger(t, cfg.Profile.Limits)
			var retainedOutputLive uint64
			borrowersRetired := true // remains true until an actual executor borrows Init/request
			t.Cleanup(func() {
				if a := ledger.Accounting(); a.LiveBytes != retainedOutputLive {
					t.Errorf("retired staging changed another owner's pending output partition: actual=%+v retained_output=%d", a, retainedOutputLive)
				}
			})
			projection, err := pisdkmedia.CompileOriginalRequestV1(ctx, id, input, cfg, ledger)
			if err != nil {
				t.Fatal("compile:", err)
			}
			t.Cleanup(func() {
				if !borrowersRetired {
					t.Error("retain projection/schema under unjoined executor")
					return
				}
				projection.Clear()
				// parseWireData's Stage-A decoded schema is staging owned by the
				// original composite, separate from RequestProjection.live. Retire
				// its EXACT four-source-byte debit only after dropping config.schema,
				// never a residual total that could consume output/later-owner bytes.
				if err := ledger.EndLive(4 * uint64(len(cfg.FinalSchema))); err != nil {
					t.Error("retire owned schema Stage A:", err)
				}
			})
			beforeRequest := ledger.Accounting().LiveBytes
			request, err := projection.PrepareRequestDataV1(model, cfg.FinalSchema)
			if err != nil {
				t.Fatal("request DATA:", err)
			}
			requestLive := ledger.Accounting().LiveBytes - beforeRequest
			t.Cleanup(func() {
				if !borrowersRetired {
					t.Error("retain request DATA under unjoined executor")
					return
				}
				for _, data := range [][]byte{request.Body, request.Model, request.Schema, request.Headers} {
					clear(data)
				}
				if err := ledger.EndLive(requestLive); err != nil {
					t.Error("retire request DATA:", err)
				}
			})
			expectedBody := bytes.Clone(request.Body) // test oracle, never execution authority
			assertCompleteOriginalRequest(t, expectedBody, input)
			issue := ports.ProviderCredentialIssueRequestV1{HarnessBinding: input.Job.HarnessBinding, WorkerID: string(id.WorkerID), LeaseID: id.LeaseID, LeaseFence: id.FenceToken, ExpiresAt: now.Add(time.Hour)}
			fixture := providercredential.NewPiHTTPConstructorTestFixture(t, issue, now)
			file, err := fixture.Service.PrepareImmutableAPIFileV1(ctx, issue, "api-key", int64(cfg.CredentialMaxBytes))
			if err != nil {
				t.Fatal("actual pre-Issue file:", err)
			}
			init, err := pisdkmedia.PrepareHTTPInitV1(ctx, projection, model, 30000, file)
			if err != nil {
				t.Fatal("actual HTTP Init:", err)
			}
			t.Cleanup(func() {
				if !borrowersRetired {
					t.Error("retain Init under unjoined executor")
					return
				}
				if err := init.Close(); err != nil {
					t.Error("Init retirement:", err)
				}
			})
			use, err := file.PreparePiFileUseV1(ledger, projection.Body(), issue.HarnessBinding.Backend.BackendProfileDigest)
			if err != nil {
				t.Fatal("actual file recipe:", err)
			}
			t.Cleanup(func() {
				if !borrowersRetired {
					t.Error("retain file recipe under unjoined executor")
					return
				}
				if err := use.Close(); err != nil {
					t.Error("file-use retirement:", err)
				}
			})
			const final = `{"answer":"real constructor and private HTTP driver","artifacts":[]}`
			entity := httpEntrySyntheticResponse(t, expectedBody, final)
			if len(entity) > 4096 {
				t.Fatalf("synthetic response exceeds declared envelope: bytes=%d cap=4096", len(entity))
			}
			if mode == "unsafe-tool-response" {
				entity = []byte("event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"item\":{\"type\":\"function_call\",\"name\":\"forbidden\",\"arguments\":\"{}\"}}\n\n")
			}
			peer, transportErr := fixture.PrepareLoopback(t, use, request, entity)
			if mode == "original-response-sizing-denied" {
				if transportErr == nil || !strings.Contains(transportErr.Error(), "actual response scanner:") || peer != nil {
					t.Fatal("original broad response limits did not deny scanner before Issue", transportErr)
				}
				if bindings, reads, recoveries, writes := fixture.Counts(); bindings != 0 || reads != 0 || recoveries != 0 || writes != 0 {
					t.Fatal("denied response sizing touched credential backend", bindings, reads, recoveries, writes)
				}
				return
			}
			if transportErr != nil {
				t.Fatal("prepare real private transport:", transportErr)
			}
			prefix := []string{"--permission", "--allow-fs-read=" + root, "--allow-fs-read=" + sdk, "--allow-fs-read=" + fixture.Root, "--no-addons", "--disable-proto=throw", filepath.Join(root, "internal/pisdkmedia/native/http_entry.mjs")}
			original := attachedworkerdaemon.AttemptSpec{Executable: node, ExecutableDigest: digest, Arguments: prefix, Environment: []attachedworkerdaemon.EnvironmentVariable{{Name: "PI_OFFLINE", Value: "1"}}}
			args := init.EntryArguments()
			process := original
			process.Arguments = append(append([]string(nil), prefix...), args[:]...)
			scratch := filepath.Join(root, "supervisor")
			if err := os.Mkdir(scratch, 0700); err != nil {
				t.Fatal(err)
			}
			launcher := &executionTestLauncher{}
			supervisor, err := attachedworkerdaemon.NewSupervisor(attachedworkerdaemon.SupervisorConfig{ScratchRoot: scratch, Launcher: launcher, Timeout: time.Hour, TerminationGrace: 3 * time.Second, AllowedEnvironmentNames: []string{"PI_OFFLINE"}, AllowedReadRoots: []string{fixture.Root}, MaxStdoutBytes: int(cfg.Profile.Limits.ProcessStdoutBytes)})
			if err != nil {
				t.Fatal(err)
			}
			constructorRequest, constructorProcess := request, process
			if mode == "foreign-request" {
				constructorRequest.Profile.Model.ModelID = "foreign"
			}
			if mode == "foreign-argv" {
				constructorProcess.Arguments = append([]string(nil), process.Arguments...)
				constructorProcess.Arguments[len(constructorProcess.Arguments)-1] = "1"
			}
			execution, err := pisdkmedia.PreparePiHTTPExecutionV1(supervisor, projection, init, use, constructorRequest, constructorProcess)
			if mode == "foreign-request" || mode == "foreign-argv" {
				if err == nil || execution != nil {
					t.Fatal("foreign constructor DATA accepted", mode)
				}
				if _, again := pisdkmedia.PreparePiHTTPExecutionV1(supervisor, projection, init, use, request, process); again == nil {
					t.Fatal("failed constructor replay accepted")
				}
				if bindings, reads, recoveries, writes := fixture.Counts(); bindings != 0 || reads != 0 || recoveries != 0 || writes != 0 || peer.Dials() != 0 || launcher.launches != 0 {
					t.Fatal("foreign DATA reached Issue/process/network", bindings, reads, recoveries, writes, peer.Dials(), launcher.launches)
				}
				return
			}
			if err != nil {
				t.Fatal("REAL constructor/private factory:", err)
			}
			borrowersRetired = false
			t.Cleanup(func() {
				cancel()
				if err := execution.Close(); err != nil {
					t.Error("executor retirement:", err)
					return
				}
				borrowersRetired = true
			})
			if _, reads, recoveries, writes := fixture.Counts(); reads != 0 || recoveries != 0 || writes != 0 {
				t.Fatal("constructor touched secret backend", reads, recoveries, writes)
			}
			copyExecution := *execution
			if init.Close() == nil {
				t.Fatal("Init retired while actual executor owns it")
			}
			if _, err := pisdkmedia.PreparePiHTTPExecutionV1(supervisor, projection, init, use, request, process); err == nil {
				t.Fatal("constructor replay accepted")
			}
			retirement, stop := context.WithTimeout(context.Background(), 2*time.Hour)
			t.Cleanup(stop)
			parent := &executionTestParent{t: t, request: request, process: process, issue: issue, ledger: ledger, retirement: retirement, execution: execution, arguments: args, launcher: launcher, deny: mode == "denied-physical", closeDuring: true}
			t.Cleanup(func() {
				cancel()
				if err := execution.Close(); err != nil {
					t.Error("join executor before parent clear:", err)
					return
				}
				borrowersRetired = true
				parent.Clear()
			})
			life := &executionTestLifecycle{service: fixture.Service, order: &parent.order}
			parent.life = life
			runner, err := attachedworkerdaemon.NewPreparedAPIInvocationRunner(execution, life, parent)
			if err != nil {
				t.Fatal(err)
			}
			result, runErr := runner.Run(ctx, attachedworkerdaemon.Invocation{Identity: id, Process: original, VerifiedInput: input})
			if mode == "history" || mode == "snapshot-tail" {
				if !result.Succeeded(runErr) {
					t.Fatal("actual constructor SDK composition:", runErr, result.FailureCode, result.Process, parent.order)
				}
				if !parent.closeBlocked {
					t.Error("running original executor did not deny Close")
				}
			} else if runErr == nil {
				t.Fatal("causal denial produced success", mode)
			}
			wantDials := 1
			if mode == "denied-physical" {
				wantDials = 0
			}
			if peer.Dials() != wantDials || launcher.launches != 1 || life.issues != 1 || life.materializes != 1 || life.releases != 1 || life.active {
				t.Fatal("send/launch/lifecycle multiplicity", peer.Dials(), launcher.launches, life.issues, life.materializes, life.releases, life.active, parent.order)
			}
			if wantDials == 1 {
				raw := peer.Request(t)
				httpRequest, err := http.ReadRequest(bufio.NewReader(bytes.NewReader(raw)))
				if err != nil {
					t.Fatal("actual TLS request:", err)
				}
				body, err := io.ReadAll(httpRequest.Body)
				if err != nil {
					t.Fatal(err)
				}
				_ = httpRequest.Body.Close()
				if httpRequest.Method != "POST" || httpRequest.Host != "fixture.invalid" || httpRequest.URL.Path != "/responses" || httpRequest.Header.Get("Authorization") != "Bearer synthetic-key" || !bytes.Equal(body, expectedBody) {
					t.Fatal("actual TLS request differed from original authorized bytes")
				}
				clear(raw)
			}
			if _, reads, recoveries, writes := fixture.Counts(); reads != 1 || recoveries != 1 || writes != 0 {
				t.Error("unexpected actual secret backend calls", reads, recoveries, writes)
			}
			if _, ok := result.Process.PiStdioObservationV1(); ok {
				t.Error("HTTP mechanics minted SPU2 observation")
			}
			if _, err := runner.ReceiptCandidate(id, result, runErr, domain.AttachedWorkerTerminalSucceeded); err == nil {
				t.Error("unqualified mechanics minted private producer custody")
			}
			retainedOutputLive = parent.retainedOutputLive
			if retainedOutputLive == 0 {
				t.Fatal("original runner lost its unresolved output partition")
			}
			beforeClose := ledger.Accounting()
			if err := runner.Close(retirement); err == nil {
				t.Error("Close silently discarded unresolved HTTP output custody")
			}
			if ledger.Accounting() != beforeClose {
				t.Error("runner Close refunded pending output custody")
			}
			// Shared executor copies must not permit a second use. Deliberately empty
			// original invocation also demonstrates no fresh transport/Source authority.
			if _, err := copyExecution.RunPrepared(ctx, attachedworkerdaemon.PreparedInvocationV1{}); err == nil {
				t.Error("copied executor replay")
			}
			if err := execution.Close(); err != nil {
				t.Error(err)
			}
			if err := copyExecution.Close(); err != nil {
				t.Error("copied retirement:", err)
			}
			if err := init.Close(); err != nil {
				t.Error("Init after actual executor join:", err)
			}
			if peer.Dials() != wantDials {
				t.Error("copy/replay opened another connection")
			}
			wantOrder := []string{"prepare", "pre_issue", "issue", "materialize", "physical_send", "unlocked", "immutable_finalize", "finalize", "release", "staging_cleanup", "clear"}
			if mode == "denied-physical" || mode == "unsafe-tool-response" {
				wantOrder = []string{"prepare", "pre_issue", "issue", "materialize", "physical_send", "unlocked", "release", "staging_cleanup", "clear"}
			}
			if !reflect.DeepEqual(parent.order, wantOrder) {
				t.Error("actual phase/retirement order", parent.order)
			}
		})
	}
}

func installHTTPNativeAssets(t *testing.T, root string) {
	t.Helper()
	// Fixed closed source list mirrors production runner_assets.go. Never copies
	// WIP, a wildcard directory, fake transport or ambient provider configuration.
	paths := []string{"internal/pisdkmedia/native/bootstrap.mjs", "internal/pisdkmedia/native/credential_file.mjs", "internal/pisdkmedia/native/entry.mjs", "internal/pisdkmedia/native/request_contract.mjs", "internal/pisdkmedia/native/http_entry.mjs"}
	for _, path := range paths {
		body, err := os.ReadFile(filepath.Join("../..", path))
		if err != nil {
			t.Fatal(err)
		}
		writeNativeAsset(t, root, path, body)
	}
	for _, asset := range []struct {
		path   string
		source func() ([]byte, string)
	}{
		{"internal/pisdkmedia/native/init.mjs", pisdkmedia.InitSource},
		{"internal/pisdkbootstrap/resources.mjs", pisdkbootstrap.ResourcesSource},
		{"internal/pisdktransport/stdio.mjs", pisdktransport.StdioSource},
		{"internal/pisdktransport/framing.mjs", pisdktransport.FramingSource},
		{"internal/pisdktransport/response.mjs", pisdktransport.ResponseSource},
		{"internal/pisdktransport/policy.mjs", pisdktransport.PolicySource},
		{"internal/pisdktransport/request.mjs", pisdktransport.RequestSource},
		{"internal/pisdktransport/http_stdio.mjs", pisdktransport.HTTPStdioSource},
		{"internal/pisdktransport/authentication.mjs", pisdktransport.AuthenticationSource},
	} {
		body, digest := asset.source()
		actual := sha256.Sum256(body)
		if hex.EncodeToString(actual[:]) != digest {
			t.Fatal("native source digest", asset.path)
		}
		writeNativeAsset(t, root, asset.path, body)
	}
}
func writeNativeAsset(t *testing.T, root, path string, body []byte) {
	t.Helper()
	path = filepath.Join(root, path)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, body, 0600); err != nil {
		t.Fatal(err)
	}
}
func assertCompleteOriginalRequest(t *testing.T, body []byte, input *attachedworkerdaemon.VerifiedAttachedInputV1) {
	t.Helper()
	for _, text := range []string{"history turn 1", "history turn 2", "historic assistant summary", "newest sentinel", "90071992547409931234567890123", "1.0000000000000000001e99"} {
		if !bytes.Contains(body, []byte(text)) {
			t.Errorf("original history lost %q", text)
		}
	}
	var r struct {
		Input []struct {
			Content json.RawMessage `json:"content"`
		} `json:"input"`
	}
	if err := json.Unmarshal(body, &r); err != nil || len(r.Input) != 2 {
		t.Fatal("original request blocks:", err)
	}
	var blocks []struct {
		Type, Detail string
		ImageURL     string `json:"image_url"`
	}
	if err := json.Unmarshal(r.Input[1].Content, &blocks); err != nil {
		t.Fatal(err)
	}
	images := 0
	for _, block := range blocks {
		if block.Type == "input_image" {
			i := strings.IndexByte(block.ImageURL, ',')
			if i < 0 {
				t.Fatal("invalid data URL")
			}
			image, err := base64.StdEncoding.DecodeString(block.ImageURL[i+1:])
			if err != nil || images >= 2 || !bytes.Equal(image, input.CanonicalContext.Attachments[images*2+1].Body) || block.Detail != "auto" {
				t.Fatal("original image transformed", images, err)
			}
			images++
		}
	}
	if images != 2 {
		t.Fatal("missing original images", images)
	}
}
