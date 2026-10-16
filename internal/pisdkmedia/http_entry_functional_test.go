//go:build pi_sdk_functional && (darwin || linux)

package pisdkmedia

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/pisdkhttpwire"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Genuine pinned Pi/AgentSession/ModelRuntime/standard Responses SDK and parser,
// NEW SPU3 entry and original file lifecycle. Parent response is synthetic:
// NOT original Supervisor/Source/HTTPS/installed-profile/native-fit/MVP proof.
// No configured account, real key, model call, install or rebuild is touched.
func TestPreparedPiNativeHTTPStdioEntry(t *testing.T) {
	const sdkSource = "/Volumes/hubdisk/workspace/ai/sessionless/.gitcode/native-build/pi-sdk-6c4f36-catalog-9c40af/source"
	const node = "/opt/homebrew/Cellar/node@24/24.19.0/bin/node"
	for _, path := range []string{node, filepath.Join(sdkSource, "packages/coding-agent/dist/index.js"), filepath.Join(sdkSource, "node_modules/openai/index.mjs")} {
		if _, err := os.Stat(path); err != nil {
			t.Fatal("existing pinned prerequisite missing; no install/rebuild", err)
		}
	}
	for _, snapshot := range []bool{false, true} {
		for _, badEnd := range []bool{false, true} {
			t.Run(map[bool]string{false: "history", true: "snapshot-tail"}[snapshot]+map[bool]string{false: "/exact", true: "/bad-End"}[badEnd], func(t *testing.T) {
				cfg := initFixtureConfig(t)
				cfg.FinalSchema = []byte(attachedworkeroutput.StructuredFinalSchemaV1)
				h := sha256.Sum256(cfg.FinalSchema)
				cfg.FinalSchemaSourceDigest = hex.EncodeToString(h[:])
				cfg.Profile.Sources.FinalSchemaDigest = cfg.FinalSchemaSourceDigest
				cfg.Headers[2].Value, cfg.Headers[10].Value = entryPlatformUserAgent(t), "30"
				cfg.Model.SupportsExplicitPromptCacheMode = false
				model := strings.Replace(initFixtureModel, `"supportsExplicitPromptCacheMode":true`, `"supportsExplicitPromptCacheMode":false`, 1)
				projection, ledger := initFixtureProjection(t, cfg, snapshot)
				credentials := initFixtureCredentials(t, projection)
				plan, err := PrepareHTTPInitV1(t.Context(), projection, []byte(model), 30000, credentials.source)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					if err := plan.Close(); err != nil {
						t.Error(err)
					}
				})
				v := cfg.Profile.Limits
				l := pisdkhttpwire.Limits{FrameBytes: v.ProcessFrameBytes, InitializeBytes: v.ProcessFrameBytes - 113,
					RequestBytes: min(v.NativeRequestBytes, v.ProcessFrameBytes-85-v.RequestHeaderBytes), FinalBytes: min(v.StructuredFinalBytes, v.RawFinalBytes, v.ProcessFrameBytes-17),
					RequestHeaderBytes: v.RequestHeaderBytes, ResponseHeaderBytes: v.ResponseHeaderBytes, ResponseBytes: v.ResponseBodyBytes, ResponseChunkBytes: v.ResponseChunkBytes,
					ParentBytes: 4 << 20, ChildBytes: v.ProcessStdoutBytes, Frames: v.ProcessEvents}
				accounting, err := pisdkhttpwire.Measure(l)
				if err != nil {
					t.Fatal(err)
				}
				// Real original-plan arguments and decoder partition, prepared before
				// Issue, not manually formatted synthetic controls or a full-frame
				// replacement of the original decoder's actual funded maximum.
				args := plan.EntryArguments()
				root, err := filepath.EvalSymlinks(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				for _, asset := range fixedHTTPNativeAssets() {
					path := filepath.Join(root, asset.path)
					if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(path, asset.body, 0600); err != nil {
						t.Fatal(err)
					}
				}
				if err := os.Symlink(sdkSource, filepath.Join(root, "internal/pisdkmedia/pi")); err != nil {
					t.Fatal(err)
				}
				credentials.consume(t, func(handle ports.ProviderInvocationCredentialV1, materialization ports.ProviderCredentialMaterializationV1) error {
					body, err := plan.MaterializedBody(t.Context(), handle, materialization)
					if err != nil {
						return err
					}
					profile, err := cfg.Profile.Digest()
					if err != nil {
						return err
					}
					var nonce, binding, p [32]byte
					for i := range nonce {
						nonce[i] = 1
						binding[i] = 2
					}
					decoded, _ := hex.DecodeString(profile)
					copy(p[:], decoded)
					ctx, cancel := context.WithTimeout(t.Context(), 45*time.Second)
					defer cancel()
					commandArgs := []string{"--permission", "--allow-fs-read=" + root, "--allow-fs-read=" + sdkSource, "--allow-fs-read=" + materialization.RootDir, "--no-addons", "--disable-proto=throw", filepath.Join(root, "internal/pisdkmedia/native/http_entry.mjs")}
					cmd := exec.CommandContext(ctx, node, append(commandArgs, args[:]...)...)
					cmd.Dir = root
					cmd.Env = []string{"PI_OFFLINE=1", "NO_COLOR=1"}
					cmd.WaitDelay = 3 * time.Second
					var diagnostic boundedInitDiagnostic
					cmd.Stderr = &diagnostic
					stdin, err := cmd.StdinPipe()
					if err != nil {
						return err
					}
					defer stdin.Close()
					stdout, err := cmd.StdoutPipe()
					if err != nil {
						return err
					}
					defer stdout.Close()
					if err := cmd.Start(); err != nil {
						return err
					}
					waited := false
					defer func() {
						if !waited {
							cancel()
							_ = cmd.Wait()
						}
					}()
					write := func(f pisdkhttpwire.Frame) error {
						b := make([]byte, l.FrameBytes)
						n, e := pisdkhttpwire.EncodeInto(b, f, l)
						if e != nil {
							return e
						}
						_, e = io.Copy(stdin, bytes.NewReader(b[:n]))
						return e
					}
					if err := write(pisdkhttpwire.Frame{Kind: pisdkhttpwire.Initialize, Nonce: nonce, Binding: binding, Profile: p, Body: body}); err != nil {
						return err
					}
					request, err := readHTTPEntryFrame(stdout, l, pisdkhttpwire.FetchRequest)
					if err != nil {
						cancel()
						_ = cmd.Wait()
						waited = true
						return fmt.Errorf("actual SDK Fetch: %w; fixed diagnostic=%s", err, diagnostic.String())
					}
					if request.Profile != p || !bytes.Equal(request.Body, projection.Body()) {
						return fmt.Errorf("actual SDK changed complete original body/profile")
					}
					headers := append([]HeaderDataV1(nil), cfg.Headers...)
					sort.Slice(headers, func(i, j int) bool { return headers[i].Name < headers[j].Name })
					var canonical bytes.Buffer
					_ = binary.Write(&canonical, binary.BigEndian, uint16(len(headers)))
					for _, h := range headers {
						_ = binary.Write(&canonical, binary.BigEndian, uint16(len(h.Name)))
						canonical.WriteString(h.Name)
						_ = binary.Write(&canonical, binary.BigEndian, uint32(len(h.Value)))
						canonical.WriteString(h.Value)
					}
					if !bytes.Equal(request.Headers, canonical.Bytes()) {
						return fmt.Errorf("actual SDK nonsecret headers changed")
					}
					mac := hmac.New(sha256.New, []byte(initPublicLiteral))
					mac.Write([]byte("Sessionless/Pi/SPU3/Auth/1\x00"))
					hq := sha256.Sum256(request.Headers)
					rq := sha256.Sum256(request.Body)
					for _, field := range [][]byte{nonce[:], binding[:], p[:], []byte("POST"), []byte(cfg.Model.Endpoint), hq[:], rq[:]} {
						_ = binary.Write(mac, binary.BigEndian, uint32(len(field)))
						mac.Write(field)
					}
					if !hmac.Equal(request.MAC[:], mac.Sum(nil)) || bytes.Contains(request.Body, []byte(initPublicLiteral)) || bytes.Contains(request.Headers, []byte(initPublicLiteral)) {
						return fmt.Errorf("actual SDK HMAC/key boundary mismatch")
					}
					const final = `{"answer":"native SPU3 answer Ω","artifacts":[]}`
					entity := httpEntrySyntheticResponse(t, request.Body, final)
					digest := sha256.Sum256(entity)
					count := uint32(1 + (uint64(len(entity))-1)/accounting.ChunkBytes)
					responseHeaders := []byte{0, 1, 0, 12}
					responseHeaders = append(responseHeaders, []byte("content-type")...)
					responseHeaders = append(responseHeaders, 0, 0, 0, 17)
					responseHeaders = append(responseHeaders, []byte("text/event-stream")...)
					if err := write(pisdkhttpwire.Frame{Kind: pisdkhttpwire.ResponseStart, Count: count, Bytes: uint64(len(entity)), Digest: digest, Headers: responseHeaders}); err != nil {
						return err
					}
					for i, offset := uint32(0), 0; offset < len(entity); i++ {
						pull, e := readHTTPEntryFrame(stdout, l, pisdkhttpwire.Pull)
						if e != nil {
							return e
						}
						if pull.Index != i {
							return fmt.Errorf("pull index mismatch")
						}
						end := min(len(entity), offset+int(accounting.ChunkBytes))
						if e := write(pisdkhttpwire.Frame{Kind: pisdkhttpwire.ResponsePart, Index: i, Body: entity[offset:end]}); e != nil {
							return e
						}
						offset = end
					}
					endDigest := digest
					if badEnd {
						endDigest[0] ^= 1
					}
					if err := write(pisdkhttpwire.Frame{Kind: pisdkhttpwire.ResponseEnd, Count: count, Bytes: uint64(len(entity)), Digest: endDigest}); err != nil {
						return err
					}
					if badEnd {
						// Actual SDK request, all pulls and complete SSE already reached;
						// invalid End MUST prevent any StructuredFinal, not merely make
						// a later successful-looking process observation fail.
						var extra [1]byte
						if n, e := stdout.Read(extra[:]); n != 0 || e != io.EOF {
							return fmt.Errorf("bad End exposed stdout: n=%d err=%v", n, e)
						}
						err := cmd.Wait()
						waited = true
						if err == nil || cmd.ProcessState.ExitCode() != 71 {
							return fmt.Errorf("bad End did not deny native process: %v", err)
						}
						if strings.Contains(diagnostic.String(), initPublicLiteral) {
							return fmt.Errorf("bad End leaked public marker")
						}
						return nil
					}
					result, err := readHTTPEntryFrame(stdout, l, pisdkhttpwire.StructuredFinal)
					if err != nil {
						return fmt.Errorf("actual SDK final: %w", err)
					}
					if string(result.Body) != final {
						return fmt.Errorf("provider-authored final bytes changed")
					}
					parsed, err := attachedworkeroutput.ParseStructuredFinalV1(ctx, result.Body, v, ledger)
					if err != nil || parsed.Answer != "native SPU3 answer Ω" {
						return fmt.Errorf("strict final: %w", err)
					}
					var extra [1]byte
					if n, e := stdout.Read(extra[:]); n != 0 || e != io.EOF {
						return fmt.Errorf("trailing stdout: n=%d err=%v", n, e)
					}
					err = cmd.Wait()
					waited = true
					if err != nil {
						return fmt.Errorf("native SPU3 child: %w; fixed diagnostic=%s", err, diagnostic.String())
					}
					return nil
				})
			})
		}
	}
}

func readHTTPEntryFrame(r io.Reader, l pisdkhttpwire.Limits, kind pisdkhttpwire.Kind) (pisdkhttpwire.Frame, error) {
	var header [17]byte
	if _, e := io.ReadFull(r, header[:]); e != nil {
		return pisdkhttpwire.Frame{}, e
	}
	h, e := pisdkhttpwire.InspectHeader(header[:], pisdkhttpwire.ChildToParent, l)
	if e != nil || h.Kind != kind {
		return pisdkhttpwire.Frame{}, fmt.Errorf("unexpected child header: %w", e)
	}
	b := make([]byte, h.TotalBytes)
	copy(b, header[:])
	if _, e := io.ReadFull(r, b[17:]); e != nil {
		return pisdkhttpwire.Frame{}, e
	}
	return pisdkhttpwire.DecodeView(b, pisdkhttpwire.ChildToParent, l)
}

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
