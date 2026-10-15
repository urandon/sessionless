package attachedworkersealedinput

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemontransport"
	"gitcode.com/urandon/sessionless/internal/codexexec"
	"gitcode.com/urandon/sessionless/internal/domain"
)

// Valid structural P and synthetic authenticated DATA, not installed sources,
// native/tariff/fit qualification or permission to construct/run a provider.
func piDecodeProfile(t *testing.T) domain.AttachedPiSDKNativeProfileV1 {
	t.Helper()
	p := domain.AttachedPiSDKNativeProfileV1{Version: 1, ProfileID: domain.AttachedPiSDKProfileIDV1, NativeProtocolVersion: domain.AttachedPiSDKNativeProtocolV1,
		Sources: domain.AttachedPiSDKNativeSourcesV1{PiSourceRevision: "6c4f360264397c59801f6da2bdac13e3b1fcbe91", OpenAISDKVersion: "6.40.0",
			OpenAISDKIntegrity: "sha512-MWtTjd/gQt4jpbji61NTgFWJLoY/PdRJ6wG9/ZDRMYNMlBKrCrSlkLI+KgHP1vR1qT6LKSAyAqIxno6lcK9JiA==", NodeVersion: "synthetic-runtime"},
		Model:    domain.AttachedPiSDKNativeModelV1{ModelVendorID: "synthetic-vendor", ModelID: "synthetic-model", Endpoint: "https://fixture.invalid/v1/responses", ModelContextTokens: 8192, MaxBilledInputTokens: 2048, MaxOutputTokens: 1024},
		Platform: domain.AttachedPiSDKNativePlatformV1{OS: "synthetic-os", Architecture: "synthetic-architecture"},
		Limits: domain.AttachedPiSDKNativeLimitsV1{CanonicalInputBytes: 1 << 20, NativeRequestBytes: 1 << 20, PreSendAllocationBytes: 128 << 20,
			RequestHeaderBytes: 4096, ResponseHeaderBytes: 4096, ResponseBodyBytes: 1 << 20, ResponseChunkBytes: 32 << 10, SSELineBytes: 32 << 10,
			SSEEventBytes: 128 << 10, SSEEvents: 512, JSONNodes: 65536, ResponseCopyWorkBytes: 16 << 20, ResponseAllocationBytes: 8 << 20,
			ProcessMemoryBytes: 256 << 20, ProcessDiskBytes: 1 << 20, ProcessFrameBytes: 1 << 20, ProcessStdoutBytes: 8 << 20,
			ProcessEvents: 2048, JSONDepth: 20, RawFinalBytes: 192 << 10, StructuredFinalBytes: 96 << 10, AnswerBytes: 16 << 10,
			ArtifactBytes: 16 << 10, ArtifactTotalBytes: 48 << 10},
		Behavior: domain.AttachedPiSDKNativeBehaviorV1{ProviderAPI: "openai-responses", ImageDerivation: "NoTransformOriginal", ImageDetail: "auto", Transport: "http-sse", LocalToolRegistry: "empty", MaxPhysicalSends: 1}}
	for _, target := range []any{&p.Sources, &p.Model, &p.Platform} {
		v := reflect.ValueOf(target).Elem()
		for i := 0; i < v.NumField(); i++ {
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

func piDecodeInput(t *testing.T, p domain.AttachedPiSDKNativeProfileV1) (attachedworkerdaemontransport.MaterializationRequestV1, attachedworkerdaemontransport.SealedInputV1, []byte) {
	t.Helper()
	service, _, _, _, request := canonicalFixture(t)
	input, err := service.Load(t.Context(), []byte("test-connection-bearer"), request)
	if err != nil {
		t.Fatal(err)
	}
	b := &input.Job.HarnessBinding
	b.Profile, b.AgentLoopPolicy = domain.HarnessExecutionSingleTurnV2, nil
	b.Backend, err = p.Descriptor()
	if err != nil {
		t.Fatal(err)
	}
	b.Resource = domain.ProviderResourceBindingV1{Kind: domain.ProviderResourceAPIAccountV1, ResourceID: "synthetic-api", OwnerUserID: request.OwnerUserID,
		Revision: 1, CredentialMode: domain.ProviderCredentialInvocationV1, CredentialGeneration: 1}
	b.ModelVendorID, b.ModelID = p.Model.ModelVendorID, p.Model.ModelID
	b.AttachedPiAPIInvocationPolicyDigest = domain.AttachedPiAPIInvocationPolicyMappingDigestV1(strings.Repeat("b", 64))
	expires := time.Unix(2000000000, 0).UTC()
	b.EvidenceExpiresAt = &expires
	if b.ValidateForScope(request.TenantID, request.OwnerUserID, input.Job.RunID, input.Job.AttemptID, input.Job.ExecutionPlacementV2) != nil {
		t.Fatal("invalid structural API binding")
	}
	digest, err := domain.AttachedWorkerJobContextDigestV1(input.Job, input.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	input.AcceptedAttempt.ContextDigest, request.Attempt.ContextDigest = digest, decodeDigest(t, string(digest))
	body, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { clearResult(&input); clear(body) })
	return request, input, body
}

func piDecodeAcquisition(t *testing.T, ctx context.Context, request attachedworkerdaemontransport.MaterializationRequestV1, p domain.AttachedPiSDKNativeProfileV1) (*attachedworkerdaemontransport.PiInputAcquisitionV1, attachedworkerdaemontransport.MaterializationRequestV1) {
	t.Helper()
	a, err := attachedworkerdaemontransport.NewPiInputAcquisitionV1(ctx, request, p)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(a.Clear)
	request, err = a.Request()
	if err != nil {
		t.Fatal(err)
	}
	return a, request
}

func piDecodeAccounting(t *testing.T, a *attachedworkerdaemontransport.PiInputAcquisitionV1) codexexec.CanonicalMediaAccountingV1 {
	t.Helper()
	l, ok := a.OriginalLedger().(interface {
		Accounting() codexexec.CanonicalMediaAccountingV1
	})
	if !ok {
		t.Fatal("acquisition lost the concrete original canonical ledger")
	}
	return l.Accounting()
}

func TestPiInputDecodeConnectedOriginalAcquisitionAndFlattenedAttachments(t *testing.T) {
	p := piDecodeProfile(t)
	request, expected, payload := piDecodeInput(t, p)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	a, request := piDecodeAcquisition(t, ctx, request, p)
	before := piDecodeAccounting(t, a)
	calls, reads := 0, 0
	body := &inputCloseBody{reader: bytes.NewReader(payload), read: func() { reads++ }}
	client, err := NewClientSource("https://control.invalid"+PathV1, &http.Client{Transport: receiptRoundTrip(func(*http.Request) (*http.Response, error) {
		calls++
		response := receiptJSONResponse(http.StatusOK, nil)
		response.Body = body
		return response, nil
	})}, []byte("test-connection-bearer"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	got, err := client.Load(ctx, request)
	defer clearResult(&got)
	after := piDecodeAccounting(t, a)
	if err != nil || !reflect.DeepEqual(got, expected) || calls != 1 || reads != 1 || body.closes.Load() != 1 ||
		after.CopyBytes <= before.CopyBytes+2*(p.Limits.CanonicalInputBytes+1) || after.LiveBytes <= before.LiveBytes+p.Limits.CanonicalInputBytes || after.Nodes <= before.Nodes ||
		a.OriginalContext() != ctx || got.CanonicalContext == nil || len(got.CanonicalContext.Attachments) != 2 {
		t.Fatalf("actual acquired HTTP load failed: err=%v calls=%d reads=%d", err, calls, reads)
	}
	if err := client.Close(); err != nil || piDecodeAccounting(t, a) != after || !reflect.DeepEqual(got, expected) {
		t.Fatal("source Close refunded original reservations or cleared transferred input")
	}
}

func TestPiInputDecodeCannotReenterSameOriginalAcquisition(t *testing.T) {
	p := piDecodeProfile(t)
	request, _, payload := piDecodeInput(t, p)
	a, request := piDecodeAcquisition(t, t.Context(), request, p)
	calls := 0
	client, err := NewClientSource("https://control.invalid"+PathV1, &http.Client{Transport: receiptRoundTrip(func(*http.Request) (*http.Response, error) {
		calls++
		r := receiptJSONResponse(http.StatusOK, nil)
		r.Body = &inputCloseBody{reader: bytes.NewReader(payload)}
		return r, nil
	})}, []byte("test-connection-bearer"))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	first, err := client.Load(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	defer clearResult(&first)
	before := piDecodeAccounting(t, a)
	second, err := client.Load(t.Context(), request)
	defer clearResult(&second)
	if err == nil || calls != 1 || !reflect.DeepEqual(second, attachedworkerdaemontransport.SealedInputV1{}) || piDecodeAccounting(t, a) != before {
		t.Fatalf("same acquisition reentered HTTP: calls=%d err=%v", calls, err)
	}
}

func TestPiInputDecodeConnectedRejectsBeforeCopyRequestAndBeforeTypedDecode(t *testing.T) {
	for _, mode := range []string{"copy_budget", "node_budget", "mutated_request", "profile_seal", "depth", "collection_limit", "duplicate", "unknown", "case_alias", "escaped_key", "missing", "uint_exponent", "uint_leading_zero", "uint_overflow", "invalid_base64", "base64_pad_bits", "unpaired_surrogate", "credential", "response_overflow"} {
		t.Run(mode, func(t *testing.T) {
			p := piDecodeProfile(t)
			if mode == "depth" {
				p.Limits.JSONDepth = 3
			}
			request, _, payload := piDecodeInput(t, p)
			var fields map[string]json.RawMessage
			if json.Unmarshal(payload, &fields) != nil {
				t.Fatal("test payload setup")
			}
			switch mode {
			case "profile_seal":
				p.Limits.CanonicalInputBytes = uint64(len(payload) - 1)
			case "duplicate":
				payload = append(append([]byte(`{"Context":`), fields["Context"]...), append([]byte(","), payload[1:]...)...)
			case "unknown":
				payload = append([]byte(`{"unknown":0,`), payload[1:]...)
			case "case_alias":
				payload = bytes.Replace(payload, []byte(`"Job":`), []byte(`"job":`), 1)
			case "escaped_key":
				payload = bytes.Replace(payload, []byte(`"Job":`), []byte(`"J\u006fb":`), 1)
			case "missing":
				delete(fields, "Context")
				payload, _ = json.Marshal(fields)
			case "collection_limit":
				var artifacts []json.RawMessage
				if json.Unmarshal(fields["Artifacts"], &artifacts) != nil || len(artifacts) != 1 {
					t.Fatal("explicit artifact fixture changed")
				}
				items := make([]json.RawMessage, 65)
				for i := range items {
					items[i] = artifacts[0]
				}
				fields["Artifacts"], _ = json.Marshal(items)
				payload, _ = json.Marshal(fields)
			case "uint_exponent":
				payload = bytes.Replace(payload, []byte(`"version":1`), []byte(`"version":1e0`), 1)
			case "uint_leading_zero":
				payload = bytes.Replace(payload, []byte(`"version":1`), []byte(`"version":01`), 1)
			case "uint_overflow":
				payload = bytes.Replace(payload, []byte(`"version":1`), []byte(`"version":18446744073709551616`), 1)
			case "invalid_base64":
				fields["Context"] = json.RawMessage(`"Zg==="`)
				payload, _ = json.Marshal(fields)
			case "base64_pad_bits":
				fields["Context"] = json.RawMessage(`"Zh=="`)
				payload, _ = json.Marshal(fields)
			case "unpaired_surrogate":
				payload = bytes.Replace(payload, []byte(`"run_id":"run-1"`), []byte(`"run_id":"\ud800"`), 1)
			case "credential":
				payload = append([]byte(`{"Credential":{},`), payload[1:]...)
			case "response_overflow":
				payload = bytes.Repeat([]byte{' '}, maxResponseBytes+1)
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			a, request := piDecodeAcquisition(t, ctx, request, p)
			before := piDecodeAccounting(t, a)
			if mode == "copy_budget" {
				if err := a.Reserve(p.Limits.PreSendAllocationBytes-before.CopyBytes, 0, 0); err != nil {
					t.Fatal(err)
				}
			} else if mode == "node_budget" {
				if err := a.Reserve(0, 0, p.Limits.JSONNodes-before.Nodes); err != nil {
					t.Fatal(err)
				}
			} else if mode == "mutated_request" {
				request.Attempt.ContextDigest[0] ^= 1
			}
			calls, reads := 0, 0
			body := &inputCloseBody{reader: bytes.NewReader(payload), read: func() { reads++ }}
			client, err := NewClientSource("https://control.invalid"+PathV1, &http.Client{Transport: receiptRoundTrip(func(*http.Request) (*http.Response, error) {
				calls++
				response := receiptJSONResponse(http.StatusOK, nil)
				response.Body = body
				return response, nil
			})}, []byte("test-connection-bearer"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = client.Close(); clear(payload) })
			got, err := client.Load(ctx, request)
			defer clearResult(&got)
			if err == nil || !reflect.DeepEqual(got, attachedworkerdaemontransport.SealedInputV1{}) {
				t.Fatalf("malformed/unfunded original source returned data: err=%v", err)
			}
			if mode == "copy_budget" || mode == "mutated_request" {
				if calls != 0 || reads != 0 {
					t.Fatal("unfunded/foreign acquisition reached HTTP")
				}
			} else if calls != 1 || reads != 1 || body.closes.Load() != 1 {
				t.Fatal("negative did not exercise the actual held body and closed decode gate")
			}
		})
	}
}

func TestPiInputDecodeOriginalAcquisitionCloseCannotReturnLateTypedInput(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := piDecodeProfile(t)
		request, _, payload := piDecodeInput(t, p)
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		a, request := piDecodeAcquisition(t, ctx, request, p)
		entered, release := make(chan struct{}), make(chan struct{})
		var releaseOnce sync.Once
		unblock := func() { releaseOnce.Do(func() { close(release) }) }
		defer unblock()
		body := &inputCloseBody{reader: bytes.NewReader(payload), close: func() { close(entered); <-release }}
		client, err := NewClientSource("https://control.invalid"+PathV1, &http.Client{Transport: receiptRoundTrip(func(*http.Request) (*http.Response, error) {
			response := receiptJSONResponse(http.StatusOK, nil)
			response.Body = body
			return response, nil
		})}, []byte("test-connection-bearer"))
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan inputLoadOutcome, 1)
		go func() { input, err := client.Load(ctx, request); done <- inputLoadOutcome{input: input, err: err} }()
		joined := false
		defer func() {
			unblock()
			if !joined {
				got := <-done
				clearResult(&got.input)
			}
		}()
		<-entered
		_ = client.Close()
		held := piDecodeAccounting(t, a)
		if held.LiveBytes <= p.Limits.CanonicalInputBytes || ctx.Err() != nil {
			t.Fatal("source Close prematurely retired owner or cancelled original caller")
		}
		synctest.Wait()
		select {
		case early := <-done:
			joined = true
			clearResult(&early.input)
			t.Fatal("source returned before actual Body.Close joined")
		default:
		}
		unblock()
		got := <-done
		joined = true
		defer clearResult(&got.input)
		if !errors.Is(got.err, ErrUnavailable) || !reflect.DeepEqual(got.input, attachedworkerdaemontransport.SealedInputV1{}) || piDecodeAccounting(t, a) != held {
			t.Fatal("late result escaped Close or refunded acquisition before outer retirement")
		}
	})
}
