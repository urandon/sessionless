package pisdkmedia

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/codexexec"
	"gitcode.com/urandon/sessionless/internal/credentiallifecycle"
	"gitcode.com/urandon/sessionless/internal/domain"
)

// This is explicit STRUCTURAL test data, not a model/profile source, serving
// value, runtime fit, key, provider or activation. It never imports an SDK.
func initFixtureConfig(t *testing.T) RequestDataV1 {
	t.Helper()
	c := fixtureConfig()
	p := c.Profile
	p.Version, p.ProfileID, p.NativeProtocolVersion = 1, domain.AttachedPiSDKProfileIDV1, domain.AttachedPiSDKNativeProtocolV1
	p.Sources.PiSourceRevision = "6c4f360264397c59801f6da2bdac13e3b1fcbe91"
	p.Sources.OpenAISDKVersion = "6.40.0"
	p.Sources.OpenAISDKIntegrity = "sha512-MWtTjd/gQt4jpbji61NTgFWJLoY/PdRJ6wG9/ZDRMYNMlBKrCrSlkLI+KgHP1vR1qT6LKSAyAqIxno6lcK9JiA=="
	p.Sources.NodeVersion = "local-init-fixture"
	p.Model.ModelContextTokens, p.Model.MaxBilledInputTokens = 65536, 8192
	p.Platform.OS, p.Platform.Architecture = "local-fixture-os", "local-fixture-arch"
	for _, section := range []any{&p.Sources, &p.Model, &p.Platform} {
		v := reflect.ValueOf(section).Elem()
		for i := 0; i < v.NumField(); i++ {
			if strings.HasSuffix(v.Type().Field(i).Name, "Digest") {
				v.Field(i).SetString(strings.Repeat("a", 64))
			}
		}
	}
	l := &p.Limits
	l.CanonicalInputBytes, l.ProcessFrameBytes = 1<<20, 1<<20
	l.ProcessStdoutBytes, l.ProcessDiskBytes = 16<<20, 16<<20
	l.ProcessEvents, l.SSEEvents, l.SSELineBytes = 4096, 4096, l.SSEEventBytes
	l.RawFinalBytes, l.StructuredFinalBytes = 256<<10, 128<<10
	l.AnswerBytes, l.ArtifactBytes, l.ArtifactTotalBytes = 32<<10, 32<<10, 64<<10
	p.Behavior = domain.AttachedPiSDKNativeBehaviorV1{ProviderAPI: "openai-responses", ImageDerivation: "NoTransformOriginal", ImageDetail: "auto", Transport: "http-sse", LocalToolRegistry: "empty", MaxPhysicalSends: 1}
	if err := p.Validate(); err != nil {
		t.Fatal("structural fixture profile:", err)
	}
	return c
}

const initFixtureModel = `{"id":"synthetic","name":"structural init fixture","api":"openai-responses","provider":"openai","baseUrl":"https://synthetic.invalid","reasoning":true,"input":["text","image"],"cost":{"input":0.125,"output":1.5,"cacheRead":1.25e-2,"cacheWrite":0,"tiers":[{"inputTokensAbove":1000,"input":0.5,"output":2.5,"cacheRead":0.1,"cacheWrite":0}]},"contextWindow":65536,"maxTokens":100,"compat":{"supportsDeveloperRole":true,"supportsExplicitPromptCacheMode":true}}`

func initFixtureFS(t *testing.T) *credentiallifecycle.PinnedSecretFileSystem {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Chmod(root, 0700); err != nil {
		t.Fatal(err)
	}
	root = filepath.Join(root, "unicode-é-quote\"-backslash\\")
	if err = os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	fs, err := credentiallifecycle.NewPinnedSecretFileSystem(root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := fs.Close(); err != nil {
			t.Error("owned fixture filesystem:", err)
		}
	})
	return fs
}

func initFixtureProjection(t *testing.T, c RequestDataV1, snapshot bool) (*RequestProjectionV1, *codexexec.CanonicalMediaLedgerV1) {
	t.Helper()
	id, input := fixtureInput(t, snapshot)
	ledger := fixtureLedger(t, c.Profile.Limits)
	p, err := CompileOriginalRequestV1(t.Context(), id, input, c, ledger)
	if err != nil {
		t.Fatal("compile original:", err)
	}
	t.Cleanup(p.Clear)
	return p, ledger
}

func TestCompleteInitOriginalHistoryImagesSchemaAndBody(t *testing.T) {
	for _, snapshot := range []bool{false, true} {
		t.Run(map[bool]string{false: "history", true: "snapshot-tail"}[snapshot], func(t *testing.T) {
			cfg := initFixtureConfig(t)
			p, ledger := initFixtureProjection(t, cfg, snapshot)
			originalBody := bytes.Clone(p.Body())
			originalMessages, err := p.OriginalMessages()
			if err != nil {
				t.Fatal(err)
			}
			// Mutate BETWEEN Compile and Prepare, not merely after Init has
			// already encoded a copy. This tests the snapshot's actual custody.
			cfg.Profile.Model.ModelID = "caller-mutated"
			cfg.Model.ID = "caller-mutated"
			cfg.Headers[0].Value = "caller-mutated"
			clear(cfg.FinalSchema)
			fs := initFixtureFS(t)
			before := ledger.Accounting()
			plan, err := PrepareInitV1(t.Context(), p, []byte(initFixtureModel), 1000, fs, "api-key")
			if err != nil {
				t.Fatal("complete pre-Issue Init:", err)
			}
			t.Cleanup(plan.Clear)
			if plan.Accounting().Before != before || plan.Accounting().After != ledger.Accounting() || ledger.Accounting().CopyBytes <= before.CopyBytes {
				t.Fatalf("same cumulative ledger: before=%+v plan=%+v actual=%+v", before, plan.Accounting(), ledger.Accounting())
			}
			literal := []byte("PUBLIC-NONCREDENTIAL-INIT-CONTROL")
			file, err := fs.CreateImmutable("api-key", literal, 128)
			if err != nil {
				t.Fatal(err)
			}
			wire, err := plan.MaterializedBody(t.Context(), file, domain.FingerprintCredential(literal))
			if err != nil {
				t.Fatal("actual immutable locator:", err)
			}
			if bytes.Contains(wire, literal) || len(wire)+17 > 1<<20 || uint64(len(wire)) > plan.Accounting().MaximumBodyBytes {
				t.Fatal("credential leak/actual frame bound")
			}
			var decoded struct {
				Version     int    `json:"version"`
				ProfileJSON string `json:"profile_json"`
				Model       struct {
					ID   string `json:"id"`
					Cost struct {
						Input     float64 `json:"input"`
						CacheRead float64 `json:"cacheRead"`
					} `json:"cost"`
				} `json:"model"`
				Messages []codexexec.CanonicalMediaMessageV1 `json:"messages"`
				Body     string                              `json:"expected_body"`
				Path     string                              `json:"credential_file"`
				Schema   json.RawMessage                     `json:"schema"`
				Headers  [][2]string                         `json:"headers"`
			}
			if err = json.Unmarshal(wire, &decoded); err != nil {
				t.Fatal("complete wire JSON:", err)
			}
			if decoded.Version != 1 || decoded.Model.ID != "synthetic" || decoded.Model.Cost.Input != 0.125 || decoded.Model.Cost.CacheRead != 0.0125 || decoded.Path != file.FilePath || decoded.Headers[0][1] != "application/json" || !bytes.Equal([]byte(decoded.Body), originalBody) || !reflect.DeepEqual(decoded.Messages, originalMessages) {
				t.Fatal("complete original content/cost/ownership/byte identity mismatch")
			}
			if _, err = domain.DecodeAttachedPiSDKNativeProfileV1([]byte(decoded.ProfileJSON)); err != nil {
				t.Fatal("full canonical P:", err)
			}
			var schema map[string]any
			if err = json.Unmarshal(decoded.Schema, &schema); err != nil {
				t.Fatal(err)
			}
			if !bytes.Contains(decoded.Schema, []byte(`"__proto__"`)) {
				t.Fatal("ordinary schema DATA key lost")
			}
			var images int
			for _, b := range decoded.Messages[0].Content {
				if b.Type == "image" {
					images++
				}
			}
			if images != 2 {
				t.Fatalf("two originals lost: %d", images)
			}
			if _, err = plan.MaterializedBody(t.Context(), file, domain.FingerprintCredential(literal)); err == nil {
				t.Fatal("second materialization accepted")
			}
			copyBefore := ledger.Accounting().CopyBytes
			plan.Clear()
			if ledger.Accounting().CopyBytes != copyBefore || ledger.Accounting().LiveBytes != before.LiveBytes {
				t.Fatal("live retirement refunded work or retained owned arena")
			}
		})
	}
}

func TestCompleteInitInvalidSourceModelAndPrerequisitesDeny(t *testing.T) {
	for _, mode := range []string{"missing-model", "fractional-context", "duplicate", "escaped-duplicate", "unknown-field", "wrong-id", "wrong-provider", "wrong-endpoint", "missing-cost", "negative-cost", "infinite-cost", "thinking-null-mismatch", "headers", "wrong-timeout", "missing-path-source", "cancelled", "frame", "mutated-body"} {
		t.Run(mode, func(t *testing.T) {
			cfg := initFixtureConfig(t)
			if mode == "frame" {
				cfg.Profile.Limits.ProcessFrameBytes = 128
			}
			p, ledger := initFixtureProjection(t, cfg, false)
			fs := initFixtureFS(t)
			model := initFixtureModel
			timeout := uint64(1000)
			ctx := t.Context()
			switch mode {
			case "missing-model":
				model = ""
			case "fractional-context":
				model = strings.Replace(model, `"contextWindow":65536`, `"contextWindow":65536.5`, 1)
			case "duplicate":
				model = strings.Replace(model, `"id":"synthetic"`, `"id":"synthetic","id":"synthetic"`, 1)
			case "escaped-duplicate":
				model = strings.Replace(model, `"id":"synthetic"`, `"id":"synthetic","\u0069d":"synthetic"`, 1)
			case "unknown-field":
				model = strings.Replace(model, `{"id"`, `{"unknown":true,"id"`, 1)
			case "wrong-id":
				model = strings.Replace(model, `"id":"synthetic"`, `"id":"other"`, 1)
			case "wrong-provider":
				model = strings.Replace(model, `"provider":"openai"`, `"provider":"vendor-not-provider"`, 1)
			case "wrong-endpoint":
				model = strings.Replace(model, "https://synthetic.invalid", "https://other.invalid", 1)
			case "missing-cost":
				model = strings.Replace(model, `"input":0.125,`, "", 1)
			case "negative-cost":
				model = strings.Replace(model, `"input":0.125`, `"input":-0.125`, 1)
			case "infinite-cost":
				model = strings.Replace(model, `"input":0.125`, `"input":1e999`, 1)
			case "thinking-null-mismatch":
				model = strings.Replace(model, `{"id"`, `{"thinkingLevelMap":{"off":null},"id"`, 1)
			case "headers":
				model = strings.Replace(model, `{"id"`, `{"headers":{"authorization":"forbidden"},"id"`, 1)
			case "wrong-timeout":
				timeout = 999
			case "missing-path-source":
				fs = nil
			case "cancelled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "mutated-body":
				p.Body()[0] = '!'
			}
			before := ledger.Accounting()
			plan, err := PrepareInitV1(ctx, p, []byte(model), timeout, fs, "api-key")
			if err == nil || plan != nil {
				t.Fatalf("mode=%s partial/invalid plan err=%v", mode, err)
			}
			if ledger.Accounting().LiveBytes != before.LiveBytes {
				t.Fatalf("mode=%s failed preparation retained temporaries before=%+v after=%+v", mode, before, ledger.Accounting())
			}
		})
	}
}

func TestCompleteInitCombinedOriginalLedgerAndForeignFileDeny(t *testing.T) {
	cfg := initFixtureConfig(t)
	p, ledger := initFixtureProjection(t, cfg, false)
	fs := initFixtureFS(t)
	// Other lawful accumulated work leaves too little for Init; no fresh pool.
	left := ledger.Remaining().CopyBytes
	if err := ledger.Reserve(left-1, 0, 0); err != nil {
		t.Fatal(err)
	}
	before := ledger.Accounting()
	if plan, err := PrepareInitV1(t.Context(), p, []byte(initFixtureModel), 1000, fs, "api-key"); err == nil || plan != nil || ledger.Accounting() != before {
		t.Fatal("joint first overflow accepted/reset/partial reserve")
	}
	p2, _ := initFixtureProjection(t, initFixtureConfig(t), false)
	plan, err := PrepareInitV1(t.Context(), p2, []byte(initFixtureModel), 1000, fs, "api-key")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(plan.Clear)
	other := initFixtureFS(t)
	literal := []byte("PUBLIC-NONCREDENTIAL-FOREIGN-CONTROL")
	foreign, err := other.CreateImmutable("api-key", literal, 128)
	if err != nil {
		t.Fatal(err)
	}
	if body, err := plan.MaterializedBody(t.Context(), foreign, domain.FingerprintCredential(literal)); err == nil || body != nil {
		t.Fatal("foreign immutable filesystem capability accepted")
	}
	if body, err := new(InitPlanV1).MaterializedBody(t.Context(), foreign, domain.FingerprintCredential(literal)); err == nil || body != nil {
		t.Fatal("zero plan accepted")
	}
}
