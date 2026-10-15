package pisdkmedia

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"math"
	"path/filepath"
	"strconv"
	"strings"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/codexexec"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/providercredential"
)

// InitAccountingV1 is logical payload accounting in the SAME request ledger.
// No field is a native/V8/SDK fit, provenance, source phase or send certificate.
// Child decode/SDK accounting remains a separate required consumer of this
// reservation; this Go producer alone cannot make a profile eligible.
type InitAccountingV1 struct {
	Before, After                                              codexexec.CanonicalMediaAccountingV1
	PrefixBytes, MaximumBodyBytes, FilePathBytes, JSONNodes    uint64
	GoArenaBytes, GoEmissionBytes, GoValidationBytes           uint64
	ChildCopyBytes, ChildJSONNodes                             uint64
	EntryArgumentBytes, BootstrapCopyBytes, BootstrapLiveBytes uint64
}

// Decoder partition of the original reservation, never a new full-P pool.
type InitDecodeLimitsV1 struct {
	BodyBytes    uint64 `json:"body_bytes"`
	CopyBytes    uint64 `json:"copy_bytes"`
	JSONNodes    uint64 `json:"json_nodes"`
	JSONDepth    uint64 `json:"json_depth"`
	ProfileBytes uint64 `json:"profile_bytes"`
	RequestBytes uint64 `json:"request_bytes"`
}

// InitPlanV1 owns a prospectively funded, complete immutable Init prefix and
// a single post-Materialize locator slot. It neither issues nor owns a lifecycle
// handle. Keep it alive until the original consumer/process/pipe joins, then
// Clear. No constructor here enables a registration or constructs any SDK.
type InitPlanV1 struct {
	ctx                      context.Context
	request                  *RequestProjectionV1
	credentialFile           *providercredential.PreparedImmutableAPIFileV1
	credentialMax, pathBytes uint64
	ledger                   *codexexec.CanonicalMediaLedgerV1
	wire                     []byte
	prefixDigest             [32]byte
	modelDigest              [32]byte
	prefixLen                int
	accounting               InitAccountingV1
	live                     uint64
	spent                    bool
	decodeLimits             InitDecodeLimitsV1
	entryArguments           [10]string
}

func (InitPlanV1) String() string     { return "InitPlanV1{data:[redacted]}" }
func (p InitPlanV1) GoString() string { return p.String() }
func (p *InitPlanV1) Accounting() InitAccountingV1 {
	if p == nil {
		return InitAccountingV1{}
	}
	return p.accounting
}
func (p *InitPlanV1) DecodeLimits() InitDecodeLimitsV1 {
	if p == nil {
		return InitDecodeLimitsV1{}
	}
	return p.decodeLimits
}

// EntryArguments returns ten already-funded decimal control values by value.
// It never serializes a key, input, profile grant or new deadline; allocation
// occurred in PrepareInit BEFORE original Issue, not in RunPrepared.
func (p *InitPlanV1) EntryArguments() [10]string {
	if p == nil || p.ctx == nil || p.ctx.Err() != nil {
		return [10]string{}
	}
	return p.entryArguments
}

// MaterializedBody accepts the original lifecycle's exact handle and callback
// materialization, not a caller-created private file or fingerprint. Its SAME
// owner verifies active custody and descriptors. It does not Finalize/Release; those stay
// with the acquired parent handle AFTER the Materialize callback has unlocked.
// The once-spent result borrows this arena; never clear before actual consumers
// join. Even a failed call spends this plan, rather than permitting a retry.
func (p *InitPlanV1) MaterializedBody(ctx context.Context, handle ports.ProviderInvocationCredentialV1, materialization ports.ProviderCredentialMaterializationV1) ([]byte, error) {
	if p == nil || p.spent {
		return nil, ErrProjection
	}
	p.spent = true
	if ctx == nil || ctx.Err() != nil || p.ctx == nil || p.ctx.Err() != nil || p.credentialFile == nil ||
		uint64(len(materialization.FilePath)) != p.pathBytes ||
		!filepath.IsAbs(materialization.FilePath) || filepath.Clean(materialization.FilePath) != materialization.FilePath ||
		!utf8.ValidString(materialization.FilePath) || sha256.Sum256(p.wire[:p.prefixLen]) != p.prefixDigest ||
		p.credentialFile.VerifyMaterialized(ctx, handle, materialization) != nil {
		return nil, ErrProjection
	}
	w := wireWriter{out: p.wire[:p.prefixLen], bytes: uint64(p.prefixLen), cap: p.accounting.MaximumBodyBytes,
		maxDepth: 1, maxNodes: 1}
	w.quote(materialization.FilePath, nil)
	w.literal("}")
	if w.err != nil || ctx.Err() != nil || p.ctx.Err() != nil {
		return nil, ErrProjection
	}
	p.wire = w.out
	return p.wire, nil
}

func (p *InitPlanV1) Clear() {
	if p == nil {
		return
	}
	clear(p.wire[:cap(p.wire)])
	if p.ledger != nil {
		_ = p.ledger.EndLive(p.live)
	}
	*p = InitPlanV1{}
	// Cleared plans cannot become reusable empty constructors.
	p.spent = true
}

func field(v *wireValue, name string) *wireValue {
	if v != nil && v.kind == 'o' {
		for _, f := range v.fields {
			if f.name == name {
				return f.value
			}
		}
	}
	return nil
}
func closedFields(v *wireValue, required, optional []string) bool {
	if v == nil || v.kind != 'o' {
		return false
	}
	for _, name := range required {
		if field(v, name) == nil {
			return false
		}
	}
	for _, f := range v.fields {
		found := false
		for _, names := range [][]string{required, optional} {
			for _, name := range names {
				if name == f.name {
					found = true
				}
			}
		}
		if !found {
			return false
		}
	}
	return true
}
func stringField(v *wireValue, name, want string) bool {
	x := field(v, name)
	return x != nil && x.kind == 's' && x.text == want
}
func boolField(v *wireValue, name string, want bool) bool {
	x := field(v, name)
	return x != nil && x.kind == 'b' && x.boolean == want
}
func positiveInteger(v *wireValue) bool { return v != nil && v.kind == 'n' && v.number > 0 }
func modelRate(v *wireValue) bool {
	if v == nil {
		return false
	}
	if v.kind == 'n' {
		return v.number >= 0
	}
	if v.kind != 'r' {
		return false
	}
	n, e := strconv.ParseFloat(v.text, 64)
	return e == nil && n >= 0 && !math.IsInf(n, 0) && !math.IsNaN(n)
}

// The exact selected catalog DATA is supplied by the original source, not
// rediscovered here. Preserve fractional rates/tiers and absence versus null.
// Unsupported headers/sampling/other API compat never silently disappear.
func validateInitModel(v *wireValue, s requestSnapshotV1) bool {
	if !closedFields(v, []string{"id", "name", "api", "provider", "baseUrl", "reasoning", "input", "cost", "contextWindow", "maxTokens"}, []string{"thinkingLevelMap", "compat"}) ||
		!stringField(v, "id", s.model.ID) || !stringField(v, "api", s.model.API) || !stringField(v, "provider", s.model.Provider) ||
		!boolField(v, "reasoning", s.model.Reasoning) {
		return false
	}
	name, base, inputs, cost := field(v, "name"), field(v, "baseUrl"), field(v, "input"), field(v, "cost")
	if name.kind != 's' || name.text == "" || base.kind != 's' || strings.TrimSuffix(base.text, "/")+"/responses" != s.model.Endpoint ||
		inputs.kind != 'a' || len(inputs.items) != 2 || inputs.items[0].kind != 's' || inputs.items[1].kind != 's' ||
		!((inputs.items[0].text == "text" && inputs.items[1].text == "image") || (inputs.items[0].text == "image" && inputs.items[1].text == "text")) ||
		!positiveInteger(field(v, "contextWindow")) || uint64(field(v, "contextWindow").number) != s.profile.Model.ModelContextTokens ||
		!positiveInteger(field(v, "maxTokens")) || uint64(field(v, "maxTokens").number) < s.maxOutput {
		return false
	}
	if !closedFields(cost, []string{"input", "output", "cacheRead", "cacheWrite"}, []string{"tiers"}) {
		return false
	}
	for _, k := range []string{"input", "output", "cacheRead", "cacheWrite"} {
		if !modelRate(field(cost, k)) {
			return false
		}
	}
	if tiers := field(cost, "tiers"); tiers != nil {
		if tiers.kind != 'a' {
			return false
		}
		for _, tier := range tiers.items {
			if !closedFields(tier, []string{"input", "output", "cacheRead", "cacheWrite", "inputTokensAbove"}, nil) {
				return false
			}
			threshold := field(tier, "inputTokensAbove")
			if threshold.kind != 'n' || threshold.number < 0 {
				return false
			}
			for _, k := range []string{"input", "output", "cacheRead", "cacheWrite"} {
				if !modelRate(field(tier, k)) {
					return false
				}
			}
		}
	}
	thinking := field(v, "thinkingLevelMap")
	if thinking != nil && !closedFields(thinking, nil, []string{"off", "minimal", "low", "medium", "high", "xhigh"}) {
		return false
	}
	if thinking != nil {
		for _, f := range thinking.fields {
			if f.value.kind != 's' && f.value.kind != 'z' {
				return false
			}
		}
	}
	off := field(thinking, "off")
	if (off != nil) != s.model.ThinkingOffPresent || (off != nil && off.kind == 's') != s.hasThinking || (s.hasThinking && off.text != s.thinking) {
		return false
	}
	compat := field(v, "compat")
	if compat != nil && !closedFields(compat, nil, []string{"supportsDeveloperRole", "sessionAffinityFormat", "supportsLongCacheRetention", "supportsStrictMode", "supportsOpenAIGrammarTools", "supportsAdditionalTools", "supportsToolSearch", "supportsExplicitPromptCacheMode"}) {
		return false
	}
	if compat != nil {
		for _, f := range compat.fields {
			if f.name == "sessionAffinityFormat" {
				if f.value.kind != 's' || (f.value.text != "openai" && f.value.text != "openai-nosession" && f.value.text != "openrouter") {
					return false
				}
			} else if f.value.kind != 'b' {
				return false
			}
		}
	}
	developer, promptCache := true, false // exact pinned provider defaults, not model selection
	if x := field(compat, "supportsDeveloperRole"); x != nil {
		developer = x.boolean
	}
	if x := field(compat, "supportsExplicitPromptCacheMode"); x != nil {
		promptCache = x.boolean
	}
	return developer == s.model.SupportsDeveloperRole && promptCache == s.model.SupportsExplicitPromptCacheMode
}

func writeInitPrefix(w *wireWriter, s requestSnapshotV1, pJSON []byte, digest string, model *wireValue, messages []codexexec.CanonicalMediaMessageV1, body []byte, timeout uint64) {
	w.open('{')
	w.key("version")
	w.integer(1)
	w.literal(",")
	w.key("profile_json")
	w.quote("", pJSON)
	w.literal(",")
	w.key("profile_digest")
	w.quote(digest, nil)
	w.literal(",")
	w.key("model")
	w.value(model)
	w.literal(",")
	w.key("system_prompt")
	w.quote(s.system, nil)
	w.literal(",")
	w.key("max_output_tokens")
	w.integer(int64(s.maxOutput))
	w.literal(",")
	w.key("timeout_ms")
	w.integer(int64(timeout))
	w.literal(",")
	w.key("cache_retention")
	w.quote("none", nil)
	for _, option := range []struct {
		name, value string
		present     bool
	}{{"reasoning_effort", s.effort, s.hasEffort}, {"reasoning_summary", s.summary, s.hasSummary}} {
		w.literal(",")
		w.key(option.name)
		if option.present {
			w.quote(option.value, nil)
		} else {
			w.node()
			w.literal("null")
		}
	}
	w.literal(",")
	w.key("temperature")
	if s.hasTemperature {
		w.integer(int64(s.temperature))
	} else {
		w.node()
		w.literal("null")
	}
	w.literal(",")
	w.key("schema_name")
	w.quote(s.schemaName, nil)
	w.literal(",")
	w.key("schema_source_digest")
	w.quote(s.schemaDigest, nil)
	w.literal(",")
	w.key("schema")
	w.value(s.schema)
	w.literal(",")
	w.key("headers")
	w.open('[')
	for i, h := range s.headers {
		if i != 0 {
			w.literal(",")
		}
		w.open('[')
		w.quote(h.Name, nil)
		w.literal(",")
		w.quote(h.Value, nil)
		w.close(']')
	}
	w.close(']')
	w.literal(",")
	w.key("messages")
	w.open('[')
	for i, m := range messages {
		if i != 0 {
			w.literal(",")
		}
		w.open('{')
		w.key("role")
		w.quote(m.Role, nil)
		w.literal(",")
		w.key("content")
		w.open('[')
		for j, b := range m.Content {
			if j != 0 {
				w.literal(",")
			}
			w.open('{')
			w.key("type")
			w.quote(b.Type, nil)
			if b.Type == "text" {
				w.literal(",")
				w.key("text")
				w.quote(b.Text, nil)
			} else {
				w.literal(",")
				w.key("mimeType")
				w.quote(b.MIMEType, nil)
				w.literal(",")
				w.key("data")
				w.node()
				w.literal("\"")
				n := uint64(base64.StdEncoding.EncodedLen(len(b.Data)))
				if w.err == nil && n <= w.cap-w.bytes {
					w.bytes += n
					w.units += n
					if w.out != nil {
						w.out = base64.StdEncoding.AppendEncode(w.out, b.Data)
					}
				} else {
					w.err = ErrProjection
				}
				w.literal("\"")
			}
			w.close('}')
		}
		w.close(']')
		w.literal(",")
		w.key("timestamp")
		w.integer(m.Timestamp)
		w.close('}')
	}
	w.close(']')
	w.literal(",")
	w.key("expected_body")
	w.quote("", body) // SAME emission, no semantic reconstruction
	w.literal(",")
	w.key("credential_max_bytes")
	w.integer(int64(s.credentialMax))
	w.literal(",")
	w.key("credential_file") // only the original Materialize may fill it later
}

// PrepareInitV1 must run BEFORE Issue, using this projection's original ledger.
// It returns no partial plan. Failed stages retain cumulative work charges;
// only retired, actually owned payload may reduce live bytes. A real runner
// still owes child/SDK/native/clock/Source/final/egress qualification.
func PrepareInitV1(ctx context.Context, request *RequestProjectionV1, modelJSON []byte, timeoutMs uint64, credentialFile *providercredential.PreparedImmutableAPIFileV1) (*InitPlanV1, error) {
	if ctx == nil || ctx.Err() != nil || request == nil || request.ctx == nil || request.ctx.Err() != nil || request.data == nil || request.ledger == nil || credentialFile == nil {
		return nil, ErrProjection
	}
	scope := credentialFile.Scope()
	scope.ExpiresAt = request.scope.ExpiresAt // compare original tuple; expiry stays with Issue/parent
	if scope != request.scope || scope.ProviderResource.Kind != domain.ProviderResourceAPIAccountV1 ||
		!credentialFile.MatchesOriginalBinding(request.binding, request.bindingEvidenceExpiry) {
		return nil, ErrProjection
	}
	s, l := request.config, request.config.profile.Limits
	if timeoutMs == 0 || timeoutMs > 2147483647 || l.ProcessFrameBytes < 147 || l.ProcessFrameBytes > 1<<20 || l.CanonicalInputBytes == 0 ||
		len(modelJSON) == 0 || s.system == "" || uint64(len(modelJSON)) > l.ProcessFrameBytes-17 || uint64(len(request.body)) != request.accounting.BodyBytes ||
		s.credentialMax == 0 || s.credentialMax > math.MaxInt64 || s.profile.Validate() != nil {
		return nil, ErrMissingProducer
	}
	pathBytes, e := credentialFile.FilePathBound(ctx, s.credentialMax)
	if e != nil || pathBytes == 0 || pathBytes > (l.ProcessFrameBytes-17)/6 {
		return nil, ErrProjection
	}
	if s.headers[10].Value != strconv.FormatFloat(float64(timeoutMs)/1000, 'f', -1, 64) {
		return nil, ErrProjection
	}
	// Stage A precedes new hashing/UTF8/schema/model walks and canonical P
	// encoding. OriginalMessages: one body hash; two canonical identity passes;
	// canonical-vs-typed comparisons (including both image hashes). Seven full
	// body lengths bound these source-byte visits; this is work, not heap fit.
	validation, e := checkedMul(uint64(len(request.body)), 7)
	if e != nil {
		return nil, e
	}
	profileCopies := uint64(domain.AttachedPiSDKCanonicalMaxBytesV1)*2 + 32 + 64 // encoder scratch + returned bytes + binary/hex digest
	stageA, e := checkedAdd(validation, profileCopies, uint64(len(modelJSON)))
	before := request.ledger.Accounting()
	if e != nil || request.ledger.Reserve(stageA, profileCopies, 0) != nil {
		return nil, ErrProjection
	}
	transientLive := profileCopies
	var profileJSON []byte
	defer func() {
		clear(profileJSON)
		_ = request.ledger.EndLive(transientLive)
	}()
	messages, e := request.OriginalMessages()
	if e != nil {
		return nil, e
	}
	profileJSON, e = s.profile.CanonicalBytes()
	if e != nil {
		return nil, ErrProjection
	}
	hash := sha256.New()
	_, _ = hash.Write([]byte("sessionless.attached-pi-sdk-native-profile.v1\x00"))
	_, _ = hash.Write(profileJSON)
	profileDigest := hex.EncodeToString(hash.Sum(nil))
	// Selected-model parser keeps the request/schema integer restriction intact.
	parseCost, e := checkedMul(uint64(len(modelJSON)), 4)
	if e != nil || request.ledger.Reserve(parseCost, parseCost, 0) != nil {
		return nil, ErrProjection
	}
	transientLive += parseCost
	if !utf8.Valid(modelJSON) {
		return nil, ErrProjection
	}
	p := wireParser{ctx: ctx, b: modelJSON, maxDepth: l.JSONDepth, maxNodes: l.JSONNodes, ledger: request.ledger, modelNumbers: true}
	model, e := p.value()
	p.ws()
	if e != nil || p.at != len(modelJSON) || !validateInitModel(model, s) {
		return nil, ErrProjection
	}
	// Count nested P with the same parser/ledger, not guessed heap costs.
	profileParseCost, e := checkedMul(uint64(len(profileJSON)), 4)
	if e != nil {
		return nil, ErrProjection
	}
	profileLiveBefore := request.ledger.Accounting().LiveBytes
	profileTree, e := parseWireData(ctx, profileJSON, l.JSONDepth, l.JSONNodes, request.ledger)
	transientLive += request.ledger.Accounting().LiveBytes - profileLiveBefore
	if e != nil {
		return nil, ErrProjection
	}
	if request.ledger.Accounting().LiveBytes-profileLiveBefore != profileParseCost {
		return nil, ErrProjection
	}
	profileNodes, _ := schemaStats(profileTree)
	m := wireWriter{cap: l.ProcessFrameBytes - 17, maxDepth: l.JSONDepth, maxNodes: l.JSONNodes}
	writeInitPrefix(&m, s, profileJSON, profileDigest, model, messages, request.body, timeoutMs)
	if m.err != nil || m.depth != 1 {
		return nil, ErrProjection
	}
	pathEncoded, e := checkedMul(pathBytes, 6) // maximum JSON control escape per original UTF8 byte
	if e != nil {
		return nil, e
	}
	maximum, e := checkedAdd(m.bytes, pathEncoded, 3) // quote + closing quote + object
	if e != nil || maximum > l.ProcessFrameBytes-17 || maximum > math.MaxInt || ctx.Err() != nil {
		return nil, ErrProjection
	}
	// Stage C reserves this exact arena capacity and one emission before make.
	// Child decoding and the SDK are NOT falsely covered by a new full-P pool;
	// they must extend this same cumulative ledger before original Issue.
	// Child payload appearances: fixed SPU2 arena; input copy I; UTF16
	// source 2I; key grammar scratch 4I; parsed strings 2I; nested P/body
	// grammar+parse (4+2 each); SAME body UTF8 bind. NOT native/V8 heap fit.
	childOuter, e := checkedMul(maximum, 1+2+4+2+2) // plus one UTF16 semantic comparison visit
	if e != nil {
		return nil, ErrProjection
	}
	nested, e := checkedAdd(uint64(len(profileJSON)), uint64(len(request.body)))
	if e != nil {
		return nil, ErrProjection
	}
	childNested, e := checkedMul(nested, 4+2)
	if e != nil {
		return nil, ErrProjection
	}
	childCopy, e := checkedAdd(l.ProcessFrameBytes, childOuter, childNested, uint64(len(request.body)), uint64(len(profileJSON))) // profile digest visit
	if e != nil {
		return nil, ErrProjection
	}
	// Grammar, parsed DATA, freeze and semantic validation are four bounded
	// visits per tree; never reset between outer Init, P and expected body.
	allNodes, e := checkedAdd(m.nodes+1, profileNodes, request.accounting.BodyNodes)
	if e != nil {
		return nil, ErrProjection
	}
	childNodes, e := checkedMul(allNodes, 4)
	if e != nil {
		return nil, ErrProjection
	}
	argumentValues := [10]uint64{l.ProcessFrameBytes - 17, min(l.NativeRequestBytes, l.ProcessFrameBytes-146),
		min(l.StructuredFinalBytes, l.RawFinalBytes, l.ProcessFrameBytes-17), l.ProcessFrameBytes,
		maximum, childCopy, childNodes, l.JSONDepth, uint64(len(profileJSON)), uint64(len(request.body))}
	var argumentBytes uint64
	for _, value := range argumentValues {
		if value == 0 || value > 9007199254740991 {
			return nil, ErrProjection
		}
		argumentBytes++
		for n := value; n >= 10; n /= 10 {
			argumentBytes++
		}
	}
	// New entry-owned payload appearances: bounded file buffer+ASCII key and
	// Bearer value/validation visits. Header/model/message trees are borrowed,
	// not cloned into another full-P pool. Fixed graph/SDK/native costs remain
	// separate original qualification; these numbers are logical payload only.
	bootstrapCopy, e := checkedMul(s.credentialMax, 8)
	bootstrapLive, e2 := checkedMul(s.credentialMax, 4)
	if e != nil || e2 != nil {
		return nil, ErrProjection
	}
	bootstrapCopy += 128 + 6*pathBytes // two canonical realpath strings and bounded pathname visits
	bootstrapLive += 128 + 2*pathBytes
	work, e := checkedAdd(maximum, maximum, childCopy, 2*argumentBytes, bootstrapCopy, uint64(len(modelJSON)))
	live, e2 := checkedAdd(maximum, childCopy, argumentBytes, bootstrapLive)
	nodes, e3 := checkedAdd(m.nodes+1, childNodes)
	if e != nil || e2 != nil || e3 != nil || request.ledger.Reserve(work, live, nodes) != nil {
		return nil, ErrProjection
	}
	var entryArguments [10]string
	for i, value := range argumentValues {
		entryArguments[i] = strconv.FormatUint(value, 10)
	}
	w := wireWriter{out: make([]byte, 0, int(maximum)), cap: maximum, maxDepth: l.JSONDepth, maxNodes: l.JSONNodes}
	writeInitPrefix(&w, s, profileJSON, profileDigest, model, messages, request.body, timeoutMs)
	if w.err != nil || w.bytes != m.bytes || w.nodes != m.nodes || w.units != m.units || ctx.Err() != nil {
		clear(w.out)
		_ = request.ledger.EndLive(live)
		return nil, ErrProjection
	}
	// Model tree/canonical P temporaries no longer escape the fully emitted
	// prefix. Their exact reserved logical live appearances retire here.
	after := request.ledger.Accounting()
	after.LiveBytes -= transientLive // retired by the deferred cleanup before return
	return &InitPlanV1{ctx: request.ctx, request: request, credentialFile: credentialFile, credentialMax: s.credentialMax, pathBytes: pathBytes, ledger: request.ledger,
		entryArguments: entryArguments,
		wire:           w.out, prefixLen: len(w.out), prefixDigest: sha256.Sum256(w.out), modelDigest: sha256.Sum256(modelJSON), live: live,
		decodeLimits: InitDecodeLimitsV1{BodyBytes: maximum, CopyBytes: childCopy, JSONNodes: childNodes, JSONDepth: l.JSONDepth, ProfileBytes: uint64(len(profileJSON)), RequestBytes: uint64(len(request.body))},
		accounting:   InitAccountingV1{Before: before, After: after, PrefixBytes: m.bytes, MaximumBodyBytes: maximum, FilePathBytes: pathBytes, JSONNodes: m.nodes + 1, GoArenaBytes: maximum, GoEmissionBytes: maximum, GoValidationBytes: validation, ChildCopyBytes: childCopy, ChildJSONNodes: childNodes, EntryArgumentBytes: argumentBytes, BootstrapCopyBytes: bootstrapCopy, BootstrapLiveBytes: bootstrapLive}}, nil
}
