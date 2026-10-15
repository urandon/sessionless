// Package pisdkmedia compiles complete original quoted task data for independent
// comparison with the genuine standard Responses request. It has no SDK/client,
// credentials, source issuer, runner, network or serving-registration capability.
package pisdkmedia

import (
	"context"
	"crypto/sha256"
	"math"
	"strings"
	"time"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
	"gitcode.com/urandon/sessionless/internal/codexexec"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type ModelDataV1 struct {
	API, Provider, ID, Endpoint                                               string
	Vision, Reasoning, SupportsDeveloperRole, SupportsExplicitPromptCacheMode bool
	ThinkingOff                                                               *string
	// Presence distinguishes source undefined (default "none") from explicit
	// null (no off-effort). This is Model DATA, never a new P field or grant.
	ThinkingOffPresent bool
}
type HeaderDataV1 struct{ Name, Value string }
type RequestDataV1 struct {
	Profile                           *domain.AttachedPiSDKNativeProfileV1
	Model                             *ModelDataV1
	SystemPrompt                      string
	MaxOutputTokens                   uint64
	ReasoningEffort, ReasoningSummary *string
	Temperature                       *float64
	// Raw ordered schema DATA and its original source digest, not a caller fit
	// certificate. Production #200 schema/source/native qualification is absent.
	FinalSchema                              []byte
	FinalSchemaSourceDigest, FinalSchemaName string
	Headers                                  []HeaderDataV1 // exactly eleven nonsecret app headers
	CredentialMaxBytes                       uint64         // original delivery bound; no key obtained here
}
type RequestAccountingV1 struct {
	Before, After                                                                           codexexec.CanonicalMediaAccountingV1
	BodyBytes, BodyUTF16, BodyNodes, TypedBlocks, HeaderBytes, GoValidationCeiling          uint64
	ResponseAllocationBytes, ResponseCopyWorkBytes, ResponseJSONNodes, EchoBytes, EchoNodes uint64
}
type RequestProjectionV1 struct {
	ctx        context.Context
	body       []byte
	data       *codexexec.CanonicalMediaDataV1
	ledger     *codexexec.CanonicalMediaLedgerV1
	accounting RequestAccountingV1
	bodyDigest [32]byte
	live       uint64
	// Scalars are immutable Go strings/values, not caller pointer/slice aliases.
	// The parsed schema already belongs to this projection's funded parse.
	config requestSnapshotV1
	// Immutable original canonical invocation/resource comparison scope. No
	// handle/expiry is minted here; the actual lifecycle still issues it later.
	scope                 ports.ProviderInvocationCredentialV1
	binding               domain.HarnessBindingV2
	bindingEvidenceExpiry time.Time
	head                  domain.AttachedWorkerAttemptV1
}

type requestSnapshotV1 struct {
	profile                                            domain.AttachedPiSDKNativeProfileV1
	model                                              ModelDataV1
	system, schemaName, schemaDigest                   string
	maxOutput, credentialMax                           uint64
	headers                                            [11]HeaderDataV1
	schema                                             *wireValue
	effort, summary, thinking                          string
	hasEffort, hasSummary, hasThinking, hasTemperature bool
	temperature                                        float64
}

func snapshotRequest(c RequestDataV1, schema *wireValue) requestSnapshotV1 {
	s := requestSnapshotV1{profile: *c.Profile, model: *c.Model, system: c.SystemPrompt,
		schemaName: c.FinalSchemaName, schemaDigest: c.FinalSchemaSourceDigest,
		maxOutput: c.MaxOutputTokens, credentialMax: c.CredentialMaxBytes, schema: schema}
	copy(s.headers[:], c.Headers)
	s.model.ThinkingOff = nil
	if c.Model.ThinkingOff != nil {
		s.thinking, s.hasThinking = *c.Model.ThinkingOff, true
	}
	if c.ReasoningEffort != nil {
		s.effort, s.hasEffort = *c.ReasoningEffort, true
	}
	if c.ReasoningSummary != nil {
		s.summary, s.hasSummary = *c.ReasoningSummary, true
	}
	if c.Temperature != nil {
		s.temperature, s.hasTemperature = *c.Temperature, true
	}
	return s
}

func (RequestProjectionV1) String() string     { return "RequestProjectionV1{data:[redacted]}" }
func (v RequestProjectionV1) GoString() string { return v.String() }
func (p *RequestProjectionV1) Body() []byte {
	if p == nil {
		return nil
	}
	return p.body
}
func (p *RequestProjectionV1) Accounting() RequestAccountingV1 {
	if p == nil {
		return RequestAccountingV1{}
	}
	return p.accounting
}
func (p *RequestProjectionV1) OriginalMessages() ([]codexexec.CanonicalMediaMessageV1, error) {
	if p == nil || p.data == nil || sha256.Sum256(p.body) != p.bodyDigest {
		return nil, ErrProjection
	}
	return p.data.OriginalMessagesV1()
}
func (p *RequestProjectionV1) Clear() {
	if p != nil {
		clear(p.body)
		if p.data != nil {
			p.data.Clear()
		}
		if p.ledger != nil {
			_ = p.ledger.EndLive(p.live)
		}
		*p = RequestProjectionV1{}
	}
}

var appHeaders = []string{"accept", "content-type", "user-agent", "x-stainless-lang", "x-stainless-package-version", "x-stainless-os", "x-stainless-arch", "x-stainless-runtime", "x-stainless-runtime-version", "x-stainless-retry-count", "x-stainless-timeout"}

func nonsecretHeaders(c RequestDataV1, l domain.AttachedPiSDKNativeLimitsV1) (uint64, uint64, error) {
	if len(c.Headers) != len(appHeaders) || c.CredentialMaxBytes == 0 || c.CredentialMaxBytes > l.RequestHeaderBytes {
		return 0, 0, ErrMissingProducer
	}
	// Protected authorization delivery is counted FIRST. Each borrowed scalar
	// is length-admitted before UTF8/trim/UTF16 walks, using the same app envelope.
	bytes, e := checkedAdd(uint64(len("authorization")+len("Bearer ")+4), c.CredentialMaxBytes)
	if e != nil || bytes > l.RequestHeaderBytes {
		return 0, 0, ErrProjection
	}
	units := uint64(len("authorization")+len("Bearer ")) + c.CredentialMaxBytes
	for i, h := range c.Headers {
		if h.Name != appHeaders[i] {
			return 0, 0, ErrProjection
		}
		cost, e := checkedAdd(uint64(len(h.Name)), uint64(len(h.Value)), 4)
		if e != nil || cost > l.RequestHeaderBytes-bytes {
			return 0, 0, ErrProjection
		}
		bytes += cost
		if h.Name != appHeaders[i] || h.Value == "" || !utf8.ValidString(h.Value) || strings.TrimSpace(h.Value) != h.Value {
			return 0, 0, ErrProjection
		}
		for _, r := range h.Value {
			if r < 32 || r == 127 {
				return 0, 0, ErrProjection
			}
		}
		switch h.Name {
		case "accept", "content-type":
			if h.Value != "application/json" {
				return 0, 0, ErrProjection
			}
		case "x-stainless-lang":
			if h.Value != "js" {
				return 0, 0, ErrProjection
			}
		case "x-stainless-package-version":
			if h.Value != "6.40.0" {
				return 0, 0, ErrProjection
			}
		case "x-stainless-runtime":
			if h.Value != "node" {
				return 0, 0, ErrProjection
			}
		case "x-stainless-retry-count":
			if h.Value != "0" {
				return 0, 0, ErrProjection
			}
		}
		units, e = checkedAdd(units, uint64(len(h.Name)), utf16Units(h.Value))
		if e != nil {
			return 0, 0, e
		}
	}
	return bytes, units, nil
}
func utf16Units(s string) uint64 {
	var n uint64
	for _, r := range s {
		n++
		if r > 0xffff {
			n++
		}
	}
	return n
}
func schemaStats(v *wireValue) (nodes, units uint64) {
	if v == nil {
		return
	}
	nodes = 1
	switch v.kind {
	case 's':
		units = utf16Units(v.text)
	case 'o':
		for _, f := range v.fields {
			a, b := schemaStats(f.value)
			nodes += 1 + a
			units += utf16Units(f.name) + b
		}
	case 'a':
		for _, x := range v.items {
			a, b := schemaStats(x)
			nodes += a
			units += b
		}
	}
	return
}

func checkData(c RequestDataV1) (domain.AttachedPiSDKNativeLimitsV1, error) {
	if c.Profile == nil || c.Model == nil || len(c.FinalSchema) == 0 || c.FinalSchemaName == "" || c.FinalSchemaSourceDigest == "" || c.Profile.Sources.FinalSchemaDigest == "" {
		return domain.AttachedPiSDKNativeLimitsV1{}, ErrMissingProducer
	}
	l := c.Profile.Limits
	for _, n := range []uint64{l.NativeRequestBytes, l.JSONDepth, l.JSONNodes, l.ResponseBodyBytes, l.ResponseChunkBytes, l.SSEEventBytes, l.ResponseHeaderBytes, l.ResponseCopyWorkBytes, l.ResponseAllocationBytes, l.RequestHeaderBytes, l.PreSendAllocationBytes, l.ProcessMemoryBytes} {
		if n == 0 || n > 9007199254740991 {
			return l, ErrMissingProducer
		}
	}
	// Cheap borrowed envelopes precede all equality/URI/UTF8/UTF16 scans.
	// Whole request allowance is a lower-bound denial cap, not field permission.
	if uint64(len(c.FinalSchema)) > l.NativeRequestBytes || len(c.FinalSchemaName) > 64 || len(c.FinalSchemaSourceDigest) != 64 || len(c.Profile.Sources.FinalSchemaDigest) != 64 {
		return l, ErrProjection
	}
	for _, s := range []string{c.SystemPrompt, c.Model.API, c.Model.Provider, c.Model.ID, c.Model.Endpoint, c.Profile.Model.ModelID, c.Profile.Model.ModelVendorID, c.Profile.Model.Endpoint} {
		if uint64(len(s)) > l.NativeRequestBytes {
			return l, ErrProjection
		}
	}
	for _, s := range []*string{c.ReasoningEffort, c.ReasoningSummary, c.Model.ThinkingOff} {
		if s != nil && uint64(len(*s)) > l.NativeRequestBytes {
			return l, ErrProjection
		}
	}
	if c.Model.API != "openai-responses" || !c.Model.Vision || c.Model.ID == "" || c.Model.Provider == "" || c.Model.Endpoint == "" || !strings.HasPrefix(c.Model.Endpoint, "https://") || strings.ContainsAny(c.Model.Endpoint, "?#@") || c.Model.ID != c.Profile.Model.ModelID || c.Model.Endpoint != c.Profile.Model.Endpoint || c.FinalSchemaSourceDigest != c.Profile.Sources.FinalSchemaDigest {
		return l, ErrProjection
	}
	if c.MaxOutputTokens < 16 || c.MaxOutputTokens > 9007199254740991 || c.MaxOutputTokens > c.Profile.Model.MaxOutputTokens || l.NativeRequestBytes == 0 || l.JSONDepth == 0 || l.JSONNodes == 0 || l.ResponseBodyBytes == 0 || l.ResponseChunkBytes == 0 || l.SSEEventBytes == 0 || l.ResponseHeaderBytes == 0 || l.ResponseCopyWorkBytes == 0 || l.ResponseAllocationBytes == 0 {
		return l, ErrMissingProducer
	}
	if len(c.FinalSchema) > int(l.NativeRequestBytes) || uint64(len(c.FinalSchemaName)) > 64 || !utf8.ValidString(c.SystemPrompt) {
		return l, ErrProjection
	}
	for _, ch := range c.FinalSchemaName {
		if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '_' || ch == '-') {
			return l, ErrProjection
		}
	}
	if c.Temperature != nil && (math.IsNaN(*c.Temperature) || math.IsInf(*c.Temperature, 0) || *c.Temperature != math.Trunc(*c.Temperature) || math.Abs(*c.Temperature) > 9007199254740991) {
		return l, ErrMissingProducer
	}
	if c.ReasoningSummary != nil && c.ReasoningEffort == nil {
		return l, ErrMissingProducer
	}
	return l, nil
}
func reasoningData(c RequestDataV1) (effort, summary string, hasSummary, include bool) {
	if !c.Model.Reasoning {
		return
	}
	if c.ReasoningEffort != nil {
		effort, summary, hasSummary, include = *c.ReasoningEffort, "auto", true, true
		if c.ReasoningSummary != nil {
			summary = *c.ReasoningSummary
		}
	} else if c.Model.Provider != "github-copilot" {
		if c.Model.ThinkingOff != nil {
			effort = *c.Model.ThinkingOff
		} else if !c.Model.ThinkingOffPresent {
			effort = "none"
		}
	}
	include = include || c.Model.Provider == "xai"
	return
}
func writeBody(w *wireWriter, c RequestDataV1, schema *wireValue, data *codexexec.CanonicalMediaDataV1) {
	w.open('{')
	w.key("model")
	w.quote(c.Model.ID, nil)
	w.literal(",")
	w.key("input")
	w.open('[')
	if c.SystemPrompt != "" {
		w.open('{')
		w.key("role")
		role := "system"
		if c.Model.Reasoning && c.Model.SupportsDeveloperRole {
			role = "developer"
		}
		w.quote(role, nil)
		w.literal(",")
		w.key("content")
		w.quote(c.SystemPrompt, nil)
		w.close('}')
		w.literal(",")
	}
	w.open('{')
	w.key("role")
	w.quote("user", nil)
	w.literal(",")
	w.key("content")
	w.open('[')
	blocks := 0
	if data.VisitBlocksV1(func(text []byte, image *codexexec.CanonicalMediaSourceDataV1) error {
		if blocks > 0 {
			w.literal(",")
		}
		blocks++
		w.open('{')
		w.key("type")
		if image == nil {
			w.quote("input_text", nil)
			w.literal(",")
			w.key("text")
			w.quote("", text)
		} else {
			w.quote("input_image", nil)
			w.literal(",")
			w.key("detail")
			w.quote("auto", nil)
			w.literal(",")
			w.key("image_url")
			w.dataURL(image.MediaType, image.Body)
		}
		w.close('}')
		return w.err
	}) != nil {
		w.err = ErrProjection
	}
	w.close(']')
	w.close('}')
	w.close(']')
	w.literal(",")
	w.key("stream")
	w.node()
	w.literal("true")
	if c.Model.SupportsExplicitPromptCacheMode {
		w.literal(",")
		w.key("prompt_cache_options")
		w.open('{')
		w.key("mode")
		w.quote("explicit", nil)
		w.close('}')
	}
	w.literal(",")
	w.key("store")
	w.node()
	w.literal("false")
	w.literal(",")
	w.key("max_output_tokens")
	w.integer(int64(c.MaxOutputTokens))
	if c.Temperature != nil {
		w.literal(",")
		w.key("temperature")
		w.integer(int64(*c.Temperature))
	}
	w.literal(",")
	w.key("tool_choice")
	w.quote("none", nil)
	if c.Model.Reasoning {
		effort, summary, hasSummary, include := reasoningData(c)
		if effort != "" {
			w.literal(",")
			w.key("reasoning")
			w.open('{')
			w.key("effort")
			w.quote(effort, nil)
			if hasSummary {
				w.literal(",")
				w.key("summary")
				w.quote(summary, nil)
			}
			w.close('}')
		}
		if include {
			w.literal(",")
			w.key("include")
			w.open('[')
			w.quote("reasoning.encrypted_content", nil)
			w.close(']')
		}
	}
	// Fixed trusted onPayload additions. Existing keys never spread/overridden.
	w.literal(",")
	w.key("truncation")
	w.quote("disabled", nil)
	w.literal(",")
	w.key("text")
	w.open('{')
	w.key("format")
	w.open('{')
	w.key("type")
	w.quote("json_schema", nil)
	w.literal(",")
	w.key("name")
	w.quote(c.FinalSchemaName, nil)
	w.literal(",")
	w.key("schema")
	w.value(schema)
	w.literal(",")
	w.key("strict")
	w.node()
	w.literal("true")
	w.close('}')
	w.close('}')
	w.close('}')
}

// CompileOriginalRequestV1 returns only DATA and accounting. Profile/source
// equality here is structural; it cannot authorize Issue/send/native fit.
// Stage A metadata parsing and canonical prerequisites remain charged on late
// Stage C failure. No retries, second expected body or partial returned plan.
func CompileOriginalRequestV1(ctx context.Context, identity attachedworkerdaemon.InvocationIdentity, input *attachedworkerdaemon.VerifiedAttachedInputV1, c RequestDataV1, ledger *codexexec.CanonicalMediaLedgerV1) (*RequestProjectionV1, error) {
	if ctx == nil || ctx.Err() != nil {
		return nil, ErrProjection
	}
	l, e := checkData(c)
	if e != nil || ledger == nil {
		if e == nil {
			e = ErrProjection
		}
		return nil, e
	}
	if input == nil || input.Job.HarnessBinding.Profile != domain.HarnessExecutionSingleTurnV2 || input.Job.HarnessBinding.AgentLoopPolicy != nil ||
		uint64(len(input.Job.HarnessBinding.ModelID)) > l.NativeRequestBytes || uint64(len(input.Job.HarnessBinding.ModelVendorID)) > l.NativeRequestBytes || c.Model.ID != input.Job.HarnessBinding.ModelID || c.Profile.Model.ModelVendorID == "" || c.Profile.Model.ModelVendorID != input.Job.HarnessBinding.ModelVendorID {
		return nil, ErrProjection
	}
	headers, h, e := nonsecretHeaders(c, l)
	if e != nil {
		return nil, e
	}
	before := ledger.Accounting()
	schema, e := parseWireData(ctx, c.FinalSchema, l.JSONDepth, l.JSONNodes, ledger)
	if e != nil || schema.kind != 'o' {
		return nil, ErrProjection
	}
	data, e := codexexec.ProjectCanonicalMediaDataV1(ctx, identity, input, ledger)
	if e != nil {
		return nil, ErrProjection
	}
	keep := false
	defer func() {
		if !keep {
			data.Clear()
		}
	}()
	measure := wireWriter{cap: l.NativeRequestBytes, maxDepth: l.JSONDepth, maxNodes: l.JSONNodes}
	writeBody(&measure, c, schema, data)
	if measure.err != nil || measure.bytes == 0 {
		return nil, ErrProjection
	}
	_, textUnits, b64, typedNodes, blocks, e := data.NativeSizesV1()
	if e != nil {
		return nil, e
	}
	schemaNodes, schemaUnits := schemaStats(schema)
	// A declared conservative logical text appearance schedule, not object/GC
	// coefficients: typed transport/JS source/provider strings, expected bytes,
	// SDK/captured strings, preparse strings, headers and original phase transfer.
	// Exact typed block keys/literals and MIME/prefix text, not a byte-per-node
	// or arbitrary per-block coefficient.
	var labels uint64
	if data.VisitBlocksV1(func(_ []byte, image *codexexec.CanonicalMediaSourceDataV1) error {
		if image == nil {
			labels += uint64(len("typetexttext"))
		} else {
			labels += uint64(len("typemimeTypedataimage")) + utf16Units(image.MediaType) + uint64(len("data:;base64,")) + utf16Units(image.MediaType)
		}
		return nil
	}) != nil {
		return nil, ErrProjection
	}
	q, e := checkedAdd(textUnits, b64, labels, utf16Units(c.SystemPrompt), utf16Units(c.Model.ID), schemaUnits)
	if e != nil {
		return nil, e
	}
	textCopies, e := checkedMul(q, 12)
	if e != nil {
		return nil, e
	}
	bodyCopies, e := checkedMul(measure.units, 4)
	if e != nil {
		return nil, e
	}
	byteCopies, e := checkedMul(measure.bytes, 5)
	if e != nil {
		return nil, e
	}
	headerCopies, e := checkedMul(h, 6)
	if e != nil {
		return nil, e
	}
	urlCopies, e := checkedMul(utf16Units(c.Model.Endpoint), 6)
	if e != nil {
		return nil, e
	}
	echoUnits, e := checkedAdd(schemaUnits, utf16Units(c.Model.ID), utf16Units(c.FinalSchemaName), uint64(len("modelmax_output_tokensstoretruncationtool_choicetextformattypenameschemastrictjson_schemadisablednone")))
	if e != nil {
		return nil, e
	}
	echoNodes, e := checkedAdd(22, schemaNodes)
	if e != nil {
		return nil, e
	}
	if c.Temperature != nil {
		echoUnits, e = checkedAdd(echoUnits, uint64(len("temperature")))
		if e != nil {
			return nil, e
		}
		echoNodes, e = checkedAdd(echoNodes, 2)
		if e != nil {
			return nil, e
		}
	}
	effort, summary, hasSummary, _ := reasoningData(c)
	if effort != "" {
		echoUnits, e = checkedAdd(echoUnits, uint64(len("reasoningeffort")), utf16Units(effort))
		if e != nil {
			return nil, e
		}
		echoNodes, e = checkedAdd(echoNodes, 5)
		if e != nil {
			return nil, e
		}
		if hasSummary {
			echoUnits, e = checkedAdd(echoUnits, uint64(len("summary")), utf16Units(summary))
			if e != nil {
				return nil, e
			}
			echoNodes, e = checkedAdd(echoNodes, 2)
			if e != nil {
				return nil, e
			}
		}
	}
	echo, e := checkedMul(echoUnits, 2)
	if e != nil {
		return nil, e
	}
	rh, e := checkedMul(l.ResponseHeaderBytes, 4)
	if e != nil {
		return nil, e
	}
	rb, e := checkedMul(l.ResponseBodyBytes, 2)
	if e != nil {
		return nil, e
	}
	response, e := checkedAdd(l.ResponseChunkBytes, rh, rb, echo)
	if e != nil {
		return nil, e
	}
	twiceBody, e := checkedMul(measure.bytes, 2)
	if e != nil {
		return nil, e
	}
	phase, e := checkedAdd(58421, twiceBody)
	if e != nil {
		return nil, e
	}
	work, e := checkedAdd(textCopies, bodyCopies, byteCopies, headerCopies, urlCopies, phase, response)
	if e != nil {
		return nil, e
	}
	twiceNodes, e := checkedMul(measure.nodes, 2)
	if e != nil {
		return nil, e
	}
	jrequest, e := checkedAdd(twiceNodes, schemaNodes)
	if e != nil {
		return nil, e
	}
	nodes, e := checkedAdd(jrequest, measure.nodes, 555)
	// F10 echo and SDK-free payload expected/actual/equality/freeze traversals
	// consume the SAME cumulative node ledger, never fresh full-P pools.
	payloadNodes, pe := checkedMul(measure.nodes, 5)
	if e == nil && pe == nil {
		nodes, e = checkedAdd(nodes, echoNodes, payloadNodes)
	}
	if e != nil || pe != nil || nodes >= l.JSONNodes {
		return nil, ErrProjection
	}
	// Accepted physical-owner equations. Source/K and typed output also remain
	// charged, so downstream nodes are reduced by their joint remaining ledger.
	if response >= l.ResponseAllocationBytes || l.PreSendAllocationBytes > l.ProcessMemoryBytes || response >= l.ProcessMemoryBytes-l.PreSendAllocationBytes || rh > l.ResponseCopyWorkBytes || rb >= l.ResponseCopyWorkBytes-rh {
		return nil, ErrProjection
	}
	a := min(l.ResponseAllocationBytes-response, l.ProcessMemoryBytes-l.PreSendAllocationBytes-response)
	d := l.ResponseCopyWorkBytes - rh - rb
	needNodes, e := checkedAdd(nodes, typedNodes, 7)
	if e != nil || needNodes >= ledger.Remaining().Nodes {
		return nil, ErrProjection
	}
	downNodes := ledger.Remaining().Nodes - needNodes
	twoEvent, e := checkedMul(l.SSEEventBytes, 2)
	if e != nil {
		return nil, e
	}
	copyFloor, e := checkedAdd(rb, l.SSEEventBytes)
	if e != nil || a <= twoEvent || echo > a || d <= copyFloor || echoNodes > downNodes {
		return nil, ErrProjection
	}
	// Atomically fund ALL still-unfunded work before the ONE expected emission.
	if _, e := data.PrepareOriginalMessagesV1(work, work, nodes); e != nil {
		return nil, ErrProjection
	}
	if measure.bytes > uint64(math.MaxInt) {
		return nil, ErrProjection
	}
	body := make([]byte, 0, int(measure.bytes))
	emit := wireWriter{out: body, cap: measure.bytes, maxDepth: l.JSONDepth, maxNodes: l.JSONNodes}
	writeBody(&emit, c, schema, data)
	if emit.err != nil || emit.bytes != measure.bytes || emit.units != measure.units || emit.nodes != measure.nodes || uint64(len(emit.out)) != measure.bytes {
		clear(emit.out)
		return nil, ErrProjection
	}
	keep = true
	binding := input.Job.HarnessBinding
	var bindingEvidenceExpiry time.Time
	if binding.EvidenceExpiresAt != nil {
		bindingEvidenceExpiry = binding.EvidenceExpiresAt.UTC()
	}
	binding.EvidenceExpiresAt = nil
	return &RequestProjectionV1{ctx: ctx, body: emit.out, data: data, ledger: ledger, bodyDigest: sha256.Sum256(emit.out), live: work, config: snapshotRequest(c, schema),
		binding: binding, bindingEvidenceExpiry: bindingEvidenceExpiry, head: input.AcceptedAttempt,
		scope: ports.ProviderInvocationCredentialV1{TenantID: identity.TenantID, OwnerUserID: identity.OwnerUserID, RunID: identity.RunID, AttemptID: identity.AttemptID,
			WorkerID: string(identity.WorkerID), LeaseID: identity.LeaseID, LeaseFence: identity.FenceToken, ProviderResource: input.Job.HarnessBinding.Resource},
		accounting: RequestAccountingV1{Before: before, After: ledger.Accounting(), BodyBytes: measure.bytes, BodyUTF16: measure.units, BodyNodes: measure.nodes, TypedBlocks: blocks, HeaderBytes: headers, GoValidationCeiling: 96 << 20, ResponseAllocationBytes: a, ResponseCopyWorkBytes: d, ResponseJSONNodes: downNodes, EchoBytes: echo, EchoNodes: echoNodes}}, nil
}
