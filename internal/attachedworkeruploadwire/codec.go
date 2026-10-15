package attachedworkeruploadwire

import (
	"math"
	"strconv"
	"time"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// writer first measures with no destination, then emits into one exact arena.
// Validation bounds every scalar before these walks. It never grows a buffer.
type writer struct {
	b        []byte
	n, limit int
	ok       bool
}

func (w *writer) put(s string) {
	if !w.ok {
		return
	}
	if len(s) > w.limit-w.n {
		w.ok = false
		return
	}
	if w.b != nil {
		copy(w.b[w.n:], s)
	}
	w.n += len(s)
}
func (w *writer) key(k string, first bool) {
	if !first {
		w.put(",")
	}
	w.put("\"")
	w.put(k)
	w.put("\":")
}
func (w *writer) str(s string) {
	w.put("\"")
	start := 0
	for i := 0; i < len(s) && w.ok; i++ {
		if s[i] == '"' || s[i] == '\\' {
			w.put(s[start:i])
			w.put("\\")
			w.put(s[i : i+1])
			start = i + 1
		}
	}
	w.put(s[start:])
	w.put("\"")
}
func (w *writer) uint(n uint64) {
	var b [20]byte
	out := strconv.AppendUint(b[:0], n, 10)
	w.put(string(out))
}
func (w *writer) sint(n int64) {
	if n < 0 {
		w.put("-")
		w.uint(uint64(-(n + 1)) + 1)
	} else {
		w.uint(uint64(n))
	}
}
func (w *writer) boolean(v bool) {
	if v {
		w.put("true")
	} else {
		w.put("false")
	}
}
func (w *writer) sf(k, s string, first bool)        { w.key(k, first); w.str(s) }
func (w *writer) uf(k string, v uint64, first bool) { w.key(k, first); w.uint(v) }
func (w *writer) bf(k string, v bool, first bool)   { w.key(k, first); w.boolean(v) }
func (w *writer) timestamp(t time.Time) {
	w.put("{\"seconds\":")
	w.sint(t.Unix())
	w.put(",\"nanoseconds\":")
	w.uint(uint64(t.Nanosecond()))
	w.put("}")
}
func (w *writer) tf(k string, t time.Time) { w.key(k, false); w.timestamp(t) }
func writeAuth(w *writer, a ports.AttachedWorkerSealedInputAuthorization) {
	w.put("{")
	w.sf("tenant_id", string(a.TenantID), true)
	w.sf("owner_user_id", string(a.OwnerUserID), false)
	w.sf("worker_id", string(a.WorkerID), false)
	w.sf("connection_id", string(a.ConnectionID), false)
	w.uf("enrollment_generation", a.EnrollmentGeneration, false)
	w.uf("connection_generation", a.ConnectionGeneration, false)
	w.sf("run_id", string(a.RunID), false)
	w.sf("attempt_id", string(a.AttemptID), false)
	w.uf("attempt_sequence", a.AttemptSequence, false)
	w.sf("lease_id", string(a.LeaseID), false)
	w.uf("lease_generation", a.LeaseGeneration, false)
	w.sf("fence_token", string(a.FenceToken), false)
	w.key("lease_expires_at_unix_micro", false)
	w.sint(a.LeaseExpiresAtUnixMicro)
	w.sf("context_digest", string(a.ContextDigest), false)
	w.sf("capability_digest", string(a.CapabilityDigest), false)
	w.sf("policy_digest", string(a.PolicyDigest), false)
	w.uf("expected_attempt_revision", a.ExpectedAttemptRevision, false)
	w.put("}")
}
func writeObservation(w *writer, o attachedworkeroutput.ProcessObservationV1) {
	w.put("{\"version\":1,\"exit_code\":0,\"cancelled\":false,\"deadline\":false,\"runner_failed\":false,\"descendants_reaped\":true,\"boundary_released\":true,\"cleanup_succeeded\":true")
	w.bf("credential_release_required", o.CredentialReleaseRequired, false)
	w.bf("credential_released", o.CredentialReleased, false)
	w.put("}")
}
func writeMetadata(w *writer, m attachedworkeroutput.UploadArtifactMetadataV1) {
	w.put("{")
	w.uf("ordinal", uint64(m.Ordinal), true)
	w.sf("name", m.Name, false)
	w.sf("media_type", m.MediaType, false)
	w.key("size", false)
	w.sint(m.Size)
	w.sf("sha256", m.SHA256, false)
	w.sf("content_md5", m.ContentMD5, false)
	w.put("}")
}
func writeRequest(w *writer, r Request) {
	w.put("{\"version\":1")
	w.sf("operation", string(r.Operation), false)
	w.key("authorization", false)
	writeAuth(w, r.Authorization)
	if r.Operation == Admit {
		w.sf("session_id", string(r.SessionID), false)
		w.sf("harness_binding_digest", string(r.HarnessBindingDigest), false)
		w.sf("profile_digest", r.ProfileDigest, false)
		w.sf("schema_digest", r.SchemaDigest, false)
		w.sf("final_digest", r.FinalDigest, false)
		w.sf("answer_digest", r.AnswerDigest, false)
		w.sf("producer_digest", r.ProducerDigest, false)
	}
	w.sf("nonce", string(r.Nonce), false)
	if r.Operation == Claim {
		w.sf("plan_digest", r.PlanDigest, false)
		w.uf("ordinal", uint64(r.Ordinal), false)
	}
	if r.Operation == Reconcile {
		w.sf("producer_digest", r.ProducerDigest, false)
		w.sf("final_digest", r.FinalDigest, false)
	}
	w.put(",\"caller_deadline\":{\"version\":1,\"remaining_nanoseconds\":")
	w.uint(r.CallerDeadline.RemainingNanoseconds)
	w.put("}")
	if r.Operation == Admit {
		w.key("observation", false)
		writeObservation(w, r.Observation)
		w.sf("observation_digest", r.ObservationDigest, false)
		w.put(",\"artifacts\":[")
		for i, m := range r.Artifacts {
			if i > 0 {
				w.put(",")
			}
			writeMetadata(w, m)
		}
		w.put("]")
	}
	w.put("}")
}
func writeHead(w *writer, h domain.AttachedWorkerAttemptV1) {
	w.put("{\"version\":1")
	w.sf("tenant_id", string(h.TenantID), false)
	w.sf("owner_user_id", string(h.OwnerUserID), false)
	w.sf("worker_id", string(h.WorkerID), false)
	w.sf("connection_id", string(h.ConnectionID), false)
	w.sf("execution_connection_id", string(h.ExecutionConnectionID), false)
	w.sf("run_id", string(h.RunID), false)
	w.sf("attempt_id", string(h.AttemptID), false)
	w.sf("reservation_id", string(h.ReservationID), false)
	w.sf("lease_id", string(h.LeaseID), false)
	w.uf("lease_generation", h.LeaseGeneration, false)
	w.sf("fence_token", string(h.FenceToken), false)
	w.uf("enrollment_generation", h.EnrollmentGeneration, false)
	w.uf("connection_generation", h.ConnectionGeneration, false)
	w.uf("execution_connection_generation", h.ExecutionConnectionGeneration, false)
	w.sf("context_digest", string(h.ContextDigest), false)
	w.sf("capability_digest", string(h.CapabilityDigest), false)
	w.sf("policy_digest", string(h.PolicyDigest), false)
	w.put(",\"state\":\"claimed\"")
	w.uf("platform_attempt_sequence", h.PlatformAttemptSequence, false)
	w.uf("worker_attempt_sequence", h.WorkerAttemptSequence, false)
	w.uf("progress_sequence", h.ProgressSequence, false)
	w.put(",\"cancel_revision\":0,\"terminal_sequence\":0,\"terminal_status\":\"\",\"terminal_evidence_digest\":\"\"")
	w.tf("lease_expires_at", h.LeaseExpiresAt)
	w.put(",\"cancel_deadline\":null")
	w.tf("created_at", h.CreatedAt)
	w.tf("updated_at", h.UpdatedAt)
	w.uf("revision", h.Revision, false)
	w.put("}")
}
func writeCapability(w *writer, c Capability) {
	w.put("{\"version\":1,\"method\":\"PUT\"")
	w.sf("url", c.URL, false)
	w.put(",\"headers\":{")
	w.sf("content-length", c.Headers.ContentLength, true)
	w.sf("content-type", c.Headers.ContentType, false)
	w.sf("content-md5", c.Headers.ContentMD5, false)
	w.sf("if-none-match", c.Headers.IfNoneMatch, false)
	w.sf("x-amz-checksum-sha256", c.Headers.ChecksumSHA256, false)
	w.put("}")
	w.tf("expires_at", c.ExpiresAt)
	w.put("}")
}
func writeReply(w *writer, r Reply) {
	w.put("{")
	w.sf("status", string(r.Status), true)
	if r.Status == Pending || r.Status == ports.AttachedWorkerExecutionDenied {
		w.put("}")
		return
	}
	w.key("accepted_attempt", false)
	writeHead(w, r.AcceptedAttempt)
	w.sf("plan_digest", r.PlanDigest, false)
	w.tf("created_at", r.CreatedAt)
	w.tf("checked_at", r.CheckedAt)
	w.tf("expires_at", r.ExpiresAt)
	w.put(",\"ordinals\":[")
	for i, o := range r.Ordinals {
		if i > 0 {
			w.put(",")
		}
		w.put("{")
		w.uf("ordinal", uint64(o.Ordinal), true)
		w.sf("state", string(o.State), false)
		w.put("}")
	}
	w.put("]")
	w.key("capability", false)
	if r.Capability == nil {
		w.put("null")
	} else {
		writeCapability(w, *r.Capability)
	}
	w.put("}")
}
func encode(limit int, emit func(*writer)) ([]byte, error) {
	w := writer{limit: limit, ok: true}
	emit(&w)
	if !w.ok || w.n == 0 {
		return nil, ErrInvalid
	}
	b := make([]byte, w.n)
	out := writer{b: b, limit: len(b), ok: true}
	emit(&out)
	if !out.ok || out.n != len(b) {
		clear(b)
		return nil, ErrInvalid
	}
	return b, nil
}
func EncodeRequest(r Request) ([]byte, error) {
	if !validRequest(r) {
		return nil, ErrInvalid
	}
	return encode(requestLimit(r.Operation), func(w *writer) { writeRequest(w, r) })
}
func EncodeReply(r Reply) ([]byte, error) {
	if !validReply(r) {
		return nil, ErrInvalid
	}
	limit := ReplyWithoutCapabilityMaxBytes
	if r.Capability != nil {
		limit = ReplyMaxBytes
	}
	return encode(limit, func(w *writer) { writeReply(w, r) })
}

// parser runs the complete fixed grammar without owning strings/slices first.
// Only after that pass succeeds may a second pass allocate admitted fields.
// No recursive/general JSON decoder, key map, RawMessage or provider callback.
type parser struct {
	b                  []byte
	i, nodes, maxNodes int
	ok, owned          bool
}

func (p *parser) node() {
	if p.ok {
		if p.nodes >= p.maxNodes {
			p.ok = false
		} else {
			p.nodes++
		}
	}
}
func (p *parser) lit(s string) {
	if !p.ok {
		return
	}
	if len(s) > len(p.b)-p.i {
		p.ok = false
		return
	}
	for j := range s {
		if p.b[p.i+j] != s[j] {
			p.ok = false
			return
		}
	}
	p.i += len(s)
}
func (p *parser) open() { p.node(); p.lit("{") }
func (p *parser) key(k string, first bool) {
	if !first {
		p.lit(",")
	}
	p.node()
	p.lit("\"")
	p.lit(k)
	p.lit("\":")
}
func (p *parser) peek(s string) bool {
	if !p.ok || len(s) > len(p.b)-p.i {
		return false
	}
	for j := range s {
		if p.b[p.i+j] != s[j] {
			return false
		}
	}
	return true
}
func (p *parser) u(max uint64) uint64 {
	p.node()
	if !p.ok || p.i >= len(p.b) {
		p.ok = false
		return 0
	}
	start := p.i
	var n uint64
	for p.i < len(p.b) && p.b[p.i] >= '0' && p.b[p.i] <= '9' {
		d := uint64(p.b[p.i] - '0')
		if n > max/10 || n == max/10 && d > max%10 {
			p.ok = false
			return 0
		}
		n = n*10 + d
		p.i++
	}
	if p.i == start || p.i-start > 1 && p.b[start] == '0' {
		p.ok = false
	}
	return n
}
func (p *parser) s() int64 {
	neg := p.peek("-")
	if neg {
		p.i++
	}
	max := uint64(math.MaxInt64)
	if neg {
		max++
	}
	n := p.u(max)
	if neg {
		if n == 0 {
			p.ok = false
		}
		return -int64(n-1) - 1
	}
	return int64(n)
}
func (p *parser) boolean() bool {
	p.node()
	if p.peek("true") {
		p.lit("true")
		return true
	}
	p.lit("false")
	return false
}
func (p *parser) enumeration(values ...string) string {
	p.node()
	p.lit("\"")
	for _, s := range values {
		if p.peek(s + "\"") {
			p.lit(s)
			p.lit("\"")
			return s
		}
	}
	p.ok = false
	return ""
}

// str admits only the canonical escaping emitted above: quote/backslash are
// escaped; valid non-control UTF8 is literal. No alternate \u or slash aliases.
func (p *parser) str(limit int) string {
	p.node()
	p.lit("\"")
	start := p.i
	n := 0
	escaped := false
	for p.ok && p.i < len(p.b) && p.b[p.i] != '"' {
		if n >= limit {
			p.ok = false
			break
		}
		c := p.b[p.i]
		if c == '\\' {
			escaped = true
			p.i++
			if p.i >= len(p.b) || (p.b[p.i] != '"' && p.b[p.i] != '\\') {
				p.ok = false
				break
			}
			p.i++
			n++
			continue
		}
		if c < 0x20 {
			p.ok = false
			break
		}
		if c >= utf8.RuneSelf {
			r, k := utf8.DecodeRune(p.b[p.i:])
			if r == utf8.RuneError && k == 1 || k > limit-n {
				p.ok = false
				break
			}
			p.i += k
			n += k
		} else {
			p.i++
			n++
		}
	}
	end := p.i
	p.lit("\"")
	if !p.owned || !p.ok {
		return ""
	}
	if !escaped {
		return string(p.b[start:end])
	}
	// Escaped scalars have at most two controlled copies, each <=limit bytes.
	b := make([]byte, n)
	j := 0
	for i := start; i < end; i++ {
		if p.b[i] == '\\' {
			i++
		}
		b[j] = p.b[i]
		j++
	}
	return string(b)
}
func (p *parser) sf(k string, limit int, first bool) string  { p.key(k, first); return p.str(limit) }
func (p *parser) uf(k string, max uint64, first bool) uint64 { p.key(k, first); return p.u(max) }
func (p *parser) bf(k string, first bool) bool               { p.key(k, first); return p.boolean() }
func parseTime(p *parser) time.Time {
	p.open()
	p.key("seconds", true)
	s := p.s()
	n := p.uf("nanoseconds", 999999999, false)
	p.lit("}")
	if !p.ok {
		return time.Time{}
	}
	t := time.Unix(s, int64(n)).UTC()
	if t.Unix() != s || t.Nanosecond() != int(n) {
		p.ok = false
	}
	return t
}
func (p *parser) tf(k string) time.Time { p.key(k, false); return parseTime(p) }
func parseAuth(p *parser) ports.AttachedWorkerSealedInputAuthorization {
	p.open()
	a := ports.AttachedWorkerSealedInputAuthorization{}
	a.TenantID = domain.TenantID(p.sf("tenant_id", 160, true))
	a.OwnerUserID = domain.UserID(p.sf("owner_user_id", 160, false))
	a.WorkerID = domain.AttachedWorkerID(p.sf("worker_id", 160, false))
	a.ConnectionID = domain.AttachedWorkerConnectionID(p.sf("connection_id", 160, false))
	a.EnrollmentGeneration = p.uf("enrollment_generation", math.MaxUint64, false)
	a.ConnectionGeneration = p.uf("connection_generation", math.MaxUint64, false)
	a.RunID = domain.RunID(p.sf("run_id", 160, false))
	a.AttemptID = domain.AttemptID(p.sf("attempt_id", 160, false))
	a.AttemptSequence = p.uf("attempt_sequence", 1, false)
	a.LeaseID = domain.LeaseID(p.sf("lease_id", 160, false))
	a.LeaseGeneration = p.uf("lease_generation", math.MaxUint64, false)
	a.FenceToken = domain.AttachedWorkerFenceToken(p.sf("fence_token", 64, false))
	a.LeaseExpiresAtUnixMicro = int64(p.uf("lease_expires_at_unix_micro", math.MaxInt64, false))
	a.ContextDigest = domain.AttachedWorkerContextDigest(p.sf("context_digest", 64, false))
	a.CapabilityDigest = domain.AttachedWorkerCapabilityDigest(p.sf("capability_digest", 64, false))
	a.PolicyDigest = domain.AttachedWorkerPolicyDigest(p.sf("policy_digest", 64, false))
	a.ExpectedAttemptRevision = p.uf("expected_attempt_revision", math.MaxUint64, false)
	p.lit("}")
	return a
}
func parseObservation(p *parser) attachedworkeroutput.ProcessObservationV1 {
	p.open()
	o := attachedworkeroutput.ProcessObservationV1{Version: uint32(p.uf("version", 1, true)), ExitCode: int(p.uf("exit_code", 0, false))}
	o.Cancelled = p.bf("cancelled", false)
	o.Deadline = p.bf("deadline", false)
	o.RunnerFailed = p.bf("runner_failed", false)
	o.DescendantsReaped = p.bf("descendants_reaped", false)
	o.BoundaryReleased = p.bf("boundary_released", false)
	o.CleanupSucceeded = p.bf("cleanup_succeeded", false)
	o.CredentialReleaseRequired = p.bf("credential_release_required", false)
	o.CredentialReleased = p.bf("credential_released", false)
	p.lit("}")
	return o
}
func parseMetadata(p *parser) attachedworkeroutput.UploadArtifactMetadataV1 {
	p.open()
	m := attachedworkeroutput.UploadArtifactMetadataV1{Ordinal: uint32(p.uf("ordinal", 1, true)), Name: p.sf("name", 255, false)}
	p.key("media_type", false)
	m.MediaType = p.enumeration("text/plain", "application/json")
	m.Size = int64(p.uf("size", 32768, false))
	m.SHA256 = p.sf("sha256", 64, false)
	m.ContentMD5 = p.sf("content_md5", 24, false)
	p.lit("}")
	return m
}
func parseRequest(p *parser) Request {
	p.open()
	r := Request{Version: uint32(p.uf("version", 1, true))}
	p.key("operation", false)
	r.Operation = Operation(p.enumeration("admit", "claim", "reconcile"))
	p.key("authorization", false)
	r.Authorization = parseAuth(p)
	if r.Operation == Admit {
		r.SessionID = domain.SessionID(p.sf("session_id", 160, false))
		r.HarnessBindingDigest = domain.HarnessBindingDigestV2(p.sf("harness_binding_digest", 64, false))
		r.ProfileDigest = p.sf("profile_digest", 64, false)
		r.SchemaDigest = p.sf("schema_digest", 64, false)
		r.FinalDigest = p.sf("final_digest", 64, false)
		r.AnswerDigest = p.sf("answer_digest", 64, false)
		r.ProducerDigest = p.sf("producer_digest", 64, false)
	}
	r.Nonce = domain.IdempotencyKey(p.sf("nonce", 160, false))
	if r.Operation == Claim {
		r.PlanDigest = p.sf("plan_digest", 64, false)
		r.Ordinal = uint32(p.uf("ordinal", 1, false))
	}
	if r.Operation == Reconcile {
		r.ProducerDigest = p.sf("producer_digest", 64, false)
		r.FinalDigest = p.sf("final_digest", 64, false)
	}
	p.key("caller_deadline", false)
	p.open()
	r.CallerDeadline = CallerDeadline{Version: uint32(p.uf("version", 1, true)), RemainingNanoseconds: p.uf("remaining_nanoseconds", math.MaxInt64, false)}
	p.lit("}")
	if r.Operation == Admit {
		p.key("observation", false)
		r.Observation = parseObservation(p)
		r.ObservationDigest = p.sf("observation_digest", 64, false)
		p.key("artifacts", false)
		p.node()
		p.lit("[")
		var a [2]attachedworkeroutput.UploadArtifactMetadataV1
		n := 0
		for p.ok {
			if n >= 2 {
				p.ok = false
				break
			}
			a[n] = parseMetadata(p)
			n++
			if !p.peek(",") {
				break
			}
			p.lit(",")
		}
		p.lit("]")
		if p.owned && p.ok {
			r.Artifacts = make([]attachedworkeroutput.UploadArtifactMetadataV1, n)
			copy(r.Artifacts, a[:n])
		}
	}
	p.lit("}")
	return r
}
func parseHead(p *parser) domain.AttachedWorkerAttemptV1 {
	p.open()
	h := domain.AttachedWorkerAttemptV1{Version: uint32(p.uf("version", 1, true))}
	h.TenantID = domain.TenantID(p.sf("tenant_id", 160, false))
	h.OwnerUserID = domain.UserID(p.sf("owner_user_id", 160, false))
	h.WorkerID = domain.AttachedWorkerID(p.sf("worker_id", 160, false))
	h.ConnectionID = domain.AttachedWorkerConnectionID(p.sf("connection_id", 160, false))
	h.ExecutionConnectionID = domain.AttachedWorkerConnectionID(p.sf("execution_connection_id", 160, false))
	h.RunID = domain.RunID(p.sf("run_id", 160, false))
	h.AttemptID = domain.AttemptID(p.sf("attempt_id", 160, false))
	h.ReservationID = domain.QuotaReservationID(p.sf("reservation_id", 160, false))
	h.LeaseID = domain.LeaseID(p.sf("lease_id", 160, false))
	h.LeaseGeneration = p.uf("lease_generation", math.MaxUint64, false)
	h.FenceToken = domain.AttachedWorkerFenceToken(p.sf("fence_token", 64, false))
	h.EnrollmentGeneration = p.uf("enrollment_generation", math.MaxUint64, false)
	h.ConnectionGeneration = p.uf("connection_generation", math.MaxUint64, false)
	h.ExecutionConnectionGeneration = p.uf("execution_connection_generation", math.MaxUint64, false)
	h.ContextDigest = domain.AttachedWorkerContextDigest(p.sf("context_digest", 64, false))
	h.CapabilityDigest = domain.AttachedWorkerCapabilityDigest(p.sf("capability_digest", 64, false))
	h.PolicyDigest = domain.AttachedWorkerPolicyDigest(p.sf("policy_digest", 64, false))
	p.key("state", false)
	h.State = domain.AttachedWorkerAttemptState(p.enumeration("claimed"))
	h.PlatformAttemptSequence = p.uf("platform_attempt_sequence", math.MaxUint64, false)
	h.WorkerAttemptSequence = p.uf("worker_attempt_sequence", math.MaxUint64, false)
	h.ProgressSequence = p.uf("progress_sequence", math.MaxUint64, false)
	h.CancelRevision = p.uf("cancel_revision", 0, false)
	h.TerminalSequence = p.uf("terminal_sequence", 0, false)
	h.TerminalStatus = domain.AttachedWorkerTerminalStatus(p.sf("terminal_status", 0, false))
	h.TerminalEvidenceDigest = domain.AttachedWorkerTerminalEvidenceDigest(p.sf("terminal_evidence_digest", 0, false))
	h.LeaseExpiresAt = p.tf("lease_expires_at")
	p.key("cancel_deadline", false)
	p.node()
	p.lit("null")
	h.CreatedAt = p.tf("created_at")
	h.UpdatedAt = p.tf("updated_at")
	h.Revision = p.uf("revision", math.MaxUint64, false)
	p.lit("}")
	return h
}
func parseCapability(p *parser) Capability {
	p.open()
	c := Capability{Version: uint32(p.uf("version", 1, true))}
	p.key("method", false)
	c.Method = p.enumeration("PUT")
	c.URL = p.sf("url", URLMaxBytes, false)
	p.key("headers", false)
	p.open()
	c.Headers.ContentLength = p.sf("content-length", 5, true)
	p.key("content-type", false)
	c.Headers.ContentType = p.enumeration("text/plain", "application/json")
	c.Headers.ContentMD5 = p.sf("content-md5", 24, false)
	p.key("if-none-match", false)
	c.Headers.IfNoneMatch = p.enumeration("*")
	c.Headers.ChecksumSHA256 = p.sf("x-amz-checksum-sha256", 44, false)
	p.lit("}")
	c.ExpiresAt = p.tf("expires_at")
	p.lit("}")
	return c
}
func parseReply(p *parser) Reply {
	p.open()
	p.key("status", true)
	r := Reply{Status: ports.AttachedWorkerExecutionStatus(p.enumeration("applied", "replayed", "denied", "pending"))}
	if r.Status == ports.AttachedWorkerExecutionDenied || r.Status == Pending {
		p.lit("}")
		return r
	}
	p.key("accepted_attempt", false)
	r.AcceptedAttempt = parseHead(p)
	r.PlanDigest = p.sf("plan_digest", 64, false)
	r.CreatedAt = p.tf("created_at")
	r.CheckedAt = p.tf("checked_at")
	r.ExpiresAt = p.tf("expires_at")
	p.key("ordinals", false)
	p.node()
	p.lit("[")
	var a [2]Ordinal
	n := 0
	for p.ok {
		if n >= 2 {
			p.ok = false
			break
		}
		p.open()
		a[n].Ordinal = uint32(p.uf("ordinal", 1, true))
		p.key("state", false)
		a[n].State = attachedworkeroutput.UploadOrdinalStateV1(p.enumeration("unissued", "write_pending", "not_dispatched", "source_verified"))
		p.lit("}")
		n++
		if !p.peek(",") {
			break
		}
		p.lit(",")
	}
	p.lit("]")
	if p.owned && p.ok {
		r.Ordinals = make([]Ordinal, n)
		copy(r.Ordinals, a[:n])
	}
	p.key("capability", false)
	if p.peek("null") {
		p.node()
		p.lit("null")
	} else {
		c := parseCapability(p)
		if p.owned && p.ok {
			owned := c
			r.Capability = &owned
		}
	}
	p.lit("}")
	return r
}
func DecodeRequest(b []byte) (Request, error) {
	if len(b) == 0 || len(b) > RequestMaxBytes {
		return Request{}, ErrInvalid
	}
	pre := parser{b: b, ok: true, maxNodes: AdmitNodes}
	q := parseRequest(&pre)
	if !pre.ok || pre.i != len(b) || len(b) > requestLimit(q.Operation) {
		return Request{}, ErrInvalid
	}
	p := parser{b: b, ok: true, owned: true, maxNodes: AdmitNodes}
	r := parseRequest(&p)
	if !p.ok || p.i != len(b) || !validRequest(r) {
		return Request{}, ErrInvalid
	}
	return r, nil
}
func DecodeReply(b []byte) (Reply, error) {
	if len(b) == 0 || len(b) > ReplyMaxBytes {
		return Reply{}, ErrInvalid
	}
	pre := parser{b: b, ok: true, maxNodes: ReplyNodes}
	parseReply(&pre)
	if !pre.ok || pre.i != len(b) {
		return Reply{}, ErrInvalid
	}
	p := parser{b: b, ok: true, owned: true, maxNodes: ReplyNodes}
	r := parseReply(&p)
	if !p.ok || p.i != len(b) || !validReply(r) || r.Capability == nil && len(b) > ReplyWithoutCapabilityMaxBytes {
		return Reply{}, ErrInvalid
	}
	return r, nil
}
