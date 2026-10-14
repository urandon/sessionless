// Package pisdkphasewire implements isolated, non-serving canonical metadata.
// Decoding, structural comparison and accounting are NOT currentness or grants.
// Authentic source/RPC ownership, original deadlines, native fit and registration
// remain external obligations. This package has no transport or issuer.
package pisdkphasewire

import (
	"errors"
	"math"
	"strconv"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

const (
	AuthorizationMaxBytes  = 1720
	CallerDeadlineMaxBytes = 57
	TimestampMaxBytes      = 56
	HeadMaxBytes           = 2555
	RequestMaxBytes        = 2075
	ReplyMaxBytes          = 2990
	DeniedReplyBytes       = 19
	RequestNodes           = 51
	AppliedReplyNodes      = 97
	DeniedReplyNodes       = 3
	SharedNodes            = 555
)

// ErrInvalid never includes untrusted input or provider/runtime diagnostics.
var ErrInvalid = errors.New("invalid Pi phase wire")

type Phase string

const (
	PreIssue          Phase = "pre_issue"
	PhysicalSend      Phase = "physical_send"
	ImmutableFinalize Phase = "immutable_finalize"
)

// CallerDeadline is the closed send-relative representation, not a local clock.
type CallerDeadline struct {
	Version              uint32
	RemainingNanoseconds uint64
}

type Request struct {
	Version                                     uint32
	Authorization                               ports.AttachedWorkerSealedInputAuthorization
	ExpectedHarnessBindingDigest                string
	ExpectedAttachedPiAPIInvocationPolicyDigest string
	CallerDeadline                              CallerDeadline
	Phase                                       Phase
}

// Reply has no version/phase: association is external to these bytes and must
// remain bound to the sole original request and SAME measured source RPC.
type Reply struct {
	Status                              ports.AttachedWorkerExecutionStatus
	AcceptedAttempt                     domain.AttachedWorkerAttemptV1
	AcceptedAttemptRevision             uint64
	HarnessBindingDigest                string
	AttachedPiAPIInvocationPolicyDigest string
	CheckedAt                           time.Time
	UseExpiresAt                        time.Time
}

func id(value string, limit int, optional bool) bool {
	if value == "" {
		return optional
	}
	return len(value) <= limit && domain.ValidateOpaqueID("phase_wire", value) == nil
}

func digest(value string) bool {
	if len(value) != 64 {
		return false
	}
	nonzero := false
	for i := range value {
		c := value[i]
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
		nonzero = nonzero || c != '0'
	}
	return nonzero
}

func validAuth(a ports.AttachedWorkerSealedInputAuthorization) bool {
	if a.PresentedSecretDigest != "" || !id(string(a.TenantID), 160, false) || !id(string(a.OwnerUserID), 160, false) ||
		!id(string(a.WorkerID), 160, false) || !id(string(a.ConnectionID), 160, false) || !id(string(a.RunID), 128, false) ||
		!id(string(a.AttemptID), 128, false) || !id(string(a.LeaseID), 128, false) || a.EnrollmentGeneration == 0 ||
		a.ConnectionGeneration == 0 || a.LeaseGeneration == 0 || a.ExpectedAttemptRevision == 0 ||
		a.AttemptSequence != 1 || a.LeaseExpiresAtUnixMicro <= 0 || !digest(string(a.FenceToken)) ||
		!digest(string(a.ContextDigest)) || !digest(string(a.CapabilityDigest)) || !digest(string(a.PolicyDigest)) {
		return false
	}
	fence, err := domain.NewAttachedWorkerFenceTokenV1(a.TenantID, a.OwnerUserID, a.WorkerID, a.RunID, a.AttemptID, a.LeaseID, a.LeaseGeneration)
	return err == nil && fence == a.FenceToken
}

func validRequest(r Request) bool {
	return r.Version == 1 && validAuth(r.Authorization) && digest(r.ExpectedHarnessBindingDigest) &&
		digest(r.ExpectedAttachedPiAPIInvocationPolicyDigest) && r.CallerDeadline.Version == 1 &&
		r.CallerDeadline.RemainingNanoseconds > 0 && r.CallerDeadline.RemainingNanoseconds <= math.MaxInt64 &&
		(r.Phase == PreIssue || r.Phase == PhysicalSend || r.Phase == ImmutableFinalize)
}

func validTime(t time.Time, micro bool) bool {
	wall := t.UTC().Round(0)
	return !wall.IsZero() && (!micro || wall.Nanosecond()%1000 == 0) &&
		time.Unix(wall.Unix(), int64(wall.Nanosecond())).UTC().Equal(wall)
}

func validHead(h domain.AttachedWorkerAttemptV1) bool {
	return h.Validate() == nil && id(string(h.RunID), 128, false) && id(string(h.AttemptID), 128, false) &&
		id(string(h.LeaseID), 128, false) && digest(string(h.ContextDigest)) && digest(string(h.CapabilityDigest)) &&
		digest(string(h.PolicyDigest)) && h.State == domain.AttachedWorkerAttemptClaimed && h.CancelRevision == 0 &&
		h.CancelDeadline.IsZero() && h.TerminalSequence == 0 && h.TerminalStatus == "" && h.TerminalEvidenceDigest == "" &&
		validTime(h.CreatedAt, true) && validTime(h.UpdatedAt, true) && validTime(h.LeaseExpiresAt, true) &&
		h.LeaseExpiresAt.UnixMicro() > 0 && time.UnixMicro(h.LeaseExpiresAt.UnixMicro()).UTC().Equal(h.LeaseExpiresAt)
}

func validReply(r Reply) bool {
	if r.Status == ports.AttachedWorkerExecutionDenied {
		return r.AcceptedAttempt == (domain.AttachedWorkerAttemptV1{}) && r.AcceptedAttemptRevision == 0 &&
			r.HarnessBindingDigest == "" && r.AttachedPiAPIInvocationPolicyDigest == "" && r.CheckedAt.IsZero() && r.UseExpiresAt.IsZero()
	}
	if r.Status != ports.AttachedWorkerExecutionApplied || !validHead(r.AcceptedAttempt) ||
		r.AcceptedAttemptRevision == 0 || r.AcceptedAttemptRevision != r.AcceptedAttempt.Revision ||
		!digest(r.HarnessBindingDigest) || !digest(r.AttachedPiAPIInvocationPolicyDigest) ||
		!validTime(r.CheckedAt, true) || !validTime(r.UseExpiresAt, false) ||
		r.CheckedAt.Before(r.AcceptedAttempt.UpdatedAt) || !r.CheckedAt.Before(r.AcceptedAttempt.LeaseExpiresAt) ||
		r.UseExpiresAt.After(r.AcceptedAttempt.LeaseExpiresAt) {
		return false
	}
	u := r.UseExpiresAt.Sub(r.CheckedAt)
	return u > 0 && r.CheckedAt.Add(u).Equal(r.UseExpiresAt) && r.UseExpiresAt.Add(-u).Equal(r.CheckedAt)
}

// CompareReply checks static original tuple/revision/binding/M/window equality.
// Success is NOT an authenticated snapshot, SAME-RPC association or permission.
func CompareReply(request Request, reply Reply) error {
	if !validRequest(request) || !validReply(reply) || reply.Status != ports.AttachedWorkerExecutionApplied {
		return ErrInvalid
	}
	matched := ports.AttachedWorkerSealedInputAuthorizationResult{Status: reply.Status, AttemptRevision: reply.AcceptedAttemptRevision, Attempt: reply.AcceptedAttempt}
	if !matched.Matches(request.Authorization) || reply.HarnessBindingDigest != request.ExpectedHarnessBindingDigest ||
		reply.AttachedPiAPIInvocationPolicyDigest != request.ExpectedAttachedPiAPIInvocationPolicyDigest ||
		uint64(reply.UseExpiresAt.Sub(reply.CheckedAt)) > request.CallerDeadline.RemainingNanoseconds {
		return ErrInvalid
	}
	return nil
}

// writer is used first with no buffer to premeasure; a second identical traversal
// writes into ONE exact allocation. It never grows a buffer or formats strings.
type writer struct {
	b []byte
	n int
}

func (w *writer) put(s string) {
	if w.b != nil {
		copy(w.b[w.n:], s)
	}
	w.n += len(s)
}
func (w *writer) key(name string, first bool) {
	if !first {
		w.put(",")
	}
	w.put("\"")
	w.put(name)
	w.put("\":")
}
func (w *writer) str(s string) { w.put("\""); w.put(s); w.put("\"") }
func (w *writer) uint(n uint64) {
	if w.b != nil {
		out := strconv.AppendUint(w.b[:w.n], n, 10)
		w.n = len(out)
		return
	}
	w.n++
	for n >= 10 {
		w.n++
		n /= 10
	}
}
func (w *writer) sint(n int64) {
	if n < 0 {
		w.put("-")
		w.uint(uint64(-(n + 1)) + 1)
	} else {
		w.uint(uint64(n))
	}
}
func (w *writer) stringField(name, s string, first bool)      { w.key(name, first); w.str(s) }
func (w *writer) uintField(name string, n uint64, first bool) { w.key(name, first); w.uint(n) }
func (w *writer) timestamp(t time.Time) {
	w.put("{\"seconds\":")
	w.sint(t.Unix())
	w.put(",\"nanoseconds\":")
	w.uint(uint64(t.Nanosecond()))
	w.put("}")
}
func (w *writer) timeField(name string, t time.Time) { w.key(name, false); w.timestamp(t) }

func writeAuth(w *writer, a ports.AttachedWorkerSealedInputAuthorization) {
	w.put("{")
	w.stringField("tenant_id", string(a.TenantID), true)
	w.stringField("owner_user_id", string(a.OwnerUserID), false)
	w.stringField("worker_id", string(a.WorkerID), false)
	w.stringField("connection_id", string(a.ConnectionID), false)
	w.uintField("enrollment_generation", a.EnrollmentGeneration, false)
	w.uintField("connection_generation", a.ConnectionGeneration, false)
	w.stringField("run_id", string(a.RunID), false)
	w.stringField("attempt_id", string(a.AttemptID), false)
	w.uintField("attempt_sequence", a.AttemptSequence, false)
	w.stringField("lease_id", string(a.LeaseID), false)
	w.uintField("lease_generation", a.LeaseGeneration, false)
	w.stringField("fence_token", string(a.FenceToken), false)
	w.key("lease_expires_at_unix_micro", false)
	w.sint(a.LeaseExpiresAtUnixMicro)
	w.stringField("context_digest", string(a.ContextDigest), false)
	w.stringField("capability_digest", string(a.CapabilityDigest), false)
	w.stringField("policy_digest", string(a.PolicyDigest), false)
	w.uintField("expected_attempt_revision", a.ExpectedAttemptRevision, false)
	w.put("}")
}

func writeRequest(w *writer, r Request) {
	w.put("{\"version\":1,\"authorization\":")
	writeAuth(w, r.Authorization)
	w.stringField("expected_harness_binding_digest", r.ExpectedHarnessBindingDigest, false)
	w.stringField("expected_attached_pi_api_invocation_policy_digest", r.ExpectedAttachedPiAPIInvocationPolicyDigest, false)
	w.put(",\"caller_deadline\":{\"version\":1,\"remaining_nanoseconds\":")
	w.uint(r.CallerDeadline.RemainingNanoseconds)
	w.put("}")
	w.stringField("phase", string(r.Phase), false)
	w.put("}")
}

func writeHead(w *writer, h domain.AttachedWorkerAttemptV1) {
	w.put("{\"version\":1")
	w.stringField("tenant_id", string(h.TenantID), false)
	w.stringField("owner_user_id", string(h.OwnerUserID), false)
	w.stringField("worker_id", string(h.WorkerID), false)
	w.stringField("connection_id", string(h.ConnectionID), false)
	w.stringField("execution_connection_id", string(h.ExecutionConnectionID), false)
	w.stringField("run_id", string(h.RunID), false)
	w.stringField("attempt_id", string(h.AttemptID), false)
	w.stringField("reservation_id", string(h.ReservationID), false)
	w.stringField("lease_id", string(h.LeaseID), false)
	w.uintField("lease_generation", h.LeaseGeneration, false)
	w.stringField("fence_token", string(h.FenceToken), false)
	w.uintField("enrollment_generation", h.EnrollmentGeneration, false)
	w.uintField("connection_generation", h.ConnectionGeneration, false)
	w.uintField("execution_connection_generation", h.ExecutionConnectionGeneration, false)
	w.stringField("context_digest", string(h.ContextDigest), false)
	w.stringField("capability_digest", string(h.CapabilityDigest), false)
	w.stringField("policy_digest", string(h.PolicyDigest), false)
	w.stringField("state", string(h.State), false)
	w.uintField("platform_attempt_sequence", h.PlatformAttemptSequence, false)
	w.uintField("worker_attempt_sequence", h.WorkerAttemptSequence, false)
	w.uintField("progress_sequence", h.ProgressSequence, false)
	w.uintField("cancel_revision", h.CancelRevision, false)
	w.uintField("terminal_sequence", h.TerminalSequence, false)
	w.stringField("terminal_status", string(h.TerminalStatus), false)
	w.stringField("terminal_evidence_digest", string(h.TerminalEvidenceDigest), false)
	w.timeField("lease_expires_at", h.LeaseExpiresAt)
	w.put(",\"cancel_deadline\":null")
	w.timeField("created_at", h.CreatedAt)
	w.timeField("updated_at", h.UpdatedAt)
	w.uintField("revision", h.Revision, false)
	w.put("}")
}

func writeReply(w *writer, r Reply) {
	if r.Status == ports.AttachedWorkerExecutionDenied {
		w.put("{\"status\":\"denied\"}")
		return
	}
	w.put("{\"status\":\"applied\",\"accepted_attempt\":")
	writeHead(w, r.AcceptedAttempt)
	w.uintField("accepted_attempt_revision", r.AcceptedAttemptRevision, false)
	w.stringField("harness_binding_digest", r.HarnessBindingDigest, false)
	w.stringField("attached_pi_api_invocation_policy_digest", r.AttachedPiAPIInvocationPolicyDigest, false)
	w.timeField("checked_at", r.CheckedAt)
	w.timeField("use_expires_at", r.UseExpiresAt)
	w.put("}")
}

func encode(limit int, emit func(*writer)) ([]byte, error) {
	w := writer{}
	emit(&w)
	if w.n <= 0 || w.n > limit {
		return nil, ErrInvalid
	}
	b := make([]byte, w.n)
	out := writer{b: b}
	emit(&out)
	if out.n != len(b) {
		return nil, ErrInvalid
	}
	return b, nil
}

func EncodeRequest(r Request) ([]byte, error) {
	if !validRequest(r) {
		return nil, ErrInvalid
	}
	return encode(RequestMaxBytes, func(w *writer) { writeRequest(w, r) })
}
func EncodeReply(r Reply) ([]byte, error) {
	if !validReply(r) {
		return nil, ErrInvalid
	}
	return encode(ReplyMaxBytes, func(w *writer) { writeReply(w, r) })
}

// parser consumes only the fixed ordered grammar. The first full pass validates
// all token bounds/structure without allocating string copies. A second bounded
// pass copies only admitted scalar strings; no generic JSON tree/key allocation.
type parser struct {
	b     []byte
	i     int
	ok    bool
	owned bool
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
func (p *parser) key(name string, first bool) {
	if !first {
		p.lit(",")
	}
	p.lit("\"")
	p.lit(name)
	p.lit("\":")
}
func (p *parser) uint(max uint64) uint64 {
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
func (p *parser) sint() int64 {
	negative := p.ok && p.i < len(p.b) && p.b[p.i] == '-'
	if negative {
		p.i++
	}
	max := uint64(math.MaxInt64)
	if negative {
		max++
	}
	n := p.uint(max)
	if negative {
		if n == 0 {
			p.ok = false
		}
		return -int64(n-1) - 1
	}
	return int64(n)
}
func (p *parser) str(limit int, kind byte, optional bool) string {
	p.lit("\"")
	start := p.i
	for p.ok && p.i < len(p.b) && p.b[p.i] != '"' {
		c := p.b[p.i]
		valid := false
		if kind == 'd' {
			valid = c >= '0' && c <= '9' || c >= 'a' && c <= 'f'
		} else {
			valid = c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || p.i > start && (c == '.' || c == '_' || c == ':' || c == '-')
		}
		if !valid || p.i-start >= limit {
			p.ok = false
			break
		}
		p.i++
	}
	n := p.i - start
	if (!optional && n == 0) || kind == 'd' && n != 64 {
		p.ok = false
	}
	if kind == 'd' && p.ok {
		nz := false
		for j := start; j < p.i; j++ {
			nz = nz || p.b[j] != '0'
		}
		p.ok = nz
	}
	p.lit("\"")
	if p.owned && p.ok {
		return string(p.b[start : start+n])
	}
	return ""
}
func (p *parser) stringField(name string, limit int, kind byte, optional, first bool) string {
	p.key(name, first)
	return p.str(limit, kind, optional)
}
func (p *parser) uintField(name string, max uint64, first bool) uint64 {
	p.key(name, first)
	return p.uint(max)
}
func (p *parser) timestamp() time.Time {
	p.lit("{\"seconds\":")
	s := p.sint()
	p.lit(",\"nanoseconds\":")
	ns := p.uint(999999999)
	p.lit("}")
	if !p.ok {
		return time.Time{}
	}
	t := time.Unix(s, int64(ns)).UTC()
	if t.Unix() != s || t.Nanosecond() != int(ns) {
		p.ok = false
	}
	return t
}
func (p *parser) timeField(name string) time.Time { p.key(name, false); return p.timestamp() }

func parseAuth(p *parser) ports.AttachedWorkerSealedInputAuthorization {
	p.lit("{")
	a := ports.AttachedWorkerSealedInputAuthorization{}
	a.TenantID = domain.TenantID(p.stringField("tenant_id", 160, 'i', false, true))
	a.OwnerUserID = domain.UserID(p.stringField("owner_user_id", 160, 'i', false, false))
	a.WorkerID = domain.AttachedWorkerID(p.stringField("worker_id", 160, 'i', false, false))
	a.ConnectionID = domain.AttachedWorkerConnectionID(p.stringField("connection_id", 160, 'i', false, false))
	a.EnrollmentGeneration = p.uintField("enrollment_generation", math.MaxUint64, false)
	a.ConnectionGeneration = p.uintField("connection_generation", math.MaxUint64, false)
	a.RunID = domain.RunID(p.stringField("run_id", 128, 'i', false, false))
	a.AttemptID = domain.AttemptID(p.stringField("attempt_id", 128, 'i', false, false))
	a.AttemptSequence = p.uintField("attempt_sequence", 1, false)
	a.LeaseID = domain.LeaseID(p.stringField("lease_id", 128, 'i', false, false))
	a.LeaseGeneration = p.uintField("lease_generation", math.MaxUint64, false)
	a.FenceToken = domain.AttachedWorkerFenceToken(p.stringField("fence_token", 64, 'd', false, false))
	p.key("lease_expires_at_unix_micro", false)
	a.LeaseExpiresAtUnixMicro = int64(p.uint(math.MaxInt64))
	a.ContextDigest = domain.AttachedWorkerContextDigest(p.stringField("context_digest", 64, 'd', false, false))
	a.CapabilityDigest = domain.AttachedWorkerCapabilityDigest(p.stringField("capability_digest", 64, 'd', false, false))
	a.PolicyDigest = domain.AttachedWorkerPolicyDigest(p.stringField("policy_digest", 64, 'd', false, false))
	a.ExpectedAttemptRevision = p.uintField("expected_attempt_revision", math.MaxUint64, false)
	p.lit("}")
	return a
}

func parseRequest(p *parser) Request {
	p.lit("{\"version\":1,\"authorization\":")
	r := Request{Version: 1, Authorization: parseAuth(p)}
	r.ExpectedHarnessBindingDigest = p.stringField("expected_harness_binding_digest", 64, 'd', false, false)
	r.ExpectedAttachedPiAPIInvocationPolicyDigest = p.stringField("expected_attached_pi_api_invocation_policy_digest", 64, 'd', false, false)
	p.lit(",\"caller_deadline\":{\"version\":1,\"remaining_nanoseconds\":")
	r.CallerDeadline = CallerDeadline{Version: 1, RemainingNanoseconds: p.uint(math.MaxInt64)}
	p.lit("}")
	p.key("phase", false)
	// Phase is selected by byte literals, not an unbounded caller string.
	if p.ok && len(p.b)-p.i >= 2 {
		switch p.b[p.i+1] {
		case 'p':
			if len(p.b)-p.i >= 4 && p.b[p.i+2] == 'r' {
				p.lit("\"pre_issue\"")
				r.Phase = PreIssue
			} else {
				p.lit("\"physical_send\"")
				r.Phase = PhysicalSend
			}
		case 'i':
			p.lit("\"immutable_finalize\"")
			r.Phase = ImmutableFinalize
		default:
			p.ok = false
		}
	} else {
		p.ok = false
	}
	p.lit("}")
	return r
}

func parseHead(p *parser) domain.AttachedWorkerAttemptV1 {
	p.lit("{\"version\":1")
	h := domain.AttachedWorkerAttemptV1{Version: 1}
	h.TenantID = domain.TenantID(p.stringField("tenant_id", 160, 'i', false, false))
	h.OwnerUserID = domain.UserID(p.stringField("owner_user_id", 160, 'i', false, false))
	h.WorkerID = domain.AttachedWorkerID(p.stringField("worker_id", 160, 'i', false, false))
	h.ConnectionID = domain.AttachedWorkerConnectionID(p.stringField("connection_id", 160, 'i', false, false))
	h.ExecutionConnectionID = domain.AttachedWorkerConnectionID(p.stringField("execution_connection_id", 160, 'i', true, false))
	h.RunID = domain.RunID(p.stringField("run_id", 128, 'i', false, false))
	h.AttemptID = domain.AttemptID(p.stringField("attempt_id", 128, 'i', false, false))
	h.ReservationID = domain.QuotaReservationID(p.stringField("reservation_id", 160, 'i', false, false))
	h.LeaseID = domain.LeaseID(p.stringField("lease_id", 128, 'i', false, false))
	h.LeaseGeneration = p.uintField("lease_generation", math.MaxUint64, false)
	h.FenceToken = domain.AttachedWorkerFenceToken(p.stringField("fence_token", 64, 'd', false, false))
	h.EnrollmentGeneration = p.uintField("enrollment_generation", math.MaxUint64, false)
	h.ConnectionGeneration = p.uintField("connection_generation", math.MaxUint64, false)
	h.ExecutionConnectionGeneration = p.uintField("execution_connection_generation", math.MaxUint64, false)
	h.ContextDigest = domain.AttachedWorkerContextDigest(p.stringField("context_digest", 64, 'd', false, false))
	h.CapabilityDigest = domain.AttachedWorkerCapabilityDigest(p.stringField("capability_digest", 64, 'd', false, false))
	h.PolicyDigest = domain.AttachedWorkerPolicyDigest(p.stringField("policy_digest", 64, 'd', false, false))
	p.lit(",\"state\":\"claimed\"")
	h.State = domain.AttachedWorkerAttemptClaimed
	h.PlatformAttemptSequence = p.uintField("platform_attempt_sequence", math.MaxUint64, false)
	h.WorkerAttemptSequence = p.uintField("worker_attempt_sequence", math.MaxUint64, false)
	h.ProgressSequence = p.uintField("progress_sequence", math.MaxUint64, false)
	p.lit(",\"cancel_revision\":0,\"terminal_sequence\":0,\"terminal_status\":\"\",\"terminal_evidence_digest\":\"\"")
	h.LeaseExpiresAt = p.timeField("lease_expires_at")
	p.lit(",\"cancel_deadline\":null")
	h.CreatedAt = p.timeField("created_at")
	h.UpdatedAt = p.timeField("updated_at")
	h.Revision = p.uintField("revision", math.MaxUint64, false)
	p.lit("}")
	return h
}

func parseReply(p *parser) Reply {
	if len(p.b) == DeniedReplyBytes {
		p.lit("{\"status\":\"denied\"}")
		return Reply{Status: ports.AttachedWorkerExecutionDenied}
	}
	p.lit("{\"status\":\"applied\",\"accepted_attempt\":")
	r := Reply{Status: ports.AttachedWorkerExecutionApplied, AcceptedAttempt: parseHead(p)}
	r.AcceptedAttemptRevision = p.uintField("accepted_attempt_revision", math.MaxUint64, false)
	r.HarnessBindingDigest = p.stringField("harness_binding_digest", 64, 'd', false, false)
	r.AttachedPiAPIInvocationPolicyDigest = p.stringField("attached_pi_api_invocation_policy_digest", 64, 'd', false, false)
	r.CheckedAt = p.timeField("checked_at")
	r.UseExpiresAt = p.timeField("use_expires_at")
	p.lit("}")
	return r
}

func DecodeRequest(b []byte) (Request, error) {
	if len(b) == 0 || len(b) > RequestMaxBytes {
		return Request{}, ErrInvalid
	}
	pre := parser{b: b, ok: true}
	parseRequest(&pre)
	if !pre.ok || pre.i != len(b) {
		return Request{}, ErrInvalid
	}
	p := parser{b: b, ok: true, owned: true}
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
	pre := parser{b: b, ok: true}
	parseReply(&pre)
	if !pre.ok || pre.i != len(b) {
		return Reply{}, ErrInvalid
	}
	p := parser{b: b, ok: true, owned: true}
	r := parseReply(&p)
	if !p.ok || p.i != len(b) || !validReply(r) {
		return Reply{}, ErrInvalid
	}
	return r, nil
}
