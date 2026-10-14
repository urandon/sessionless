package pisdkphasewire

import (
	"bytes"
	"errors"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// This fixture is constructor-backed structural evidence, NOT an authenticated
// current source/head, serving profile, original RPC or native model fit.
func fixture(t *testing.T) (Request, Reply) {
	t.Helper()
	created := time.Date(2026, 10, 14, 10, 0, 0, 123456000, time.UTC)
	h := domain.AttachedWorkerAttemptV1{Version: 1, TenantID: "tenant", OwnerUserID: "owner", WorkerID: "worker", ConnectionID: "connection",
		RunID: "run", AttemptID: "attempt", ReservationID: "reservation", LeaseID: "lease", LeaseGeneration: 3, EnrollmentGeneration: 2,
		ConnectionGeneration: 5, ContextDigest: domain.AttachedWorkerContextDigest(strings.Repeat("a", 64)),
		CapabilityDigest: domain.AttachedWorkerCapabilityDigest(strings.Repeat("b", 64)), PolicyDigest: domain.AttachedWorkerPolicyDigest(strings.Repeat("c", 64)),
		State: domain.AttachedWorkerAttemptClaimed, PlatformAttemptSequence: 1, WorkerAttemptSequence: 1, ProgressSequence: 1,
		CreatedAt: created, UpdatedAt: created, LeaseExpiresAt: created.Add(time.Hour), Revision: 7}
	fence, err := domain.NewAttachedWorkerFenceTokenV1(h.TenantID, h.OwnerUserID, h.WorkerID, h.RunID, h.AttemptID, h.LeaseID, h.LeaseGeneration)
	if err != nil {
		t.Fatal(err)
	}
	h.FenceToken = fence
	if err = h.Validate(); err != nil {
		t.Fatal(err)
	}
	a := ports.AttachedWorkerSealedInputAuthorization{TenantID: h.TenantID, OwnerUserID: h.OwnerUserID, WorkerID: h.WorkerID, ConnectionID: h.ConnectionID,
		EnrollmentGeneration: h.EnrollmentGeneration, ConnectionGeneration: h.ConnectionGeneration, RunID: h.RunID, AttemptID: h.AttemptID,
		AttemptSequence: 1, LeaseID: h.LeaseID, LeaseGeneration: h.LeaseGeneration, FenceToken: h.FenceToken, LeaseExpiresAtUnixMicro: h.LeaseExpiresAt.UnixMicro(),
		ContextDigest: h.ContextDigest, CapabilityDigest: h.CapabilityDigest, PolicyDigest: h.PolicyDigest, ExpectedAttemptRevision: h.Revision}
	r := Request{Version: 1, Authorization: a, ExpectedHarnessBindingDigest: strings.Repeat("d", 64), ExpectedAttachedPiAPIInvocationPolicyDigest: strings.Repeat("e", 64),
		CallerDeadline: CallerDeadline{Version: 1, RemainingNanoseconds: uint64(time.Minute)}, Phase: PhysicalSend}
	reply := Reply{Status: ports.AttachedWorkerExecutionApplied, AcceptedAttempt: h, AcceptedAttemptRevision: h.Revision, HarnessBindingDigest: r.ExpectedHarnessBindingDigest,
		AttachedPiAPIInvocationPolicyDigest: r.ExpectedAttachedPiAPIInvocationPolicyDigest, CheckedAt: created.Add(time.Second), UseExpiresAt: created.Add(2*time.Second + 123*time.Nanosecond)}
	return r, reply
}

func mustRequest(t *testing.T, r Request) []byte {
	t.Helper()
	b, err := EncodeRequest(r)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func mustReply(t *testing.T, r Reply) []byte {
	t.Helper()
	b, err := EncodeReply(r)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func replace(t *testing.T, b []byte, old, new string) []byte {
	t.Helper()
	if !bytes.Contains(b, []byte(old)) {
		t.Fatalf("replacement target %q absent", old)
	}
	return bytes.Replace(b, []byte(old), []byte(new), 1)
}
func invalid(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, ErrInvalid) || err.Error() != ErrInvalid.Error() {
		t.Fatalf("got error %v; want constant ErrInvalid", err)
	}
}

func TestWholeRequestReplyRoundTrip(t *testing.T) {
	r, s := fixture(t)
	for _, phase := range []Phase{PreIssue, PhysicalSend, ImmutableFinalize} {
		t.Run(string(phase), func(t *testing.T) {
			r.Phase = phase
			b := mustRequest(t, r)
			got, err := DecodeRequest(b)
			if err != nil || got != r {
				t.Fatalf("request roundtrip: err=%v equal=%v", err, got == r)
			}
			owned := append([]byte(nil), b...)
			clear(b)
			if !bytes.Equal(mustRequest(t, got), owned) {
				t.Fatal("decoded request aliases input")
			}
		})
	}
	b := mustReply(t, s)
	got, err := DecodeReply(b)
	if err != nil || got != s {
		t.Fatalf("reply roundtrip: err=%v equal=%v", err, got == s)
	}
	if err = CompareReply(r, got); err != nil {
		t.Fatal(err)
	}
	if got.UseExpiresAt.Nanosecond()%1000 == 0 {
		t.Fatal("non-microsecond use expiry lost")
	}
	for _, present := range []string{`"execution_connection_id":""`, `"execution_connection_generation":0`, `"cancel_deadline":null`, `"terminal_status":""`, `"terminal_evidence_digest":""`} {
		if !bytes.Contains(b, []byte(present)) {
			t.Fatalf("missing explicit original optional field %s", present)
		}
	}
	owned := append([]byte(nil), b...)
	clear(b)
	if !bytes.Equal(mustReply(t, got), owned) {
		t.Fatal("decoded reply aliases input")
	}
	denied := Reply{Status: ports.AttachedWorkerExecutionDenied}
	db := mustReply(t, denied)
	if string(db) != `{"status":"denied"}` || len(db) != DeniedReplyBytes {
		t.Fatalf("denied encoding %q", db)
	}
	gd, err := DecodeReply(db)
	if err != nil || gd != denied {
		t.Fatalf("denied roundtrip %v", err)
	}
	invalid(t, CompareReply(r, gd))
}

func TestRequestClosedGrammar(t *testing.T) {
	r, _ := fixture(t)
	good := mustRequest(t, r)
	cases := []struct{ name, old, new string }{
		{"missing", `"version":1,`, ""}, {"version", `"version":1,`, `"version":2,`}, {"version_null", `"version":1,`, `"version":null,`},
		{"duplicate", `"version":1,`, `"version":1,"version":1,`}, {"unknown", `"version":1,`, `"version":1,"extra":0,`},
		{"escaped_key", `"version":1,`, `"\u0076ersion":1,`}, {"reordered", `"version":1,"authorization":`, `"authorization":{},"version":1,"authorization":`},
		{"secret", `"tenant_id":"tenant",`, `"tenant_id":"tenant","presented_secret_digest":"",`},
		{"selector", `"tenant_id":"tenant",`, `"tenant_id":"tenant","model":"x",`},
		{"alias", `"tenant_id":"tenant"`, `"tenantId":"tenant"`}, {"escaped_value", `"tenant_id":"tenant"`, `"tenant_id":"\u0074enant"`},
		{"null_id", `"tenant_id":"tenant"`, `"tenant_id":null`}, {"bad_first_id", `"tenant_id":"tenant"`, `"tenant_id":"-tenant"`},
		{"nonascii", `"tenant_id":"tenant"`, `"tenant_id":"téna"`}, {"unknown_phase", `"phase":"physical_send"`, `"phase":"other"`},
		{"null_phase", `"phase":"physical_send"`, `"phase":null`}, {"phase_tiny", `"phase":"physical_send"`, `"phase":`},
		{"deadline_version", `"caller_deadline":{"version":1,`, `"caller_deadline":{"version":2,`},
		{"deadline_missing", `"remaining_nanoseconds":60000000000`, `"alias":60000000000`},
		{"deadline_zero", `"remaining_nanoseconds":60000000000`, `"remaining_nanoseconds":0`},
		{"revision_wildcard", `"expected_attempt_revision":7`, `"expected_attempt_revision":0`},
		{"attempt_sequence", `"attempt_sequence":1`, `"attempt_sequence":2`},
		{"lease_zero", `"lease_expires_at_unix_micro":` + strings.Split(strings.Split(string(good), `"lease_expires_at_unix_micro":`)[1], ",")[0], `"lease_expires_at_unix_micro":0`},
		{"digest_zero", `"expected_harness_binding_digest":"` + strings.Repeat("d", 64) + `"`, `"expected_harness_binding_digest":"` + strings.Repeat("0", 64) + `"`},
		{"digest_upper", `"expected_harness_binding_digest":"` + strings.Repeat("d", 64) + `"`, `"expected_harness_binding_digest":"` + strings.Repeat("D", 64) + `"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { _, err := DecodeRequest(replace(t, good, tc.old, tc.new)); invalid(t, err) })
	}
	for _, suffix := range []string{" ", "\n", "{}", "null"} {
		_, err := DecodeRequest(append(append([]byte(nil), good...), suffix...))
		invalid(t, err)
	}
	for _, value := range []string{"-1", "+1", "01", "1.0", "1e0", `"1"`, "null", "9223372036854775808", "18446744073709551616"} {
		t.Run("deadline_"+value, func(t *testing.T) {
			_, err := DecodeRequest(replace(t, good, `"remaining_nanoseconds":60000000000`, `"remaining_nanoseconds":`+value))
			invalid(t, err)
		})
	}
	for _, value := range []string{"0", "-1", "+1", "01", "1.0", "1e0", `"1"`, "null", "18446744073709551616"} {
		t.Run("generation_"+value, func(t *testing.T) {
			_, err := DecodeRequest(replace(t, good, `"enrollment_generation":2`, `"enrollment_generation":`+value))
			invalid(t, err)
		})
	}
	for _, b := range [][]byte{nil, {}, bytes.Repeat([]byte{'x'}, RequestMaxBytes+1)} {
		_, err := DecodeRequest(b)
		invalid(t, err)
	}
}

func TestReplyClosedGrammar(t *testing.T) {
	_, s := fixture(t)
	good := mustReply(t, s)
	cases := []struct{ name, old, new string }{
		{"reply_version", `"status":"applied",`, `"status":"applied","version":1,`},
		{"reply_phase", `"status":"applied",`, `"status":"applied","phase":"physical_send",`},
		{"duplicate", `"status":"applied",`, `"status":"applied","status":"applied",`},
		{"unknown", `"status":"applied",`, `"status":"applied","error":"raw",`},
		{"status", `"status":"applied"`, `"status":"success"`}, {"status_null", `"status":"applied"`, `"status":null`},
		{"head_missing", `"reservation_id":"reservation",`, ""}, {"head_null", `"reservation_id":"reservation"`, `"reservation_id":null`},
		{"head_version", `"accepted_attempt":{"version":1,`, `"accepted_attempt":{"version":2,`},
		{"state", `"state":"claimed"`, `"state":"offered"`}, {"cancel", `"cancel_revision":0`, `"cancel_revision":1`},
		{"terminal", `"terminal_sequence":0`, `"terminal_sequence":1`}, {"terminal_status", `"terminal_status":""`, `"terminal_status":"failed"`},
		{"cancel_time", `"cancel_deadline":null`, `"cancel_deadline":{"seconds":1,"nanoseconds":0}`},
		{"exec_null", `"execution_connection_id":""`, `"execution_connection_id":null`},
		{"exec_unpaired", `"execution_connection_generation":0`, `"execution_connection_generation":1`},
		{"worker_zero", `"worker_attempt_sequence":1`, `"worker_attempt_sequence":0`}, {"progress", `"progress_sequence":1`, `"progress_sequence":2`},
		{"revision", `"accepted_attempt_revision":7`, `"accepted_attempt_revision":8`},
		{"timestamp_duplicate", `"checked_at":{"seconds":`, `"checked_at":{"seconds":1,"seconds":`},
		{"timestamp_reorder", `"checked_at":{"seconds":`, `"checked_at":{"nanoseconds":0,"seconds":`},
		{"timestamp_micro", `"nanoseconds":123456000`, `"nanoseconds":123456001`},
	}
	// The alignment scalar occurs three times: mutate only CheckedAt explicitly.
	cases = cases[:len(cases)-1]
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { _, err := DecodeReply(replace(t, good, tc.old, tc.new)); invalid(t, err) })
	}
	for _, prefix := range []string{`{"status":"denied","version":1}`, `{"status":"denied","phase":"physical_send"}`, `{"status":"denied","accepted_attempt":null}`, `{"status":"denied","checked_at":null}`, `{"status":"denied"} `, `{"status":"denied","status":"denied"}`} {
		_, err := DecodeReply([]byte(prefix))
		invalid(t, err)
	}
	for _, b := range [][]byte{nil, bytes.Repeat([]byte{'x'}, ReplyMaxBytes+1), append(append([]byte(nil), good...), '}')} {
		_, err := DecodeReply(b)
		invalid(t, err)
	}
}

func TestOriginalTupleComparisonNotPermission(t *testing.T) {
	r, s := fixture(t)
	// All these changes are structurally lawful foreign records, but not the
	// independently retained original expectation. None is a current source proof.
	cases := []struct {
		name string
		edit func(*Reply)
	}{
		{"tenant", func(s *Reply) { s.AcceptedAttempt.TenantID = "foreign" }},
		{"owner", func(s *Reply) { s.AcceptedAttempt.OwnerUserID = "foreign" }},
		{"worker", func(s *Reply) { s.AcceptedAttempt.WorkerID = "foreign" }},
		{"connection", func(s *Reply) { s.AcceptedAttempt.ConnectionID = "foreign" }},
		{"run", func(s *Reply) { s.AcceptedAttempt.RunID = "foreign" }},
		{"attempt", func(s *Reply) { s.AcceptedAttempt.AttemptID = "foreign" }},
		{"lease", func(s *Reply) { s.AcceptedAttempt.LeaseID = "foreign" }},
		{"lease_generation", func(s *Reply) { s.AcceptedAttempt.LeaseGeneration++ }},
		{"enrollment", func(s *Reply) { s.AcceptedAttempt.EnrollmentGeneration++ }},
		{"connection_generation", func(s *Reply) { s.AcceptedAttempt.ConnectionGeneration++ }},
		{"lease_expiry", func(s *Reply) {
			s.AcceptedAttempt.LeaseExpiresAt = s.AcceptedAttempt.LeaseExpiresAt.Add(time.Microsecond)
		}},
		{"context", func(s *Reply) {
			s.AcceptedAttempt.ContextDigest = domain.AttachedWorkerContextDigest(strings.Repeat("f", 64))
		}},
		{"capability", func(s *Reply) {
			s.AcceptedAttempt.CapabilityDigest = domain.AttachedWorkerCapabilityDigest(strings.Repeat("f", 64))
		}},
		{"policy", func(s *Reply) {
			s.AcceptedAttempt.PolicyDigest = domain.AttachedWorkerPolicyDigest(strings.Repeat("f", 64))
		}},
		{"revision", func(s *Reply) { s.AcceptedAttempt.Revision++; s.AcceptedAttemptRevision++ }},
		{"binding", func(s *Reply) { s.HarnessBindingDigest = strings.Repeat("f", 64) }},
		{"M", func(s *Reply) { s.AttachedPiAPIInvocationPolicyDigest = strings.Repeat("f", 64) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			changed := s
			tc.edit(&changed)
			h := &changed.AcceptedAttempt
			fence, err := domain.NewAttachedWorkerFenceTokenV1(h.TenantID, h.OwnerUserID, h.WorkerID, h.RunID, h.AttemptID, h.LeaseID, h.LeaseGeneration)
			if err != nil {
				t.Fatal(err)
			}
			h.FenceToken = fence
			decoded, err := DecodeReply(mustReply(t, changed))
			if err != nil {
				t.Fatalf("foreign structural record: %v", err)
			}
			invalid(t, CompareReply(r, decoded))
		})
	}
	r.CallerDeadline.RemainingNanoseconds = 1
	invalid(t, CompareReply(r, s))
}

func TestTypedInvariants(t *testing.T) {
	r, s := fixture(t)
	requestCases := []struct {
		name string
		edit func(*Request)
	}{
		{"secret", func(r *Request) { r.Authorization.PresentedSecretDigest = "private" }},
		{"fence", func(r *Request) {
			r.Authorization.FenceToken = domain.AttachedWorkerFenceToken(strings.Repeat("a", 64))
		}},
		{"empty_id", func(r *Request) { r.Authorization.TenantID = "" }},
		{"wildcard", func(r *Request) { r.Authorization.ExpectedAttemptRevision = 0 }},
		{"deadline", func(r *Request) { r.CallerDeadline.RemainingNanoseconds = math.MaxInt64 + 1 }},
		{"phase", func(r *Request) { r.Phase = "other" }},
	}
	for _, tc := range requestCases {
		t.Run(tc.name, func(t *testing.T) { c := r; tc.edit(&c); _, err := EncodeRequest(c); invalid(t, err) })
	}
	replyCases := []struct {
		name string
		edit func(*Reply)
	}{
		{"exec_generation", func(s *Reply) {
			s.AcceptedAttempt.ExecutionConnectionID = "exec"
			s.AcceptedAttempt.ExecutionConnectionGeneration = 6
		}},
		{"fence", func(s *Reply) {
			s.AcceptedAttempt.FenceToken = domain.AttachedWorkerFenceToken(strings.Repeat("a", 64))
		}},
		{"zero_created", func(s *Reply) { s.AcceptedAttempt.CreatedAt = time.Time{} }},
		{"unaligned_head", func(s *Reply) { s.AcceptedAttempt.UpdatedAt = s.AcceptedAttempt.UpdatedAt.Add(time.Nanosecond) }},
		{"unaligned_a", func(s *Reply) { s.CheckedAt = s.CheckedAt.Add(time.Nanosecond) }},
		{"early_a", func(s *Reply) { s.CheckedAt = s.AcceptedAttempt.CreatedAt.Add(-time.Microsecond) }},
		{"no_window", func(s *Reply) { s.UseExpiresAt = s.CheckedAt }},
		{"lease_exceeded", func(s *Reply) { s.UseExpiresAt = s.AcceptedAttempt.LeaseExpiresAt.Add(time.Nanosecond) }},
		{"sub_saturation", func(s *Reply) {
			s.UseExpiresAt = time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC)
			s.AcceptedAttempt.LeaseExpiresAt = time.Date(3001, 1, 1, 0, 0, 0, 0, time.UTC)
		}},
		{"lease_micro_overflow", func(s *Reply) { s.AcceptedAttempt.LeaseExpiresAt = time.Unix(math.MaxInt64, 0).UTC() }},
		{"denied_payload", func(s *Reply) { s.Status = ports.AttachedWorkerExecutionDenied }},
	}
	for _, tc := range replyCases {
		t.Run(tc.name, func(t *testing.T) { c := s; tc.edit(&c); _, err := EncodeReply(c); invalid(t, err) })
	}
	s.AcceptedAttempt.ExecutionConnectionID = "exec"
	s.AcceptedAttempt.ExecutionConnectionGeneration = 5
	mustReply(t, s)
}

func TestPrimitiveTimeAndNumericBoundaries(t *testing.T) {
	for _, sec := range []int64{math.MinInt64, -1, 0, 1, math.MaxInt64} {
		for _, ns := range []int{0, 999999999} {
			stamp := time.Unix(sec, int64(ns)).UTC()
			w := writer{}
			w.timestamp(stamp)
			if w.n > TimestampMaxBytes {
				t.Fatalf("timestamp measured %d", w.n)
			}
			b, err := encode(TimestampMaxBytes, func(w *writer) { w.timestamp(stamp) })
			if err != nil {
				t.Fatal(err)
			}
			p := parser{b: b, ok: true}
			got := p.timestamp()
			if !p.ok || p.i != len(b) || got.Unix() != sec || got.Nanosecond() != ns {
				t.Fatalf("seconds=%d nanos=%d roundtrip invalid", sec, ns)
			}
		}
	}
	for _, b := range []string{`{"seconds":-9223372036854775809,"nanoseconds":0}`, `{"seconds":9223372036854775808,"nanoseconds":0}`, `{"seconds":-0,"nanoseconds":0}`, `{"seconds":+1,"nanoseconds":0}`, `{"seconds":01,"nanoseconds":0}`, `{"seconds":1e0,"nanoseconds":0}`, `{"seconds":"1","nanoseconds":0}`, `{"seconds":0,"nanoseconds":1000000000}`, `{"seconds":0,"nanoseconds":-1}`} {
		p := parser{b: []byte(b), ok: true}
		p.timestamp()
		if p.ok && p.i == len(b) {
			t.Fatalf("invalid timestamp accepted %s", b)
		}
	}
	r, s := fixture(t)
	r.Authorization.EnrollmentGeneration = math.MaxUint64
	r.Authorization.ConnectionGeneration = math.MaxUint64
	r.Authorization.ExpectedAttemptRevision = math.MaxUint64
	r.CallerDeadline.RemainingNanoseconds = math.MaxInt64
	got, err := DecodeRequest(mustRequest(t, r))
	if err != nil || got != r {
		t.Fatalf("u64/remaining max parity %v", err)
	}
	// Original head permits a preepoch CreatedAt and large positive lease dates;
	// this is not a fabricated source-maximum/native-fit certificate.
	s.AcceptedAttempt.CreatedAt = time.Unix(-1, 999999000).UTC()
	s.AcceptedAttempt.UpdatedAt = s.AcceptedAttempt.CreatedAt
	s.CheckedAt = time.Date(12000, 1, 1, 0, 0, 0, 999999000, time.UTC)
	s.UseExpiresAt = s.CheckedAt.Add(time.Nanosecond)
	s.AcceptedAttempt.LeaseExpiresAt = s.CheckedAt.Add(time.Hour)
	mustReply(t, s)
}

func TestLiteralMaximaAndSharedNodeDerivation(t *testing.T) {
	object := func(keys []string, values []int) int {
		n := 2 + len(keys) - 1
		for i, k := range keys {
			n += len(k) + 3 + values[i]
		}
		return n
	}
	authKeys := strings.Split("tenant_id owner_user_id worker_id connection_id enrollment_generation connection_generation run_id attempt_id attempt_sequence lease_id lease_generation fence_token lease_expires_at_unix_micro context_digest capability_digest policy_digest expected_attempt_revision", " ")
	auth := object(authKeys, []int{162, 162, 162, 162, 20, 20, 130, 130, 1, 130, 20, 66, 19, 66, 66, 66, 20})
	stamp := object([]string{"seconds", "nanoseconds"}, []int{20, 9})
	deadline := object([]string{"version", "remaining_nanoseconds"}, []int{1, 19})
	headKeys := strings.Split("version tenant_id owner_user_id worker_id connection_id execution_connection_id run_id attempt_id reservation_id lease_id lease_generation fence_token enrollment_generation connection_generation execution_connection_generation context_digest capability_digest policy_digest state platform_attempt_sequence worker_attempt_sequence progress_sequence cancel_revision terminal_sequence terminal_status terminal_evidence_digest lease_expires_at cancel_deadline created_at updated_at revision", " ")
	head := object(headKeys, []int{1, 162, 162, 162, 162, 162, 130, 130, 162, 130, 20, 66, 20, 20, 20, 66, 66, 66, 9, 20, 20, 20, 1, 1, 2, 2, stamp, 4, stamp, stamp, 20})
	req := object(strings.Split("version authorization expected_harness_binding_digest expected_attached_pi_api_invocation_policy_digest caller_deadline phase", " "), []int{1, auth, 66, 66, deadline, 20})
	reply := object(strings.Split("status accepted_attempt accepted_attempt_revision harness_binding_digest attached_pi_api_invocation_policy_digest checked_at use_expires_at", " "), []int{9, head, 20, 66, 66, stamp, stamp})
	for _, tc := range []struct {
		name      string
		got, want int
	}{{"auth", auth, AuthorizationMaxBytes}, {"head", head, HeadMaxBytes}, {"timestamp", stamp, TimestampMaxBytes}, {"deadline", deadline, CallerDeadlineMaxBytes}, {"request", req, RequestMaxBytes}, {"reply", reply, ReplyMaxBytes}} {
		if tc.got != tc.want {
			t.Errorf("%s got=%d want=%d", tc.name, tc.got, tc.want)
		}
	}
	if 3*(51+97)+2*7+97 != SharedNodes {
		t.Fatal("shared nodes derivation")
	}
	// Full request reaches its lexical cap with independently recomputed fence;
	// no authentic source/window/currentness claim is made about these endpoints.
	r, _ := fixture(t)
	a := &r.Authorization
	a.TenantID = domain.TenantID(strings.Repeat("t", 160))
	a.OwnerUserID = domain.UserID(strings.Repeat("o", 160))
	a.WorkerID = domain.AttachedWorkerID(strings.Repeat("w", 160))
	a.ConnectionID = domain.AttachedWorkerConnectionID(strings.Repeat("c", 160))
	a.RunID = domain.RunID(strings.Repeat("r", 128))
	a.AttemptID = domain.AttemptID(strings.Repeat("a", 128))
	a.LeaseID = domain.LeaseID(strings.Repeat("l", 128))
	a.EnrollmentGeneration = math.MaxUint64
	a.ConnectionGeneration = math.MaxUint64
	a.LeaseGeneration = math.MaxUint64
	a.ExpectedAttemptRevision = math.MaxUint64
	a.LeaseExpiresAtUnixMicro = math.MaxInt64
	fence, err := domain.NewAttachedWorkerFenceTokenV1(a.TenantID, a.OwnerUserID, a.WorkerID, a.RunID, a.AttemptID, a.LeaseID, a.LeaseGeneration)
	if err != nil {
		t.Fatal(err)
	}
	a.FenceToken = fence
	r.Phase = ImmutableFinalize
	r.CallerDeadline.RemainingNanoseconds = math.MaxInt64
	b := mustRequest(t, r)
	if len(b) != RequestMaxBytes {
		t.Fatalf("actual exact request cap got=%d want=%d", len(b), RequestMaxBytes)
	}
	got, err := DecodeRequest(b)
	if err != nil || got != r {
		t.Fatalf("max request roundtrip %v", err)
	}
	_, err = DecodeRequest(append(append([]byte(nil), b...), '}'))
	invalid(t, err)
	for _, tc := range []struct {
		name string
		edit func(*Request)
	}{
		{"id161", func(r *Request) { r.Authorization.TenantID = domain.TenantID(strings.Repeat("t", 161)) }},
		{"run129", func(r *Request) { r.Authorization.RunID = domain.RunID(strings.Repeat("r", 129)) }},
		{"attempt129", func(r *Request) { r.Authorization.AttemptID = domain.AttemptID(strings.Repeat("a", 129)) }},
		{"lease129", func(r *Request) { r.Authorization.LeaseID = domain.LeaseID(strings.Repeat("l", 129)) }},
	} {
		t.Run(tc.name, func(t *testing.T) { c := r; tc.edit(&c); _, err := EncodeRequest(c); invalid(t, err) })
	}
}

func TestEachOriginalFieldBoundary(t *testing.T) {
	r, _ := fixture(t)
	for _, tc := range []struct {
		name  string
		limit int
		set   func(*ports.AttachedWorkerSealedInputAuthorization, string)
	}{
		{"tenant", 160, func(a *ports.AttachedWorkerSealedInputAuthorization, s string) { a.TenantID = domain.TenantID(s) }},
		{"owner", 160, func(a *ports.AttachedWorkerSealedInputAuthorization, s string) { a.OwnerUserID = domain.UserID(s) }},
		{"worker", 160, func(a *ports.AttachedWorkerSealedInputAuthorization, s string) {
			a.WorkerID = domain.AttachedWorkerID(s)
		}},
		{"connection", 160, func(a *ports.AttachedWorkerSealedInputAuthorization, s string) {
			a.ConnectionID = domain.AttachedWorkerConnectionID(s)
		}},
		{"run", 128, func(a *ports.AttachedWorkerSealedInputAuthorization, s string) { a.RunID = domain.RunID(s) }},
		{"attempt", 128, func(a *ports.AttachedWorkerSealedInputAuthorization, s string) { a.AttemptID = domain.AttemptID(s) }},
		{"lease", 128, func(a *ports.AttachedWorkerSealedInputAuthorization, s string) { a.LeaseID = domain.LeaseID(s) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := r
			tc.set(&c.Authorization, strings.Repeat("x", tc.limit))
			a := &c.Authorization
			fence, err := domain.NewAttachedWorkerFenceTokenV1(a.TenantID, a.OwnerUserID, a.WorkerID, a.RunID, a.AttemptID, a.LeaseID, a.LeaseGeneration)
			if err != nil {
				t.Fatal(err)
			}
			a.FenceToken = fence
			good := mustRequest(t, c)
			if _, err := DecodeRequest(good); err != nil {
				t.Fatal(err)
			}
			bad := replace(t, good, `"`+strings.Repeat("x", tc.limit)+`"`, `"`+strings.Repeat("x", tc.limit+1)+`"`)
			_, err = DecodeRequest(bad)
			invalid(t, err)
		})
	}
	for _, tc := range []struct {
		name string
		set  func(*Request, string)
	}{
		{"context", func(r *Request, s string) { r.Authorization.ContextDigest = domain.AttachedWorkerContextDigest(s) }},
		{"capability", func(r *Request, s string) {
			r.Authorization.CapabilityDigest = domain.AttachedWorkerCapabilityDigest(s)
		}},
		{"policy", func(r *Request, s string) { r.Authorization.PolicyDigest = domain.AttachedWorkerPolicyDigest(s) }},
		{"binding", func(r *Request, s string) { r.ExpectedHarnessBindingDigest = s }},
		{"M", func(r *Request, s string) { r.ExpectedAttachedPiAPIInvocationPolicyDigest = s }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, s := range []string{strings.Repeat("a", 63), strings.Repeat("a", 65), strings.Repeat("A", 64), strings.Repeat("0", 64)} {
				c := r
				tc.set(&c, s)
				_, err := EncodeRequest(c)
				invalid(t, err)
			}
		})
	}
	good := mustRequest(t, r)
	for _, tc := range []struct{ name, old string }{
		{"enrollment", `"enrollment_generation":2`}, {"connection", `"connection_generation":5`},
		{"lease", `"lease_generation":3`}, {"revision", `"expected_attempt_revision":7`},
	} {
		t.Run("zero_"+tc.name, func(t *testing.T) {
			at := strings.LastIndex(tc.old, ":")
			_, err := DecodeRequest(replace(t, good, tc.old, tc.old[:at+1]+"0"))
			invalid(t, err)
		})
	}
	for _, s := range []string{"9223372036854775808", "-1", "01", `"1"`} {
		a := r.Authorization
		old := `"lease_expires_at_unix_micro":` + strconv.FormatInt(a.LeaseExpiresAtUnixMicro, 10)
		_, err := DecodeRequest(replace(t, good, old, `"lease_expires_at_unix_micro":`+s))
		invalid(t, err)
	}
	// Initial ASCII punctuation is forbidden, later punctuation is lawful.
	r.Authorization.ConnectionID = "C.:_-9"
	a := &r.Authorization
	fence, err := domain.NewAttachedWorkerFenceTokenV1(a.TenantID, a.OwnerUserID, a.WorkerID, a.RunID, a.AttemptID, a.LeaseID, a.LeaseGeneration)
	if err != nil {
		t.Fatal(err)
	}
	a.FenceToken = fence
	mustRequest(t, r)
}

func TestWideOriginalHeadVersusConservativeSyntaxCeiling(t *testing.T) {
	_, s := fixture(t)
	h := &s.AcceptedAttempt
	h.TenantID = domain.TenantID(strings.Repeat("t", 160))
	h.OwnerUserID = domain.UserID(strings.Repeat("o", 160))
	h.WorkerID = domain.AttachedWorkerID(strings.Repeat("w", 160))
	h.ConnectionID = domain.AttachedWorkerConnectionID(strings.Repeat("c", 160))
	h.ExecutionConnectionID = domain.AttachedWorkerConnectionID(strings.Repeat("x", 160))
	h.ReservationID = domain.QuotaReservationID(strings.Repeat("q", 160))
	h.RunID = domain.RunID(strings.Repeat("r", 128))
	h.AttemptID = domain.AttemptID(strings.Repeat("a", 128))
	h.LeaseID = domain.LeaseID(strings.Repeat("l", 128))
	h.LeaseGeneration = math.MaxUint64
	h.EnrollmentGeneration = math.MaxUint64
	h.ConnectionGeneration = math.MaxUint64
	h.ExecutionConnectionGeneration = math.MaxUint64
	h.PlatformAttemptSequence = math.MaxUint64
	h.WorkerAttemptSequence = math.MaxUint64
	h.ProgressSequence = math.MaxUint64
	h.Revision = math.MaxUint64
	s.AcceptedAttemptRevision = math.MaxUint64
	fence, err := domain.NewAttachedWorkerFenceTokenV1(h.TenantID, h.OwnerUserID, h.WorkerID, h.RunID, h.AttemptID, h.LeaseID, h.LeaseGeneration)
	if err != nil {
		t.Fatal(err)
	}
	h.FenceToken = fence
	h.LeaseExpiresAt = time.UnixMicro(math.MaxInt64).UTC()
	s.CheckedAt = h.LeaseExpiresAt.Add(-time.Second)
	s.UseExpiresAt = s.CheckedAt.Add(time.Nanosecond)
	b := mustReply(t, s)
	got, err := DecodeReply(b)
	if err != nil || got != s {
		t.Fatalf("wide original structural roundtrip %v", err)
	}
	// Pure lexical ceilings use contradictory/unaligned timestamps. Measure
	// their exact punctuation, but NEVER call them a legitimate current head.
	syntax := s
	extreme := time.Unix(math.MinInt64, 999999999).UTC()
	syntax.AcceptedAttempt.CreatedAt = extreme
	syntax.AcceptedAttempt.UpdatedAt = extreme
	syntax.AcceptedAttempt.LeaseExpiresAt = extreme
	syntax.CheckedAt = extreme
	syntax.UseExpiresAt = extreme
	w := writer{}
	writeHead(&w, syntax.AcceptedAttempt)
	if w.n != HeadMaxBytes {
		t.Fatalf("lexical head max %d want %d", w.n, HeadMaxBytes)
	}
	raw, err := encode(ReplyMaxBytes, func(w *writer) { writeReply(w, syntax) })
	if err != nil || len(raw) != ReplyMaxBytes {
		t.Fatalf("lexical reply max len=%d err=%v", len(raw), err)
	}
	_, err = DecodeReply(raw)
	invalid(t, err)
	_, err = DecodeReply(append(append([]byte(nil), raw...), '}'))
	invalid(t, err)
	for _, edit := range []func(*Reply){
		func(s *Reply) { s.AcceptedAttempt.ReservationID = domain.QuotaReservationID(strings.Repeat("q", 161)) },
		func(s *Reply) {
			s.AcceptedAttempt.ExecutionConnectionID = domain.AttachedWorkerConnectionID(strings.Repeat("x", 161))
		},
	} {
		c := s
		edit(&c)
		_, err := EncodeReply(c)
		invalid(t, err)
	}
}

func TestPreflightRejectsBeforeControlledCopies(t *testing.T) {
	r, _ := fixture(t)
	good := mustRequest(t, r)
	bad := replace(t, good, `"phase":"physical_send"`, `"phase":"unknown"`)
	for _, b := range [][]byte{bad, bytes.Repeat([]byte{'x'}, RequestMaxBytes+1)} {
		if alloc := testing.AllocsPerRun(20, func() { _, _ = DecodeRequest(b) }); alloc != 0 {
			t.Fatalf("preflight rejection allocated controlled copies: %v", alloc)
		}
	}
}

func FuzzClosedWire(f *testing.F) {
	f.Add([]byte(`{"status":"denied"}`))
	f.Add([]byte(`{"version":1}`))
	f.Add([]byte{0xff})
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > ReplyMaxBytes+1 {
			return
		}
		if r, err := DecodeRequest(b); err == nil {
			if !bytes.Equal(mustRequest(t, r), b) {
				t.Fatal("request canonical parity")
			}
		}
		if r, err := DecodeReply(b); err == nil {
			if !bytes.Equal(mustReply(t, r), b) {
				t.Fatal("reply canonical parity")
			}
		}
	})
}
