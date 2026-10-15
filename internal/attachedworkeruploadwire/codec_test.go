package attachedworkeruploadwire

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// All fixtures are bounded structural DATA, not source/provenance/currentness,
// native fit, private cleanup, a real signed capability or a send permission.
func fixture(t *testing.T) (Request, Reply, attachedworkeroutput.AttemptOutputUploadPlanV1) {
	t.Helper()
	at := time.Date(2026, 10, 14, 10, 0, 0, 123456000, time.UTC)
	h := domain.AttachedWorkerAttemptV1{Version: 1, TenantID: "tenant", OwnerUserID: "owner", WorkerID: "worker", ConnectionID: "connection", ExecutionConnectionID: "connection", RunID: "run", AttemptID: "attempt", ReservationID: "reservation", LeaseID: "lease", LeaseGeneration: 3, EnrollmentGeneration: 2, ConnectionGeneration: 5, ExecutionConnectionGeneration: 5, ContextDigest: domain.AttachedWorkerContextDigest(strings.Repeat("a", 64)), CapabilityDigest: domain.AttachedWorkerCapabilityDigest(strings.Repeat("b", 64)), PolicyDigest: domain.AttachedWorkerPolicyDigest(strings.Repeat("c", 64)), State: domain.AttachedWorkerAttemptClaimed, PlatformAttemptSequence: 1, WorkerAttemptSequence: 1, ProgressSequence: 1, CreatedAt: at, UpdatedAt: at, LeaseExpiresAt: at.Add(time.Hour), Revision: 7}
	f, e := domain.NewAttachedWorkerFenceTokenV1(h.TenantID, h.OwnerUserID, h.WorkerID, h.RunID, h.AttemptID, h.LeaseID, h.LeaseGeneration)
	if e != nil {
		t.Fatal(e)
	}
	h.FenceToken = f
	if e = h.Validate(); e != nil {
		t.Fatalf("head fixture: %v", e)
	}
	a := ports.AttachedWorkerSealedInputAuthorization{TenantID: h.TenantID, OwnerUserID: h.OwnerUserID, WorkerID: h.WorkerID, ConnectionID: h.ConnectionID, EnrollmentGeneration: h.EnrollmentGeneration, ConnectionGeneration: h.ConnectionGeneration, RunID: h.RunID, AttemptID: h.AttemptID, AttemptSequence: 1, LeaseID: h.LeaseID, LeaseGeneration: h.LeaseGeneration, FenceToken: h.FenceToken, LeaseExpiresAtUnixMicro: h.LeaseExpiresAt.UnixMicro(), ContextDigest: h.ContextDigest, CapabilityDigest: h.CapabilityDigest, PolicyDigest: h.PolicyDigest, ExpectedAttemptRevision: h.Revision}
	o := attachedworkeroutput.ProcessObservationV1{Version: 1, DescendantsReaped: true, BoundaryReleased: true, CleanupSucceeded: true, CredentialReleaseRequired: true, CredentialReleased: true}
	d, e := o.Digest()
	if e != nil {
		t.Fatal(e)
	}
	s, m := sha256.Sum256([]byte("a")), md5.Sum([]byte("a"))
	r := Request{Version: 1, Operation: Admit, Authorization: a, SessionID: "session", HarnessBindingDigest: domain.HarnessBindingDigestV2(strings.Repeat("d", 64)), ProfileDigest: strings.Repeat("e", 64), SchemaDigest: strings.Repeat("f", 64), FinalDigest: strings.Repeat("1", 64), AnswerDigest: strings.Repeat("2", 64), ProducerDigest: strings.Repeat("3", 64), Nonce: "nonce", CallerDeadline: CallerDeadline{Version: 1, RemainingNanoseconds: uint64(15 * time.Second)}, Observation: o, ObservationDigest: d, Artifacts: []attachedworkeroutput.UploadArtifactMetadataV1{{Ordinal: 0, Name: "answer-\"雪\".txt", MediaType: "text/plain", Size: 1, SHA256: hex.EncodeToString(s[:]), ContentMD5: base64.StdEncoding.EncodeToString(m[:])}}}
	identity := attachedworkeroutput.UploadPlanIdentityV1{Binding: a, SessionID: r.SessionID, HarnessBindingDigest: r.HarnessBindingDigest, ProfileDigest: r.ProfileDigest, SchemaDigest: r.SchemaDigest, FinalDigest: r.FinalDigest, AnswerDigest: r.AnswerDigest, ProducerDigest: r.ProducerDigest, Nonce: r.Nonce, CreatedAt: at.Add(time.Second), ExpiresAt: at.Add(10 * time.Second)}
	p, e := attachedworkeroutput.NewAttemptOutputUploadPlanV1(identity, r.Artifacts)
	if e != nil {
		t.Fatalf("plan fixture: %v", e)
	}
	reply := Reply{Status: ports.AttachedWorkerExecutionApplied, AcceptedAttempt: h, PlanDigest: p.PlanDigest, CreatedAt: p.Identity.CreatedAt, CheckedAt: at.Add(2 * time.Second), ExpiresAt: p.Identity.ExpiresAt, Ordinals: []Ordinal{{Ordinal: 0, State: attachedworkeroutput.UploadOrdinalUnissuedV1}}}
	return r, reply, p
}
func claimFor(r Request, p attachedworkeroutput.AttemptOutputUploadPlanV1) Request {
	return Request{Version: 1, Operation: Claim, Authorization: r.Authorization, Nonce: r.Nonce, PlanDigest: p.PlanDigest, Ordinal: 0, CallerDeadline: r.CallerDeadline}
}
func reconcileFor(r Request) Request {
	return Request{Version: 1, Operation: Reconcile, Authorization: r.Authorization, Nonce: r.Nonce, ProducerDigest: r.ProducerDigest, FinalDigest: r.FinalDigest, CallerDeadline: r.CallerDeadline}
}
func mustRequest(t *testing.T, r Request) []byte {
	t.Helper()
	b, e := EncodeRequest(r)
	if e != nil {
		t.Fatalf("encode %s: %v", r.Operation, e)
	}
	return b
}
func mustReply(t *testing.T, r Reply) []byte {
	t.Helper()
	b, e := EncodeReply(r)
	if e != nil {
		t.Fatalf("encode reply: %v", e)
	}
	return b
}
func invalid(t *testing.T, e error) {
	t.Helper()
	if !errors.Is(e, ErrInvalid) || e.Error() != ErrInvalid.Error() {
		t.Fatalf("error=%v; want safe constant ErrInvalid", e)
	}
}
func changed(t *testing.T, b []byte, old, next string) []byte {
	t.Helper()
	if !bytes.Contains(b, []byte(old)) {
		t.Fatalf("control source absent: %s", old)
	}
	return bytes.Replace(b, []byte(old), []byte(next), 1)
}

func TestAttemptOutputUploadWholeWireRoundTripAndOwnership(t *testing.T) {
	r, reply, p := fixture(t)
	for _, q := range []Request{r, claimFor(r, p), reconcileFor(r)} {
		t.Run(string(q.Operation), func(t *testing.T) {
			b := mustRequest(t, q)
			saved := bytes.Clone(b)
			got, e := DecodeRequest(b)
			if e != nil || !reflect.DeepEqual(got, q) {
				t.Fatalf("roundtrip %s: %v equal=%v", q.Operation, e, reflect.DeepEqual(got, q))
			}
			clear(b)
			if !bytes.Equal(mustRequest(t, got), saved) {
				t.Fatal("decoded request aliases source arena")
			}
			invalid(t, CompareReply(r, q, Reply{Status: Pending}, nil))
		})
	}
	b := mustReply(t, reply)
	saved := bytes.Clone(b)
	got, e := DecodeReply(b)
	if e != nil || !reflect.DeepEqual(got, reply) {
		t.Fatalf("reply roundtrip: %v", e)
	}
	clear(b)
	if !bytes.Equal(mustReply(t, got), saved) {
		t.Fatal("decoded reply aliases source arena")
	}
	if e = CompareReply(r, r, got, nil); e != nil {
		t.Fatalf("original admit association: %v", e)
	}
	for _, status := range []ports.AttachedWorkerExecutionStatus{Pending, ports.AttachedWorkerExecutionDenied} {
		q := Reply{Status: status}
		b := mustReply(t, q)
		want := `{"status":"` + string(status) + `"}`
		if string(b) != want {
			t.Fatalf("closed negative status encoding len=%d", len(b))
		}
		got, e := DecodeReply(b)
		if e != nil || !reflect.DeepEqual(got, q) {
			t.Fatalf("negative roundtrip: %v", e)
		}
	}
}

func TestAttemptOutputUploadClaimCapabilityAndEarlierExpiryRemainData(t *testing.T) {
	r, reply, p := fixture(t)
	q := claimFor(r, p)
	reply.Ordinals[0].State = attachedworkeroutput.UploadOrdinalWritePendingV1
	sha, _ := hex.DecodeString(r.Artifacts[0].SHA256)
	reply.Capability = &Capability{Version: 1, Method: "PUT", URL: "https://storage.example/bucket/" + p.Ordinals[0].Key + "?X-Amz-Signature=private", Headers: SignedHeaders{ContentLength: "1", ContentType: "text/plain", ContentMD5: r.Artifacts[0].ContentMD5, IfNoneMatch: "*", ChecksumSHA256: base64.StdEncoding.EncodeToString(sha)}, ExpiresAt: reply.CheckedAt.Add(time.Second)}
	// Current W is smaller than the immutable plan's original lifetime. Neither
	// decoding nor comparison replaces original L/S/RPC or claims use authority.
	q.CallerDeadline.RemainingNanoseconds = 1
	b := mustReply(t, reply)
	got, e := DecodeReply(b)
	if e != nil || !reflect.DeepEqual(got, reply) {
		t.Fatalf("capability roundtrip: %v", e)
	}
	if e = CompareReply(r, q, got, &p); e != nil {
		t.Fatalf("earlier cap preserves immutable plan expiry: %v", e)
	}
	if !got.Capability.ExpiresAt.Before(got.ExpiresAt) {
		t.Fatal("earlier effective expiry lost")
	}
	clear(b)
	if got.Capability.URL != reply.Capability.URL {
		t.Fatal("capability aliases wire arena")
	}
	for _, v := range []any{q, reply, *reply.Capability, reply.Capability.Headers} {
		b, e := json.Marshal(v)
		if e != nil || string(b) != `"[REDACTED]"` {
			t.Fatalf("generic JSON redaction: %v", e)
		}
		if strings.Contains(fmt.Sprintf("%v %#v", v, v), "private") && !strings.Contains(fmt.Sprintf("%v", v), "[redacted]") {
			t.Fatal("ordinary formatting leaks capability")
		}
	}
	bad := got
	capCopy := *got.Capability
	bad.Capability = &capCopy
	bad.Capability.Headers.ContentLength = "2"
	invalid(t, CompareReply(r, q, bad, &p))
	bad.Capability.Headers.ContentLength = "1"
	bad.Capability.ExpiresAt = got.ExpiresAt.Add(time.Microsecond)
	_, e = EncodeReply(bad)
	invalid(t, e)
}

func TestAttemptOutputUploadRequestRejectsClosedGrammarAndSemanticMutations(t *testing.T) {
	r, _, _ := fixture(t)
	good := mustRequest(t, r)
	cases := []struct{ name, old, next string }{
		{"duplicate", `"version":1,`, `"version":1,"version":1,`},
		{"unknown", `"version":1,`, `"version":1,"extra":0,`},
		{"version", `"version":1,`, `"version":2,`},
		{"reordered", `"version":1,"operation":"admit"`, `"operation":"admit","version":1`},
		{"alias", `"session_id":`, `"sessionId":`},
		{"escaped_key", `"session_id":`, `"\u0073ession_id":`},
		{"escaped_value", `"tenant_id":"tenant"`, `"tenant_id":"\u0074enant"`},
		{"secret", `"tenant_id":"tenant",`, `"tenant_id":"tenant","presented_secret_digest":"secret",`},
		{"leading_zero", `"attempt_sequence":1`, `"attempt_sequence":01`},
		{"exponent", `"attempt_sequence":1`, `"attempt_sequence":1e0`},
		{"zero_deadline", `"remaining_nanoseconds":15000000000`, `"remaining_nanoseconds":0`},
		{"negative_deadline", `"remaining_nanoseconds":15000000000`, `"remaining_nanoseconds":-1`},
		{"overflow_deadline", `"remaining_nanoseconds":15000000000`, `"remaining_nanoseconds":9223372036854775808`},
		{"wrong_observation", `"credential_released":true`, `"credential_released":false`},
		{"runner_failure", `"runner_failed":false`, `"runner_failed":true`},
		{"missing_cleanup", `"cleanup_succeeded":true,`, ``},
		{"observation_extension", `"exit_code":0,`, `"exit_code":0,"process_failure_code":"",`},
		{"malformed_md5", r.Artifacts[0].ContentMD5, strings.Repeat("A", 24)},
		{"ordinal", `"ordinal":0`, `"ordinal":1`},
		{"size", `"size":1`, `"size":32769`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { _, e := DecodeRequest(changed(t, good, tc.old, tc.next)); invalid(t, e) })
	}
	for _, b := range [][]byte{append(bytes.Clone(good), ' '), append([]byte(" "), good...), append(bytes.Clone(good), good...), []byte(`{"version":1,"operation":[[[[[[]]]]]]}`), bytes.Repeat([]byte("x"), RequestMaxBytes+1), []byte("null")} {
		_, e := DecodeRequest(b)
		invalid(t, e)
	}
	for i := 0; i < len(good); i++ {
		if _, e := DecodeRequest(good[:i]); !errors.Is(e, ErrInvalid) {
			t.Fatalf("truncated prefix %d accepted", i)
		}
	}
	bad := r
	bad.Operation = Claim
	invalid(t, ValidateRequest(bad))
	bad = r
	bad.Authorization.PresentedSecretDigest = "secret"
	invalid(t, ValidateAuthorization(bad.Authorization))
}

func TestAttemptOutputUploadReplyRejectsClosedGrammarAndTuples(t *testing.T) {
	r, reply, p := fixture(t)
	good := mustReply(t, reply)
	cases := []struct{ name, old, next string }{
		{"status_unknown", `"status":"applied"`, `"status":"ready"`},
		{"missing_optional", `"cancel_deadline":null,`, ``},
		{"cancelled", `"cancel_revision":0`, `"cancel_revision":1`},
		{"terminal", `"terminal_status":""`, `"terminal_status":"succeeded"`},
		{"state", `"state":"unissued"`, `"state":"ready"`},
		{"duplicate_head", `"version":1,`, `"version":1,"version":1,`},
		{"invalid_timestamp", `"nanoseconds":123456000`, `"nanoseconds":1000000000`},
		{"negative_zero", `"seconds":`, `"seconds":-0,"bogus":`},
		{"null_cap_extension", `"capability":null`, `"capability":null,"source_grant":true`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { _, e := DecodeReply(changed(t, good, tc.old, tc.next)); invalid(t, e) })
	}
	for _, b := range [][]byte{[]byte(`{"status":"pending","capability":null}`), []byte(`{"status":"denied","accepted_attempt":{}}`), append(bytes.Clone(good), ' '), bytes.Repeat([]byte("x"), ReplyMaxBytes+1)} {
		_, e := DecodeReply(b)
		invalid(t, e)
	}
	for i := 0; i < len(good); i++ {
		if _, e := DecodeReply(good[:i]); !errors.Is(e, ErrInvalid) {
			t.Fatalf("truncated reply %d accepted", i)
		}
	}
	bad := reply
	bad.AcceptedAttempt.Revision++
	invalid(t, CompareReply(r, r, bad, nil))
	bad = reply
	bad.PlanDigest = strings.Repeat("a", 64)
	invalid(t, CompareReply(r, r, bad, nil))
	q := reconcileFor(r)
	if e := CompareReply(r, q, reply, &p); e != nil {
		t.Fatalf("retained original plan: %v", e)
	}
	invalid(t, CompareReply(r, q, reply, nil))
	foreign := p.Clone()
	foreign.Identity.ProducerDigest = strings.Repeat("4", 64)
	invalid(t, CompareReply(r, q, reply, &foreign))
}

func TestAttemptOutputUploadExactMetadataAndScalarBoundaries(t *testing.T) {
	r, _, _ := fixture(t)
	// Full160-byte run/attempt/lease IDs, not the unrelated phase128 ceiling.
	for _, n := range []int{128, 160} {
		t.Run(fmt.Sprintf("id_%d", n), func(t *testing.T) {
			q := r
			q.Authorization.RunID = domain.RunID(strings.Repeat("r", n))
			q.Authorization.AttemptID = domain.AttemptID(strings.Repeat("a", n))
			q.Authorization.LeaseID = domain.LeaseID(strings.Repeat("l", n))
			f, e := domain.NewAttachedWorkerFenceTokenV1(q.Authorization.TenantID, q.Authorization.OwnerUserID, q.Authorization.WorkerID, q.Authorization.RunID, q.Authorization.AttemptID, q.Authorization.LeaseID, q.Authorization.LeaseGeneration)
			if e != nil {
				t.Fatal(e)
			}
			q.Authorization.FenceToken = f
			_, e = DecodeRequest(mustRequest(t, q))
			if e != nil {
				t.Fatal(e)
			}
		})
	}
	bad := r
	bad.Authorization.RunID = domain.RunID(strings.Repeat("r", 161))
	invalid(t, ValidateRequest(bad))
	for _, name := range []string{strings.Repeat(`"`, 255), "literal-__proto__-雪.txt", "<>&\u2028\u2029.txt"} {
		q := r
		q.Artifacts = append([]attachedworkeroutput.UploadArtifactMetadataV1(nil), r.Artifacts...)
		q.Artifacts[0].Name = name
		got, e := DecodeRequest(mustRequest(t, q))
		if e != nil || got.Artifacts[0].Name != name {
			t.Fatalf("legitimate name scalar: %v", e)
		}
	}
	bad = r
	bad.Artifacts = append([]attachedworkeroutput.UploadArtifactMetadataV1(nil), r.Artifacts...)
	bad.Artifacts[0].Name = strings.Repeat("x", 256)
	invalid(t, ValidateRequest(bad))
	bad.Artifacts[0].Name = "line\nfeed"
	invalid(t, ValidateRequest(bad))
	bad.Artifacts[0].Name = string([]byte{0xff})
	invalid(t, ValidateRequest(bad))
	q := r
	s, m := sha256.Sum256(nil), md5.Sum(nil)
	q.Artifacts = []attachedworkeroutput.UploadArtifactMetadataV1{{Ordinal: 0, Name: "empty.txt", MediaType: "text/plain", Size: 0, SHA256: hex.EncodeToString(s[:]), ContentMD5: base64.StdEncoding.EncodeToString(m[:])}}
	if _, e := DecodeRequest(mustRequest(t, q)); e != nil {
		t.Fatalf("empty text: %v", e)
	}
	q.Artifacts[0].MediaType = "application/json"
	invalid(t, ValidateRequest(q))
	q = r
	q.Artifacts = append([]attachedworkeroutput.UploadArtifactMetadataV1(nil), r.Artifacts...)
	q.Artifacts[0].Size = 32768
	other := q.Artifacts[0]
	other.Ordinal = 1
	other.Name = "other.json"
	other.MediaType = "application/json"
	q.Artifacts = append(q.Artifacts, other)
	if _, e := DecodeRequest(mustRequest(t, q)); e != nil {
		t.Fatalf("two32KiB metadata cohort: %v", e)
	}
	q.Artifacts[1].Name = strings.ToUpper(q.Artifacts[0].Name)
	invalid(t, ValidateRequest(q))
}

func TestAttemptOutputUploadCapabilityBoundariesAndMalformedInput(t *testing.T) {
	r, reply, _ := fixture(t)
	reply.Ordinals[0].State = attachedworkeroutput.UploadOrdinalWritePendingV1
	sha, err := hex.DecodeString(r.Artifacts[0].SHA256)
	if err != nil {
		t.Fatal(err)
	}
	prefix := "https://storage.example/"
	reply.Capability = &Capability{Version: 1, Method: "PUT", URL: prefix + strings.Repeat("a", URLMaxBytes-len(prefix)), Headers: SignedHeaders{ContentLength: "1", ContentType: "text/plain", ContentMD5: r.Artifacts[0].ContentMD5, IfNoneMatch: "*", ChecksumSHA256: base64.StdEncoding.EncodeToString(sha)}, ExpiresAt: reply.CheckedAt.Add(time.Second)}
	b := mustReply(t, reply)
	got, err := DecodeReply(b)
	if err != nil || len(got.Capability.URL) != URLMaxBytes {
		t.Fatalf("URL boundary: %v", err)
	}
	pre := parser{b: b, ok: true, maxNodes: ReplyNodes}
	parseReply(&pre)
	if !pre.ok || pre.i != len(b) || pre.nodes > ReplyNodes {
		t.Fatalf("cap preflight nodes=%d", pre.nodes)
	}
	if a := testing.AllocsPerRun(10, func() { q := parser{b: b, ok: true, maxNodes: ReplyNodes}; parseReply(&q) }); a != 0 {
		t.Fatalf("cap structural pass allocations=%v", a)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*Capability)
	}{
		{"first_over_url", func(c *Capability) { c.URL += "a" }},
		{"userinfo", func(c *Capability) { c.URL = "https://user@storage.example/key" }},
		{"http", func(c *Capability) { c.URL = "http://storage.example/key" }},
		{"invalid_utf8", func(c *Capability) { c.URL = prefix + string([]byte{0xff}) }},
		{"md5_without_padding", func(c *Capability) { c.Headers.ContentMD5 = strings.Repeat("A", 24) }},
		{"sha_without_padding", func(c *Capability) { c.Headers.ChecksumSHA256 = strings.Repeat("A", 44) }},
		{"length_first_over", func(c *Capability) { c.Headers.ContentLength = "32769" }},
		{"length_alias", func(c *Capability) { c.Headers.ContentLength = "01" }},
		{"unconditional", func(c *Capability) { c.Headers.IfNoneMatch = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := reply
			c := *reply.Capability
			q.Capability = &c
			tc.mutate(&c)
			_, e := EncodeReply(q)
			invalid(t, e)
		})
	}
	_, err = DecodeReply(changed(t, b, `"content-type":"text/plain"`, `"content-type":"text/plain","content-type":"text/plain"`))
	invalid(t, err)
	_, err = DecodeReply(changed(t, b, `"headers":{`, `"headers":{"extra":"value",`))
	invalid(t, err)
}

func TestAttemptOutputUploadMeasuredCeilingsAndAllocationFreeStructuralPass(t *testing.T) {
	r, reply, p := fixture(t)
	for _, q := range []Request{r, claimFor(r, p), reconcileFor(r)} {
		b := mustRequest(t, q)
		pre := parser{b: b, ok: true, maxNodes: AdmitNodes}
		parseRequest(&pre)
		if !pre.ok || pre.i != len(b) || pre.nodes > AdmitNodes {
			t.Fatalf("request preflight: op=%s nodes=%d", q.Operation, pre.nodes)
		}
		if a := testing.AllocsPerRun(10, func() { s := parser{b: b, ok: true, maxNodes: AdmitNodes}; parseRequest(&s) }); a != 0 {
			t.Fatalf("preflight allocations=%v", a)
		}
	}
	b := mustReply(t, reply)
	if a := testing.AllocsPerRun(10, func() { s := parser{b: b, ok: true, maxNodes: ReplyNodes}; parseReply(&s) }); a != 0 {
		t.Fatalf("reply preflight allocations=%v", a)
	}
	// A valid source-derived wide request approaches but need not attain the
	// conservative ceiling (distinct names cannot both be the same255 quotes).
	wide := r
	wide.Authorization.TenantID = domain.TenantID(strings.Repeat("t", 160))
	wide.Authorization.OwnerUserID = domain.UserID(strings.Repeat("o", 160))
	wide.Authorization.WorkerID = domain.AttachedWorkerID(strings.Repeat("w", 160))
	wide.Authorization.ConnectionID = domain.AttachedWorkerConnectionID(strings.Repeat("c", 160))
	wide.Authorization.RunID = domain.RunID(strings.Repeat("r", 160))
	wide.Authorization.AttemptID = domain.AttemptID(strings.Repeat("a", 160))
	wide.Authorization.LeaseID = domain.LeaseID(strings.Repeat("l", 160))
	wide.Authorization.EnrollmentGeneration = math.MaxUint64
	wide.Authorization.ConnectionGeneration = math.MaxUint64
	wide.Authorization.LeaseGeneration = math.MaxUint64
	wide.Authorization.ExpectedAttemptRevision = math.MaxUint64
	wide.Authorization.LeaseExpiresAtUnixMicro = math.MaxInt64
	f, e := domain.NewAttachedWorkerFenceTokenV1(wide.Authorization.TenantID, wide.Authorization.OwnerUserID, wide.Authorization.WorkerID, wide.Authorization.RunID, wide.Authorization.AttemptID, wide.Authorization.LeaseID, wide.Authorization.LeaseGeneration)
	if e != nil {
		t.Fatal(e)
	}
	wide.Authorization.FenceToken = f
	wide.Nonce = domain.IdempotencyKey(strings.Repeat("n", 160))
	wide.SessionID = domain.SessionID(strings.Repeat("s", 160))
	wide.CallerDeadline.RemainingNanoseconds = math.MaxInt64
	wide.Observation.CredentialReleaseRequired = false
	wide.Observation.CredentialReleased = false
	wide.ObservationDigest, e = wide.Observation.Digest()
	if e != nil {
		t.Fatal(e)
	}
	wide.Artifacts = []attachedworkeroutput.UploadArtifactMetadataV1{r.Artifacts[0], r.Artifacts[0]}
	wide.Artifacts[0].Name = strings.Repeat(`"`, 255)
	wide.Artifacts[1].Name = strings.Repeat(`"`, 254) + "x"
	wide.Artifacts[1].Ordinal = 1
	for i := range wide.Artifacts {
		wide.Artifacts[i].MediaType = "application/json"
		wide.Artifacts[i].Size = 32768
	}
	b = mustRequest(t, wide)
	if len(b) != AdmitMaxBytes-1 {
		t.Fatalf("measured wide admit=%d want=%d", len(b), AdmitMaxBytes-1)
	}
	if _, e = DecodeRequest(b); e != nil {
		t.Fatalf("wide decode: %v", e)
	}
	for _, q := range []Request{claimFor(wide, p), reconcileFor(wide)} {
		b = mustRequest(t, q)
		if len(b) != requestLimit(q.Operation) {
			t.Fatalf("measured %s=%d want=%d", q.Operation, len(b), requestLimit(q.Operation))
		}
	}
	if SharedNodes != 951 || SharedRequestBytes != 13511 || SharedReplyBytes != 48714 || SharedControlBytes != 62225 || MinimumWireCopyBytes != 157822 || ByteArenaCapacity != 45066 {
		t.Fatal("shared prospective inventory drift")
	}
	// Structural controls reject before the admitted-string ownership pass.
	for _, b := range [][]byte{[]byte(`{"version":1,"operation":"admit","authorization":[[[[]]]]}`), []byte(`{"version":1,"version":1}`)} {
		pre := parser{b: b, ok: true, maxNodes: AdmitNodes}
		parseRequest(&pre)
		if pre.ok && pre.i == len(b) {
			t.Fatal("malformed structure admitted")
		}
	}
}
