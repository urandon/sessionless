package sessioncontext_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
	"gitcode.com/urandon/sessionless/internal/sessionmaterial"
)

func prefixGenesis(t *testing.T) sessioncontext.SessionMaterialPrefixV1 {
	t.Helper()
	p, err := sessioncontext.NewSessionMaterialGenesisV1("prefix-tenant", "prefix-session")
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func prefixEvent(t *testing.T, sequence uint64, kind domain.SessionEventKind, body string) (domain.SessionEvent, *sessioncontext.FinalMaterialMeasureV1) {
	t.Helper()
	user, run := domain.UserID("prefix-user"), domain.RunID("prefix-run")
	sum := sha256.Sum256([]byte(body))
	e := domain.SessionEvent{ID: domain.SessionEventID(fmt.Sprintf("event-%d", sequence)), TenantID: "prefix-tenant", SessionID: "prefix-session", Sequence: sequence, Kind: kind, AuthorUserID: &user, RunID: &run, IdempotencyKey: domain.IdempotencyKey(fmt.Sprintf("idempotency-%d", sequence)), Payload: domain.BlobRef{TenantID: "prefix-tenant", Key: fmt.Sprintf("tenants/prefix-tenant/sessions/prefix-session/events/event-%d/message.json", sequence), Size: int64(len(body)), SHA256: hex.EncodeToString(sum[:])}, CreatedAt: time.Date(2026, 10, 11, 23, 1, 2, 123456789, time.UTC)}
	p, err := sessionmaterial.PrepareV1(e, []byte(body), nil)
	if err != nil {
		t.Fatalf("prepare event fixture: %v", err)
	}
	t.Cleanup(p.Release)
	m, err := sessioncontext.MeasurePreparedRecordV1(e, p)
	if err != nil {
		t.Fatalf("measure event fixture: %v", err)
	}
	return e, &m
}

func prefixFold(t *testing.T, p sessioncontext.SessionMaterialPrefixV1, e domain.SessionEvent, m *sessioncontext.FinalMaterialMeasureV1) sessioncontext.SessionMaterialPrefixV1 {
	t.Helper()
	got, err := sessioncontext.FoldSessionMaterialV1(p, e, m)
	if err != nil {
		t.Fatalf("fold sequence=%d kind=%s: %v", e.Sequence, e.Kind, err)
	}
	if err := got.Validate(); err != nil {
		t.Fatalf("fold returned invalid prefix: %v", err)
	}
	return got
}

// This test encoder deliberately does not use production digest/framing helpers.
func independentPrefixFrames(values ...string) string {
	var encoded bytes.Buffer
	for _, value := range values {
		var n [8]byte
		binary.BigEndian.PutUint64(n[:], uint64(len(value)))
		encoded.Write(n[:])
		encoded.WriteString(value)
	}
	digest := sha256.Sum256(encoded.Bytes())
	return hex.EncodeToString(digest[:])
}

func TestMaterialPrefixLiteralSeedsScopeAndNoLegacyRepair(t *testing.T) {
	g := prefixGenesis(t)
	if g.PrefixDigest != "070d6a05960e3e564fe704d95c083bfed27d442f853856537722e36c23893620" || g.ThroughSequence != 0 || g.Coverage != sessioncontext.SessionMaterialCompleteV1 || g.UnknownReasons != 0 || g.LastEventID != "" || g.LastContribution != nil || g.RenderedFrameBytes != 0 {
		t.Fatalf("literal genesis invariant: %+v", g)
	}
	legacy, err := sessioncontext.NewUnknownSessionMaterialPrefixV1(g.TenantID, g.SessionID, 7)
	if err != nil || legacy.PrefixDigest != "23b5dd453ecf9af295f66ba9e677b1053bca3a20f439da3221e8f5267f0b0a37" || legacy.Coverage != sessioncontext.SessionMaterialUnknownV1 || legacy.UnknownRecords != 7 || legacy.UserMessageCount != 0 || legacy.Validate() != nil {
		t.Fatalf("literal legacy seed: %+v %v", legacy, err)
	}
	for _, seq := range []uint64{0, 7, math.MaxInt64, math.MaxInt64 + 1, math.MaxUint64} {
		p, err := sessioncontext.NewUnknownSessionMaterialPrefixV1(g.TenantID, g.SessionID, seq)
		if err != nil || p.Validate() != nil || p.UnknownRecords != min(seq, uint64(math.MaxInt64)) || p.PrefixDigest == g.PrefixDigest {
			t.Fatalf("unknown seed seq=%d conflated with genesis: %+v %v", seq, p, err)
		}
		if (p.UnknownReasons&sessioncontext.SessionMaterialSaturatedV1 != 0) != (seq > math.MaxInt64) {
			t.Fatalf("seed saturation seq=%d reasons=%d", seq, p.UnknownReasons)
		}
	}
	for _, scope := range [][2]string{{"prefix-tenant", "other-session"}, {"other-tenant", "prefix-session"}, {"a", "bc"}, {"ab", "c"}} {
		p, err := sessioncontext.NewSessionMaterialGenesisV1(domain.TenantID(scope[0]), domain.SessionID(scope[1]))
		if err != nil || p.PrefixDigest == g.PrefixDigest {
			t.Fatalf("scope not sealed: %v", err)
		}
	}
	a, _ := sessioncontext.NewSessionMaterialGenesisV1("a", "bc")
	b, _ := sessioncontext.NewSessionMaterialGenesisV1("ab", "c")
	if a.PrefixDigest == b.PrefixDigest {
		t.Fatal("scope field boundaries collide")
	}
	for _, scope := range [][2]string{{"", "prefix-session"}, {"prefix-tenant", ""}, {"foreign/tenant", "prefix-session"}, {"prefix-tenant", strings.Repeat("x", 161)}} {
		if p, err := sessioncontext.NewSessionMaterialGenesisV1(domain.TenantID(scope[0]), domain.SessionID(scope[1])); !errors.Is(err, sessioncontext.ErrSessionMaterialPrefix) || p.Version != 0 {
			t.Fatalf("invalid scope accepted: %v", err)
		}
	}
	e, m := prefixEvent(t, 8, domain.SessionEventUserMessage, `{"version":1,"text":"hello"}`)
	next := prefixFold(t, legacy, e, m)
	if next.Coverage != sessioncontext.SessionMaterialUnknownV1 || next.UnknownReasons&sessioncontext.SessionMaterialLegacyV1 == 0 || next.UnknownRecords != 7 || next.UserMessageCount != 1 || next.CodexCompatibleRecords != 1 {
		t.Fatalf("valid text repaired legacy: %+v", next)
	}
	for _, mutate := range []func(*sessioncontext.SessionMaterialPrefixV1){func(p *sessioncontext.SessionMaterialPrefixV1) { p.PrefixDigest = g.PrefixDigest }, func(p *sessioncontext.SessionMaterialPrefixV1) { p.UnknownRecords = 0 }, func(p *sessioncontext.SessionMaterialPrefixV1) {
		p.Coverage = sessioncontext.SessionMaterialCompleteV1
		p.UnknownReasons = 0
	}} {
		bad := legacy
		mutate(&bad)
		if bad.Validate() == nil {
			t.Fatal("forged legacy special seed accepted")
		}
	}
}

func TestMaterialPrefixFoldIndependentFramesAndExactCodec(t *testing.T) {
	g := prefixGenesis(t)
	e, m := prefixEvent(t, 1, domain.SessionEventUserMessage, `{"version":1,"text":"hello"}`)
	p := prefixFold(t, g, e, m)
	// Frozen golden independently derived from literal codec/previous JSON and
	// big-endian framing using Perl Digest::SHA, not production folding helpers.
	if p.PrefixDigest != "331580b9a3270aefcc54cd091363cc29438262a733edcabdc27815cd2cbc76fc" || p.LastContribution.IdentityDigest != "41a6673b556d650a6fa53ae80bd55923c1518139bd862d2cffde2d6fb15adf3a" || p.LastContribution.RecordDigest != "7ec00e1a6b5b7cd375e3ba9f0f5bca0fbb85f90de5b5de1995ac4dca4c562e6a" || p.EncodedJSONLBytes != 540 {
		t.Fatalf("literal independent fold/record golden changed: digest=%s", p.PrefixDigest)
	}
	encoded, err := sessioncontext.EncodeRecord(e, []byte(`{"version":1,"text":"hello"}`))
	if err != nil {
		t.Fatal(err)
	}
	recordHash := sha256.Sum256(encoded)
	if p.LastContribution.RecordDigest != hex.EncodeToString(recordHash[:]) || p.EncodedJSONLBytes != uint64(len(encoded)) || p.MaxEncodedRecordBytes != uint64(len(encoded)) || p.VerifiedPayloadBytes != 28 || p.UserMessageCount != 1 || p.CodexCompatibleRecords != 1 || p.RenderedFrameBytes != 13 || p.UnknownRecords != 0 || p.Coverage != sessioncontext.SessionMaterialCompleteV1 {
		t.Fatalf("exact source/frame/record contribution: %+v", p)
	}
	var seq [8]byte
	binary.BigEndian.PutUint64(seq[:], 1)
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], 28)
	identity := independentPrefixFrames("sessionless.session-material.final-identity.v1", "event-1", "prefix-tenant", "prefix-session", string(seq[:]), "user_message", "present", "prefix-user", "present", "prefix-run", "idempotency-1", "prefix-tenant", "tenants/prefix-tenant/sessions/prefix-session/events/event-1/message.json", string(size[:]), e.Payload.SHA256, "2026-10-11T23:01:02.123456789Z", "2026-10-11T23:01:02.123456789Z")
	if identity != p.LastContribution.IdentityDigest {
		t.Fatal("final identity framing differs from independent implementation")
	}
	previousJSON, err := sessioncontext.EncodeSessionMaterialPrefixV1(g)
	if err != nil {
		t.Fatal(err)
	}
	contributionJSON, err := json.Marshal(p.LastContribution)
	if err != nil {
		t.Fatal(err)
	}
	want := independentPrefixFrames("sessionless.session-material.fold.v1", g.PrefixDigest, string(previousJSON), identity, p.LastContribution.RecordDigest, "verified-contribution.v1", string(contributionJSON))
	if p.PrefixDigest != want {
		t.Fatalf("fold digest=%s independent=%s", p.PrefixDigest, want)
	}
	clone := p.Clone()
	clone.LastContribution.RecordDigest = strings.Repeat("f", 64)
	if p.LastContribution.RecordDigest == clone.LastContribution.RecordDigest || sessioncontext.EqualSessionMaterialPrefixV1(p, clone) {
		t.Fatal("clone retained mutable contribution/equality ignores fields")
	}
	if g.LastContribution != nil || g.ThroughSequence != 0 {
		t.Fatal("fold mutated previous prefix")
	}
	for _, format := range []string{"%v", "%+v", "%#v", "%s", "%x"} {
		if got := fmt.Sprintf(format, p); got != "sessioncontext.SessionMaterialPrefixV1" {
			t.Fatalf("private prefix format disclosed facts: %s", got)
		}
	}
}

func TestMaterialPrefixAllKindsAndHistoryDoesNotSkipIncompatibility(t *testing.T) {
	p := prefixGenesis(t)
	var source, records, tools, frames uint64
	for i, test := range []struct {
		kind       domain.SessionEventKind
		body       string
		compatible bool
	}{
		{domain.SessionEventUserMessage, `{"version":1,"text":"hello"}`, true},
		{domain.SessionEventAssistantMessage, `{"schema":"sessionless.assistant-message.v1","summary":"done","artifact_manifest_id":"manifest"}`, true},
		{domain.SessionEventToolCall, `{"schema":"sessionless.tool-event.v1","call_id":"call","tool_name":"tool","content":{}}`, false},
		{domain.SessionEventToolResult, `{"schema":"sessionless.tool-event.v1","call_id":"call","tool_name":"tool","content":null}`, false},
		{domain.SessionEventSystemNotice, `{"schema":"sessionless.run-terminal-notice.v1","code":"failed","cancelled":false}`, false},
		{domain.SessionEventUserMessage, `{"version":1,"text":"hello"}`, true},
	} {
		e, m := prefixEvent(t, uint64(i+1), test.kind, test.body)
		line, _ := sessioncontext.EncodeRecord(e, []byte(test.body))
		source += uint64(len(test.body))
		records += uint64(len(line))
		if test.kind == domain.SessionEventToolCall || test.kind == domain.SessionEventToolResult {
			tools += uint64(len(test.body))
		}
		if test.compatible {
			s, _ := m.SourceMeasures()
			frames += s.RenderedFrameBytes
		}
		p = prefixFold(t, p, e, m)
	}
	if p.UserMessageCount != 2 || p.AssistantMessageCount != 1 || p.ToolCallCount != 1 || p.ToolResultCount != 1 || p.SystemNoticeCount != 1 || p.VerifiedPayloadBytes != source || p.EncodedJSONLBytes != records || p.ToolEnvelopeBytes != tools || p.CodexCompatibleRecords != 3 || p.CodexIncompatibleRecords != 3 || p.RenderedFrameBytes != frames || p.Coverage != sessioncontext.SessionMaterialCompleteV1 {
		t.Fatalf("source kind/renderer partition lost historical evidence: %+v", p)
	}
}

func TestMaterialPrefixAttachmentsReferenceOnlyAndSaturate(t *testing.T) {
	p := prefixGenesis(t)
	for i, size := range []int64{math.MaxInt64, 1} {
		sequence := uint64(i + 1)
		body := `{"version":1,"text":"hello"}`
		e, unused := prefixEvent(t, sequence, domain.SessionEventUserMessage, body)
		_ = unused
		descriptor := domain.Artifact{Name: "private-photo.png", MediaType: "image/png", Blob: domain.BlobRef{TenantID: e.TenantID, Key: domain.SessionEventObjectPrefix(e.TenantID, e.SessionID, e.ID) + "private-photo.png", Size: size, SHA256: strings.Repeat("a", 64)}}
		payload, err := json.Marshal(struct {
			Version     uint32            `json:"version"`
			Text        string            `json:"text"`
			Attachments []domain.Artifact `json:"attachments"`
		}{1, "hello", []domain.Artifact{descriptor}})
		if err != nil {
			t.Fatal(err)
		}
		hash := sha256.Sum256(payload)
		e.Payload.Size, e.Payload.SHA256 = int64(len(payload)), hex.EncodeToString(hash[:])
		ref := descriptor
		ref.Name = "attachment-01-private-photo.png"
		prepared, err := sessionmaterial.PrepareV1(e, payload, []domain.Artifact{ref})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(prepared.Release)
		m, err := sessioncontext.MeasurePreparedRecordV1(e, prepared)
		if err != nil {
			t.Fatal(err)
		}
		p = prefixFold(t, p, e, &m)
		if p.AttachmentCount != sequence || p.ReferencedAttachmentBytes != math.MaxInt64 || p.CodexCompatibleRecords != 0 || p.CodexIncompatibleRecords != sequence || p.RenderedFrameBytes != 0 {
			t.Fatal("attachment reference became body/media/rendering proof or wrapped")
		}
		want := sessioncontext.SessionMaterialCompleteV1
		if i == 1 {
			want = sessioncontext.SessionMaterialUnknownV1
		}
		if p.Coverage != want || (p.UnknownReasons&sessioncontext.SessionMaterialSaturatedV1 != 0) != (i == 1) {
			t.Fatal("referenced-size overflow did not permanently mark unknown")
		}
		encoded, err := sessioncontext.EncodeSessionMaterialPrefixV1(p)
		if err != nil || bytes.Contains(encoded, []byte("private-photo")) || bytes.Contains(encoded, []byte("image/png")) || bytes.Contains(encoded, []byte("attachment-01")) {
			t.Fatalf("private attachment hint retained in prefix: %v", err)
		}
	}
}

func TestMaterialPrefixUnknownNoForgeOrNormalization(t *testing.T) {
	g := prefixGenesis(t)
	e, m := prefixEvent(t, 1, domain.SessionEventUserMessage, `{"version":1,"text":"hello"}`)
	var zero sessioncontext.FinalMaterialMeasureV1
	for _, measure := range []*sessioncontext.FinalMaterialMeasureV1{nil, &zero} {
		p := prefixFold(t, g, e, measure)
		if p.Coverage != sessioncontext.SessionMaterialUnknownV1 || p.UnknownReasons != sessioncontext.SessionMaterialMissingInvalidV1 || p.UnknownRecords != 1 || p.UserMessageCount != 1 || p.VerifiedPayloadBytes != 0 || p.LastContribution.Verified || p.LastContribution.RecordDigest != "" {
			t.Fatalf("zero/nil forged verified contribution: %+v", p)
		}
		nextEvent, nextMeasure := prefixEvent(t, 2, domain.SessionEventUserMessage, `{"version":1,"text":"hello"}`)
		next := prefixFold(t, p, nextEvent, nextMeasure)
		if next.Coverage != sessioncontext.SessionMaterialUnknownV1 || next.UnknownRecords != 1 || next.CodexCompatibleRecords != 1 {
			t.Fatal("known text repaired unknown prefix")
		}
	}
	changed := e
	changed.Payload.Key += ".other"
	p := prefixFold(t, g, changed, m)
	if p.LastContribution.Verified || p.UnknownRecords != 1 {
		t.Fatal("mismatched opaque measure verified substituted source")
	}
	// Same instant with a different SERIALIZED offset is not the same record.
	offset := e
	offset.CreatedAt = e.CreatedAt.In(time.FixedZone("different", 3600))
	p = prefixFold(t, g, offset, m)
	if p.LastContribution.Verified || p.LastContribution.IdentityDigest == prefixFold(t, g, e, m).LastContribution.IdentityDigest {
		t.Fatal("timestamp offset normalized or reused old measure")
	}
	same := e
	same.CreatedAt = e.CreatedAt.In(time.FixedZone("same offset", 0))
	if !sessioncontext.EqualSessionMaterialPrefixV1(prefixFold(t, g, same, m), prefixFold(t, g, e, m)) {
		t.Fatal("location name changed exact codec facts")
	}
	wallZero, wallSeconds := e, e
	wallZero.CreatedAt = time.Date(2026, 10, 11, 23, 1, 2, 123456789, time.FixedZone("minute", 60))
	wallSeconds.CreatedAt = time.Date(2026, 10, 11, 23, 1, 2, 123456789, time.FixedZone("seconds", 61))
	if wallZero.CreatedAt.Format(time.RFC3339Nano) != wallSeconds.CreatedAt.Format(time.RFC3339Nano) || wallZero.CreatedAt.Equal(wallSeconds.CreatedAt) {
		t.Fatal("timezone-seconds collision fixture not independent")
	}
	if prefixFold(t, g, wallZero, nil).LastContribution.IdentityDigest == prefixFold(t, g, wallSeconds, nil).LastContribution.IdentityDigest {
		t.Fatal("serialized timestamp concealed different semantic instant")
	}
	// A huge domain-valid key is hashed, never retained in the bounded fact and
	// never turned into a new canonical history prohibition.
	huge := e
	huge.Payload.Key += strings.Repeat("x", 20000)
	p = prefixFold(t, g, huge, nil)
	encoded, err := sessioncontext.EncodeSessionMaterialPrefixV1(p)
	if err != nil || len(encoded) > sessioncontext.MaxSessionMaterialPrefixBytesV1 || bytes.Contains(encoded, []byte(huge.Payload.Key)) {
		t.Fatalf("unknown metadata not bounded/private: bytes=%d err=%v", len(encoded), err)
	}
}

func TestMaterialPrefixRejectsScopeSequenceAndInvalidPrevious(t *testing.T) {
	g := prefixGenesis(t)
	e, m := prefixEvent(t, 1, domain.SessionEventUserMessage, `{"version":1,"text":"hello"}`)
	for _, change := range []func(*domain.SessionEvent){func(e *domain.SessionEvent) { e.Sequence = 0 }, func(e *domain.SessionEvent) { e.Sequence = 2 }, func(e *domain.SessionEvent) { e.TenantID = "other-tenant" }, func(e *domain.SessionEvent) { e.SessionID = "other-session" }, func(e *domain.SessionEvent) { e.ID = "" }, func(e *domain.SessionEvent) { e.CreatedAt = time.Time{} }, func(e *domain.SessionEvent) { e.Kind = "unknown" }} {
		bad := e
		change(&bad)
		if p, err := sessioncontext.FoldSessionMaterialV1(g, bad, m); p.Version != 0 || !errors.Is(err, sessioncontext.ErrSessionMaterialPrefix) || err.Error() != "canonical material prefix invalid" {
			t.Fatalf("invalid final identity accepted: %v", err)
		}
	}
	if p, err := sessioncontext.FoldSessionMaterialV1(sessioncontext.SessionMaterialPrefixV1{}, e, m); p.Version != 0 || !errors.Is(err, sessioncontext.ErrSessionMaterialPrefix) {
		t.Fatal("zero previous prefix accepted")
	}
	max, _ := sessioncontext.NewUnknownSessionMaterialPrefixV1(g.TenantID, g.SessionID, math.MaxUint64)
	if p, err := sessioncontext.FoldSessionMaterialV1(max, e, m); p.Version != 0 || !errors.Is(err, sessioncontext.ErrSessionMaterialPrefix) {
		t.Fatal("sequence overflow wrapped")
	}
	first := prefixFold(t, g, e, m)
	if _, err := sessioncontext.FoldSessionMaterialV1(first, e, m); !errors.Is(err, sessioncontext.ErrSessionMaterialPrefix) {
		t.Fatal("fold silently treated replay as fresh append")
	}
}

func TestMaterialPrefixIdentityPresenceFieldBoundariesAndEventOrder(t *testing.T) {
	g := prefixGenesis(t)
	e, _ := prefixEvent(t, 1, domain.SessionEventSystemNotice, `{"schema":"sessionless.run-terminal-notice.v1","code":"failed"}`)
	baseline := prefixFold(t, g, e, nil)
	for _, change := range []func(*domain.SessionEvent){
		func(e *domain.SessionEvent) {
			e.ID = "other-event"
			e.Payload.Key = domain.SessionEventObjectPrefix(e.TenantID, e.SessionID, e.ID) + "message.json"
		},
		func(e *domain.SessionEvent) { e.AuthorUserID = nil }, func(e *domain.SessionEvent) { e.RunID = nil },
		func(e *domain.SessionEvent) { v := domain.UserID("other-user"); e.AuthorUserID = &v }, func(e *domain.SessionEvent) { v := domain.RunID("other-run"); e.RunID = &v },
		func(e *domain.SessionEvent) { e.IdempotencyKey = "other-idempotency" }, func(e *domain.SessionEvent) { e.Payload.Key += ".other" }, func(e *domain.SessionEvent) { e.Payload.Size++ }, func(e *domain.SessionEvent) { e.Payload.SHA256 = strings.Repeat("f", 64) }, func(e *domain.SessionEvent) { e.CreatedAt = e.CreatedAt.Add(time.Nanosecond) },
	} {
		other := e
		change(&other)
		p := prefixFold(t, g, other, nil)
		if p.LastContribution.IdentityDigest == baseline.LastContribution.IdentityDigest || p.PrefixDigest == baseline.PrefixDigest {
			t.Fatal("unverified final metadata substitution not sealed")
		}
	}
	a, b := e, e
	aUser, bUser := domain.UserID("a"), domain.UserID("ab")
	aRun, bRun := domain.RunID("bc"), domain.RunID("c")
	a.AuthorUserID, a.RunID = &aUser, &aRun
	b.AuthorUserID, b.RunID = &bUser, &bRun
	if prefixFold(t, g, a, nil).LastContribution.IdentityDigest == prefixFold(t, g, b, nil).LastContribution.IdentityDigest {
		t.Fatal("optional identity field framing collision")
	}
	ordered := func(first, second string) sessioncontext.SessionMaterialPrefixV1 {
		one, oneMeasure := prefixEvent(t, 1, domain.SessionEventUserMessage, first)
		two, twoMeasure := prefixEvent(t, 2, domain.SessionEventUserMessage, second)
		return prefixFold(t, prefixFold(t, g, one, oneMeasure), two, twoMeasure)
	}
	ab := ordered(`{"version":1,"text":"a"}`, `{"version":1,"text":"b"}`)
	ba := ordered(`{"version":1,"text":"b"}`, `{"version":1,"text":"a"}`)
	if ab.VerifiedPayloadBytes != ba.VerifiedPayloadBytes || ab.EncodedJSONLBytes != ba.EncodedJSONLBytes || ab.RenderedFrameBytes != ba.RenderedFrameBytes || ab.PrefixDigest == ba.PrefixDigest {
		t.Fatal("ordered chain collapsed to unordered/scalar-only history")
	}
}

func TestMaterialPrefixCheckedSaturationAndPriorFactsSealed(t *testing.T) {
	g := prefixGenesis(t)
	e, m := prefixEvent(t, 1, domain.SessionEventUserMessage, `{"version":1,"text":"hello"}`)
	first := prefixFold(t, g, e, m)
	// Persisted facts are not opaque authority objects. This structurally valid
	// synthetic large history exercises arithmetic, not production provenance.
	large := first.Clone()
	large.VerifiedPayloadBytes, large.EncodedJSONLBytes = math.MaxInt64-1, math.MaxInt64-1
	nextEvent, nextMeasure := prefixEvent(t, 2, domain.SessionEventUserMessage, `{"version":1,"text":"hello"}`)
	if large.Validate() != nil {
		t.Fatal("synthetic valid large byte history malformed")
	}
	saturated := prefixFold(t, large, nextEvent, nextMeasure)
	if saturated.VerifiedPayloadBytes != math.MaxInt64 || saturated.EncodedJSONLBytes != math.MaxInt64 || saturated.Coverage != sessioncontext.SessionMaterialUnknownV1 || saturated.UnknownReasons&sessioncontext.SessionMaterialSaturatedV1 == 0 {
		t.Fatal("checked byte addition wrapped or stayed complete")
	}
	thirdEvent, thirdMeasure := prefixEvent(t, 3, domain.SessionEventUserMessage, `{"version":1,"text":"hello"}`)
	again := prefixFold(t, saturated, thirdEvent, thirdMeasure)
	if again.Coverage != sessioncontext.SessionMaterialUnknownV1 || again.VerifiedPayloadBytes != math.MaxInt64 {
		t.Fatal("saturation repaired by later small append")
	}
	other := large.Clone()
	other.VerifiedPayloadBytes--
	if other.Validate() != nil {
		t.Fatal("second synthetic previous history malformed")
	}
	if prefixFold(t, other, nextEvent, nextMeasure).PrefixDigest == saturated.PrefixDigest {
		t.Fatal("changed coherent prior common facts reused next digest")
	}
	legacy, _ := sessioncontext.NewUnknownSessionMaterialPrefixV1(g.TenantID, g.SessionID, math.MaxInt64)
	bigEvent, bigMeasure := prefixEvent(t, math.MaxInt64+1, domain.SessionEventUserMessage, `{"version":1,"text":"hello"}`)
	known := prefixFold(t, legacy, bigEvent, bigMeasure)
	if known.ThroughSequence != math.MaxInt64+1 || known.UnknownRecords != math.MaxInt64 || known.CodexCompatibleRecords != 1 || known.Validate() != nil {
		t.Fatal("identity sequence was capped with numeric observations")
	}
	unknown := prefixFold(t, legacy, bigEvent, nil)
	if unknown.UnknownRecords != math.MaxInt64 || unknown.UnknownReasons&sessioncontext.SessionMaterialSaturatedV1 == 0 {
		t.Fatal("unknown count overflow not saturated")
	}
	// A saturated, structurally valid large prefix exercises named kind and
	// compatible-frame count overflow independently of byte-only saturation.
	counters := first.Clone()
	counters.ThroughSequence, counters.LastContribution.Sequence = math.MaxInt64, math.MaxInt64
	counters.UserMessageCount, counters.CodexCompatibleRecords = math.MaxInt64, math.MaxInt64
	counters.VerifiedPayloadBytes, counters.EncodedJSONLBytes, counters.RenderedFrameBytes = math.MaxInt64, math.MaxInt64, math.MaxInt64
	counters.Coverage, counters.UnknownReasons = sessioncontext.SessionMaterialUnknownV1, sessioncontext.SessionMaterialSaturatedV1
	if counters.Validate() != nil {
		t.Fatal("synthetic saturated counter partition malformed")
	}
	counted := prefixFold(t, counters, bigEvent, bigMeasure)
	if counted.UserMessageCount != math.MaxInt64 || counted.CodexCompatibleRecords != math.MaxInt64 || counted.RenderedFrameBytes != math.MaxInt64 || counted.ThroughSequence != math.MaxInt64+1 || counted.Coverage != sessioncontext.SessionMaterialUnknownV1 {
		t.Fatal("kind/frame counters wrapped or capped identity sequence")
	}
}

func TestMaterialPrefixStrictPersistedDecoderAndWholeEquality(t *testing.T) {
	g := prefixGenesis(t)
	e, m := prefixEvent(t, 1, domain.SessionEventUserMessage, `{"version":1,"text":"hello"}`)
	p := prefixFold(t, g, e, m)
	encoded, err := sessioncontext.EncodeSessionMaterialPrefixV1(p)
	if err != nil {
		t.Fatal(err)
	}
	got, err := sessioncontext.DecodeSessionMaterialPrefixV1(encoded)
	if err != nil || !sessioncontext.EqualSessionMaterialPrefixV1(got, p) {
		t.Fatalf("canonical fact round-trip: %v", err)
	}
	for _, test := range []struct {
		name string
		body []byte
	}{
		{"empty", nil}, {"null", []byte("null")}, {"array", []byte("[]")},
		{"unknown", bytes.Replace(encoded, []byte(`"version":1`), []byte(`"approved":true,"version":1`), 1)},
		{"alias", bytes.Replace(encoded, []byte(`"tenant_id"`), []byte(`"tenant"`), 1)},
		{"wrong case", bytes.Replace(encoded, []byte(`"tenant_id"`), []byte(`"TENANT_ID"`), 1)},
		{"duplicate", bytes.Replace(encoded, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1)},
		{"nested duplicate", bytes.Replace(encoded, []byte(`"identity_digest":`), []byte(`"verified":true,"identity_digest":`), 1)},
		{"nested unknown", bytes.Replace(encoded, []byte(`"identity_digest":`), []byte(`"approved":true,"identity_digest":`), 1)},
		{"nested null scalar", bytes.Replace(encoded, []byte(`"verified":true`), []byte(`"verified":null`), 1)},
		{"null scalar", bytes.Replace(encoded, []byte(`"unknown_records":0`), []byte(`"unknown_records":null`), 1)},
		{"missing scalar", bytes.Replace(encoded, []byte(`,"unknown_records":0`), nil, 1)},
		{"null contribution", bytes.Replace(encoded, []byte(`"last_contribution":{`), []byte(`"last_contribution":null,"ignored":{`), 1)},
		{"fraction", bytes.Replace(encoded, []byte(`"through_sequence":1`), []byte(`"through_sequence":1.0`), 1)},
		{"exponent", bytes.Replace(encoded, []byte(`"through_sequence":1`), []byte(`"through_sequence":1e0`), 1)},
		{"negative", bytes.Replace(encoded, []byte(`"user_message_count":1`), []byte(`"user_message_count":-1`), 1)},
		{"trailing", append(bytes.Clone(encoded), []byte(` {}`)...)},
		{"malformed", []byte(`{"version":`)},
		{"utf8", append(bytes.Clone(encoded), 0xff)},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := sessioncontext.DecodeSessionMaterialPrefixV1(test.body)
			if got.Version != 0 || !errors.Is(err, sessioncontext.ErrSessionMaterialPrefix) || err.Error() != "canonical material prefix invalid" {
				t.Fatalf("strict fact decoder accepted %s: %v", test.name, err)
			}
		})
	}
	for _, extra := range []int{-1, 0, 1} {
		padded := append(bytes.Clone(encoded), bytes.Repeat([]byte(" "), sessioncontext.MaxSessionMaterialPrefixBytesV1+extra-len(encoded))...)
		got, err := sessioncontext.DecodeSessionMaterialPrefixV1(padded)
		if extra <= 0 {
			if err != nil || !sessioncontext.EqualSessionMaterialPrefixV1(got, p) {
				t.Fatalf("byte ceiling %+d: %v", extra, err)
			}
		} else if !errors.Is(err, sessioncontext.ErrSessionMaterialPrefix) {
			t.Fatal("8KiB+1 decoded before rejection")
		}
	}
	for _, mutate := range []func(*sessioncontext.SessionMaterialPrefixV1){
		func(p *sessioncontext.SessionMaterialPrefixV1) { p.Version = 2 }, func(p *sessioncontext.SessionMaterialPrefixV1) { p.RecordSchema = "old" }, func(p *sessioncontext.SessionMaterialPrefixV1) { p.RendererContract = "other" }, func(p *sessioncontext.SessionMaterialPrefixV1) { p.UnknownReasons = 128 }, func(p *sessioncontext.SessionMaterialPrefixV1) { p.UserMessageCount = 0 }, func(p *sessioncontext.SessionMaterialPrefixV1) { p.UnknownRecords = 1 }, func(p *sessioncontext.SessionMaterialPrefixV1) { p.VerifiedPayloadBytes = math.MaxInt64 + 1 }, func(p *sessioncontext.SessionMaterialPrefixV1) { p.MaxEncodedRecordBytes = p.EncodedJSONLBytes + 1 }, func(p *sessioncontext.SessionMaterialPrefixV1) { p.LastContribution.Sequence = 2 }, func(p *sessioncontext.SessionMaterialPrefixV1) { p.LastContribution.Verified = false }, func(p *sessioncontext.SessionMaterialPrefixV1) { p.LastContribution.RenderedFrameBytes = 0 }, func(p *sessioncontext.SessionMaterialPrefixV1) { p.LastContribution.Kind = domain.SessionEventToolCall }, func(p *sessioncontext.SessionMaterialPrefixV1) { p.LastContribution = nil }, func(p *sessioncontext.SessionMaterialPrefixV1) { p.PrefixDigest = strings.Repeat("A", 64) },
	} {
		bad := p.Clone()
		mutate(&bad)
		if sessioncontext.EqualSessionMaterialPrefixV1(p, bad) || bad.Validate() == nil {
			t.Fatal("changed invalid scalar combination/equality accepted")
		}
		if data, err := sessioncontext.EncodeSessionMaterialPrefixV1(bad); data != nil || !errors.Is(err, sessioncontext.ErrSessionMaterialPrefix) {
			t.Fatal("invalid scalar fact encoded")
		}
		data, _ := json.Marshal(bad)
		if _, err := sessioncontext.DecodeSessionMaterialPrefixV1(data); !errors.Is(err, sessioncontext.ErrSessionMaterialPrefix) {
			t.Fatal("invalid scalar fact decoded")
		}
	}
}

func TestMaterialPrefixHistoricalRendererKindConservation(t *testing.T) {
	g := prefixGenesis(t)
	tool, toolMeasure := prefixEvent(t, 1, domain.SessionEventToolCall, `{"schema":"sessionless.tool-event.v1","call_id":"call","tool_name":"tool","content":{}}`)
	one := prefixFold(t, g, tool, toolMeasure)
	user, userMeasure := prefixEvent(t, 2, domain.SessionEventUserMessage, `{"version":1,"text":"hello"}`)
	p := prefixFold(t, one, user, userMeasure)
	for _, test := range []struct {
		name   string
		change func(*sessioncontext.SessionMaterialPrefixV1)
	}{
		{"tool erased into compatible", func(p *sessioncontext.SessionMaterialPrefixV1) {
			p.CodexCompatibleRecords = 2
			p.CodexIncompatibleRecords = 0
		}},
		{"attachments without user", func(p *sessioncontext.SessionMaterialPrefixV1) {
			p.UserMessageCount = 0
			p.SystemNoticeCount = 1
			p.LastContribution.Kind = domain.SessionEventSystemNotice
			p.LastContribution.CodexCompatible = false
			p.LastContribution.RenderedFrameBytes = 0
			p.CodexCompatibleRecords = 0
			p.CodexIncompatibleRecords = 2
			p.RenderedFrameBytes = 0
			p.AttachmentCount = 1
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			bad := p.Clone()
			test.change(&bad)
			if bad.Validate() == nil {
				t.Fatal("historical kind/renderer contradiction accepted")
			}
			data, _ := json.Marshal(bad)
			if _, err := sessioncontext.DecodeSessionMaterialPrefixV1(data); !errors.Is(err, sessioncontext.ErrSessionMaterialPrefix) {
				t.Fatal("historical kind/renderer contradiction decoded")
			}
		})
	}
}

func FuzzMaterialPrefixPersistedDecoder(f *testing.F) {
	g, err := sessioncontext.NewSessionMaterialGenesisV1("prefix-tenant", "prefix-session")
	if err != nil {
		f.Fatal(err)
	}
	encoded, err := sessioncontext.EncodeSessionMaterialPrefixV1(g)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(encoded)
	f.Add([]byte(`{"version":1,"version":1}`))
	f.Add([]byte("null"))
	f.Add(bytes.Repeat([]byte("x"), sessioncontext.MaxSessionMaterialPrefixBytesV1+1))
	f.Fuzz(func(t *testing.T, data []byte) {
		p, err := sessioncontext.DecodeSessionMaterialPrefixV1(data)
		if err != nil {
			if !errors.Is(err, sessioncontext.ErrSessionMaterialPrefix) || p.Version != 0 || p.LastContribution != nil {
				t.Fatalf("decoder returned partial facts/private error: %v", err)
			}
			return
		}
		if len(data) > sessioncontext.MaxSessionMaterialPrefixBytesV1 {
			t.Fatal("oversize fact accepted")
		}
		canonical, err := sessioncontext.EncodeSessionMaterialPrefixV1(p)
		if err != nil || len(canonical) > sessioncontext.MaxSessionMaterialPrefixBytesV1 {
			t.Fatalf("decoded non-encodable fact: %v", err)
		}
		again, err := sessioncontext.DecodeSessionMaterialPrefixV1(canonical)
		if err != nil || !sessioncontext.EqualSessionMaterialPrefixV1(p, again) {
			t.Fatalf("canonical round-trip changed fact: %v", err)
		}
	})
}
