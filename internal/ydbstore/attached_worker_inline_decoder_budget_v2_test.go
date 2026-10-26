package ydbstore

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/strictjson"
)

func TestInlineReceiptDecoderAvoidsLargeShapeSnapshots(t *testing.T) {
	r := maximumInlineIdentityFixtureV2(t)
	raw := marshalInlineStoredReceiptTestV2(t, r)
	defer clear(raw)
	allocations := testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			value, err := decodeAttachedWorkerReceiptEnvelope(raw)
			if err != nil || value.v2 == nil || value.v2.Candidate.Summary != r.Candidate.Summary || value.v2.Materialization.Completion.Events[0].DisplayText != r.Candidate.Summary {
				b.Fatal("maximum receipt decoder changed original semantic content", err)
			}
		}
	})
	// This is decoder-only allocation evidence, NOT an invocation heap/peak
	// claim. Storage contains one answer; decode reconstructs its display and
	// performs the full original identity/digest validation. Keep the existing
	// allocation guard: no full escaped answer shape trees may be reintroduced.
	if got := allocations.AllocedBytesPerOp(); got > 768<<10 {
		t.Fatalf("decoder still snapshots large receipt payloads: allocated=%d limit=%d record=%d", got, 768<<10, len(raw))
	}
	t.Logf("decoder allocated=%d B/op, original record=%d B; not whole-invocation peak", allocations.AllocedBytesPerOp(), len(raw))
}

func TestInlineReceiptDecoderMaximumMetadataScope(t *testing.T) {
	r := maximumInlineIdentityFixtureV2(t)
	// Construct a conservative identity-valid receipt, not another native
	// producer: even server-derived event/manifest IDs use their domain maximum
	// here. Both artifact names need six JSON bytes per original byte.
	id := strings.Repeat("i", 160)
	a := &r.Binding
	a.TenantID, a.OwnerUserID = domain.TenantID(id), domain.UserID(id)
	a.WorkerID, a.ConnectionID = domain.AttachedWorkerID(id), domain.AttachedWorkerConnectionID(id)
	a.RunID, a.AttemptID, a.LeaseID = domain.RunID(id), domain.AttemptID(id), domain.LeaseID(id)
	a.EnrollmentGeneration, a.ConnectionGeneration, a.LeaseGeneration = ^uint64(0), ^uint64(0), ^uint64(0)
	r.Nonce = domain.IdempotencyKey(id)
	r.Producer.SessionID, r.Producer.ReservationID = domain.SessionID(id), domain.QuotaReservationID(id)
	// The longest positive-offset RFC3339Nano representation, near the last
	// JSON-encodable year, also remains below the unchanged original expiry.
	r.CreatedAt = time.Date(9999, 12, 31, 23, 59, 50, 999999999, time.FixedZone("fixture offset", 23*3600+59*60))
	r.Producer.AcceptedAt, r.ExpiresAt = r.CreatedAt.Add(-time.Second), r.CreatedAt.Add(6*time.Second)
	a.LeaseExpiresAtUnixMicro = r.ExpiresAt.Add(time.Minute).UnixMicro()
	c := r.Materialization.Completion
	c.TenantID, c.RunID, c.AttemptID = a.TenantID, a.RunID, a.AttemptID
	c.ReservationID, c.LeaseID, c.Fence = r.Producer.ReservationID, a.LeaseID, a.LeaseGeneration
	c.At, c.Manifest.CreatedAt = r.CreatedAt, r.CreatedAt
	c.Manifest.ID, c.Manifest.TenantID, c.Manifest.RunID = domain.ArtifactManifestID(id), a.TenantID, a.RunID
	r.Candidate.Artifacts, c.Manifest.Artifacts = nil, nil
	for _, ch := range []string{"<", ">"} {
		decl := attachedworkeroutput.InlineArtifactV2{Name: strings.Repeat(ch, 255), MediaType: "application/json", Size: 32 << 10, SHA256: strings.Repeat("e", 64)}
		r.Candidate.Artifacts = append(r.Candidate.Artifacts, decl)
		c.Manifest.Artifacts = append(c.Manifest.Artifacts, domain.Artifact{Name: decl.Name, MediaType: decl.MediaType,
			Blob: domain.BlobRef{TenantID: a.TenantID, Key: domain.SessionRunObjectPrefix(a.TenantID, r.Producer.SessionID, a.RunID) + "artifacts/sha256/" + decl.SHA256, Size: int64(decl.Size), SHA256: decl.SHA256}})
	}
	e := &c.Events[0]
	e.ID, e.IdempotencyKey = domain.SessionEventID(id), domain.IdempotencyKey(id)
	e.CreatedAt = r.CreatedAt
	e.Payload.TenantID = a.TenantID
	e.Payload.Key = domain.SessionEventObjectPrefix(a.TenantID, r.Producer.SessionID, e.ID) + "payloads/sha256/" + e.Payload.SHA256 + ".json"
	var err error
	r.CandidateFingerprint, err = inlineReceiptPlanFingerprintV2(r)
	if err != nil {
		t.Fatal(err)
	}
	r.CanonicalDigest, err = attachedWorkerTerminalMaterializationDigest(domain.AttachedWorkerTerminalSucceeded, r.Materialization)
	if err != nil {
		t.Fatal(err)
	}
	r.Materialization.EvidenceDigest = r.CanonicalDigest
	if err := validateInlineFixtureV2(r); err != nil {
		t.Fatal("maximum metadata fixture is not identity-valid", err)
	}
	raw := marshalInlineStoredReceiptTestV2(t, r)
	defer clear(raw)
	metadata, _, err := strictjson.ProjectStrings(raw, maxInlineReceiptStoredBytesV2, []strictjson.StringProjection{
		{Path: []string{"candidate", "summary"}, MaxBytes: attachedworkeroutput.MaxInlineAnswerBytesV2},
		{Path: []string{"materialization", "Completion", "Events", "0", "display_text"}, MaxBytes: 0},
	})
	if err != nil || len(metadata) > 24<<10 {
		t.Fatal("maximum producer-cohort scope did not fit fixture budget", len(metadata), err)
	}
	defer clear(metadata)
	// Escaped equivalent spellings, including every opaque ID, remain valid
	// JsonDocument representations rather than a new encoded-metadata limit.
	escaped := bytes.ReplaceAll(raw, []byte("i"), []byte(`\u0069`))
	defer clear(escaped)
	for name, input := range map[string][]byte{"maximum": raw, "escaped": escaped} {
		t.Run(name, func(t *testing.T) {
			got, err := decodeAttachedWorkerReceiptEnvelope(input)
			if err != nil || got.v2 == nil || !sameInlineReceiptV2(r, *got.v2) || validateInlineFixtureV2(*got.v2) != nil {
				t.Fatal("maximum scope decoder changed valid receipt", err)
			}
		})
	}
	t.Logf("maximum conservative scope metadata=%d B, receipt=%d B", len(metadata), len(raw))
}

func TestInlineReceiptDecoderPreservesStageAndContainerShapes(t *testing.T) {
	prepared, _, _ := inlineReceiptFixtureV2(t)
	canonical := inlineCanonicalFixtureV2(t)
	claimed, retired, ready := prepared, prepared, canonical
	claimed.Stage, claimed.BodyClaimID = attachedworkeroutput.InlineBodyClaimedV2, strings.Repeat("c", 64)
	retired.Stage = attachedworkeroutput.InlineRetiredWithoutWritesV2
	ready.Stage = attachedworkeroutput.InlineReadyV2
	for _, base := range []AttachedWorkerOutputReceiptV2{prepared, claimed, retired, canonical, ready} {
		t.Run(string(base.Stage), func(t *testing.T) {
			raw := marshalInlineStoredReceiptTestV2(t, base)
			defer clear(raw)
			var shape map[string]any
			cases := map[string][]byte{"original": raw}
			// Every case starts from the original stored tree. The independent
			// oracle checks both closed shape and full reconstructed identity.
			mutations := map[string]func(map[string]any){
				"artifacts_empty":  func(v map[string]any) { v["candidate"].(map[string]any)["artifacts"] = []any{} },
				"artifacts_null":   func(v map[string]any) { v["candidate"].(map[string]any)["artifacts"] = nil },
				"completion_empty": func(v map[string]any) { v["materialization"].(map[string]any)["Completion"] = map[string]any{} },
				"completion_array": func(v map[string]any) { v["materialization"].(map[string]any)["Completion"] = []any{} },
				"summary_object":   func(v map[string]any) { v["candidate"].(map[string]any)["summary"] = map[string]any{"summary": "x"} },
			}
			if base.Materialization.Completion != nil {
				for name, value := range map[string]any{"empty": []any{}, "null": nil, "object": map[string]any{"0": map[string]any{"display_text": "x"}}, "two": []any{map[string]any{"display_text": "x"}, map[string]any{"display_text": "y"}}} {
					mutations["events_"+name] = func(v map[string]any) {
						v["materialization"].(map[string]any)["Completion"].(map[string]any)["Events"] = value
					}
				}
				mutations["different_original_strings"] = func(v map[string]any) { v["candidate"].(map[string]any)["summary"] = " \tUseful answer\n" }
			}
			for name, change := range mutations {
				d := json.NewDecoder(bytes.NewReader(raw))
				d.UseNumber()
				if err := d.Decode(&shape); err != nil {
					t.Fatal(err)
				}
				before, err := json.Marshal(shape)
				if err != nil {
					t.Fatal(err)
				}
				change(shape)
				cases[name], err = json.Marshal(shape)
				if err != nil {
					t.Fatal(err)
				}
				if bytes.Equal(before, cases[name]) {
					t.Fatalf("mutation %s did not change the stored shape", name)
				}
				clear(before)
			}
			for name, input := range cases {
				t.Run(name, func(t *testing.T) {
					if accepted := assertInlineDecoderMatchesCanonicalV2(t, input); name == "original" && !accepted {
						t.Fatal("original identity-valid stored stage was denied")
					}
				})
			}
		})
	}
}

func TestInlineReceiptDecoderPreservesLargeStoredUsage(t *testing.T) {
	r := inlineCanonicalFixtureV2(t)
	_, loaded, _ := inlineReceiptFixtureV2(t)
	// The current materializer emits nil Usage, but stored canonical shape and
	// identity currently accept it. A memory fix must not silently introduce a
	// narrower metadata admission limit or erase existing auxiliary fields.
	tokens := uint64(1)
	for i := 0; i < 256; i++ {
		u := domain.UsageObservation{ID: domain.UsageObservationID(fmt.Sprintf("%03d%s", i, strings.Repeat("u", 157))),
			TenantID: r.Binding.TenantID, RunID: r.Binding.RunID, AttemptID: r.Binding.AttemptID,
			SubscriptionConnectionID: loaded.Run.SubscriptionConnectionID, Source: domain.UsageSourceHarness,
			InputTokens: &tokens, ObservedAt: r.CreatedAt}
		if err := u.ValidateForAttempt(loaded.Run, loaded.Attempt); err != nil {
			t.Fatal("usage counterexample is not independently scope-valid", err)
		}
		r.Materialization.Completion.Usage = append(r.Materialization.Completion.Usage, u)
	}
	if err := validateInlineFixtureV2(r); err != nil {
		t.Fatal("large stored usage receipt is not identity-valid", err)
	}
	raw := marshalInlineStoredReceiptTestV2(t, r)
	defer clear(raw)
	if len(raw) <= 24<<10 || len(raw) > maxInlineReceiptStoredBytesV2 {
		t.Fatal("usage counterexample did not cross proposed hidden metadata limit", len(raw))
	}
	if !assertInlineDecoderMatchesCanonicalV2(t, raw) {
		t.Fatal("original identity-valid large Usage was denied")
	}
}

// Intentionally slow, test-only reference: decode the complete stored shape
// without ProjectStrings/the optimized codec, then apply the accepted display
// derivation and the ORIGINAL full identity validator. This is DATA validation;
// actual SQL row/context authority remains a separate caller responsibility.
func slowInlineStoredReceiptReferenceV2(input []byte) (AttachedWorkerOutputReceiptV2, error) {
	if strictjson.ValidateDocument(input, maxInlineReceiptStoredBytesV2, 64, maxInlineReceiptStoredBytesV2) != nil {
		return AttachedWorkerOutputReceiptV2{}, ErrAttachedWorkerAttemptConflict
	}
	var stored storedInlineReceiptV2
	if decodeOwnedEffectCanonicalJSON(string(input), &stored) != nil || stored.PersistenceEncoding != "single-answer-v1" {
		return AttachedWorkerOutputReceiptV2{}, ErrAttachedWorkerAttemptConflict
	}
	r := stored.AttachedWorkerOutputReceiptV2
	if c := r.Materialization.Completion; c != nil {
		for _, event := range c.Events {
			if event.DisplayText != "" {
				return AttachedWorkerOutputReceiptV2{}, ErrAttachedWorkerAttemptConflict
			}
		}
	}
	if r.Stage == attachedworkeroutput.InlineCanonicalCopyingV2 || r.Stage == attachedworkeroutput.InlineReadyV2 {
		if r.Materialization.Completion == nil || len(r.Materialization.Completion.Events) != 1 {
			return AttachedWorkerOutputReceiptV2{}, ErrAttachedWorkerAttemptConflict
		}
		r.Materialization.Completion.Events[0].DisplayText = strings.TrimSpace(r.Candidate.Summary)
	}
	a := r.Binding
	if (attachedWorkerReceiptEnvelope{v2: &r}).validateIdentity(a.TenantID, a.RunID, a.OwnerUserID, a.WorkerID, a.AttemptID, a.LeaseGeneration) != nil {
		return AttachedWorkerOutputReceiptV2{}, ErrAttachedWorkerAttemptConflict
	}
	return r, nil
}

func assertInlineDecoderMatchesCanonicalV2(t *testing.T, input []byte) bool {
	t.Helper()
	before := bytes.Clone(input)
	defer clear(before)
	old, oldErr := slowInlineStoredReceiptReferenceV2(input)
	value, err := decodeAttachedWorkerReceiptEnvelope(input)
	if (oldErr == nil) != (err == nil) {
		t.Fatalf("closed shape acceptance changed: old=%v new=%v", oldErr, err)
	}
	if err == nil {
		if value.v2 == nil || value.v1 != nil {
			t.Fatal("V2 decoder changed version arm")
		}
		want, e1 := json.Marshal(old)
		got, e2 := json.Marshal(value.v2)
		if e1 != nil || e2 != nil || !bytes.Equal(want, got) {
			t.Fatal("decoder changed typed original data", e1, e2)
		}
	}
	if !bytes.Equal(input, before) {
		t.Fatal("decoder modified original payload")
	}
	return err == nil
}

func TestInlineReceiptDecoderPreservesClosedShape(t *testing.T) {
	r := inlineCanonicalFixtureV2(t)
	raw := marshalInlineStoredReceiptTestV2(t, r)
	defer clear(raw)
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, raw, "", "  "); err != nil {
		t.Fatal(err)
	}
	var ordered map[string]json.RawMessage
	if json.Unmarshal(raw, &ordered) != nil {
		t.Fatal("reordered fixture")
	}
	reordered, err := json.Marshal(ordered)
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string][]byte{"original": raw, "whitespace": pretty.Bytes(), "reordered": reordered,
		"escaped_keys_values": []byte(strings.ReplaceAll(strings.ReplaceAll(string(raw), `"candidate"`, `"cand\u0069date"`), "Useful answer", `Useful \u0061nswer`)),
		"lots_of_whitespace":  []byte(" " + strings.Repeat("\n\t ", 12_000) + string(raw)),
	}
	for _, key := range []string{"persistence_encoding", "candidate", "summary", "materialization", "Completion", "Events", "binding", "tenant_id"} {
		changed := strings.Replace(string(raw), `"`+key+`"`, `"`+strings.ToUpper(key)+`"`, 1)
		if changed == string(raw) {
			t.Fatalf("alias mutation %s did not change its intended original field", key)
		}
		cases["case_alias_"+key] = []byte(changed)
	}
	for name, change := range map[string][2]string{
		"encoding_missing":    {`"persistence_encoding":"single-answer-v1",`, ""},
		"encoding_null":       {`"persistence_encoding":"single-answer-v1"`, `"persistence_encoding":null`},
		"encoding_wrong":      {`"persistence_encoding":"single-answer-v1"`, `"persistence_encoding":"other"`},
		"encoding_duplicate":  {`"persistence_encoding":"single-answer-v1"`, `"persistence_encoding":"single-answer-v1","persistence_encoding":"single-answer-v1"`},
		"summary_missing":     {`"summary":"Useful answer",`, ""},
		"summary_null":        {`"summary":"Useful answer"`, `"summary":null`},
		"summary_empty":       {`"summary":"Useful answer"`, `"summary":""`},
		"display_null":        {`"Events":[{`, `"Events":[{"display_text":null,`},
		"display_empty":       {`"Events":[{`, `"Events":[{"display_text":"",`},
		"display_nonempty":    {`"Events":[{`, `"Events":[{"display_text":"Useful answer",`},
		"display_alias":       {`"Events":[{`, `"Events":[{"DISPLAY_TEXT":"Useful answer",`},
		"display_alias_empty": {`"Events":[{`, `"Events":[{"DISPLAY_TEXT":"",`},
		"display_alias_null":  {`"Events":[{`, `"Events":[{"DISPLAY_TEXT":null,`},
		"unknown_root":        {`"version":2`, `"version":2,"unknown":true`},
		"unknown_nested":      {`"summary":"Useful answer"`, `"summary":"Useful answer","unknown":true`},
		"duplicate_key":       {`"summary":"Useful answer"`, `"summary":"Useful answer","summ\u0061ry":"other"`},
		"numeric_lexeme":      {`"version":2`, `"version":2.0`},
		"timezone":            {`Z"`, `+00:00"`},
	} {
		changed := strings.Replace(string(raw), change[0], change[1], 1)
		if changed == string(raw) {
			t.Fatalf("mutation %s did not change its intended original field", name)
		}
		cases[name] = []byte(changed)
	}
	for name, input := range cases {
		t.Run(name, func(t *testing.T) {
			accepted := assertInlineDecoderMatchesCanonicalV2(t, input)
			switch name {
			case "original", "whitespace", "reordered", "escaped_keys_values", "lots_of_whitespace":
				if !accepted {
					t.Fatal("equivalent original stored representation was denied")
				}
			default:
				if accepted {
					t.Fatal("contrary stored shape was accepted")
				}
			}
		})
	}
}
