package ydbstore

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/domain"
)

// Pure persistence DATA fixtures, not authenticated receipts, original native
// producer evidence, SQL transactions, or whole-phase memory qualification.
func resealInlineCodecFixtureV2(t *testing.T, r *AttachedWorkerOutputReceiptV2) {
	t.Helper()
	sum := sha256.Sum256([]byte(r.Candidate.Summary))
	r.Producer.AnswerDigest = hex.EncodeToString(sum[:])
	var err error
	r.CandidateFingerprint, err = inlineReceiptPlanFingerprintV2(*r)
	if err != nil {
		t.Fatal(err)
	}
	if r.Materialization.Completion != nil {
		r.CanonicalDigest, err = attachedWorkerTerminalMaterializationDigest(domain.AttachedWorkerTerminalSucceeded, r.Materialization)
		if err != nil {
			t.Fatal(err)
		}
		r.Materialization.EvidenceDigest = r.CanonicalDigest
	}
	if err := validateInlineFixtureV2(*r); err != nil {
		t.Fatal("codec fixture must satisfy full original identity", err)
	}
}

func marshalInlineCodecFixtureV2(t *testing.T, r AttachedWorkerOutputReceiptV2) []byte {
	t.Helper()
	stored, err := encodeInlineReceiptProjectionV2(r)
	if err != nil {
		t.Fatal("encode original codec fixture", err)
	}
	raw, err := json.Marshal(stored)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func requireInlineCodecRoundTripV2(t *testing.T, r AttachedWorkerOutputReceiptV2) []byte {
	t.Helper()
	before, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(before)
	raw := marshalInlineCodecFixtureV2(t, r)
	input := bytes.Clone(raw)
	defer clear(input)
	restored, err := decodeInlineStoredReceiptV2(raw)
	if err != nil || !sameInlineReceiptV2(r, restored) || validateInlineFixtureV2(restored) != nil {
		t.Fatalf("codec changed full-domain identity stage=%s err=%v", r.Stage, err)
	}
	after, err := json.Marshal(r)
	defer clear(after)
	if err != nil || !bytes.Equal(before, after) || !bytes.Equal(raw, input) {
		t.Fatal("codec mutated borrowed original domain data or stored bytes", err)
	}
	if bytes.Contains(raw, []byte(`"display_text"`)) || !bytes.Contains(raw, []byte(`"persistence_encoding":"single-answer-v1"`)) {
		t.Fatal("stored shape contains redundant display or lacks its exact discriminator")
	}
	return raw
}

func TestInlineStoredCodecV2AllStages(t *testing.T) {
	prepared, _, _ := inlineReceiptFixtureV2(t)
	claimed, retired := prepared, prepared
	claimed.Stage, claimed.BodyClaimID = attachedworkeroutput.InlineBodyClaimedV2, strings.Repeat("c", 64)
	retired.Stage = attachedworkeroutput.InlineRetiredWithoutWritesV2
	copying := inlineCanonicalFixtureV2(t)
	copying.Candidate.Summary = " \tUseful answer\n"
	copying.Materialization.Completion.Events[0].DisplayText = "Useful answer"
	resealInlineCodecFixtureV2(t, &copying)
	ready := copying
	ready.Stage = attachedworkeroutput.InlineReadyV2
	for _, r := range []AttachedWorkerOutputReceiptV2{prepared, claimed, retired, copying, ready} {
		t.Run(string(r.Stage), func(t *testing.T) {
			raw := requireInlineCodecRoundTripV2(t, r)
			defer clear(raw)
			if r.Materialization.Completion == nil && !bytes.Contains(raw, []byte(`"Completion":null,"Failure":null`)) {
				t.Fatal("pre-copy stage lost explicit null materialization shape")
			}
		})
	}
}

func TestInlineStoredCodecV2MaximumAnswerAndTwoBodies(t *testing.T) {
	r := maximumInlineIdentityFixtureV2(t)
	_, loaded, _ := inlineReceiptFixtureV2(t)
	text := bytes.Repeat([]byte("<"), attachedworkeroutput.MaxInlineArtifactBytesV2)
	jsonBody := append([]byte("0"), bytes.Repeat([]byte(" "), attachedworkeroutput.MaxInlineArtifactBytesV2-1)...)
	body := append(bytes.Clone(text), jsonBody...)
	defer clear(text)
	defer clear(jsonBody)
	defer clear(body)
	r.Candidate.Artifacts = nil
	for i, artifact := range []struct {
		name, media string
		body        []byte
	}{{strings.Repeat("<", 255), "text/plain", text}, {strings.Repeat(">", 255), "application/json", jsonBody}} {
		sum := sha256.Sum256(artifact.body)
		r.Candidate.Artifacts = append(r.Candidate.Artifacts, attachedworkeroutput.InlineArtifactV2{Name: artifact.name, MediaType: artifact.media, Size: uint64(len(artifact.body)), SHA256: hex.EncodeToString(sum[:])})
		if len(r.Candidate.Artifacts[i].Name) != 255 {
			t.Fatal("maximum artifact name changed")
		}
	}
	verified, err := attachedworkeroutput.VerifyInlineV2(r.Candidate, body)
	if err != nil {
		t.Fatal(err)
	}
	defer verified.Clear()
	planned := new(plannedReceiptBlobs)
	t.Cleanup(func() {
		for _, item := range planned.items {
			clear(item.body)
		}
	})
	r.Materialization, err = attachedworkeroutput.MaterializeInlineV2(t.Context(), planned, loaded, r.Binding.LeaseID, r.Binding.LeaseGeneration, verified, r.CreatedAt)
	if err != nil {
		t.Fatal(err)
	}
	resealInlineCodecFixtureV2(t, &r)
	raw := requireInlineCodecRoundTripV2(t, r)
	defer clear(raw)
	if len(r.Candidate.Summary) != 32<<10 || len(body) != 64<<10 || bytes.Count(raw, []byte(`\u2028`)) != 384 {
		t.Fatal("maximum semantic cohort was narrowed or original answer serialized twice")
	}
}

// Arithmetic chooses a bounded number of real typed Usage entries. Shrinking
// only their valid opaque IDs reaches the exact old full-domain byte boundary;
// no production Usage-count/metadata ceiling or filler field is introduced.
func nearLimitInlineCodecFixtureV2(t *testing.T) AttachedWorkerOutputReceiptV2 {
	t.Helper()
	r := inlineCanonicalFixtureV2(t)
	_, loaded, _ := inlineReceiptFixtureV2(t)
	r.Candidate.Summary = "x"
	c := r.Materialization.Completion
	c.Events[0].DisplayText = "x"
	// The existing receipt validator accepts this auxiliary metadata as DATA;
	// round-tripping it is not substrate attestation/authority qualification.
	c.SubstrateEvidence = &domain.SubstrateExecutionEvidenceV1{Version: 1}
	resealInlineCodecFixtureV2(t, &r)
	base, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(base)
	tokens := uint64(1)
	u := domain.UsageObservation{ID: domain.UsageObservationID(strings.Repeat("u", 160)), TenantID: r.Binding.TenantID, RunID: r.Binding.RunID, AttemptID: r.Binding.AttemptID,
		SubscriptionConnectionID: loaded.Run.SubscriptionConnectionID, Source: domain.UsageSourceHarness, InputTokens: &tokens, ObservedAt: r.CreatedAt}
	entry, err := json.Marshal(u)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(entry)
	count := (maxInlineReceiptRecordBytesV2-len(base)+3)/(len(entry)+1) + 1
	c.Usage = make([]domain.UsageObservation, count)
	for i := range c.Usage {
		c.Usage[i] = u
		prefix := fmt.Sprintf("usage-%06d-", i)
		c.Usage[i].ID = domain.UsageObservationID(prefix + strings.Repeat("u", 160-len(prefix)))
	}
	oversize, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	extra := len(oversize) - maxInlineReceiptRecordBytesV2
	clear(oversize)
	if extra <= 0 || extra > len(entry)+1 {
		t.Fatal("near-limit fixture arithmetic failed", extra)
	}
	for i := len(c.Usage) - 1; extra > 0 && i >= 0; i-- {
		prefix := fmt.Sprintf("usage-%06d-", i)
		remove := min(extra, len(c.Usage[i].ID)-len(prefix))
		c.Usage[i].ID = c.Usage[i].ID[:len(c.Usage[i].ID)-remove]
		extra -= remove
	}
	if extra != 0 {
		t.Fatal("bounded Usage IDs cannot reach original byte boundary", extra)
	}
	for _, usage := range c.Usage {
		if err := usage.ValidateForAttempt(loaded.Run, loaded.Attempt); err != nil {
			t.Fatal("near-limit Usage is not independently scope-valid", err)
		}
	}
	resealInlineCodecFixtureV2(t, &r)
	return r
}

func TestInlineStoredCodecV2PreservesOriginalWriterLimitAndFramingAllowance(t *testing.T) {
	r := nearLimitInlineCodecFixtureV2(t)
	full, err := json.Marshal(r)
	if err != nil || len(full) != maxInlineReceiptRecordBytesV2 {
		t.Fatal("full original fixture must reach exactly the old writer limit", len(full), err)
	}
	defer clear(full)
	raw := requireInlineCodecRoundTripV2(t, r)
	defer clear(raw)
	if len(raw) != len(full)+23 || len(raw) <= maxInlineReceiptRecordBytesV2 {
		t.Fatal("short-answer projection must retain the required larger frame", len(raw), len(full))
	}
	atLimit := append(bytes.Clone(raw), bytes.Repeat([]byte(" "), maxInlineReceiptStoredBytesV2-len(raw))...)
	defer clear(atLimit)
	got, err := decodeInlineStoredReceiptV2(atLimit)
	if err != nil || !sameInlineReceiptV2(r, got) || !reflect.DeepEqual(r.Materialization.Completion.Usage, got.Materialization.Completion.Usage) ||
		!reflect.DeepEqual(r.Materialization.Completion.SubstrateEvidence, got.Materialization.Completion.SubstrateEvidence) {
		t.Fatal("raw +42 frame imposed a hidden metadata/Usage/canonical reencoding limit", err)
	}
	overLimit := append(bytes.Clone(atLimit), ' ')
	defer clear(overLimit)
	if _, err := decodeInlineStoredReceiptV2(overLimit); !errors.Is(err, ErrAttachedWorkerAttemptConflict) {
		t.Fatal("stored raw frame exceeded +42 allowance", err)
	}
	// One more byte of identity-valid Usage exceeds only the old writer cap.
	c := r.Materialization.Completion
	c.Usage[len(c.Usage)-1].ID += "u"
	resealInlineCodecFixtureV2(t, &r)
	overflow, err := json.Marshal(r)
	if err != nil || len(overflow) != maxInlineReceiptRecordBytesV2+1 {
		t.Fatal("full-domain overflow control did not cross exactly one byte", len(overflow), err)
	}
	defer clear(overflow)
	if _, err := encodeInlineReceiptProjectionV2(r); !errors.Is(err, ErrAttachedWorkerAttemptConflict) {
		t.Fatal("projection hid a full-domain writer overflow", err)
	}
	after, err := json.Marshal(r)
	defer clear(after)
	if err != nil || !bytes.Equal(overflow, after) {
		t.Fatal("failed projection changed caller-owned receipt", err)
	}
}

func TestInlineStoredCodecV2RejectsOldAmbiguousAndTamperedShapes(t *testing.T) {
	r := inlineCanonicalFixtureV2(t)
	raw := marshalInlineCodecFixtureV2(t, r)
	defer clear(raw)
	old, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(old)
	stored, err := encodeInlineReceiptProjectionV2(r)
	if err != nil {
		t.Fatal(err)
	}
	completionJSON, err := json.Marshal(stored.Materialization.Completion)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(completionJSON)
	eventsJSON, err := json.Marshal(stored.Materialization.Completion.Events)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(eventsJSON)
	mutations := map[string][2]string{
		"missing encoding":         {`"persistence_encoding":"single-answer-v1",`, ""},
		"null encoding":            {`"persistence_encoding":"single-answer-v1"`, `"persistence_encoding":null`},
		"wrong encoding":           {`single-answer-v1`, `single-answer-v2`},
		"encoding alias":           {`"persistence_encoding"`, `"Persistence_Encoding"`},
		"duplicate encoding":       {`"persistence_encoding":"single-answer-v1"`, `"persistence_encoding":"single-answer-v1","persistence_encoding":"single-answer-v1"`},
		"encoding duplicate alias": {`"persistence_encoding":"single-answer-v1"`, `"persistence_encoding":"single-answer-v1","PERSISTENCE_ENCODING":"single-answer-v1"`},
		"unknown root":             {`"version":2`, `"version":2,"unknown":true`},
		"unsupported version":      {`"version":2`, `"version":3`},
		"missing summary":          {`"summary":"Useful answer",`, ""},
		"null summary":             {`"summary":"Useful answer"`, `"summary":null`},
		"summary alias":            {`"summary"`, `"Summary"`},
		"summary duplicate alias":  {`"summary":"Useful answer"`, `"summary":"Useful answer","SUMMARY":"other"`},
		"tampered summary":         {`"summary":"Useful answer"`, `"summary":"Changed answer"`},
		"tampered fingerprint":     {`"candidate_fingerprint":"` + r.CandidateFingerprint + `"`, `"candidate_fingerprint":"` + strings.Repeat("e", 64) + `"`},
		"missing completion":       {`"Completion":` + string(completionJSON) + `,`, ""},
		"null completion":          {`"Completion":` + string(completionJSON), `"Completion":null`},
		"missing events":           {`"Events":` + string(eventsJSON) + `,`, ""},
		"null events":              {`"Events":` + string(eventsJSON), `"Events":null`},
		"empty events":             {`"Events":` + string(eventsJSON), `"Events":[]`},
		"display empty":            {`"Events":[{`, `"Events":[{"display_text":"",`},
		"display null":             {`"Events":[{`, `"Events":[{"display_text":null,`},
		"display original":         {`"Events":[{`, `"Events":[{"display_text":"Useful answer",`},
		"display alias":            {`"Events":[{`, `"Events":[{"DISPLAY_TEXT":"Useful answer",`},
		"display escaped key":      {`"Events":[{`, `"Events":[{"display_\u0074ext":"Useful answer",`},
	}
	prepared, _, _ := inlineReceiptFixtureV2(t)
	preparedJSON := marshalInlineCodecFixtureV2(t, prepared)
	defer clear(preparedJSON)
	for _, change := range [][2]string{{`"Completion":null,`, ""}, {`,"Failure":null`, ""}, {`"Completion":null`, `"Completion":{}`}} {
		mutant := bytes.Replace(preparedJSON, []byte(change[0]), []byte(change[1]), 1)
		if bytes.Equal(mutant, preparedJSON) {
			t.Fatal("pre-copy materialization shape mutation did not change its input")
		}
		_, err := decodeInlineStoredReceiptV2(mutant)
		clear(mutant)
		if !errors.Is(err, ErrAttachedWorkerAttemptConflict) {
			t.Fatal("pre-copy codec accepted missing/null-shape substitution", err)
		}
	}
	for name, change := range mutations {
		t.Run(name, func(t *testing.T) {
			mutant := bytes.Replace(raw, []byte(change[0]), []byte(change[1]), 1)
			defer clear(mutant)
			if bytes.Equal(mutant, raw) {
				t.Fatal("mutation failed to change intended original field")
			}
			if _, err := decodeInlineStoredReceiptV2(mutant); !errors.Is(err, ErrAttachedWorkerAttemptConflict) {
				t.Fatal("closed stored codec accepted ambiguous or tampered data", err)
			}
		})
	}
	for name, bad := range map[string][]byte{"old domain shape": old, "trailing document": append(bytes.Clone(raw), []byte(` {}`)...), "invalid Unicode": bytes.Replace(raw, []byte("Useful answer"), []byte{0xff}, 1)} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeInlineStoredReceiptV2(bad); !errors.Is(err, ErrAttachedWorkerAttemptConflict) {
				t.Fatal("codec accepted invalid framing/old full-domain shape", err)
			}
		})
	}
	// Harmless JsonDocument formatting and equivalent key/value escaping are
	// accepted without a new canonical-reencoding or metadata byte ceiling.
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, raw, "", "  "); err != nil {
		t.Fatal(err)
	}
	for _, input := range [][]byte{pretty.Bytes(), bytes.ReplaceAll(raw, []byte("Useful answer"), []byte(`Useful \u0061nswer`))} {
		got, err := decodeInlineStoredReceiptV2(input)
		if err != nil || !sameInlineReceiptV2(r, got) {
			t.Fatal("codec rejected equivalent stored formatting", err)
		}
	}
	// Validation precedes omission: a stored projection cannot hide a wrong
	// derived DisplayText even if a caller re-seals its materialization digest.
	r.Materialization.Completion.Events[0].DisplayText = "contrary display"
	r.CanonicalDigest, err = attachedWorkerTerminalMaterializationDigest(domain.AttachedWorkerTerminalSucceeded, r.Materialization)
	if err != nil {
		t.Fatal(err)
	}
	r.Materialization.EvidenceDigest = r.CanonicalDigest
	if _, err := encodeInlineReceiptProjectionV2(r); !errors.Is(err, ErrAttachedWorkerAttemptConflict) || r.Materialization.Completion.Events[0].DisplayText != "contrary display" {
		t.Fatal("encoding omitted/mutated a contrary original display before validation", err)
	}
}
