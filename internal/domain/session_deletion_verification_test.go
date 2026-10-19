package domain

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// Codec-only DATA. These values do not stand in for a genuine Store verifier,
// writer-stop proof, successful capsule finalization or physical object deletion.
func retainedDeletionData(t *testing.T) (SessionDeletion, SessionDeletionInventory) {
	t.Helper()
	started := time.Unix(20, 0).UTC()
	d := SessionDeletion{TenantID: "tenant-a", SessionID: "session-a", RequestedBy: "user-a", Reason: "requested deletion", State: SessionDeletionDeleting, RequestedAt: time.Unix(10, 0).UTC(), StartedAt: &started}
	i := SessionDeletionInventory{TenantID: d.TenantID, SessionID: d.SessionID, EventRows: 2, RunRows: 1, RunIDs: []RunID{"run-a"}, RetainedRows: 3, TotalBytes: 7, Objects: []BlobRef{
		{TenantID: d.TenantID, Key: "tenants/tenant-a/sessions/session-a/a", Size: 3, SHA256: strings.Repeat("1", 64)},
		{TenantID: d.TenantID, Key: "tenants/tenant-a/sessions/session-a/b", Size: 4, SHA256: strings.Repeat("2", 64)},
	}}
	r, err := SessionDeletionRequestDigestV1(d)
	if err != nil {
		t.Fatal(err)
	}
	h, err := SessionDeletionInventoryDigestV1(i)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := SessionDeletionInventoryRowsV1(i)
	if err != nil {
		t.Fatal(err)
	}
	d.RetainedVerification = &RetainedDeletionVerificationV1{Schema: "sessionless.retained-deletion.v1", TenantID: d.TenantID, SessionID: d.SessionID, RequestDigest: r, MetadataDigest: strings.Repeat("a", 64), InventoryDigest: h, Rows: rows, Objects: 2, Bytes: 7, VerifiedRuns: 1, VerifiedAt: started}
	if err := d.Validate(); err != nil {
		t.Fatalf("valid deleting DATA: %v", err)
	}
	if err := d.RetainedVerification.ValidateInventory(d, i); err != nil {
		t.Fatalf("valid bound inventory DATA: %v", err)
	}
	return d, i
}

func TestRetainedDeletionVerificationCompletedCounters(t *testing.T) {
	for _, test := range []struct {
		name    string
		objects uint64
		bytes   uint64
		valid   bool
	}{
		{name: "exact", objects: 2, bytes: 7, valid: true},
		{name: "object_count_only", objects: 3, bytes: 7},
		{name: "byte_count_only", objects: 2, bytes: 8},
	} {
		t.Run(test.name, func(t *testing.T) {
			d, _ := retainedDeletionData(t)
			completed := time.Unix(21, 0).UTC()
			d.State, d.CompletedAt = SessionDeletionCompleted, &completed
			d.DeletedObjects, d.DeletedBytes = test.objects, test.bytes
			err := d.Validate()
			if (err == nil) != test.valid {
				t.Fatalf("completed counters objects=%d bytes=%d: err=%v, want valid=%t with capture objects=2 bytes=7", test.objects, test.bytes, err, test.valid)
			}
		})
	}
}

func TestRetainedDeletionVerificationClosedEncoding(t *testing.T) {
	d, _ := retainedDeletionData(t)
	raw, err := json.Marshal(d.RetainedVerification)
	if err != nil || len(raw) > MaxRetainedDeletionVerificationBytesV1 {
		t.Fatalf("encode bounded DATA: size=%d err=%v", len(raw), err)
	}
	var got RetainedDeletionVerificationV1
	if err := json.Unmarshal(raw, &got); err != nil || got != *d.RetainedVerification {
		t.Fatalf("closed DATA roundtrip: got=%+v err=%v", got, err)
	}
	for _, test := range []struct{ name, body string }{
		{name: "extra", body: strings.TrimSuffix(string(raw), "}") + `,"extra":true}`},
		{name: "duplicate", body: strings.TrimSuffix(string(raw), "}") + `,"schema":"sessionless.retained-deletion.v1"}`},
		{name: "escaped_duplicate", body: strings.TrimSuffix(string(raw), "}") + `,"sch\u0065ma":"sessionless.retained-deletion.v1"}`},
		{name: "alias", body: strings.Replace(string(raw), `"schema":`, `"Schema":`, 1)},
		{name: "missing", body: strings.Replace(string(raw), `"verified_runs":1,`, "", 1)},
		{name: "trailing_value", body: string(raw) + "\nnull"},
		{name: "oversize", body: strings.Repeat(" ", MaxRetainedDeletionVerificationBytesV1) + string(raw)},
		{name: "null", body: "null"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var candidate RetainedDeletionVerificationV1
			// Exercise this codec's exact input boundary directly: encoding/json
			// strips outer framing whitespace before invoking UnmarshalJSON.
			if err := candidate.UnmarshalJSON([]byte(test.body)); err == nil {
				t.Fatalf("noncanonical/invalid capture DATA accepted: %s", test.name)
			}
		})
	}
	var members map[string]json.RawMessage
	if err := json.Unmarshal(raw, &members); err != nil {
		t.Fatal(err)
	}
	for key := range members {
		t.Run("required_"+key, func(t *testing.T) {
			for _, null := range []bool{false, true} {
				copy := make(map[string]json.RawMessage, len(members))
				for name, value := range members {
					copy[name] = value
				}
				if null {
					copy[key] = json.RawMessage("null")
				} else {
					delete(copy, key)
				}
				body, err := json.Marshal(copy)
				if err != nil {
					t.Fatal(err)
				}
				var candidate RetainedDeletionVerificationV1
				if err := candidate.UnmarshalJSON(body); err == nil {
					t.Errorf("required field %s accepted missing/null substitution: null=%t", key, null)
				}
			}
		})
	}
}

func TestRetainedDeletionVerificationRequestAndInventoryBinding(t *testing.T) {
	d, i := retainedDeletionData(t)
	changed := d
	changed.Reason = "different request"
	if err := d.RetainedVerification.ValidateForDeletion(changed); err == nil {
		t.Fatal("capture accepted a different original request")
	}
	changed = d
	changed.State, changed.StartedAt = SessionDeletionRequested, nil
	if err := changed.Validate(); err == nil {
		t.Fatal("requested deletion accepted a caller-supplied capture")
	}
	i.Objects[0].SHA256 = strings.Repeat("3", 64)
	if err := d.RetainedVerification.ValidateInventory(d, i); err == nil {
		t.Fatal("capture accepted changed object identity with identical counts/bytes")
	}
}

func TestRetainedDeletionVerificationStorageMemberOrder(t *testing.T) {
	d, inventory := retainedDeletionData(t)
	raw, err := json.Marshal(d.RetainedVerification)
	if err != nil {
		t.Fatal(err)
	}
	// JsonDocument does not preserve a Go struct's member order. RawMessage
	// keeps integer/time representations intact while map encoding reorders
	// keys, without inventing a different capture or authority.
	var members map[string]json.RawMessage
	if err := json.Unmarshal(raw, &members); err != nil {
		t.Fatal(err)
	}
	reordered, err := json.Marshal(members)
	if err != nil || string(reordered) == string(raw) {
		t.Fatalf("storage-order counterexample not established: err=%v", err)
	}
	var restored RetainedDeletionVerificationV1
	if err := json.Unmarshal(reordered, &restored); err != nil || restored != *d.RetainedVerification {
		t.Fatalf("same closed capture with storage member order rejected: err=%v got=%+v", err, restored)
	}
	outer, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	members = nil // Unmarshal into a non-nil map otherwise retains inner keys.
	if err := json.Unmarshal(outer, &members); err != nil {
		t.Fatal(err)
	}
	members["retained_verification"] = reordered
	stored, err := json.Marshal(members)
	if err != nil {
		t.Fatal(err)
	}
	var deletion SessionDeletion
	if err := json.Unmarshal(stored, &deletion); err != nil || deletion.Validate() != nil || deletion.RetainedVerification == nil {
		t.Fatalf("stored tombstone roundtrip: err=%v deletion=%+v", err, deletion)
	}
	if err := deletion.RetainedVerification.ValidateInventory(deletion, inventory); err != nil {
		t.Fatalf("storage-order roundtrip changed original request/inventory binding: %v", err)
	}
}
