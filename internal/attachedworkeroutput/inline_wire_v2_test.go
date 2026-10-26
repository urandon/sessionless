package attachedworkeroutput

import (
	"reflect"
	"testing"
)

func TestInlineWireV2RetainsClosedFieldNamesAndStageValues(t *testing.T) {
	for _, tc := range []struct {
		name   string
		value  any
		fields []string
		tags   []string
	}{
		{"prepare", AttachedWorkerInlinePrepareV2{}, []string{"Version", "Authorization", "Nonce", "Candidate", "Producer", "Observation", "ExpiresAt"}, []string{"version", "authorization", "nonce", "candidate", "producer", "observation", "expires_at"}},
		{"body", AttachedWorkerInlineBodyV2{}, []string{"Version", "Authorization", "Nonce", "Fingerprint", "Capability", "ExpiresAt"}, []string{"version", "authorization", "nonce", "fingerprint", "capability", "expires_at"}},
		{"reconcile", AttachedWorkerInlineReconcileV2{}, []string{"Version", "Authorization", "Nonce", "Fingerprint", "BodySendBurned"}, []string{"version", "authorization", "nonce", "fingerprint", "body_send_burned"}},
		{"result", AttachedWorkerInlineResultV2{}, []string{"Version", "Stage", "Nonce", "Fingerprint", "Capability", "ExpiresAt", "CanonicalDigest"}, []string{"version", "stage", "nonce", "fingerprint", "capability", "expires_at", "canonical_digest"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			typ := reflect.TypeOf(tc.value)
			if typ.NumField() != len(tc.fields) {
				t.Fatalf("wire %s field count=%d want=%d", tc.name, typ.NumField(), len(tc.fields))
			}
			for i, name := range tc.fields {
				field := typ.Field(i)
				if field.Name != name || field.Tag.Get("json") != tc.tags[i] {
					t.Errorf("wire %s field%d=%s/json:%q want=%s/json:%q", tc.name, i, field.Name, field.Tag.Get("json"), name, tc.tags[i])
				}
			}
		})
	}
	for _, tc := range []struct {
		stage AttachedWorkerInlineStageV2
		wire  string
	}{{InlinePreparedV2, "prepared"}, {InlineBodyClaimedV2, "body_claimed"}, {InlineCanonicalCopyingV2, "canonical_copying"}, {InlineReadyV2, "ready"}, {InlineRetiredWithoutWritesV2, "retired_without_writes"}} {
		if string(tc.stage) != tc.wire {
			t.Errorf("stage=%q want=%q", tc.stage, tc.wire)
		}
	}
}
