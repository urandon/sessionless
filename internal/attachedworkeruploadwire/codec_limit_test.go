package attachedworkeruploadwire

import (
	"bytes"
	"math"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
)

// PF200-UC-R01: the no-capability class is known after structural preflight.
// Reject its smaller envelope BEFORE any admitted-string/slice ownership pass,
// even when its individually bounded scalars later fail semantic validation.
func TestAttemptOutputUploadReplyClassBudgetBeforeOwnedDecode(t *testing.T) {
	_, reply, _ := fixture(t)
	body := mustReply(t, reply)
	h := reply.AcceptedAttempt
	for key, value := range map[string]string{
		"tenant_id": string(h.TenantID), "owner_user_id": string(h.OwnerUserID), "worker_id": string(h.WorkerID),
		"connection_id": string(h.ConnectionID), "execution_connection_id": string(h.ExecutionConnectionID),
		"run_id": string(h.RunID), "attempt_id": string(h.AttemptID), "reservation_id": string(h.ReservationID), "lease_id": string(h.LeaseID),
	} {
		old := []byte(`"` + key + `":"` + value + `"`)
		if !bytes.Contains(body, old) {
			t.Fatalf("fixture key absent: %s", key)
		}
		body = bytes.Replace(body, old, []byte(`"`+key+`":"`+strings.Repeat(`\"`, 160)+`"`), 1)
	}
	if len(body) <= ReplyWithoutCapabilityMaxBytes || len(body) > ReplyMaxBytes {
		t.Fatalf("class regression fixture length=%d want (%d,%d]", len(body), ReplyWithoutCapabilityMaxBytes, ReplyMaxBytes)
	}
	pre := parser{b: body, ok: true, maxNodes: ReplyNodes}
	parseReply(&pre)
	if !pre.ok || pre.i != len(body) {
		t.Fatal("fixture must pass complete bounded structural grammar before class rejection")
	}
	if _, err := DecodeReply(body); err == nil {
		t.Fatal("over-class malformed reply accepted")
	}
	if allocs := testing.AllocsPerRun(20, func() { _, _ = DecodeReply(body) }); allocs != 0 {
		t.Fatalf("non-capability %d-byte envelope entered owned decode: allocations=%v; want 0 before class rejection", len(body), allocs)
	}
}

func TestAttemptOutputUploadReplyClassRetainsWideLegitimateHead(t *testing.T) {
	_, reply, _ := fixture(t)
	h := reply.AcceptedAttempt
	h.TenantID = domain.TenantID(strings.Repeat("t", 160))
	h.OwnerUserID = domain.UserID(strings.Repeat("o", 160))
	h.WorkerID = domain.AttachedWorkerID(strings.Repeat("w", 160))
	h.ConnectionID = domain.AttachedWorkerConnectionID(strings.Repeat("c", 160))
	h.ExecutionConnectionID = h.ConnectionID
	h.RunID = domain.RunID(strings.Repeat("r", 160))
	h.AttemptID = domain.AttemptID(strings.Repeat("a", 160))
	h.ReservationID = domain.QuotaReservationID(strings.Repeat("q", 160))
	h.LeaseID = domain.LeaseID(strings.Repeat("l", 160))
	h.LeaseGeneration, h.EnrollmentGeneration, h.ConnectionGeneration = math.MaxUint64, math.MaxUint64, math.MaxUint64
	h.ExecutionConnectionGeneration, h.ProgressSequence, h.Revision = math.MaxUint64, math.MaxUint64, math.MaxUint64
	h.WorkerAttemptSequence, h.PlatformAttemptSequence = math.MaxUint64, math.MaxUint64
	var err error
	h.FenceToken, err = domain.NewAttachedWorkerFenceTokenV1(h.TenantID, h.OwnerUserID, h.WorkerID, h.RunID, h.AttemptID, h.LeaseID, h.LeaseGeneration)
	if err != nil {
		t.Fatal("wide positive fence:", err)
	}
	if err := h.Validate(); err != nil {
		t.Fatal("invalid wide positive head:", err)
	}
	reply.AcceptedAttempt = h
	body := mustReply(t, reply)
	if len(body) > ReplyWithoutCapabilityMaxBytes {
		t.Fatal("wide legitimate non-capability reply exceeded selected class")
	}
	decoded, err := DecodeReply(body)
	if err != nil || decoded.AcceptedAttempt != h || decoded.Capability != nil {
		t.Fatalf("wide legitimate class rejected: bytes=%d err=%v", len(body), err)
	}
}
