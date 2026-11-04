package ydbstore

import (
	"bytes"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

// This joins the existing SQL/writer/ownership/body consumer over sealed DATA.
// It is not a native database, a serving capability or deletion/erasure proof.
func searchDeletionWithOriginalInputV3(t *testing.T) terminalSearchV3Data {
	t.Helper()
	value := terminalSearchV3Fixture(t)
	for i := range value.records {
		value.records[i].Event.Sequence++
	}
	value.snapshot.Anchor = value.records[len(value.records)-1].Event
	run, owner := value.snapshot.Run, value.snapshot.Admission.Source.Scope.OwnerUserID
	payload := []byte(`{"text":"original public request"}`)
	digest := rawLocalFailureHash(payload)
	event := domain.SessionEvent{
		ID: run.TriggerEventID, TenantID: run.TenantID, SessionID: run.SessionID,
		Sequence: 1, Kind: domain.SessionEventUserMessage, AuthorUserID: &owner,
		RunID: &run.ID, IdempotencyKey: "original-search-deletion-trigger", CreatedAt: run.CreatedAt,
		Payload: domain.BlobRef{TenantID: run.TenantID,
			Key:  domain.SessionEventObjectPrefix(run.TenantID, run.SessionID, run.TriggerEventID) + "payloads/sha256/" + digest + ".json",
			Size: int64(len(payload)), SHA256: digest},
	}
	if err := event.Validate(); err != nil {
		t.Fatal("valid original input DATA", err)
	}
	if _, err := sessioncontext.EncodeRecord(event, payload); err != nil {
		t.Fatal("canonical original input payload", err)
	}
	value.records = append([]sessioncontext.EventPayload{{Event: event, Payload: payload}}, value.records...)
	return value
}

func TestLocalTerminalSearchV3DeletionRejectsNonOriginalInput(t *testing.T) {
	for _, mode := range []string{"foreign_run", "nil_run", "foreign_owner", "foreign_tenant", "foreign_session", "wrong_kind", "wrong_id", "duplicate", "inside_output", "late_time", "body_hash"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				baseline := searchDeletionWithOriginalInputV3(t)
				if err := terminalSearchV3ReadDeletion(t, baseline, terminalSearchV3DeletionData(t, baseline)); err != nil {
					t.Fatal("valid original input before mutation", err)
				}
				value := searchDeletionWithOriginalInputV3(t)
				input := &value.records[0]
				switch mode {
				case "foreign_run":
					other := domain.RunID("foreign-input-run")
					input.Event.RunID = &other
				case "nil_run":
					input.Event.RunID = nil
				case "foreign_owner":
					other := domain.UserID("foreign-input-owner")
					input.Event.AuthorUserID = &other
				case "foreign_tenant":
					input.Event.TenantID = "foreign-input-tenant"
				case "foreign_session":
					input.Event.SessionID = "foreign-input-session"
				case "wrong_kind":
					input.Event.Kind = domain.SessionEventSystemNotice
				case "wrong_id":
					input.Event.ID = "unsealed-input-id"
					input.Event.Payload.Key = domain.SessionEventObjectPrefix(input.Event.TenantID, input.Event.SessionID, input.Event.ID) + "payloads/sha256/" + input.Event.Payload.SHA256 + ".json"
				case "duplicate":
					value.records = append(value.records, *input)
				case "inside_output":
					input.Event.Sequence = value.records[1].Event.Sequence
				case "late_time":
					input.Event.CreatedAt = value.snapshot.Anchor.CreatedAt.Add(time.Microsecond)
				case "body_hash":
					input.Payload = append(bytes.Clone(input.Payload), ' ')
				}
				storage := terminalSearchV3DeletionData(t, value)
				if err := terminalSearchV3ReadDeletion(t, value, storage); err == nil {
					t.Fatal("nonoriginal or malformed input accepted", mode)
				}
				if storage.writes != 0 {
					t.Fatal("retained DATA verification wrote state")
				}
			})
		})
	}
}

func TestLocalTerminalSearchV3DeletionInputUsesExistingBodyCeiling(t *testing.T) {
	for _, size := range []int{int(domain.MaxTerminalCapsuleBytesV1) + 1, 1 << 20, (1 << 20) + 1} {
		t.Run(fmt.Sprintf("bytes_%d", size), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				value := searchDeletionWithOriginalInputV3(t)
				input := &value.records[0]
				input.Payload = append([]byte(`{"text":"`), bytes.Repeat([]byte("x"), size-len(`{"text":""}`))...)
				input.Payload = append(input.Payload, '"', '}')
				if len(input.Payload) != size {
					t.Fatal("exact input boundary setup")
				}
				digest := rawLocalFailureHash(input.Payload)
				input.Event.Payload.Size, input.Event.Payload.SHA256 = int64(size), digest
				input.Event.Payload.Key = domain.SessionEventObjectPrefix(input.Event.TenantID, input.Event.SessionID, input.Event.ID) + "payloads/sha256/" + digest + ".json"
				storage := terminalSearchV3DeletionData(t, value)
				err := terminalSearchV3ReadDeletion(t, value, storage)
				if (size <= 1<<20) != (err == nil) {
					t.Fatalf("original input byte bound size=%d err=%v", size, err)
				}
				if storage.writes != 0 {
					t.Fatal("retained DATA verification wrote state")
				}
			})
		})
	}
}

func TestLocalTerminalSearchV3DeletionIncludesOriginalInput(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		baseline := terminalSearchV3Fixture(t)
		if err := terminalSearchV3ReadDeletion(t, baseline, terminalSearchV3DeletionData(t, baseline)); err != nil {
			t.Fatal("existing output-only retained reader baseline", err)
		}
		value := searchDeletionWithOriginalInputV3(t)
		if err := validateHistoricalCapsuleDataV3(value.snapshot, value.capsule, value.records); err != nil {
			t.Fatal("unchanged sealed output plus authenticated original input", err)
		}
		storage := terminalSearchV3DeletionData(t, value)
		if err := terminalSearchV3ReadDeletion(t, value, storage); err != nil {
			t.Fatal("retained deletion rejected the original canonical user input as an output", err)
		}
		if storage.writes != 0 {
			t.Fatal("retained DATA verification wrote state")
		}
	})
}
