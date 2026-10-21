package sessionapi_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/opssignal"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessionapi"
)

type opsStore struct {
	ports.SessionAPIStore
	err   error
	found bool
}

func (store opsStore) GetSessionForUser(context.Context, domain.TenantID, domain.UserID, domain.SessionID, bool) (ports.SessionRecord, bool, error) {
	return ports.SessionRecord{}, store.found, store.err
}

type opsSink struct {
	records  []opssignal.Record
	contexts []context.Context
}

func (sink *opsSink) Observe(ctx context.Context, record opssignal.Record) error {
	sink.records = append(sink.records, record)
	sink.contexts = append(sink.contexts, ctx)
	return errors.New("writer failed")
}

func TestOpsMaskedSessionLookupFailureAndExpectedDenial(t *testing.T) {
	for _, tc := range []struct {
		name  string
		err   error
		found bool
		want  int
		class opssignal.ErrorClass
	}{
		{"missing", nil, false, 0, opssignal.Store},
		{"membership", domain.ErrMembershipDenied, false, 0, opssignal.Store},
		{"validation", domain.ValidationError{Field: "private-id", Reason: "secret-payload"}, false, 0, opssignal.Store},
		{"store", errors.New("secret-payload tenant-private"), false, 1, opssignal.Store},
		{"canceled", context.Canceled, false, 1, opssignal.Canceled},
		{"deadline", context.DeadlineExceeded, false, 1, opssignal.DeadlineExceeded},
		{"unmasked", errors.New("unmasked-store"), true, 0, opssignal.Store},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sink := &opsSink{}
			store := opsStore{err: tc.err, found: tc.found}
			service, err := sessionapi.New(sessionapi.Config{OpsSignals: sink, CursorKey: bytes.Repeat([]byte("c"), 32), IDKey: bytes.Repeat([]byte("i"), 32)}, store, &memoryBlobs{}, fixedClock{})
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.WithValue(context.Background(), struct{}{}, "private-context")
			_, err = service.Get(ctx, "tenant-private", "user-private", "session-private")
			if tc.found {
				if !errors.Is(err, tc.err) {
					t.Fatalf("unmasked error=%v want=%v", err, tc.err)
				}
			} else if !errors.Is(err, sessionapi.ErrSessionUnavailable) {
				t.Fatalf("public mask changed: %v", err)
			}
			if len(sink.records) != tc.want {
				t.Fatalf("records=%+v want=%d", sink.records, tc.want)
			}
			if tc.want == 1 && (sink.records[0].SignalID != opssignal.WebSessionFailure || sink.records[0].Stage != opssignal.SessionRead || sink.records[0].ErrorClass != tc.class || sink.contexts[0] != ctx) {
				t.Fatalf("record=%+v originalContext=%t", sink.records[0], sink.contexts[0] == ctx)
			}
		})
	}
}
