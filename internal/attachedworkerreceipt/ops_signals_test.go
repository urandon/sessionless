package attachedworkerreceipt

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/opssignal"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

type opsReceiptSink struct {
	records  []opssignal.Record
	contexts []context.Context
	err      error
	observe  func(context.Context)
}

func (sink *opsReceiptSink) Observe(ctx context.Context, record opssignal.Record) error {
	sink.records = append(sink.records, record)
	sink.contexts = append(sink.contexts, ctx)
	if sink.observe != nil {
		sink.observe(ctx)
	}
	return sink.err
}

func TestOpsAttachedReceiptPublicationDistinguishesExpectedDenial(t *testing.T) {
	for _, tc := range []struct {
		name    string
		failure error
		status  ports.AttachedWorkerExecutionStatus
		want    int
	}{
		{"publication-error", errors.New("private-object-url secret-body"), ports.AttachedWorkerExecutionApplied, 1},
		{"candidate-denied", attachedworkeroutput.ErrCandidateInvalid, ports.AttachedWorkerExecutionApplied, 0},
		{"validation", domain.ValidationError{Field: "private-id", Reason: "secret-body"}, ports.AttachedWorkerExecutionApplied, 0},
		{"validation-pointer", &domain.ValidationError{Field: "private-id", Reason: "secret-body"}, ports.AttachedWorkerExecutionApplied, 0},
		{"membership-denied", domain.ErrMembershipDenied, ports.AttachedWorkerExecutionApplied, 0},
		{"copy-in-progress", ydbstore.ErrAttachedWorkerReceiptCopyInProgress, ports.AttachedWorkerExecutionApplied, 0},
		{"conflict-error", ydbstore.ErrAttachedWorkerAttemptConflict, ports.AttachedWorkerExecutionApplied, 0},
		{"canceled", context.Canceled, ports.AttachedWorkerExecutionApplied, 0},
		{"status-denied", nil, ports.AttachedWorkerExecutionFenced, 0},
		{"status-conflict", nil, ports.AttachedWorkerExecutionConflict, 0},
		{"receipt-not-canonical", nil, ports.AttachedWorkerExecutionApplied, 0},
		{"replayed-not-canonical", nil, ports.AttachedWorkerExecutionReplayed, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			binder := &receiptBinderFixture{}
			store := &receiptStoreFixture{status: tc.status, err: tc.failure}
			sink := &opsReceiptSink{err: errors.New("writer failed")}
			service, err := NewServiceWithOpsSignals(binder, store, receiptBlobFixture{}, sink)
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.WithValue(context.Background(), struct{}{}, "private-context")
			result, err := service.Create(ctx, []byte("bound-bearer"), ydbstore.AttachedWorkerOutputReceiptRequest{Authorization: ports.AttachedWorkerSealedInputAuthorization{OwnerUserID: "owner-a"}})
			if tc.failure != nil && err != ErrUnavailable || tc.failure == nil && tc.status == ports.AttachedWorkerExecutionFenced && err != ErrUnauthorized || tc.failure == nil && tc.status == ports.AttachedWorkerExecutionConflict && err != ErrConflict {
				t.Fatalf("public error=%v status=%s storeErr=%v", err, tc.status, tc.failure)
			}
			if tc.failure == nil && (tc.status == ports.AttachedWorkerExecutionApplied || tc.status == ports.AttachedWorkerExecutionReplayed) && (err != nil || result.Receipt.CanonicalDigest != "canonical-digest") {
				t.Fatalf("successful receipt changed result=%+v err=%v", result, err)
			}
			if binder.calls != 1 || store.calls != 1 || len(sink.records) != tc.want {
				t.Fatalf("binder=%d store=%d records=%+v want=%d", binder.calls, store.calls, sink.records, tc.want)
			}
			if tc.want == 1 {
				record := sink.records[0]
				if record.SignalID != opssignal.AttachedPublicationFailure || record.Stage != opssignal.Publication || record.ErrorClass != opssignal.Unexpected || sink.contexts[0] != ctx {
					t.Fatalf("record=%+v originalContext=%t", record, sink.contexts[0] == ctx)
				}
				encoded, _ := json.Marshal(record)
				for _, secret := range []string{"private-object-url", "secret-body", "owner-a", "bound-bearer", "canonical-digest"} {
					if strings.Contains(string(encoded), secret) {
						t.Fatalf("record leaked %q: %s", secret, encoded)
					}
				}
			}
		})
	}
}

func TestOpsAttachedReceiptSinkCannotRetryPublicationOrUpgradeDenial(t *testing.T) {
	binder := &receiptBinderFixture{}
	store := &receiptStoreFixture{status: ports.AttachedWorkerExecutionApplied, err: errors.New("ambiguous object publication")}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sink := &opsReceiptSink{err: errors.New("dropped"), observe: func(got context.Context) {
		if got != ctx {
			t.Error("sink detached original context")
		}
		cancel()
	}}
	service, err := NewServiceWithOpsSignals(binder, store, receiptBlobFixture{}, sink)
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.Create(ctx, []byte("bound-bearer"), ydbstore.AttachedWorkerOutputReceiptRequest{Authorization: ports.AttachedWorkerSealedInputAuthorization{OwnerUserID: "owner-a"}})
	if err != ErrUnavailable || store.calls != 1 || len(sink.records) != 1 || ctx.Err() != context.Canceled {
		t.Fatalf("err=%v writes=%d observations=%d ctx=%v", err, store.calls, len(sink.records), ctx.Err())
	}
	// The same expired original context cannot gain a new publication attempt.
	_, err = service.Create(ctx, []byte("bound-bearer"), ydbstore.AttachedWorkerOutputReceiptRequest{Authorization: ports.AttachedWorkerSealedInputAuthorization{OwnerUserID: "owner-a"}})
	if err != ErrUnauthorized || store.calls != 1 || len(sink.records) != 1 {
		t.Fatalf("expired context replay err=%v writes=%d observations=%d", err, store.calls, len(sink.records))
	}
}
