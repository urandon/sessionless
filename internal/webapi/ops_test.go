package webapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/opssignal"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
	"gitcode.com/urandon/sessionless/internal/webapi"
	"gitcode.com/urandon/sessionless/internal/webcontract"
)

type opsSink struct {
	records  []opssignal.Record
	contexts []context.Context
	err      error
}

func (sink *opsSink) Observe(ctx context.Context, record opssignal.Record) error {
	sink.records = append(sink.records, record)
	sink.contexts = append(sink.contexts, ctx)
	return sink.err
}

func assertOpsFailure(t *testing.T, sink *opsSink, ctx context.Context, signal opssignal.SignalID, stage opssignal.Stage, class opssignal.ErrorClass) {
	t.Helper()
	if len(sink.records) != 1 {
		t.Fatalf("observations=%+v want one %s/%s", sink.records, signal, stage)
	}
	record := sink.records[0]
	if record.SignalID != signal || record.Stage != stage || record.ErrorClass != class || sink.contexts[0] != ctx {
		t.Fatalf("record=%+v contextPreserved=%t want=%s/%s/%s", record, sink.contexts[0] == ctx, signal, stage, class)
	}
	encoded, err := json.Marshal(record)
	if err != nil || len(encoded)+1 > opssignal.MaxRecordBytes {
		t.Fatalf("safe record encoding size=%d err=%v", len(encoded), err)
	}
	for _, forbidden := range []string{"tenant-a", "user-a", "session-a", "secret-payload", "private.pdf", "https://"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("observation leaked %q: %s", forbidden, encoded)
		}
	}
}

func TestOpsUploadFailureBoundariesAndDenials(t *testing.T) {
	for _, tc := range []struct {
		name    string
		stage   opssignal.Stage
		class   opssignal.ErrorClass
		failure error
	}{
		{"intent-store", opssignal.UploadIntent, opssignal.Store, errors.New("secret-payload tenant-a")},
		{"presign", opssignal.UploadPresign, opssignal.ObjectStore, errors.New("https://private.pdf/secret-payload")},
		{"stat", opssignal.UploadStat, opssignal.ObjectStore, errors.New("secret-payload")},
		{"commit", opssignal.UploadCommit, opssignal.Store, errors.New("secret-payload")},
		{"canceled", opssignal.UploadPresign, opssignal.Canceled, context.Canceled},
		{"deadline", opssignal.UploadPresign, opssignal.DeadlineExceeded, context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sink := &opsSink{err: errors.New("sink unavailable")}
			h := newHarnessWithBinder(t, sessionlessharness.NewDeterministicFixtureBinderV1(), sink)
			ctx := context.WithValue(context.Background(), struct{}{}, "private-context")
			if tc.name == "canceled" {
				canceled, cancel := context.WithCancel(ctx)
				cancel()
				ctx = canceled
			}
			if tc.name == "deadline" {
				expired, cancel := context.WithDeadline(ctx, time.Time{})
				t.Cleanup(cancel)
				ctx = expired
			}
			request := uploadRequest("ops-upload", "private.pdf", "application/pdf", []byte("secret-payload"))
			var err error
			if tc.stage == opssignal.UploadIntent || tc.stage == opssignal.UploadPresign {
				if tc.stage == opssignal.UploadIntent {
					h.backend.uploadCreateErr = tc.failure
				} else {
					h.objects.uploadErr = tc.failure
				}
				_, _, err = h.service.CreateUpload(ctx, "tenant-a", "user-a", request)
			} else {
				upload, _, createErr := h.service.CreateUpload(ctx, "tenant-a", "user-a", request)
				if createErr != nil {
					t.Fatal(createErr)
				}
				intent := h.backend.intents[upload.UploadID]
				h.objects.put(intent.ObjectKey, "application/pdf", []byte("secret-payload"), "etag-ops")
				if tc.stage == opssignal.UploadStat {
					h.objects.statErr = tc.failure
				} else {
					h.backend.uploadCommitErr = tc.failure
				}
				_, err = h.service.CommitUpload(ctx, "tenant-a", "user-a", upload.UploadID)
			}
			if !errors.Is(err, tc.failure) {
				t.Fatalf("operation error=%v want original=%v", err, tc.failure)
			}
			if ctx.Err() != nil {
				if len(sink.records) != 0 {
					t.Fatalf("canceled original context was replaced for telemetry: %+v", sink.records)
				}
				return
			}
			assertOpsFailure(t, sink, ctx, opssignal.WebUploadFailure, tc.stage, tc.class)
		})
	}
	for _, denied := range []error{domain.ErrMembershipDenied, domain.ErrUploadIntentExpired, domain.ErrUploadMismatch, &domain.ClassifiedError{Kind: domain.ErrorQuotaBlocked, Code: "private-code"}} {
		sink := &opsSink{}
		h := newHarnessWithBinder(t, sessionlessharness.NewDeterministicFixtureBinderV1(), sink)
		h.backend.uploadCreateErr = denied
		_, _, err := h.service.CreateUpload(context.Background(), "tenant-a", "user-a", uploadRequest("ops-denied", "private.pdf", "application/pdf", []byte("secret-payload")))
		if !errors.Is(err, denied) || len(sink.records) != 0 {
			t.Fatalf("denial=%v err=%v observations=%+v", denied, err, sink.records)
		}
	}
}

func TestOpsAttachmentCapabilityFailureIsNotBucketGET(t *testing.T) {
	sink := &opsSink{err: errors.New("dropped observation")}
	h := newHarnessWithBinder(t, sessionlessharness.NewDeterministicFixtureBinderV1(), sink)
	upload := prepareCommittedUpload(t, h, []byte("secret-payload"), "private.pdf", "application/pdf")
	created, err := h.service.SubmitMessage(context.Background(), "tenant-a", "user-a", "session-a", webcontract.CreateMessageRequest{IdempotencyKey: "ops-message", UploadIDs: []domain.UploadIntentID{upload}})
	if err != nil {
		t.Fatal(err)
	}
	if len(sink.records) != 0 {
		t.Fatalf("presign/commit/message manufactured success signals: %+v", sink.records)
	}
	failure := errors.New("private.pdf https://objects.invalid secret-payload")
	h.objects.downloadErr = failure
	ctx := context.Background()
	_, err = h.service.DownloadAttachment(ctx, "tenant-a", "user-a", "session-a", created.Sequence, 0)
	if !errors.Is(err, failure) {
		t.Fatalf("capability error=%v want original=%v", err, failure)
	}
	assertOpsFailure(t, sink, ctx, opssignal.WebArtifactAccessFailure, opssignal.AttachmentCapability, opssignal.ObjectStore)
	h.objects.downloadErr = nil
	sink.records, sink.contexts = nil, nil
	if _, err = h.service.DownloadAttachment(ctx, "tenant-a", "user-a", "session-a", created.Sequence, 0); err != nil {
		t.Fatal(err)
	}
	if len(sink.records) != 0 {
		t.Fatalf("capability issuance claimed GET success: %+v", sink.records)
	}
}

func TestOpsArtifactLookupFailureObservedBeforePublicNotFoundMask(t *testing.T) {
	sink := &opsSink{err: errors.New("dropped observation")}
	h := newHarnessWithBinder(t, sessionlessharness.NewDeterministicFixtureBinderV1(), sink)
	h.backend.artifactErr = errors.New("secret-payload tenant-a")
	ctx := context.Background()
	_, err := h.service.DownloadRunArtifact(ctx, "tenant-a", "user-a", "session-a", "run-private", "manifest-private", 0)
	if !errors.Is(err, webapi.ErrResourceUnavailable) {
		t.Fatalf("public error changed: %v", err)
	}
	assertOpsFailure(t, sink, ctx, opssignal.WebArtifactAccessFailure, opssignal.ArtifactCapability, opssignal.Store)
	sink.records = nil
	h.backend.artifactErr = domain.ErrMembershipDenied
	_, err = h.service.DownloadRunArtifact(ctx, "tenant-a", "user-a", "session-a", "run-private", "manifest-private", 0)
	if !errors.Is(err, webapi.ErrResourceUnavailable) || len(sink.records) != 0 {
		t.Fatalf("expected denial err=%v observations=%+v", err, sink.records)
	}
}

func TestOpsCanonicalMessageFailureVersusExpectedRejection(t *testing.T) {
	for _, tc := range []struct {
		name    string
		failure error
		want    int
	}{
		{"store", errors.New("secret-payload session-a private-storage"), 1},
		{"quota", &domain.ClassifiedError{Kind: domain.ErrorQuotaBlocked, Code: "private-quota"}, 0},
		{"policy", &domain.ClassifiedError{Kind: domain.ErrorPolicyDenied, Code: "private-policy"}, 0},
		{"conflict", domain.ErrEventIdempotencyConflict, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sink := &opsSink{err: errors.New("sink failed")}
			h := newHarnessWithBinder(t, sessionlessharness.NewDeterministicFixtureBinderV1(), sink)
			h.backend.canonicalCommitErr = tc.failure
			ctx := context.Background()
			_, err := h.service.SubmitMessage(ctx, "tenant-a", "user-a", "session-a", webcontract.CreateMessageRequest{IdempotencyKey: "ops-canonical-failure", Text: "secret-payload"})
			if !errors.Is(err, tc.failure) || len(h.backend.commits) != 0 || len(h.backend.events) != 0 || len(sink.records) != tc.want {
				t.Fatalf("err=%v want=%v commits=%d events=%d observations=%+v", err, tc.failure, len(h.backend.commits), len(h.backend.events), sink.records)
			}
			if tc.want == 1 {
				assertOpsFailure(t, sink, ctx, opssignal.WebSessionFailure, opssignal.MessageCommit, opssignal.Store)
			}
		})
	}
}
