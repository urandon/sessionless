package opssignal

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

type captureSink struct {
	records []Record
	err     error
}

func (s *captureSink) Observe(_ context.Context, r Record) error {
	s.records = append(s.records, r)
	return s.err
}

type writeResult struct {
	calls int
	bytes int
	short bool
	err   error
}

func (w *writeResult) Write(p []byte) (int, error) {
	w.calls++
	w.bytes += len(p)
	if w.short {
		return len(p) - 1, w.err
	}
	return len(p), w.err
}

type signalCase struct {
	signal    SignalID
	stage     Stage
	class     ErrorClass
	component Component
	route     Route
	outcome   Outcome
}

func signalCases() []signalCase {
	return []signalCase{
		{WebAuthFailure, AuthProvider, Provider, Web, WebRoute, Failure},
		{WebSessionFailure, SessionRead, Store, Web, WebRoute, Failure},
		{WebUploadFailure, UploadPresign, ObjectStore, Web, WebRoute, Failure},
		{WebArtifactAccessFailure, ArtifactCapability, Store, Web, WebRoute, Failure},
		{WebAuthSuccess, AuthSession, None, Web, WebRoute, Success},
		{ControlWakeDeferred, WakePublish, Queue, Control, NoRoute, Deferred},
		{ControlDispatchFailure, Dispatch, Store, Control, NoRoute, Failure},
		{ControlQueueFailure, QueueReceive, Queue, Control, NoRoute, Failure},
		{ManagedExecutionFailure, Execution, Provider, Managed, ManagedRoute, Failure},
		{ManagedPublicationFailure, Publication, ObjectStore, Managed, ManagedRoute, Failure},
		{ManagedCleanupFailure, Cleanup, Store, Managed, ManagedRoute, Failure},
		{ManagedDeadlineExceeded, OriginalDeadline, DeadlineExceeded, Managed, ManagedRoute, Failure},
		{ManagedCanonicalUnknown, CanonicalReadback, None, Managed, ManagedRoute, UnknownOutcome},
		{ManagedCanonicalSuccess, CanonicalReadback, None, Managed, ManagedRoute, Success},
		{ManagedCleanupSuccess, Cleanup, None, Managed, ManagedRoute, Success},
		{AttachedTransportFailure, Transport, Provider, Attached, AttachedRoute, Failure},
		{AttachedExecutionFailure, Execution, Provider, Attached, AttachedRoute, Failure},
		{AttachedPublicationFailure, Publication, ObjectStore, Attached, AttachedRoute, Failure},
		{AttachedCleanupFailure, Cleanup, Store, Attached, AttachedRoute, Failure},
		{AttachedDeadlineExceeded, OriginalDeadline, DeadlineExceeded, Attached, AttachedRoute, Failure},
		{AttachedCanonicalUnknown, CanonicalReadback, None, Attached, AttachedRoute, UnknownOutcome},
		{AttachedCanonicalSuccess, CanonicalReadback, None, Attached, AttachedRoute, Success},
	}
}
func validRecord() Record {
	return Record{SchemaVersion: SchemaVersion, SignalID: WebAuthSuccess, Component: Web, Route: WebRoute, Stage: AuthSession, Outcome: Success, ErrorClass: None, DeadlineState: NotApplicable}
}

func TestOpsAllNamedSignalsFixedMetadataAndBound(t *testing.T) {
	t.Parallel()
	cases := signalCases()
	if len(cases) != 22 {
		t.Fatalf("named signal slots: got=%d want=22", len(cases))
	}
	seen := map[SignalID]bool{}
	for _, tc := range cases {
		t.Run(string(tc.signal), func(t *testing.T) {
			if seen[tc.signal] {
				t.Fatalf("duplicate signal test case: %s", tc.signal)
			}
			seen[tc.signal] = true
			capture := &captureSink{}
			Emit(context.Background(), capture, tc.signal, tc.stage, tc.class)
			if len(capture.records) != 1 {
				t.Fatalf("valid signal not observed: signal=%s stage=%s class=%s got=%d", tc.signal, tc.stage, tc.class, len(capture.records))
			}
			r := capture.records[0]
			if r.SchemaVersion != SchemaVersion || r.Component != tc.component || r.Route != tc.route || r.Outcome != tc.outcome || r.DeadlineState != NotApplicable {
				t.Fatalf("fixed signal metadata: got=%+v want component=%s route=%s outcome=%s", r, tc.component, tc.route, tc.outcome)
			}
			var output bytes.Buffer
			if err := NewWriter(&output).Observe(context.Background(), r); err != nil {
				t.Fatalf("Observe valid record: %v", err)
			}
			if output.Len() > MaxRecordBytes || bytes.Count(output.Bytes(), []byte{'\n'}) != 1 {
				t.Fatalf("record bound/framing: got=%d bytes payload=%q", output.Len(), output.Bytes())
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(bytes.TrimSpace(output.Bytes()), &fields); err != nil {
				t.Fatal(err)
			}
			if len(fields) != 8 {
				t.Errorf("record shape expanded: got=%d fields want=8", len(fields))
			}
		})
	}
}

func TestOpsSignalStageClassRelation(t *testing.T) {
	t.Parallel()
	// This expected relation independently pins every permitted stage/class pair.
	type permitted struct {
		signal  SignalID
		stages  []Stage
		classes []ErrorClass
	}
	fail := []ErrorClass{Unexpected, Canceled, DeadlineExceeded}
	with := func(extra ...ErrorClass) []ErrorClass { return append(append([]ErrorClass{}, fail...), extra...) }
	relation := []permitted{
		{WebAuthFailure, []Stage{AuthStart, AuthChallenge, AuthIdentity, AuthMembership, AuthSession}, with(Store)},
		{WebAuthFailure, []Stage{AuthProvider}, with(Provider)},
		{WebAuthSuccess, []Stage{AuthSession}, []ErrorClass{None}},
		{WebSessionFailure, []Stage{SessionRead, SessionWrite}, with(Store)},
		{WebSessionFailure, []Stage{MessageCommit}, with(Store, ObjectStore)},
		{WebUploadFailure, []Stage{UploadIntent, UploadCommit}, with(Store)},
		{WebUploadFailure, []Stage{UploadPresign, UploadStat}, with(ObjectStore)},
		{WebArtifactAccessFailure, []Stage{AttachmentCapability, ArtifactCapability}, with(Store, ObjectStore)},
		{ControlWakeDeferred, []Stage{WakePublish}, with(Queue)},
		{ControlDispatchFailure, []Stage{Dispatch}, with(Store, Queue, Provider)},
		{ControlQueueFailure, []Stage{QueuePublish, QueueReceive, QueueAck, QueueRetry, QueueDeadLetter}, with(Queue)},
		{ControlQueueFailure, []Stage{Ingress}, with(Queue, Store)},
		{ManagedExecutionFailure, []Stage{Execution}, with(Provider)},
		{AttachedExecutionFailure, []Stage{Execution}, with(Provider)},
		{ManagedPublicationFailure, []Stage{Publication}, with(Store, ObjectStore)},
		{AttachedPublicationFailure, []Stage{Publication}, with(Store, ObjectStore)},
		{ManagedCleanupFailure, []Stage{Cleanup}, with(Store, ObjectStore, Provider)},
		{AttachedCleanupFailure, []Stage{Cleanup}, with(Store, ObjectStore, Provider)},
		{ManagedDeadlineExceeded, []Stage{OriginalDeadline}, []ErrorClass{DeadlineExceeded}},
		{AttachedDeadlineExceeded, []Stage{OriginalDeadline}, []ErrorClass{DeadlineExceeded}},
		{ManagedCanonicalUnknown, []Stage{CanonicalReadback}, []ErrorClass{None, Store, Unexpected}},
		{AttachedCanonicalUnknown, []Stage{CanonicalReadback}, []ErrorClass{None, Store, Unexpected}},
		{ManagedCanonicalSuccess, []Stage{CanonicalReadback}, []ErrorClass{None}},
		{AttachedCanonicalSuccess, []Stage{CanonicalReadback}, []ErrorClass{None}},
		{ManagedCleanupSuccess, []Stage{Cleanup}, []ErrorClass{None}},
		{AttachedTransportFailure, []Stage{Transport}, with(Provider, Queue)},
	}
	type tuple struct {
		signal SignalID
		stage  Stage
		class  ErrorClass
	}
	want := map[tuple]bool{}
	for _, entry := range relation {
		for _, stage := range entry.stages {
			for _, class := range entry.classes {
				want[tuple{entry.signal, stage, class}] = true
			}
		}
	}
	stages := []Stage{AuthStart, AuthChallenge, AuthProvider, AuthIdentity, AuthMembership, AuthSession, SessionRead, SessionWrite, UploadIntent, UploadPresign, UploadStat, UploadCommit, MessageCommit, AttachmentCapability, ArtifactCapability, WakePublish, Dispatch, QueuePublish, QueueReceive, QueueAck, QueueRetry, QueueDeadLetter, Ingress, Execution, Publication, Cleanup, CanonicalReadback, Transport, OriginalDeadline, Stage("expected_denial")}
	classes := []ErrorClass{Unexpected, Store, Provider, ObjectStore, Queue, Canceled, DeadlineExceeded, None, ErrorClass("expected_denial")}
	for _, entry := range signalCases() {
		for _, stage := range stages {
			for _, class := range classes {
				capture := &captureSink{}
				Emit(context.Background(), capture, entry.signal, stage, class)
				got := len(capture.records) == 1
				if got != want[tuple{entry.signal, stage, class}] {
					t.Errorf("signal relation: signal=%s stage=%s class=%s got=%v want=%v", entry.signal, stage, class, got, !got)
				}
				if got {
					encoded, err := json.Marshal(capture.records[0])
					if err != nil || len(encoded)+1 > MaxRecordBytes {
						t.Fatalf("valid crossproduct exceeds framing bound: bytes=%d err=%v", len(encoded)+1, err)
					}
				}
			}
		}
	}
}

func TestOpsManualRecordPrivacyAndContradictions(t *testing.T) {
	t.Parallel()
	for _, sentinel := range []string{"private-sentinel", "https://private.example/object?token=sentinel", "tenant-session-run-id", "Bearer secret-cookie", "line\nbreak", strings.Repeat("x", 1000)} {
		for _, field := range []string{"schema", "signal", "component", "route", "stage", "outcome", "class", "deadline"} {
			t.Run(field+"/"+strings.ReplaceAll(sentinel[:min(len(sentinel), 16)], "/", "_"), func(t *testing.T) {
				r := validRecord()
				switch field {
				case "schema":
					r.SchemaVersion = sentinel
				case "signal":
					r.SignalID = SignalID(sentinel)
				case "component":
					r.Component = Component(sentinel)
				case "route":
					r.Route = Route(sentinel)
				case "stage":
					r.Stage = Stage(sentinel)
				case "outcome":
					r.Outcome = Outcome(sentinel)
				case "class":
					r.ErrorClass = ErrorClass(sentinel)
				case "deadline":
					r.DeadlineState = DeadlineState(sentinel)
				}
				writer := &writeResult{}
				err := NewWriter(writer).Observe(context.Background(), r)
				if !errors.Is(err, ErrInvalidRecord) || writer.calls != 0 {
					t.Fatalf("invalid %s field reached writer: err=%v calls=%d", field, err, writer.calls)
				}
				if strings.Contains(err.Error(), sentinel) {
					t.Errorf("private input leaked in error: %v", err)
				}
			})
		}
	}
	for _, change := range []func(*Record){func(r *Record) { r.Route = ManagedRoute }, func(r *Record) { r.Component = Attached }, func(r *Record) { r.Outcome = Failure }, func(r *Record) { r.Stage = AuthProvider }, func(r *Record) { r.ErrorClass = Store }, func(r *Record) { r.DeadlineState = Within }} {
		r := validRecord()
		change(&r)
		writer := &writeResult{}
		if err := NewWriter(writer).Observe(context.Background(), r); !errors.Is(err, ErrInvalidRecord) || writer.calls != 0 {
			t.Fatalf("contradictory record reached writer: record=%+v err=%v calls=%d", r, err, writer.calls)
		}
	}
}

func TestOpsOneWriteNoRetryAndSilentDrop(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		short bool
		err   error
	}{{"success", false, nil}, {"short", true, nil}, {"error", false, errors.New("private-error-sentinel")}, {"short_error", true, io.ErrClosedPipe}} {
		t.Run(tc.name, func(t *testing.T) {
			writer := &writeResult{short: tc.short, err: tc.err}
			err := NewWriter(writer).Observe(context.Background(), validRecord())
			if writer.calls != 1 || writer.bytes > MaxRecordBytes {
				t.Fatalf("writer bound: calls=%d bytes=%d", writer.calls, writer.bytes)
			}
			if tc.short || tc.err != nil {
				if !errors.Is(err, ErrWrite) || strings.Contains(err.Error(), "private-error-sentinel") {
					t.Fatalf("sanitized write failure: got=%v", err)
				}
			} else if err != nil {
				t.Fatalf("successful writer: %v", err)
			}
		})
	}
	failed := &captureSink{err: errors.New("private-sink-sentinel")}
	Emit(context.Background(), failed, WebAuthSuccess, AuthSession, None)
	if len(failed.records) != 1 {
		t.Errorf("sink error triggered missing/duplicate observation: got=%d", len(failed.records))
	}
	Emit(context.Background(), nil, WebAuthSuccess, AuthSession, None)
	Emit(nil, failed, WebAuthSuccess, AuthSession, None)
	if len(failed.records) != 1 {
		t.Error("nil context must not reach arbitrary sink")
	}
	if err := NewWriter(nil).Observe(context.Background(), validRecord()); !errors.Is(err, ErrUnavailableWriter) {
		t.Errorf("nil writer: got=%v", err)
	}
}

func TestOpsCancellationAndOriginalDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		originalDeadline, ok := ctx.Deadline()
		if !ok {
			t.Fatal("fixture deadline missing")
		}
		capture := &captureSink{}
		Emit(ctx, capture, WebAuthSuccess, AuthSession, None)
		if len(capture.records) != 1 || capture.records[0].DeadlineState != Within {
			t.Fatalf("active original deadline: records=%+v", capture.records)
		}
		if after, _ := ctx.Deadline(); after != originalDeadline {
			t.Error("Emit changed original deadline")
		}
		time.Sleep(time.Minute) // Advances the synctest clock to the controlled expiry.
		synctest.Wait()
		writer := &writeResult{}
		err := NewWriter(writer).Observe(ctx, capture.records[0])
		if !errors.Is(err, context.DeadlineExceeded) || writer.calls != 0 {
			t.Fatalf("expired original context wrote: err=%v calls=%d", err, writer.calls)
		}
		Emit(ctx, capture, WebAuthFailure, AuthSession, DeadlineExceeded)
		if len(capture.records) != 1 {
			t.Error("expired observation attempted a new delivery")
		}
		if deadlineState(ctx) != Exceeded {
			t.Errorf("expired state: got=%s", deadlineState(ctx))
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	writer := &writeResult{}
	if err := NewWriter(writer).Observe(ctx, validRecord()); !errors.Is(err, context.Canceled) || writer.calls != 0 {
		t.Fatalf("canceled context wrote: err=%v calls=%d", err, writer.calls)
	}
}

func TestOpsConcurrentWholeRecordsAndDuplicates(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	sink := NewWriter(&output)
	const count = 64
	start := make(chan struct{})
	var done sync.WaitGroup
	for i := 0; i < count; i++ {
		done.Add(1)
		go func() {
			defer done.Done()
			<-start
			Emit(context.Background(), sink, ControlQueueFailure, QueueAck, Queue)
		}()
	}
	close(start)
	done.Wait()
	lines := bytes.Split(bytes.TrimSuffix(output.Bytes(), []byte{'\n'}), []byte{'\n'})
	if len(lines) != count {
		t.Fatalf("whole concurrent records: got=%d want=%d", len(lines), count)
	}
	for i, line := range lines {
		var record Record
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatalf("concurrent line %d malformed: %v", i, err)
		}
		if err := record.Validate(); err != nil || record.SignalID != ControlQueueFailure {
			t.Fatalf("concurrent line %d: record=%+v err=%v", i, record, err)
		}
	}
	// Repeated actual boundary calls remain observations, not exactly-once effects.
	Emit(context.Background(), sink, WebAuthSuccess, AuthSession, None)
	Emit(context.Background(), sink, WebAuthSuccess, AuthSession, None)
	if got := bytes.Count(output.Bytes(), []byte{'\n'}); got != count+2 {
		t.Fatalf("duplicate observations silently deduplicated: got=%d want=%d", got, count+2)
	}
}

func TestOpsCancellationAfterPrecheckDoesNotStartWrite(t *testing.T) {
	// sync.Mutex waits are not durably blocked for synctest.Wait. Instead,
	// pause at the last context precheck, then cancel before allowing the
	// writer to proceed. This pins the post-lock cancellation check without
	// pretending to observe a scheduler-dependent mutex-wait interleaving.
	base, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := &deadlineGateContext{Context: base, entered: make(chan struct{}), release: make(chan struct{})}
	writer := &writeResult{}
	sink := NewWriter(writer).(*writerSink)
	sink.mu.Lock()
	var releaseOnce, unlockOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(ctx.release) }); unlockOnce.Do(sink.mu.Unlock) }
	done := make(chan struct{})
	var observedErr error
	go func() { defer close(done); observedErr = sink.Observe(ctx, validRecord()) }()
	defer func() {
		cancel()
		release()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("observation goroutine did not terminate during cleanup")
		}
	}()
	select {
	case <-ctx.entered:
	case <-time.After(5 * time.Second): // failure bound, never readiness synchronization
		t.Fatal("Observe never reached the final precheck")
	}
	cancel()
	release()
	select {
	case <-done:
		if !errors.Is(observedErr, context.Canceled) || writer.calls != 0 {
			t.Fatalf("canceled prechecked observation: err=%v writes=%d want canceled/zero", observedErr, writer.calls)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Observe did not terminate after cancellation/release")
	}
}

type deadlineGateContext struct {
	context.Context
	entered chan struct{}
	release chan struct{}
}

func (ctx *deadlineGateContext) Deadline() (time.Time, bool) {
	deadline, ok := ctx.Context.Deadline()
	close(ctx.entered)
	<-ctx.release
	return deadline, ok
}

func FuzzRejectUntrustedEnumStrings(f *testing.F) {
	for _, seed := range []string{"private-sentinel", "https://private.example/?token=sentinel", "web.auth_failure", "line\nbreak", "", strings.Repeat("x", 769)} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, input string) {
		if len(input) > 4096 {
			return
		}
		r := validRecord()
		r.SignalID = SignalID(input)
		var output bytes.Buffer
		err := NewWriter(&output).Observe(context.Background(), r)
		if input != string(WebAuthSuccess) {
			if !errors.Is(err, ErrInvalidRecord) || output.Len() != 0 {
				t.Fatalf("untrusted signal string accepted: bytes=%d err=%v", output.Len(), err)
			}
		}
	})
}
