package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localoperator"
	"gitcode.com/urandon/sessionless/internal/opssignal"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/serverlesshttp"
	"gitcode.com/urandon/sessionless/internal/sqsqueue"
	"gitcode.com/urandon/sessionless/internal/worker"
)

func TestWorkerTriggerDrainWaitsForBothCallbackOwners(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		drain := newWorkerTriggerDrain(t.Context())
		t.Cleanup(drain.cancel)
		first, finishFirst, err := drain.begin(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(finishFirst)
		second, finishSecond, err := drain.begin(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(finishSecond)
		joined := make(chan error, 1)
		go func() { joined <- drain.close(t.Context()) }()
		synctest.Wait()
		if first.Err() != context.Canceled || second.Err() != context.Canceled {
			t.Fatal("process drain did not cancel both request contexts")
		}
		if _, finish, err := drain.begin(t.Context()); !errors.Is(err, context.Canceled) || finish != nil {
			t.Fatal("late callback acquired ownership during drain", err)
		}
		select {
		case err := <-joined:
			t.Fatal("drain returned before callback cleanup", err)
		default:
		}
		finishFirst()
		finishFirst() // idempotence must not retire the second callback
		synctest.Wait()
		select {
		case err := <-joined:
			t.Fatal("one callback completion retired both owners", err)
		default:
		}
		finishSecond()
		if err := <-joined; err != nil {
			t.Fatal("completed callbacks did not permit join", err)
		}
		cancelled, cancel := context.WithCancel(t.Context())
		cancel()
		if err := drain.close(cancelled); err != nil {
			t.Fatal("already completed join became a false timeout", err)
		}
	})
}

func TestWorkerTriggerDrainDeadlineDoesNotInventCompletion(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		drain := newWorkerTriggerDrain(t.Context())
		t.Cleanup(drain.cancel)
		request, finish, err := drain.begin(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(finish)
		bound, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if err := drain.close(bound); !errors.Is(err, errWorkerTriggerDrain) || request.Err() != context.Canceled {
			t.Fatal("bounded active drain claimed completion or missed cancellation", err)
		}
		select {
		case <-drain.done:
			t.Fatal("deadline retired an unfinished callback")
		default:
		}
		finish()
		if err := drain.close(t.Context()); err != nil {
			t.Fatal("actual later callback completion was not joinable", err)
		}
	})
}

func TestWorkerTriggerParentCancellationReachesRequest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		process, stop := context.WithCancel(t.Context())
		defer stop()
		drain := newWorkerTriggerDrain(process)
		t.Cleanup(drain.cancel)
		request, finish, err := drain.begin(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(finish)
		stop()
		synctest.Wait()
		if request.Err() != context.Canceled {
			t.Fatal("process cancellation was not linked to request")
		}
		if _, _, err := drain.begin(t.Context()); !errors.Is(err, context.Canceled) {
			t.Fatal("cancelled process admitted a fresh callback", err)
		}
		finish()
		if err := drain.close(t.Context()); err != nil {
			t.Fatal(err)
		}
	})
}

type workerStartupSignals struct{ failures int }

func (s *workerStartupSignals) Observe(_ context.Context, record opssignal.Record) error {
	if record.SignalID == opssignal.ControlQueueFailure && record.Stage == opssignal.Ingress {
		s.failures++
	}
	return nil
}

func TestWorkerLocalRedactionPreservesExpectedIngressOutcomes(t *testing.T) {
	for _, test := range []struct {
		name     string
		marker   error
		failures int
	}{
		{name: "retry", marker: ports.ErrQueueRetryRequested},
		{name: "redrive", marker: ports.ErrQueueRedrivePending},
		{name: "empty", marker: ports.ErrTriggerQueueEmpty},
		{name: "cancelled", marker: context.Canceled},
		{name: "membership", marker: domain.ErrMembershipDenied},
		{name: "quota", marker: &domain.ClassifiedError{Kind: domain.ErrorQuotaBlocked, Code: "private-marker", Operation: "private-marker"}},
		{name: "policy", marker: &domain.ClassifiedError{Kind: domain.ErrorPolicyDenied, Code: "private-marker"}},
		{name: "reauth", marker: &domain.ClassifiedError{Kind: domain.ErrorReauthRequired, Code: "private-marker"}},
		{name: "cleanup overrides retry", marker: errors.Join(ports.ErrQueueRetryRequested, worker.ErrLocalRuntime), failures: 1},
		{name: "unexpected", marker: errors.New("private-marker"), failures: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			private := errors.New("private-marker-cause")
			redacted := workerRuntimeError(true, "private-marker-stage", errors.Join(private, test.marker))
			if errors.Is(redacted, private) || strings.Contains(redacted.Error(), "private-marker") || !errors.Is(redacted, errWorkerLocalExecution) {
				t.Fatal("redaction retained private cause or lost local failure identity")
			}
			if test.failures != 0 && ports.IsQueueControlFlow(redacted) {
				t.Fatal("cleanup/unexpected failure became ordinary queue control flow")
			}
			var logs bytes.Buffer
			signals := &workerStartupSignals{}
			handler := serverlesshttp.NewHandler(slog.New(slog.NewTextHandler(&logs, nil)), func(context.Context, *http.Request) (any, error) {
				return nil, redacted
			}, signals)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/", nil))
			if response.Code != http.StatusServiceUnavailable || signals.failures != test.failures || strings.Contains(logs.String(), "private-marker") {
				t.Fatalf("redacted ingress status=%d failures=%d want=%d", response.Code, signals.failures, test.failures)
			}
		})
	}
}

func workerLocalTestEnvironment() map[string]string {
	return map[string]string{"SESSIONLESS_ENVIRONMENT": "local", "WORKER_LOCAL_EXECUTION_ENABLED": "true",
		"WEB_LOCAL_INSTALLATION_ROOT": "/private/operator/installation", "WEB_LOCAL_WORKER_EXECUTABLE": "/private/operator/worker-runtime", "WEB_LOCAL_PROFILE_RECEIPT_FILE": "/private/operator/receipt.json"}
}

// These are inert command denials, not successful boot/HTTP-batch evidence.
// Genuine receipt intake and fresh per-delivery Manager execution have their
// existing controlled Source/Store and connected Runtime tests respectively.
func TestWorkerRuntimeRejectsBeforeDependencies(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	for _, test := range []struct {
		name   string
		ctx    context.Context
		logger *slog.Logger
	}{
		{name: "nil context", logger: logger},
		{name: "cancelled", ctx: canceled, logger: logger},
		{name: "nil logger", ctx: t.Context()},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := run(test.ctx, test.logger); err != errWorkerLocalExecution {
				t.Fatal("invalid command dependencies reached startup", err)
			}
		})
	}
	// No Source/DB constructor can be reached after this malformed selector.
	t.Setenv("WORKER_LOCAL_EXECUTION_ENABLED", "private-canary-invalid")
	if err := run(t.Context(), logger); err != errWorkerLocalExecution || strings.Contains(err.Error(), "private-canary") {
		t.Fatal("invalid command selector escaped inert redacted denial", err)
	}
}

func TestWorkerEmptyQueueDoesNotHideCleanupFailure(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		local bool
		err   error
		empty bool
	}{
		{name: "default empty", err: sqsqueue.ErrNoMessage, empty: true},
		{name: "local empty", local: true, err: sqsqueue.ErrNoMessage, empty: true},
		{name: "local empty and cleanup failure", local: true, err: errors.Join(sqsqueue.ErrNoMessage, worker.ErrLocalRuntime)},
		{name: "local cleanup failure", local: true, err: worker.ErrLocalRuntime},
		{name: "cancelled", local: true, err: context.Canceled},
		{name: "nil", local: true},
		{name: "text is not sentinel", local: true, err: errors.New(sqsqueue.ErrNoMessage.Error())},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := workerQueueEmpty(test.local, test.err); got != test.empty {
				t.Fatalf("empty classification got=%t want=%t", got, test.empty)
			}
		})
	}
}

func TestWorkerLocalExecutionExplicitConfig(t *testing.T) {
	t.Parallel()
	for _, fault := range []string{"none", "cloud", "missing environment", "implicit", "flag alias", "padded flag", "missing root", "missing executable", "missing receipt", "relative root", "unclean executable", "unclean receipt", "root directory", "attached", "attached pins", "receipt alias"} {
		t.Run(fault, func(t *testing.T) {
			values := workerLocalTestEnvironment()
			switch fault {
			case "cloud":
				values["SESSIONLESS_ENVIRONMENT"] = "cloud-dev"
			case "missing environment":
				delete(values, "SESSIONLESS_ENVIRONMENT")
			case "implicit":
				delete(values, "WORKER_LOCAL_EXECUTION_ENABLED")
			case "flag alias":
				values["WORKER_LOCAL_EXECUTION_ENABLED"] = "TRUE"
			case "padded flag":
				values["WORKER_LOCAL_EXECUTION_ENABLED"] = "true "
			case "missing root":
				delete(values, "WEB_LOCAL_INSTALLATION_ROOT")
			case "missing executable":
				delete(values, "WEB_LOCAL_WORKER_EXECUTABLE")
			case "missing receipt":
				delete(values, "WEB_LOCAL_PROFILE_RECEIPT_FILE")
			case "relative root":
				values["WEB_LOCAL_INSTALLATION_ROOT"] = "private-canary"
			case "unclean executable":
				values["WEB_LOCAL_WORKER_EXECUTABLE"] = "/private/operator/../worker"
			case "unclean receipt":
				values["WEB_LOCAL_PROFILE_RECEIPT_FILE"] = "/private//operator/receipt.json"
			case "root directory":
				values["WEB_LOCAL_INSTALLATION_ROOT"] = "/"
			case "attached":
				values["WEB_ATTACHED_EXECUTION_ENABLED"] = "true"
			case "attached pins":
				values["WEB_ATTACHED_RESOURCE_PINS"] = "private-canary"
			case "receipt alias":
				delete(values, "WEB_LOCAL_PROFILE_RECEIPT_FILE")
				values["WORKER_LOCAL_PROFILE_RECEIPT_FILE"] = "/private/operator/receipt.json"
			}
			got, err := workerLocalExecutionFromEnv(func(k string) string { return values[k] })
			if fault == "none" {
				want := workerLocalExecutionConfig{enabled: true, installation: values["WEB_LOCAL_INSTALLATION_ROOT"], executable: values["WEB_LOCAL_WORKER_EXECUTABLE"], receipt: values["WEB_LOCAL_PROFILE_RECEIPT_FILE"]}
				if err != nil || got != want {
					t.Fatalf("explicit worker selection: %+v err=%v", got, err)
				}
			} else if !errors.Is(err, errWorkerLocalExecution) || !reflect.DeepEqual(got, workerLocalExecutionConfig{}) || strings.Contains(err.Error(), "private-canary") {
				t.Fatalf("%s accepted/leaked invalid local selection: err=%v", fault, err)
			}
		})
	}
}

func TestWorkerLocalExecutionDefaultOffAndStrayPaths(t *testing.T) {
	t.Parallel()
	for _, flag := range []string{"", "false"} {
		for _, stray := range []string{"", "WEB_LOCAL_INSTALLATION_ROOT", "WEB_LOCAL_WORKER_EXECUTABLE", "WEB_LOCAL_PROFILE_RECEIPT_FILE"} {
			t.Run(flag+"/"+stray, func(t *testing.T) {
				c, err := workerLocalExecutionFromEnv(func(k string) string {
					if k == "WORKER_LOCAL_EXECUTION_ENABLED" {
						return flag
					}
					if stray != "" && k == stray {
						return "/private/canary"
					}
					return ""
				})
				if stray != "" {
					if !errors.Is(err, errWorkerLocalExecution) || !reflect.DeepEqual(c, workerLocalExecutionConfig{}) {
						t.Fatal("disabled worker silently consumed local paths")
					}
					return
				}
				if err != nil || c.enabled {
					t.Fatal("default worker selected local runtime")
				}
				if e, err := openWorkerLocalExecution(nil, c); err != nil || e != nil {
					t.Fatal("disabled worker touched installation intake")
				}
			})
		}
	}
	if _, err := workerLocalExecutionFromEnv(nil); !errors.Is(err, errWorkerLocalExecution) {
		t.Fatal("nil configuration source accepted")
	}
}

func TestWorkerLocalExecutionStartupFailClosed(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil || os.Chmod(root, 0700) != nil {
		t.Fatal("owned private directory setup failed")
	}
	path := filepath.Join(root, "receipt.json")
	if err := os.WriteFile(path, []byte(`{"private-canary":"not a receipt"}`), 0600); err != nil {
		t.Fatal(err)
	}
	c := workerLocalExecutionConfig{enabled: true, installation: root, executable: filepath.Join(root, "absent-executable"), receipt: path}
	for _, mode := range []string{"malformed receipt", "nil context", "cancelled context"} {
		t.Run(mode, func(t *testing.T) {
			ctx := t.Context()
			if mode == "nil context" {
				ctx = nil
			} else if mode == "cancelled context" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			got, err := openWorkerLocalExecution(ctx, c)
			if got != nil || !errors.Is(err, errWorkerLocalExecution) || strings.Contains(err.Error(), "private-canary") || strings.Contains(err.Error(), root) {
				t.Fatalf("invalid installation fell back or leaked: err=%v", err)
			}
			files, err := os.ReadDir(root)
			if err != nil || len(files) != 1 || files[0].Name() != "receipt.json" {
				t.Fatal("worker startup created missing installation/private files")
			}
		})
	}
}

func TestWorkerLocalExecutionOriginalPointerOptionsAndRedaction(t *testing.T) {
	t.Parallel()
	// Wiring DATA only: this uninitialized Source cannot pass real startup or
	// grant any current authority. Genuine LocalRuntime/Manager tests are separate.
	source := &localoperator.Source{}
	e := &workerLocalExecution{source: source}
	options := e.storeOptions()
	if options.OwnedProfilePublicationVerifier != source || options.AttemptSpendingPublicationVerifier != source || options.OwnedAttemptEffectSourceVerifier != source {
		t.Fatal("worker startup substituted original Source pointers")
	}
	if !errors.Is(e.close(), localoperator.ErrDenied) || !errors.Is(e.close(), localoperator.ErrDenied) {
		t.Fatal("failed Source closure was hidden or changed on repeat")
	}
	var disabled *workerLocalExecution
	options = disabled.storeOptions()
	if options.OwnedProfilePublicationVerifier != nil || options.AttemptSpendingPublicationVerifier != nil || options.OwnedAttemptEffectSourceVerifier != nil {
		t.Fatal("disabled worker installed Source verifiers")
	}
	disabled.close()
	cause := errors.New("private-canary-database-connection")
	if err := workerRuntimeError(true, "private-canary-stage", cause); err != errWorkerLocalExecution || strings.Contains(err.Error(), "private-canary") || errors.Is(err, cause) {
		t.Fatal("local runtime error retained private cause or stage")
	}
	if err := workerRuntimeError(false, "ordinary worker", cause); !errors.Is(err, cause) {
		t.Fatal("default runtime lost its original diagnostic cause")
	}
}
