package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"gitcode.com/urandon/sessionless/internal/deterministicharness"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/opssignal"
	"gitcode.com/urandon/sessionless/internal/outboxwake"
	"gitcode.com/urandon/sessionless/internal/portlog"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/s3store"
	"gitcode.com/urandon/sessionless/internal/serverlessharness"
	"gitcode.com/urandon/sessionless/internal/serverlesshttp"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
	"gitcode.com/urandon/sessionless/internal/sqsqueue"
	"gitcode.com/urandon/sessionless/internal/worker"
	"gitcode.com/urandon/sessionless/internal/yandextriggers"
	"gitcode.com/urandon/sessionless/internal/ydbclient"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	err := run(ctx, logger)
	stop()
	if err != nil {
		logger.Error("worker invocation failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, logger *slog.Logger) (resultErr error) {
	if ctx == nil || ctx.Err() != nil || logger == nil {
		return errWorkerLocalExecution
	}
	localConfig, err := workerLocalExecutionFromEnv(os.Getenv)
	if err != nil {
		return errWorkerLocalExecution
	}
	signals := opssignal.NewWriter(os.Stdout)
	var localExecution *workerLocalExecution
	var ydb *ydbclient.Client
	var invocations *workerTriggerDrain
	// Registered before acquisition. Invocation cleanup happens inside RunOnce;
	// installation closure precedes the borrowed database's process-owned close.
	defer func() {
		drainCtx, stopDrain := context.WithTimeout(context.Background(), 30*time.Second)
		drainErr := invocations.close(drainCtx)
		stopDrain()
		if drainErr != nil {
			// Do not close borrowed dependencies under a still-active callback.
			// main exits with failure; this is not successful join/retirement proof.
			resultErr = errors.Join(resultErr, drainErr)
		} else {
			if err := localExecution.close(); err != nil {
				resultErr = errors.Join(resultErr, err)
			}
			if ydb != nil {
				closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := ydb.Close(closeCtx); err != nil {
					resultErr = errors.Join(resultErr, err)
				}
			}
		}
		if resultErr != nil && localConfig.enabled {
			resultErr = errWorkerLocalExecution
		}
	}()
	localExecution, err = openWorkerLocalExecution(ctx, localConfig)
	if err != nil {
		return errWorkerLocalExecution
	}
	ydb, err = ydbclient.Open(ctx, os.Getenv("YDB_CONNECTION_STRING"))
	if err != nil {
		return workerRuntimeError(localConfig.enabled, "open YDB", err)
	}
	deliveryWakeQueue, err := sqsqueue.New(ctx, sqsqueue.Config{
		Endpoint: os.Getenv("QUEUE_ENDPOINT"), Region: envOrDefault("QUEUE_REGION", "ru-central1"),
		QueueURL:        os.Getenv("DELIVERY_QUEUE_URL"),
		AccessKeyID:     firstNonEmpty(os.Getenv("OUTBOX_QUEUE_ACCESS_KEY_ID"), os.Getenv("QUEUE_ACCESS_KEY_ID")),
		SecretAccessKey: firstNonEmpty(os.Getenv("OUTBOX_QUEUE_SECRET_ACCESS_KEY"), os.Getenv("QUEUE_SECRET_ACCESS_KEY")),
	})
	if err != nil {
		return workerRuntimeError(localConfig.enabled, "create delivery wake queue", err)
	}
	deliveryWakePublisher, err := outboxwake.NewPublisher(deliveryWakeQueue, signals)
	if err != nil {
		return workerRuntimeError(localConfig.enabled, "create delivery wake publisher", err)
	}
	effectIssuer, err := serverlessharness.NewCapabilityIssuer(time.Now, nil)
	if err != nil {
		return workerRuntimeError(localConfig.enabled, "create provider effect grant issuer", err)
	}
	storeOptions := localExecution.storeOptions()
	storeOptions.AttemptEffectGrantIssuer = effectIssuer
	state, err := ydbstore.New(ydb.DB, storeOptions)
	if err != nil {
		return workerRuntimeError(localConfig.enabled, "create worker state store", err)
	}
	if err := state.RequireExecutionPlacementCutover(ctx); err != nil {
		return workerRuntimeError(localConfig.enabled, "require execution placement cutover", err)
	}
	if err := state.RequireHarnessBindingCutover(ctx); err != nil {
		return workerRuntimeError(localConfig.enabled, "require harness binding cutover", err)
	}
	if err := state.RequireManagedExecutionAuthorityV2Cutover(ctx); err != nil {
		return workerRuntimeError(localConfig.enabled, "require managed execution authority v2 cutover", err)
	}
	if err := state.RequireAdmissionCostV2Cutover(ctx); err != nil {
		return workerRuntimeError(localConfig.enabled, "require admission cost v2 cutover", err)
	}
	blobs, err := s3store.New(ctx, s3store.Config{
		Endpoint: os.Getenv("S3_ENDPOINT"), Region: envOrDefault("S3_REGION", "ru-central1"),
		Bucket: os.Getenv("S3_BUCKET"), AccessKeyID: os.Getenv("S3_ACCESS_KEY_ID"),
		SecretAccessKey:        os.Getenv("S3_SECRET_ACCESS_KEY"),
		ForcePathStyle:         envBool("S3_FORCE_PATH_STYLE"),
		IAMMetadataCredentials: envBool("S3_IAM_METADATA_CREDENTIALS"),
		MaxObjectBytes:         int64(envUint64("WORKER_MAX_BLOB_BYTES", 64<<20)),
	})
	if err != nil {
		return workerRuntimeError(localConfig.enabled, "create worker blob store", err)
	}
	var harnessRegistry *sessionlessharness.Registry
	var executionPreparer *serverlessharness.ExactExecutionPreparerV1
	if !localConfig.enabled {
		harness, err := deterministicharness.New(deterministicharness.Config{
			Turns:                 envUint64("DETERMINISTIC_HARNESS_TURNS", 2),
			Artifacts:             envUint64("DETERMINISTIC_HARNESS_ARTIFACTS", 1),
			FailBeforeFirstTurn:   envBool("DETERMINISTIC_HARNESS_FAIL_BEFORE_FIRST_TURN"),
			FailAtTurn:            envUint64("DETERMINISTIC_HARNESS_FAIL_AT_TURN", 0),
			RetryableFail:         envBool("DETERMINISTIC_HARNESS_RETRYABLE_FAIL"),
			CaptureContextHistory: envBool("DETERMINISTIC_HARNESS_CAPTURE_CONTEXT_HISTORY"),
		})
		if err != nil {
			return workerRuntimeError(localConfig.enabled, "create deterministic harness", err)
		}
		harnessRegistry, err = sessionlessharness.NewRegistry(time.Now, sessionlessharness.Registration{
			Descriptor:      sessionlessharness.DeterministicFixtureDescriptorV1(),
			Enabled:         true,
			ValidateBinding: sessionlessharness.ValidateDeterministicFixtureBindingV1,
			Driver:          harness,
		})
		if err != nil {
			return workerRuntimeError(localConfig.enabled, "create Sessionless harness registry", err)
		}
		inProcessSubstrate, err := serverlessharness.NewInProcessExecutionSubstrateV1(
			time.Now, effectIssuer, sessionlessharness.DeterministicFixtureDescriptorV1(),
		)
		if err != nil {
			return workerRuntimeError(localConfig.enabled, "create deterministic execution substrate", err)
		}
		executionPreparer, err = serverlessharness.NewExactExecutionPreparerV1(
			time.Now,
			effectIssuer,
			func(authority domain.ServerlessInvocationAuthorityV1) (serverlessharness.SubstrateRegistrationV1, error) {
				if err := sessionlessharness.ValidateDeterministicFixtureInvocationAuthorityV2(authority); err != nil {
					return serverlessharness.SubstrateRegistrationV1{}, err
				}
				return serverlessharness.SubstrateRegistrationV1{
					Binding: authority.SubstrateBinding, Enabled: true, Driver: inProcessSubstrate,
				}, nil
			},
		)
		if err != nil {
			return workerRuntimeError(localConfig.enabled, "create exact execution preparer", err)
		}
	}
	managerConfig := worker.Config{
		OpsSignals:            signals,
		ScratchRoot:           envOrDefault("WORKER_SCRATCH_ROOT", "/tmp/sessionless-worker"),
		WorkerID:              envOrDefault("WORKER_ID", defaultWorkerID()),
		LeaseTTL:              envDuration("WORKER_LEASE_TTL", 2*time.Minute),
		LeaseWatchdogInterval: envDuration("WORKER_LEASE_WATCHDOG_INTERVAL", 0),
		RetryDelay:            envDuration("WORKER_RETRY_DELAY", 5*time.Second),
		RetryObserver: func(cause error) {
			if localConfig.enabled {
				cause = errWorkerLocalExecution
			}
			logger.Warn("worker invocation scheduled for retry", "error", cause)
		},
		MaxDeliveryCount:      uint32(envUint64("WORKER_MAX_DELIVERY_COUNT", 5)),
		MaxMaterializedBytes:  int64(envUint64("WORKER_MAX_BLOB_BYTES", 64<<20)),
		MaxSnapshotFallbacks:  uint32(envUint64("WORKER_MAX_SNAPSHOT_FALLBACKS", 4)),
		DeliveryWakePublisher: deliveryWakePublisher,
		ExecutionPreparer:     executionPreparer,
	}
	var localRuntime *worker.LocalRuntime
	if localExecution != nil {
		localRuntime, err = worker.NewLocalRuntime(worker.LocalRuntimeConfig{
			Source: localExecution.source, Receipt: localExecution.receipt,
			DB: ydb.DB, Blobs: blobs, Worker: managerConfig,
		}, systemClock{})
		if err != nil {
			return errWorkerLocalExecution
		}
	}
	newManager := func(queue ports.Queue) (*worker.Manager, error) {
		return worker.New(
			managerConfig, systemClock{}, portlog.NewQueue(logger, "worker-runtime", queue, signals),
			state, blobs, harnessRegistry,
		)
	}
	if envBool("SERVERLESS_TRIGGER_HTTP") {
		invocations = newWorkerTriggerDrain(ctx)
		err = serverlesshttp.Serve(
			ctx, ":"+envOrDefault("PORT", "8080"), logger,
			func(invocationCtx context.Context, request *http.Request) (any, error) {
				invocationCtx, finish, beginErr := invocations.begin(invocationCtx)
				if beginErr != nil {
					return nil, workerRuntimeError(localConfig.enabled, "worker trigger draining", beginErr)
				}
				defer finish()
				queue, parseErr := yandextriggers.NewYMQQueue(request.Body, 10)
				if parseErr != nil {
					return nil, workerRuntimeError(localConfig.enabled, "parse worker trigger", parseErr)
				}
				var manager *worker.Manager
				if localRuntime == nil {
					var managerErr error
					manager, managerErr = newManager(queue)
					if managerErr != nil {
						return nil, managerErr
					}
				}
				outcomes := make([]worker.Outcome, 0, queue.Remaining())
				for queue.Remaining() > 0 {
					var outcome worker.Outcome
					var runErr error
					if localRuntime != nil {
						outcome, runErr = localRuntime.RunOnce(invocationCtx, portlog.NewQueue(logger, "worker-runtime", queue, signals))
					} else {
						outcome, runErr = manager.RunOnce(invocationCtx)
					}
					if runErr != nil {
						return nil, workerRuntimeError(localConfig.enabled, "worker trigger invocation", runErr)
					}
					outcomes = append(outcomes, outcome)
				}
				return map[string]any{"processed": len(outcomes), "outcomes": outcomes}, nil
			}, signals,
		)
		if err != nil {
			return workerRuntimeError(localConfig.enabled, "worker trigger server stopped", err)
		}
		return nil
	}

	queue, err := sqsqueue.New(ctx, sqsqueue.Config{
		Endpoint: os.Getenv("QUEUE_ENDPOINT"), Region: envOrDefault("QUEUE_REGION", "ru-central1"),
		QueueURL: os.Getenv("DISPATCH_QUEUE_URL"), DeadLetterURL: os.Getenv("DEAD_LETTER_QUEUE_URL"),
		AccessKeyID: os.Getenv("QUEUE_ACCESS_KEY_ID"), SecretAccessKey: os.Getenv("QUEUE_SECRET_ACCESS_KEY"),
		WaitTime: envDuration("WORKER_QUEUE_WAIT", 2*time.Second),
	})
	if err != nil {
		return workerRuntimeError(localConfig.enabled, "create worker queue", err)
	}
	var outcome worker.Outcome
	if localRuntime != nil {
		outcome, err = localRuntime.RunOnce(ctx, portlog.NewQueue(logger, "worker-runtime", queue, signals))
	} else {
		manager, managerErr := newManager(queue)
		if managerErr != nil {
			return workerRuntimeError(false, "create worker manager", managerErr)
		}
		outcome, err = manager.RunOnce(ctx)
	}
	if workerQueueEmpty(localRuntime != nil, err) {
		logger.Info("worker queue empty")
		return nil
	}
	if err != nil {
		return workerRuntimeError(localConfig.enabled, "worker invocation failed", err)
	}
	logger.Info("worker invocation finished", "outcome", outcome)
	return nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func envDuration(name string, fallback time.Duration) time.Duration {
	value, err := time.ParseDuration(os.Getenv(name))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}

func envUint64(name string, fallback uint64) uint64 {
	value, err := strconv.ParseUint(os.Getenv(name), 10, 64)
	if err != nil {
		return fallback
	}
	return value
}

func envBool(name string) bool {
	value, _ := strconv.ParseBool(os.Getenv(name))
	return value
}

func defaultWorkerID() string {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		return "worker-runtime"
	}
	return "worker-runtime-" + hostname
}
