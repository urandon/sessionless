package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedresourcegeneration"
	"gitcode.com/urandon/sessionless/internal/attachedworker"
	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
	"gitcode.com/urandon/sessionless/internal/attachedworkerux"
	"gitcode.com/urandon/sessionless/internal/buildinfo"
	"gitcode.com/urandon/sessionless/internal/idgen"
	"gitcode.com/urandon/sessionless/internal/localcontrolplane"
	"gitcode.com/urandon/sessionless/internal/opssignal"
	"gitcode.com/urandon/sessionless/internal/outboxwake"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/s3store"
	"gitcode.com/urandon/sessionless/internal/sessionapi"
	"gitcode.com/urandon/sessionless/internal/sessioningress"
	"gitcode.com/urandon/sessionless/internal/sqsqueue"
	"gitcode.com/urandon/sessionless/internal/webapi"
	"gitcode.com/urandon/sessionless/internal/webbff"
	"gitcode.com/urandon/sessionless/internal/webstatic"
	"gitcode.com/urandon/sessionless/internal/ydbclient"
)

const component = "web-bff"

const defaultWebBlobLimit = int64(64 << 20)

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	handler, closeDependencies, err := buildHandler(ctx, logger)
	if err != nil {
		logger.Error("web BFF configuration failed", "component", component, "error", err)
		os.Exit(1)
	}
	defer closeDependencies()

	server := &http.Server{
		Addr:              webListenAddress(),
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Error("web BFF graceful shutdown failed", "component", component, "error", err)
		}
	}()
	logger.Info("starting web BFF", "component", component, "address", server.Addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("web BFF stopped", "component", component, "error", err)
		os.Exit(1)
	}
}

func webListenAddress() string {
	return ":" + firstNonEmpty(os.Getenv("PORT"), os.Getenv("WEB_PORT"), "8083")
}

func buildHandler(ctx context.Context, logger *slog.Logger) (http.Handler, func(), error) {
	// One bounded content-free carrier, independent of legacy correlation logs.
	signals := opssignal.NewWriter(os.Stdout)
	baseURL := os.Getenv("WEB_BASE_URL")
	environment := envOrDefault("SESSIONLESS_ENVIRONMENT", "cloud-dev")
	allowLocal := environment == "local"
	login, err := webLoginFromEnvironment(os.LookupEnv, baseURL, allowLocal)
	if err != nil {
		return nil, func() {}, err
	}
	identityKey := []byte(os.Getenv("SESSION_API_ID_HMAC_KEY"))
	identity, err := webIdentityFromEnvironment(os.LookupEnv, login, baseURL, identityKey)
	if err != nil {
		return nil, func() {}, err
	}
	// Install one immutable operator registry before opening dependencies; the
	// same pointer must reach both preflight binding and allocating Store.
	generations, err := attachedresourcegeneration.LoadRegistryFromEnv(os.Getenv)
	if err != nil {
		return nil, func() {}, err
	}
	localConfig, err := localcontrolplane.ConfigFromEnv(os.Getenv)
	if err != nil || (localConfig.Enabled && generations != nil) {
		return nil, func() {}, localcontrolplane.ErrUnavailable
	}
	local, err := localcontrolplane.Open(ctx, localConfig)
	if err != nil {
		return nil, func() {}, err
	}
	ydb, err := ydbclient.Open(ctx, os.Getenv("YDB_CONNECTION_STRING"))
	if err != nil {
		_ = local.Close()
		return nil, func() {}, fmt.Errorf("open YDB: %w", err)
	}
	closeYDB := func() {
		_ = local.Close()
		closeCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := ydb.Close(closeCtx); err != nil {
			logger.Error("close web BFF YDB", "component", component, "error", err)
		}
	}
	maxUploadBytes, err := envPositiveInt64("WEB_MAX_UPLOAD_BYTES", 32<<20)
	if err != nil {
		closeYDB()
		return nil, func() {}, err
	}
	maxBlobBytes := defaultWebBlobLimit
	if maxUploadBytes > maxBlobBytes {
		maxBlobBytes = maxUploadBytes
	}
	blobs, err := s3store.New(ctx, s3store.Config{
		Endpoint: os.Getenv("S3_ENDPOINT"), Region: os.Getenv("S3_REGION"),
		Bucket: os.Getenv("S3_BUCKET"), AccessKeyID: os.Getenv("S3_ACCESS_KEY_ID"),
		SecretAccessKey: os.Getenv("S3_SECRET_ACCESS_KEY"), ForcePathStyle: envBool("S3_FORCE_PATH_STYLE"),
		IAMMetadataCredentials: envBool("S3_IAM_METADATA_CREDENTIALS"),
		MaxObjectBytes:         maxBlobBytes,
	})
	if err != nil {
		closeYDB()
		return nil, func() {}, fmt.Errorf("open web BFF object storage: %w", err)
	}
	storeOptions, err := local.WebStoreOptions(blobs)
	if err != nil {
		closeYDB()
		return nil, func() {}, err
	}
	storeOptions.AttachedResourceGenerations = generations
	store, identityAccounts, err := identity.newStore(ydb.DB, storeOptions, identityKey)
	if err != nil {
		closeYDB()
		return nil, func() {}, err
	}
	if err := store.RequireExecutionPlacementCutover(ctx); err != nil {
		closeYDB()
		return nil, func() {}, fmt.Errorf("require execution placement cutover: %w", err)
	}
	if err := store.RequireHarnessBindingCutover(ctx); err != nil {
		closeYDB()
		return nil, func() {}, fmt.Errorf("require harness binding cutover: %w", err)
	}
	if err := store.RequireManagedExecutionAuthorityV2Cutover(ctx); err != nil {
		closeYDB()
		return nil, func() {}, fmt.Errorf("require managed execution authority v2 cutover: %w", err)
	}
	if err := store.RequireAdmissionCostV2Cutover(ctx); err != nil {
		closeYDB()
		return nil, func() {}, fmt.Errorf("require admission cost v2 cutover: %w", err)
	}
	runExplanations, err := runExplanationsFromEnvironment(ctx, os.Getenv, store)
	if err != nil {
		closeYDB()
		return nil, func() {}, fmt.Errorf("require run explanation cutover: %w", err)
	}
	sessions, err := sessionapi.New(sessionapi.Config{
		OpsSignals: signals,
		CursorKey:  []byte(os.Getenv("SESSION_API_CURSOR_HMAC_KEY")),
		IDKey:      identityKey,
	}, store, blobs, systemClock{})
	if err != nil {
		closeYDB()
		return nil, func() {}, err
	}
	dispatchWakeQueue, err := sqsqueue.New(ctx, sqsqueue.Config{
		Endpoint: os.Getenv("QUEUE_ENDPOINT"), Region: envOrDefault("QUEUE_REGION", "ru-central1"),
		QueueURL: os.Getenv("SCHEDULER_WAKE_QUEUE_URL"),
		AccessKeyID: firstNonEmpty(
			os.Getenv("OUTBOX_QUEUE_ACCESS_KEY_ID"), os.Getenv("QUEUE_ACCESS_KEY_ID"),
		),
		SecretAccessKey: firstNonEmpty(
			os.Getenv("OUTBOX_QUEUE_SECRET_ACCESS_KEY"), os.Getenv("QUEUE_SECRET_ACCESS_KEY"),
		),
	})
	if err != nil {
		closeYDB()
		return nil, func() {}, fmt.Errorf("open web scheduler wake queue: %w", err)
	}
	dispatchWakePublisher, err := outboxwake.NewPublisher(dispatchWakeQueue, signals)
	if err != nil {
		closeYDB()
		return nil, func() {}, err
	}
	var harnessBinder ports.HarnessBinder
	var resources ports.WebResourceStore = store
	if local != nil {
		harnessBinder, err = local.Binder(store)
		if err == nil {
			resources, err = local.Resources(store)
		}
	} else {
		harnessBinder, err = webHarnessBinderFromEnv(os.Getenv, store, generations)
	}
	if err != nil {
		closeYDB()
		return nil, func() {}, err
	}
	canonicalIngress, err := sessioningress.New(sessioningress.Config{
		IDKey: identityKey, HarnessBinder: harnessBinder, DispatchWakePublisher: dispatchWakePublisher,
		WakePublishError: func(publishErr error) {
			logger.Warn("durable web dispatch wake publication deferred to recovery", "error", publishErr)
		},
	}, store, blobs)
	if err != nil {
		closeYDB()
		return nil, func() {}, err
	}
	api, err := webapi.New(webapi.Config{
		OpsSignals:        signals,
		IDKey:             identityKey,
		MaxUploadBytes:    maxUploadBytes,
		AllowedMCPServers: envCSV("WEB_ALLOWED_MCP_SERVERS"),
	}, sessions, canonicalIngress, store, resources, blobs, systemClock{})
	if err != nil {
		closeYDB()
		return nil, func() {}, err
	}
	ids := idgen.New()
	attachedWorkers, err := attachedworkerux.NewService(store, time.Now)
	if err != nil {
		closeYDB()
		return nil, func() {}, err
	}
	workerLifecycle, err := attachedworker.New(attachedworker.Config{
		Clock: systemClock{}, IDs: ids, MaxEnrollmentTTL: 15 * time.Minute, EnrollmentRetention: 24 * time.Hour,
	}, store)
	if err != nil {
		closeYDB()
		return nil, func() {}, err
	}
	drainService, err := attachedworkertransport.NewDrainService(store)
	if err != nil {
		closeYDB()
		return nil, func() {}, err
	}
	attachedWorkerControls, err := attachedworkerux.NewControlService(attachedworkerux.ControlConfig{
		Clock: systemClock{}, IDs: ids,
	}, store, store, drainService, workerLifecycle)
	if err != nil {
		closeYDB()
		return nil, func() {}, err
	}
	var searchConsents webbff.WebSearchConsentService
	if local != nil {
		searchConsents = store
	}
	backend, err := webbff.New(webbff.Config{
		OpsSignals: signals,
		BaseURL:    baseURL, LoginProviders: login.providers, IdentityAccounts: identityAccounts,
		ObjectStorageOrigin:        os.Getenv("WEB_OBJECT_STORAGE_ORIGIN"),
		AllowLoopbackObjectStorage: allowLocal,
		Store:                      store, Sessions: sessions, API: api, RunExplanations: runExplanations, AttachedWorkers: attachedWorkers,
		AttachedWorkerControls: attachedWorkerControls, IDs: ids, Clock: systemClock{},
		SearchConsents: searchConsents,
		Logger:         logger, Build: buildinfo.Current(component),
	})
	if err != nil {
		closeYDB()
		return nil, func() {}, err
	}
	handler, err := webstatic.New(webstatic.Config{
		Backend:                    backend,
		ObjectStorageOrigin:        os.Getenv("WEB_OBJECT_STORAGE_ORIGIN"),
		AllowLoopbackObjectStorage: allowLocal,
	})
	if err != nil {
		closeYDB()
		return nil, func() {}, err
	}
	return handler, closeYDB, nil
}

func envBool(name string) bool {
	value, _ := strconv.ParseBool(os.Getenv(name))
	return value
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func envPositiveInt64(name string, fallback int64) (int64, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return parsed, nil
}

func envCSV(name string) []string {
	var result []string
	for _, value := range strings.Split(os.Getenv(name), ",") {
		if value = strings.TrimSpace(value); value != "" {
			result = append(result, value)
		}
	}
	return result
}
