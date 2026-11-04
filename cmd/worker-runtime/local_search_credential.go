package main

import (
	"context"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/idgen"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/providercredential"
	"gitcode.com/urandon/sessionless/internal/s3store"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

// Process-owned constructor result, not another invocation/key lifecycle.
// The original InvocationService owns handles; the encrypted adapter owns its
// copied root key and joins actual backend consumers before erasing that copy.
type workerLocalSearchCredentials struct {
	service *providercredential.InvocationService
	secrets *s3store.ProviderCredentialObjectStore
}

// The no-auth local MODEL has no credential delivery template. SEARCH uses
// the same service's distinct purpose-bound issue arm, never this planner.
type localSearchClosedModelPlanner struct{}

func (localSearchClosedModelPlanner) PlanProviderCredentialDelivery(context.Context, domain.HarnessBindingV2) (ports.ProviderCredentialDeliveryTemplateV1, error) {
	return ports.ProviderCredentialDeliveryTemplateV1{}, errWorkerLocalExecution
}

func newWorkerLocalSearchCredentials(ctx context.Context, config workerLocalExecutionConfig, installation *workerLocalExecution, bindings *ydbstore.Store, objects *s3store.Store) (*workerLocalSearchCredentials, error) {
	if installation == nil {
		if config.enabled || config.searchKeyFile != "" || config.searchKeyEpoch != "" {
			return nil, errWorkerLocalExecution
		}
		return nil, nil
	}
	if ctx == nil || ctx.Err() != nil || !config.enabled || installation.source == nil || !config.searchKeySelectionValid() {
		return nil, errWorkerLocalExecution
	}
	declaration, err := installation.source.SearchCredentialConfiguration()
	if err != nil {
		return nil, errWorkerLocalExecution
	}
	if declaration == nil {
		if config.searchKeyFile != "" || config.searchKeyEpoch != "" {
			return nil, errWorkerLocalExecution
		}
		return nil, nil // authenticated omission; no key-file or backend I/O
	}
	if bindings == nil || objects == nil || config.searchKeyFile == "" || s3store.ValidateProviderCredentialKeyEpoch(config.searchKeyEpoch) != nil {
		return nil, errWorkerLocalExecution
	}
	key, err := readLocalSearchKey(ctx, config.searchKeyFile)
	if err != nil {
		return nil, errWorkerLocalExecution
	}
	return assembleWorkerLocalSearchCredentials(ctx, config.searchKeyEpoch, bindings, objects, key)
}

// Taking the input slice transfers only clearing responsibility, not ownership
// of the caller's allocation. Every constructor outcome clears that allocation.
func assembleWorkerLocalSearchCredentials(ctx context.Context, epoch string, bindings *ydbstore.Store, objects *s3store.Store, key []byte) (*workerLocalSearchCredentials, error) {
	defer clear(key)
	if ctx == nil || ctx.Err() != nil || bindings == nil || objects == nil || s3store.ValidateProviderCredentialKeyEpoch(epoch) != nil {
		return nil, errWorkerLocalExecution
	}
	secrets, err := s3store.NewProviderCredentialSecretStore(objects, s3store.ProviderCredentialObjectConfig{KeyEpoch: epoch, Clock: systemClock{}}, bindings, key)
	if err != nil {
		return nil, errWorkerLocalExecution
	}
	service, err := providercredential.NewInvocationService(providercredential.InvocationConfig{Clock: systemClock{}, IDs: idgen.New()}, localSearchClosedModelPlanner{}, bindings, secrets)
	if err != nil {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), workerDependencyCloseTimeout)
		defer cancel()
		_ = secrets.Close(closeCtx)
		return nil, errWorkerLocalExecution
	}
	return &workerLocalSearchCredentials{service: service, secrets: secrets}, nil
}

func (s *workerLocalSearchCredentials) close(ctx context.Context) error {
	if s == nil {
		return nil
	}
	if s.secrets == nil || s.secrets.Close(ctx) != nil {
		return errWorkerLocalExecution
	}
	return nil
}

type workerSearchProcessCloser interface{ close(context.Context) error }
type workerSourceProcessCloser interface{ close() error }
type workerDatabaseProcessCloser interface{ Close(context.Context) error }

// A fixed process teardown sequence, after the existing callback drain. There
// is no retry, renewed invocation deadline or success after a failed join.
func closeWorkerProcessDependencies(ctx context.Context, search workerSearchProcessCloser, source workerSourceProcessCloser, database workerDatabaseProcessCloser) error {
	if search != nil {
		if err := search.close(ctx); err != nil {
			return err
		}
	}
	if source != nil {
		if err := source.close(); err != nil {
			return err
		}
	}
	if database != nil {
		return database.Close(ctx)
	}
	return nil
}
