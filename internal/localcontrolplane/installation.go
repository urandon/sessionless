// Package localcontrolplane borrows an existing operator installation for the
// opt-in local control plane. It does not install profiles or own an invocation.
package localcontrolplane

import (
	"context"
	"errors"
	"path/filepath"
	"sync"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localoperator"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

var ErrUnavailable = errors.New("owner-local control plane unavailable")

type Config struct {
	Enabled                           bool
	Installation, Executable, Receipt string
}

// ConfigFromEnv accepts only a trusted local deployment selection. Paths never
// come from a browser, queue or model; disabled mode rejects stray local config.
func ConfigFromEnv(getenv func(string) string) (Config, error) {
	if getenv == nil {
		return Config{}, ErrUnavailable
	}
	c := Config{Installation: getenv("WEB_LOCAL_INSTALLATION_ROOT"), Executable: getenv("WEB_LOCAL_WORKER_EXECUTABLE"), Receipt: getenv("WEB_LOCAL_PROFILE_RECEIPT_FILE")}
	switch getenv("WEB_LOCAL_EXECUTION_ENABLED") {
	case "", "false":
		if c.Installation != "" || c.Executable != "" || c.Receipt != "" {
			return Config{}, ErrUnavailable
		}
		return c, nil
	case "true":
		c.Enabled = true
	default:
		return Config{}, ErrUnavailable
	}
	attached := getenv("WEB_ATTACHED_EXECUTION_ENABLED")
	if getenv("SESSIONLESS_ENVIRONMENT") != "local" || (attached != "" && attached != "false") || getenv("WEB_ATTACHED_RESOURCE_PINS") != "" {
		return Config{}, ErrUnavailable
	}
	for _, path := range []string{c.Installation, c.Executable, c.Receipt} {
		if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path || path == string(filepath.Separator) {
			return Config{}, ErrUnavailable
		}
	}
	return c, nil
}

type Installation struct {
	source   *localoperator.Source
	receipt  ports.OwnedProfilePublicationReceiptV1
	once     sync.Once
	closeErr error
}

// Open only reopens the original private Source and historical receipt. There
// is no OpenFresh, publication, database call, socket or inference here.
func Open(ctx context.Context, c Config) (*Installation, error) {
	if !c.Enabled {
		return nil, nil
	}
	if ctx == nil || ctx.Err() != nil {
		return nil, ErrUnavailable
	}
	r, err := localoperator.ReadProfileReceipt(ctx, c.Receipt)
	if err != nil {
		return nil, ErrUnavailable
	}
	s, err := localoperator.OpenExisting(ctx, c.Installation, c.Executable, r)
	if err != nil {
		return nil, ErrUnavailable
	}
	p, err := s.Configuration()
	if err != nil || p.Auth != domain.ConfiguredInferenceAuthNoneV1 {
		_ = s.Close()
		return nil, ErrUnavailable
	}
	return &Installation{source: s, receipt: r}, nil
}

func (i *Installation) Close() error {
	if i == nil {
		return nil
	}
	i.once.Do(func() { i.closeErr = i.source.Close() })
	return i.closeErr
}

func (i *Installation) StoreOptions() ydbstore.Options {
	if i == nil {
		return ydbstore.Options{}
	}
	return ydbstore.Options{OwnedProfilePublicationVerifier: i.source, AttemptSpendingPublicationVerifier: i.source, OwnedAttemptEffectSourceVerifier: i.source}
}

func (i *Installation) Binder(store *ydbstore.Store) (ports.HarnessBinder, error) {
	if i == nil {
		return nil, ErrUnavailable
	}
	return ydbstore.NewLocalHarnessBinder(store, i.source, i.receipt)
}

func (i *Installation) Resources(store *ydbstore.Store) (ports.WebResourceStore, error) {
	if i == nil {
		return nil, ErrUnavailable
	}
	return ydbstore.NewLocalWebResourceStore(store, i.source, i.receipt)
}
