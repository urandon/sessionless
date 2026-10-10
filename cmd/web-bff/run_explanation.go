package main

import (
	"context"
	"errors"
	"strconv"

	"gitcode.com/urandon/sessionless/internal/ports"
)

type runExplanationStartupStore interface {
	ports.RunExplanationRatedReadStoreV1
	RequireRunExplanationCutover(context.Context, string) error
}

func runExplanationsFromEnvironment(ctx context.Context, getenv func(string) string, store runExplanationStartupStore) (ports.RunExplanationRatedReadStoreV1, error) {
	raw := getenv("WEB_RUN_EXPLANATION_ENABLED")
	if raw == "" {
		return nil, nil
	}
	enabled, err := strconv.ParseBool(raw)
	if err != nil {
		return nil, errors.New("WEB_RUN_EXPLANATION_ENABLED must be a boolean")
	}
	if !enabled {
		return nil, nil
	}
	if store == nil {
		return nil, errors.New("enabled run explanation requires the rated YDB store")
	}
	if err := store.RequireRunExplanationCutover(ctx, getenv("WEB_RUN_EXPLANATION_WRITER_COMMIT")); err != nil {
		return nil, err
	}
	return store, nil
}
