package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"syscall"
	"time"

	"gitcode.com/urandon/sessionless/internal/ydbclient"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
)

func main() {
	if err := run(); err != nil {
		slog.Error("YDB partition backfill failed", "error", err)
		os.Exit(1)
	}
}

func run() error {
	connectionString := os.Getenv("YDB_CONNECTION_STRING")
	if connectionString == "" {
		return errors.New("YDB_CONNECTION_STRING is required")
	}
	if len(os.Args) > 2 || (len(os.Args) == 2 && os.Args[1] != "--dry-run") {
		return fmt.Errorf("usage: schema-backfill [--dry-run]")
	}
	dryRun := slices.Contains(os.Args[1:], "--dry-run")
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	ctx, timeout := context.WithTimeout(ctx, 10*time.Minute)
	defer timeout()

	client, err := ydbclient.Open(ctx, connectionString)
	if err != nil {
		return err
	}
	defer client.Close(context.Background())
	results, err := ydbpartition.BackfillReadyExpiryV2(ctx, client.DB, dryRun)
	if err != nil {
		return err
	}
	output, err := json.MarshalIndent(struct {
		ContractVersion string                        `json:"contract_version"`
		DryRun          bool                          `json:"dry_run"`
		Tables          []ydbpartition.BackfillResult `json:"tables"`
	}{
		ContractVersion: ydbpartition.ContractVersion,
		DryRun:          dryRun,
		Tables:          results,
	}, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(output))
	return nil
}
