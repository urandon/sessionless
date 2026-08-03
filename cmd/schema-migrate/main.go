package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"syscall"
	"time"

	"gitcode.com/urandon/sessionless/internal/ydbmigrate"
	ydbmigrations "gitcode.com/urandon/sessionless/migrations/ydb"
)

func main() {
	if err := run(); err != nil {
		slog.Error("YDB migration failed", "error", err)
		os.Exit(1)
	}
	if slices.Contains(os.Args[1:], "status") {
		slog.Info("YDB schema status reported")
		return
	}
	slog.Info("YDB schema is current")
}

func run() error {
	connectionString := os.Getenv("YDB_CONNECTION_STRING")
	if connectionString == "" {
		return errors.New("YDB_CONNECTION_STRING is required")
	}
	owner, err := ownerID()
	if err != nil {
		return err
	}
	migrator, err := ydbmigrate.New(ydbmigrate.Config{
		ConnectionString: connectionString,
		OwnerID:          owner,
		LockTTL:          5 * time.Minute,
	}, ydbmigrations.Files)
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, 2*time.Minute)
	defer timeoutCancel()
	if slices.Contains(os.Args[1:], "status") {
		statuses, err := migrator.Status(timeoutCtx)
		if err != nil {
			return err
		}
		for _, status := range statuses {
			fmt.Printf(
				"%05d %-9s %-10s %s\n",
				status.Version, status.GooseState, status.ChecksumState, status.Filename,
			)
		}
		return nil
	}
	if len(os.Args) > 1 {
		return fmt.Errorf("usage: schema-migrate [status]")
	}
	return migrator.Up(timeoutCtx)
}

func ownerID() (string, error) {
	var value [12]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("generate migration owner ID: %w", err)
	}
	return "schema-" + hex.EncodeToString(value[:]), nil
}
