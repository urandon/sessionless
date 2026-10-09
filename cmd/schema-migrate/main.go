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
	timeout, err := migrationTimeout(os.Getenv("SCHEMA_MIGRATION_TIMEOUT"))
	if err != nil {
		return err
	}
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
		LockTTL:          migrationLockTTL(timeout),
	}, ydbmigrations.Files)
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, timeout)
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

// Keep ordinary/local invocations bounded as before, while allowing operators
// to admit a longer complete cloud baseline.
func migrationTimeout(value string) (time.Duration, error) {
	if value == "" {
		return 2 * time.Minute, nil
	}
	timeout, err := time.ParseDuration(value)
	if err != nil {
		return 0, fmt.Errorf("parse SCHEMA_MIGRATION_TIMEOUT: %w", err)
	}
	if timeout <= 0 || timeout > 20*time.Minute {
		return 0, errors.New("SCHEMA_MIGRATION_TIMEOUT must be positive and no greater than 20m")
	}
	return timeout, nil
}

// Renewal occurs between migrations, so a single migration's lease must
// outlive the admitted request plus bounded cleanup. Preserve the default
// five-minute lease while leaving a one-minute margin for longer requests.
func migrationLockTTL(timeout time.Duration) time.Duration {
	return max(5*time.Minute, timeout+time.Minute)
}

func ownerID() (string, error) {
	var value [12]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("generate migration owner ID: %w", err)
	}
	return "schema-" + hex.EncodeToString(value[:]), nil
}
