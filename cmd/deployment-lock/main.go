package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	"gitcode.com/urandon/sessionless/internal/ydbclient"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if len(os.Args) < 3 || os.Args[1] != "with" || os.Args[2] != "--" || len(os.Args) == 3 {
		logger.Error("usage: deployment-lock with -- command [arguments...]")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	client, err := ydbclient.Open(ctx, os.Getenv("TERRAFORM_LOCK_YDB_CONNECTION_STRING"))
	if err != nil {
		logger.Error("open deployment lock database", "error", err)
		os.Exit(1)
	}
	defer client.Close(context.Background())

	environment := envOrDefault("DEPLOYMENT_ENVIRONMENT", "cloud-dev")
	owner, err := ownerID()
	if err != nil {
		logger.Error("create deployment lock owner", "error", err)
		os.Exit(1)
	}
	ttl := envDuration("DEPLOYMENT_LOCK_TTL", 2*time.Minute)
	fence, err := claim(ctx, client.DB, environment, owner, ttl)
	if err != nil {
		logger.Error("claim deployment lock", "environment", environment, "error", err)
		os.Exit(1)
	}
	logger.Info("deployment lock claimed", "environment", environment, "owner", owner, "fence", fence)

	commandCtx, cancelCommand := context.WithCancel(ctx)
	defer cancelCommand()
	renewErrors := make(chan error, 1)
	go func() {
		renewErr := renewLoop(commandCtx, client.DB, environment, owner, fence, ttl)
		if renewErr != nil {
			cancelCommand()
		}
		renewErrors <- renewErr
	}()

	command := exec.CommandContext(commandCtx, os.Args[3], os.Args[4:]...)
	command.Stdin = os.Stdin
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	runErr := command.Run()
	cancelCommand()
	if renewErr := <-renewErrors; renewErr != nil {
		runErr = errors.Join(runErr, renewErr)
	}
	if err := release(context.Background(), client.DB, environment, owner, fence); err != nil {
		runErr = errors.Join(runErr, fmt.Errorf("release deployment lock: %w", err))
	} else {
		logger.Info("deployment lock released", "environment", environment, "fence", fence)
	}
	if runErr != nil {
		logger.Error("locked command failed", "error", runErr)
		var exitErr *exec.ExitError
		if errors.As(runErr, &exitErr) {
			os.Exit(exitErr.ExitCode())
		}
		os.Exit(1)
	}
}

func claim(ctx context.Context, db *sql.DB, environment, owner string, ttl time.Duration) (uint64, error) {
	now := time.Now().UTC()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	var currentOwner string
	var fence uint64
	var expiresAt time.Time
	err = tx.QueryRowContext(ctx,
		`SELECT owner_id, fence_token, expires_at FROM terraform_locks WHERE environment = $1`,
		environment,
	).Scan(&currentOwner, &fence, &expiresAt)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		fence = 1
	case err != nil:
		return 0, err
	case expiresAt.After(now) && currentOwner != owner:
		return 0, fmt.Errorf("environment is locked by %s until %s", currentOwner, expiresAt.Format(time.RFC3339))
	default:
		fence++
	}
	_, err = tx.ExecContext(ctx,
		`UPSERT INTO terraform_locks (environment, owner_id, fence_token, acquired_at, expires_at)
		 VALUES ($1, $2, $3, $4, $5)`,
		environment, owner, fence, now, now.Add(ttl),
	)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return fence, nil
}

func renewLoop(
	ctx context.Context, db *sql.DB, environment, owner string, fence uint64,
	ttl time.Duration,
) error {
	ticker := time.NewTicker(ttl / 3)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := renew(ctx, db, environment, owner, fence, ttl); err != nil {
				return fmt.Errorf("renew deployment lock: %w", err)
			}
		}
	}
}

func renew(ctx context.Context, db *sql.DB, environment, owner string, fence uint64, ttl time.Duration) error {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := verifyFence(ctx, tx, environment, owner, fence); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx,
		`UPDATE terraform_locks SET expires_at = $1
		 WHERE environment = $2 AND owner_id = $3 AND fence_token = $4`,
		time.Now().UTC().Add(ttl), environment, owner, fence,
	)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func release(ctx context.Context, db *sql.DB, environment, owner string, fence uint64) error {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := verifyFence(ctx, tx, environment, owner, fence); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx,
		`DELETE FROM terraform_locks
		 WHERE environment = $1 AND owner_id = $2 AND fence_token = $3`,
		environment, owner, fence,
	)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func verifyFence(ctx context.Context, tx *sql.Tx, environment, owner string, fence uint64) error {
	var currentOwner string
	var currentFence uint64
	err := tx.QueryRowContext(ctx,
		`SELECT owner_id, fence_token FROM terraform_locks WHERE environment = $1`,
		environment,
	).Scan(&currentOwner, &currentFence)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("deployment lock fence was lost")
	}
	if err != nil {
		return err
	}
	if currentOwner != owner || currentFence != fence {
		return fmt.Errorf("deployment lock fence ownership changed")
	}
	return nil
}

func ownerID() (string, error) {
	random := make([]byte, 8)
	if _, err := rand.Read(random); err != nil {
		return "", err
	}
	hostname, _ := os.Hostname()
	return fmt.Sprintf("%s-%d-%s", hostname, os.Getpid(), hex.EncodeToString(random)), nil
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func envDuration(name string, fallback time.Duration) time.Duration {
	value, err := time.ParseDuration(os.Getenv(name))
	if err != nil || value < 30*time.Second {
		return fallback
	}
	return value
}
