// Package ydbmigrate implements the repository-owned safety protocol around
// Goose. YDB does not provide transactional DDL, so migrations are locked,
// checksummed before execution, and limited to one idempotent schema operation
// per file.
package ydbmigrate

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pressly/goose/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/retry"

	"gitcode.com/urandon/sessionless/internal/ydbclient"
)

const (
	gooseTable    = "sessionless_goose_versions"
	lockName      = "sessionless-schema"
	checksumState = "applied"
)

var migrationName = regexp.MustCompile(`^([0-9]+)_.+\.sql$`)

var (
	ErrMigrationLocked = errors.New("schema migration lock is held by another owner")
	ErrChecksumDrift   = errors.New("applied migration checksum drift")
)

type Migration struct {
	Version  int64
	Filename string
	SHA256   string
}

type Status struct {
	Version       int64
	Filename      string
	GooseState    string
	ChecksumState string
}

type Config struct {
	ConnectionString string
	OwnerID          string
	LockTTL          time.Duration
	Now              func() time.Time
}

type Migrator struct {
	config Config
	files  fs.FS
}

func New(config Config, files fs.FS) (*Migrator, error) {
	if config.ConnectionString == "" {
		return nil, errors.New("YDB_CONNECTION_STRING must not be empty")
	}
	if strings.TrimSpace(config.OwnerID) == "" {
		return nil, errors.New("migration owner ID must not be empty")
	}
	if config.LockTTL <= 0 {
		config.LockTTL = 5 * time.Minute
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	if files == nil {
		return nil, errors.New("migration filesystem must not be nil")
	}
	return &Migrator{config: config, files: files}, nil
}

func (migrator *Migrator) Up(ctx context.Context) (retErr error) {
	migrations, err := LoadMigrations(migrator.files)
	if err != nil {
		return err
	}
	if len(migrations) == 0 {
		return errors.New("no YDB migrations found")
	}

	dataClient, err := ydbclient.Open(ctx, migrator.config.ConnectionString)
	if err != nil {
		return fmt.Errorf("open YDB data connection: %w", err)
	}
	defer func() {
		retErr = errors.Join(retErr, dataClient.Close(context.Background()))
	}()
	schemaClient, err := ydbclient.OpenScripting(ctx, migrator.config.ConnectionString)
	if err != nil {
		return fmt.Errorf("open YDB schema connection: %w", err)
	}
	defer func() {
		retErr = errors.Join(retErr, schemaClient.Close(context.Background()))
	}()

	if err := bootstrap(ctx, schemaClient.DB); err != nil {
		return fmt.Errorf("bootstrap migration metadata: %w", err)
	}
	lock, err := acquire(ctx, dataClient.DB, migrator.config)
	if err != nil {
		return err
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		retErr = errors.Join(retErr, release(unlockCtx, dataClient.DB, lock))
	}()

	provider, err := goose.NewProvider(
		goose.DialectYdB,
		schemaClient.DB,
		migrator.files,
		goose.WithTableName(gooseTable),
	)
	if err != nil {
		return fmt.Errorf("construct Goose provider: %w", err)
	}
	statuses, err := provider.Status(ctx)
	if err != nil {
		return fmt.Errorf("read migration status: %w", err)
	}
	statusByVersion := make(map[int64]goose.State, len(statuses))
	for _, status := range statuses {
		statusByVersion[status.Source.Version] = status.State
	}

	for _, migration := range migrations {
		if err := renew(ctx, dataClient.DB, &lock, migrator.config); err != nil {
			return err
		}
		state, exists, err := checksumRecord(ctx, dataClient.DB, migration)
		if err != nil {
			return err
		}
		if !exists {
			if statusByVersion[migration.Version] == goose.StateApplied {
				return fmt.Errorf("%w: version %d has no pre-execution checksum", ErrChecksumDrift, migration.Version)
			}
			if err := recordPending(ctx, dataClient.DB, migration, migrator.config.Now().UTC()); err != nil {
				return err
			}
		} else if state == checksumState && statusByVersion[migration.Version] != goose.StateApplied {
			return fmt.Errorf("migration %d is checksummed as applied but absent from Goose history", migration.Version)
		}

		if statusByVersion[migration.Version] == goose.StateApplied {
			if state != checksumState {
				if err := markApplied(ctx, dataClient.DB, migration.Version, migrator.config.Now().UTC()); err != nil {
					return err
				}
			}
			continue
		}
		result, err := provider.UpByOne(ctx)
		if err != nil {
			return fmt.Errorf("apply migration %d: %w", migration.Version, err)
		}
		if result.Source.Version != migration.Version {
			return fmt.Errorf("Goose applied version %d, expected %d", result.Source.Version, migration.Version)
		}
		if err := markApplied(ctx, dataClient.DB, migration.Version, migrator.config.Now().UTC()); err != nil {
			return err
		}
		statusByVersion[migration.Version] = goose.StateApplied
	}
	return nil
}

func (migrator *Migrator) Status(ctx context.Context) (_ []Status, retErr error) {
	migrations, err := LoadMigrations(migrator.files)
	if err != nil {
		return nil, err
	}
	dataClient, err := ydbclient.Open(ctx, migrator.config.ConnectionString)
	if err != nil {
		return nil, fmt.Errorf("open YDB data connection: %w", err)
	}
	defer func() {
		retErr = errors.Join(retErr, dataClient.Close(context.Background()))
	}()
	schemaClient, err := ydbclient.OpenScripting(ctx, migrator.config.ConnectionString)
	if err != nil {
		return nil, fmt.Errorf("open YDB schema connection: %w", err)
	}
	defer func() {
		retErr = errors.Join(retErr, schemaClient.Close(context.Background()))
	}()
	if err := bootstrap(ctx, schemaClient.DB); err != nil {
		return nil, fmt.Errorf("bootstrap migration metadata: %w", err)
	}
	provider, err := goose.NewProvider(
		goose.DialectYdB,
		schemaClient.DB,
		migrator.files,
		goose.WithTableName(gooseTable),
	)
	if err != nil {
		return nil, err
	}
	gooseStatuses, err := provider.Status(ctx)
	if err != nil {
		return nil, err
	}
	gooseByVersion := make(map[int64]goose.State, len(gooseStatuses))
	for _, status := range gooseStatuses {
		gooseByVersion[status.Source.Version] = status.State
	}
	result := make([]Status, 0, len(migrations))
	for _, migration := range migrations {
		checksumState, exists, err := checksumRecord(ctx, dataClient.DB, migration)
		if err != nil {
			return nil, err
		}
		if !exists {
			checksumState = "unrecorded"
		}
		result = append(result, Status{
			Version:       migration.Version,
			Filename:      migration.Filename,
			GooseState:    string(gooseByVersion[migration.Version]),
			ChecksumState: checksumState,
		})
	}
	return result, nil
}

func LoadMigrations(files fs.FS) ([]Migration, error) {
	entries, err := fs.ReadDir(files, ".")
	if err != nil {
		return nil, err
	}
	var migrations []Migration
	seen := make(map[int64]string)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		match := migrationName.FindStringSubmatch(entry.Name())
		if match == nil {
			continue
		}
		version, err := strconv.ParseInt(match[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse migration version %q: %w", entry.Name(), err)
		}
		if previous, exists := seen[version]; exists {
			return nil, fmt.Errorf("duplicate migration version %d in %q and %q", version, previous, entry.Name())
		}
		body, err := fs.ReadFile(files, entry.Name())
		if err != nil {
			return nil, err
		}
		if err := validateSingleOperation(entry.Name(), string(body)); err != nil {
			return nil, err
		}
		sum := sha256.Sum256(body)
		migrations = append(migrations, Migration{
			Version:  version,
			Filename: entry.Name(),
			SHA256:   hex.EncodeToString(sum[:]),
		})
		seen[version] = entry.Name()
	}
	sort.Slice(migrations, func(left, right int) bool {
		return migrations[left].Version < migrations[right].Version
	})
	return migrations, nil
}

func validateSingleOperation(filename, body string) error {
	up, _, ok := strings.Cut(body, "-- +goose Down")
	if !ok {
		return fmt.Errorf("%s: missing Goose Down marker", filename)
	}
	up = strings.ReplaceAll(up, "-- +goose Up", "")
	var statements int
	for _, line := range strings.Split(up, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "--") || trimmed == "" {
			continue
		}
		statements += strings.Count(line, ";")
	}
	if statements != 1 {
		return fmt.Errorf("%s: up migration must contain exactly one schema statement, found %d", filename, statements)
	}
	return nil
}

type migrationLock struct {
	OwnerID   string
	Fence     uint64
	ExpiresAt time.Time
}

func bootstrap(ctx context.Context, db *sql.DB) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS schema_migration_lock (
			lock_name Utf8,
			owner_id Utf8,
			fence_token Uint64,
			expires_at Timestamp,
			updated_at Timestamp,
			PRIMARY KEY (lock_name)
		);`,
		`CREATE TABLE IF NOT EXISTS schema_migration_checksums (
			version Uint64,
			filename Utf8,
			sha256 Utf8,
			state Utf8,
			recorded_at Timestamp,
			applied_at Timestamp,
			PRIMARY KEY (version)
		);`,
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func acquire(ctx context.Context, db *sql.DB, config Config) (result migrationLock, retErr error) {
	now := config.Now().UTC()
	err := retry.DoTx(ctx, db, func(ctx context.Context, tx *sql.Tx) error {
		var owner string
		var fence uint64
		var expires time.Time
		err := tx.QueryRowContext(ctx,
			`SELECT owner_id, fence_token, expires_at
			 FROM schema_migration_lock
			 WHERE lock_name = $1`,
			lockName,
		).Scan(&owner, &fence, &expires)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			fence = 0
		case err != nil:
			return err
		case expires.After(now) && owner != config.OwnerID:
			return fmt.Errorf("%w: owner=%s expires_at=%s", ErrMigrationLocked, owner, expires.Format(time.RFC3339))
		}
		result = migrationLock{
			OwnerID:   config.OwnerID,
			Fence:     fence + 1,
			ExpiresAt: now.Add(config.LockTTL),
		}
		_, err = tx.ExecContext(ctx,
			`UPSERT INTO schema_migration_lock
			 (lock_name, owner_id, fence_token, expires_at, updated_at)
			 VALUES ($1, $2, $3, $4, $5)`,
			lockName, result.OwnerID, result.Fence, result.ExpiresAt, now,
		)
		return err
	}, retry.WithIdempotent(true), retry.WithTxOptions(&sql.TxOptions{
		Isolation: sql.LevelSerializable,
	}))
	return result, err
}

func renew(ctx context.Context, db *sql.DB, lock *migrationLock, config Config) error {
	now := config.Now().UTC()
	var owner string
	var fence uint64
	err := retry.DoTx(ctx, db, func(ctx context.Context, tx *sql.Tx) error {
		if err := tx.QueryRowContext(ctx,
			`SELECT owner_id, fence_token
			 FROM schema_migration_lock
			 WHERE lock_name = $1`,
			lockName,
		).Scan(&owner, &fence); err != nil {
			return err
		}
		if owner != lock.OwnerID || fence != lock.Fence {
			return fmt.Errorf("%w: migration lock ownership changed", ErrMigrationLocked)
		}
		lock.ExpiresAt = now.Add(config.LockTTL)
		_, err := tx.ExecContext(ctx,
			`UPDATE schema_migration_lock
			 SET expires_at = $1, updated_at = $2
			 WHERE lock_name = $3 AND owner_id = $4 AND fence_token = $5`,
			lock.ExpiresAt, now, lockName, lock.OwnerID, lock.Fence,
		)
		return err
	}, retry.WithIdempotent(true), retry.WithTxOptions(&sql.TxOptions{
		Isolation: sql.LevelSerializable,
	}))
	return err
}

func release(ctx context.Context, db *sql.DB, lock migrationLock) error {
	_, err := db.ExecContext(ctx,
		`DELETE FROM schema_migration_lock
		 WHERE lock_name = $1 AND owner_id = $2 AND fence_token = $3`,
		lockName, lock.OwnerID, lock.Fence,
	)
	return err
}

func checksumRecord(ctx context.Context, db *sql.DB, migration Migration) (state string, exists bool, err error) {
	var filename, checksum string
	err = db.QueryRowContext(ctx,
		`SELECT filename, sha256, state
		 FROM schema_migration_checksums
		 WHERE version = $1`,
		uint64(migration.Version),
	).Scan(&filename, &checksum, &state)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	if filename != migration.Filename || checksum != migration.SHA256 {
		return "", true, fmt.Errorf("%w: version %d", ErrChecksumDrift, migration.Version)
	}
	return state, true, nil
}

func recordPending(ctx context.Context, db *sql.DB, migration Migration, at time.Time) error {
	_, err := db.ExecContext(ctx,
		`INSERT INTO schema_migration_checksums
		 (version, filename, sha256, state, recorded_at)
		 VALUES ($1, $2, $3, $4, $5)`,
		uint64(migration.Version), migration.Filename, migration.SHA256, "pending", at,
	)
	return err
}

func markApplied(ctx context.Context, db *sql.DB, version int64, at time.Time) error {
	_, err := db.ExecContext(ctx,
		`UPDATE schema_migration_checksums
		 SET state = $1, applied_at = $2
		 WHERE version = $3`,
		checksumState, at, uint64(version),
	)
	return err
}
