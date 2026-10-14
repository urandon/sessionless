// ready-expiry-repair is an operator-only metadata command. Existing YDB IAM
// credentials authenticate the database principal, not an app tenant. Tenant
// flags/plan output are selectors, never authorization or execution permission.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ydbclient"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
	"io"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"
)

const repairMaxArgs = 15
const repairMaxArgBytes = 256
const repairMaxAllArgBytes = 4096
const repairMaxDiagnosticBytes = 128

var (
	errArguments     = errors.New("invalid ready-expiry-repair arguments")
	errDependency    = errors.New("ready-expiry-repair dependency unavailable")
	errOutput        = errors.New("ready-expiry-repair output unavailable")
	errAppliedOutput = errors.New("ready-expiry-repair output unavailable; apply metadata may already be committed")
)

type repairConfig struct {
	request ydbstore.ReadyExpiryRepairRequest
	timeout time.Duration
}
type repairBackend interface {
	RepairReadyExpiryV2(context.Context, ydbstore.ReadyExpiryRepairRequest) ([]byte, error)
}
type repairConnection struct {
	backend repairBackend
	close   func(context.Context) error
}
type repairOpen func(context.Context, string) (repairConnection, error)

func parseRepair(args []string) (repairConfig, error) {
	var c repairConfig
	if len(args) < 1 || len(args) > repairMaxArgs {
		return c, errArguments
	}
	total := 0
	for _, a := range args {
		if len(a) > repairMaxArgBytes {
			return c, errArguments
		}
		total += len(a)
		if total > repairMaxAllArgBytes {
			return c, errArguments
		}
	}
	c.request.Mode = args[0]
	if c.request.Mode != "plan" && c.request.Mode != "apply" {
		return repairConfig{}, errArguments
	}
	seen := map[string]bool{}
	for i := 1; i < len(args); i++ {
		if !strings.HasPrefix(args[i], "--") {
			return repairConfig{}, errArguments
		}
		name, value, inline := strings.Cut(args[i][2:], "=")
		if seen[name] {
			return repairConfig{}, errArguments
		}
		seen[name] = true
		if !inline {
			if i+1 >= len(args) {
				return repairConfig{}, errArguments
			}
			i++
			value = args[i]
		}
		var err error
		switch name {
		case "tenant":
			c.request.TenantID = domain.TenantID(value)
		case "kind":
			c.request.Kind = value
		case "run":
			c.request.RunID = domain.RunID(value)
		case "id":
			c.request.ID = value
		case "timeout":
			c.timeout, err = time.ParseDuration(value)
		case "max-source-bytes":
			c.request.MaxSourceBytes, err = strconv.Atoi(value)
		case "max-output-bytes":
			c.request.MaxOutputBytes, err = strconv.Atoi(value)
		default:
			return repairConfig{}, errArguments
		}
		if err != nil {
			return repairConfig{}, errArguments
		}
	}
	if len(seen) != 7 || c.request.Validate() != nil || c.timeout <= 0 || c.timeout > ydbstore.ReadyExpiryRepairMaxDuration {
		return repairConfig{}, errArguments
	}
	return c, nil
}
func openRepair(ctx context.Context, dsn string) (repairConnection, error) {
	client, err := ydbclient.Open(ctx, dsn)
	if err != nil {
		return repairConnection{}, errDependency
	}
	store, err := ydbstore.New(client.DB, ydbstore.Options{})
	if err != nil {
		_ = client.Close(ctx)
		return repairConnection{}, errDependency
	}
	return repairConnection{backend: store, close: client.Close}, nil
}
func safeRepairError(err error) error {
	for _, known := range []error{context.Canceled, context.DeadlineExceeded, ydbstore.ErrReadyExpiryRepairInvalid, ydbstore.ErrReadyExpiryRepairUnavailable, ydbstore.ErrReadyExpiryRepairPossiblyApplied} {
		if errors.Is(err, known) {
			return known
		}
	}
	return ydbstore.ErrReadyExpiryRepairFailed
}

// Dependency injection is private test composition only, not a public authority
// hook. main fixes the actual IAM client and store; tests NEVER call that opener.
func runRepair(parent context.Context, args []string, getenv func(string) string, open repairOpen, out io.Writer) error {
	c, err := parseRepair(args)
	if err != nil {
		return err
	}
	if parent == nil || getenv == nil || open == nil || out == nil {
		return errArguments
	}
	// This single original timeout covers open, every callback/retry and teardown.
	ctx, cancel := context.WithTimeout(parent, c.timeout)
	defer cancel()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	dsn := getenv("YDB_CONNECTION_STRING")
	if len(dsn) == 0 || len(dsn) > 4096 {
		return errDependency
	}
	conn, err := open(ctx, dsn)
	if err != nil {
		return errDependency
	}
	if conn.close == nil {
		return errDependency
	}
	closed := false
	closeOnce := func() error {
		if closed {
			return nil
		}
		closed = true
		return conn.close(ctx)
	}
	defer func() { _ = closeOnce() }()
	if conn.backend == nil {
		return errDependency
	}
	encoded, err := conn.backend.RepairReadyExpiryV2(ctx, c.request)
	terminal := error(nil)
	if err != nil {
		terminal = safeRepairError(err)
		if !errors.Is(terminal, ydbstore.ErrReadyExpiryRepairPossiblyApplied) {
			return terminal
		}
	}
	// The real store already bounds/checks before DML. This extra boundary stops
	// broken test/private composition from emitting unchecked output.
	if len(encoded) == 0 || len(encoded) > c.request.MaxOutputBytes || !utf8.Valid(encoded) || !json.Valid(encoded) {
		if c.request.Mode == "apply" {
			return errAppliedOutput
		}
		return errOutput
	}
	if closeOnce() != nil {
		if c.request.Mode == "apply" {
			return ydbstore.ErrReadyExpiryRepairPossiblyApplied
		}
		return errDependency
	}
	n, err := out.Write(encoded)
	if err != nil || n != len(encoded) {
		if c.request.Mode == "apply" {
			return errAppliedOutput
		}
		return errOutput
	}
	return terminal
}
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := runRepair(ctx, os.Args[1:], os.Getenv, openRepair, os.Stdout); err != nil {
		// One closed constant diagnostic, separate from the bounded JSON stdout.
		// Never echo flag values, SQL cells, credentials, DSN or driver error chains.
		message := err.Error()
		if len(message)+1 > repairMaxDiagnosticBytes {
			message = "ready-expiry-repair failed"
		}
		fmt.Fprintln(os.Stderr, message)
		os.Exit(1)
	}
}
