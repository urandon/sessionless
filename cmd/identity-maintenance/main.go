// identity-maintenance is operator-only. It neither freezes writers nor grants
// migration/cutover authority. Database credentials use the existing YDB chain.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ydbclient"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

var errArguments = errors.New("identity maintenance invalid arguments")
var errArtifact = errors.New("identity maintenance private artifact unavailable")
var errDependency = errors.New("identity maintenance dependency unavailable")
var errOutput = errors.New("identity maintenance output unavailable")

type maintenanceConfig struct {
	request  ydbstore.IdentityMaintenanceRequestV1
	artifact string
}
type maintenanceConnection struct {
	run   func(context.Context, ydbstore.IdentityMaintenanceRequestV1) (ydbstore.IdentityMaintenanceResultV1, error)
	close func(context.Context) error
}
type maintenanceOpen func(context.Context, string) (maintenanceConnection, error)

func parseMaintenance(args []string) (maintenanceConfig, error) {
	var c maintenanceConfig
	if len(args) < 7 || len(args) > 13 || len(args)%2 != 1 {
		return c, errArguments
	}
	total := 0
	for _, a := range args {
		total += len(a)
		if len(a) > 4096 || total > 8192 {
			return c, errArguments
		}
	}
	c.request.Mode = args[0]
	seen := map[string]bool{}
	for i := 1; i < len(args); i += 2 {
		key, value := args[i], args[i+1]
		if seen[key] || value == "" {
			return maintenanceConfig{}, errArguments
		}
		seen[key] = true
		switch key {
		case "--environment-digest":
			c.request.EnvironmentDigest = domain.SecretDigest(value)
		case "--manifest-digest":
			c.request.ManifestDigest = domain.SecretDigest(value)
		case "--source-commit":
			c.request.SourceCommit = value
		case "--kind":
			c.request.Kind = value
		case "--selector":
			c.request.Selector = value
		case "--candidate":
			c.artifact = value
		default:
			return maintenanceConfig{}, errArguments
		}
	}
	if c.request.EnvironmentDigest.Validate("environment") != nil || c.request.ManifestDigest.Validate("manifest") != nil || len(c.request.SourceCommit) != 40 || strings.Trim(c.request.SourceCommit, "0123456789abcdef") != "" {
		return maintenanceConfig{}, errArguments
	}
	switch c.request.Mode {
	case "inventory":
		if len(seen) != 3 {
			return maintenanceConfig{}, errArguments
		}
	case "prepare":
		if len(seen) != 6 || c.artifact == "" || !maintenanceSelector(c.request.Kind, c.request.Selector) {
			return maintenanceConfig{}, errArguments
		}
	case "apply", "reconcile":
		if len(seen) != 4 || c.artifact == "" {
			return maintenanceConfig{}, errArguments
		}
	default:
		return maintenanceConfig{}, errArguments
	}
	if c.artifact != "" && (!filepath.IsAbs(c.artifact) || filepath.Clean(c.artifact) != c.artifact) {
		return maintenanceConfig{}, errArguments
	}
	return c, nil
}
func maintenanceSelector(kind, key string) bool {
	if kind == "identity" {
		return domain.UserID(key).Validate() == nil
	}
	return (kind == "cookie" || kind == "challenge") && domain.SecretDigest(key).Validate("selector") == nil
}

// OpenRoot confines path resolution and disallows symlink escapes. The final
// file is also opened O_NOFOLLOW; mode/regular file/owner/link count are checked
// on the actual descriptor, not a racy pre-open permission observation.
func readMaintenanceArtifact(ctx context.Context, name string) ([]byte, error) {
	if ctx.Err() != nil {
		return nil, errArtifact
	}
	root, err := os.OpenRoot(filepath.Dir(name))
	if err != nil {
		return nil, errArtifact
	}
	defer root.Close()
	// A FIFO must not block before the descriptor's regular-file check. For a
	// regular file O_NONBLOCK has no effect; other descriptor kinds are refused.
	file, err := root.OpenFile(filepath.Base(name), os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, errArtifact
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 || info.Size() <= 0 || info.Size() > ydbstore.IdentityMaintenanceArtifactBytesV1 {
		return nil, errArtifact
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Getuid()) || stat.Nlink != 1 {
		return nil, errArtifact
	}
	raw, err := io.ReadAll(io.LimitReader(file, ydbstore.IdentityMaintenanceArtifactBytesV1+1))
	if err != nil || ctx.Err() != nil || len(raw) == 0 || len(raw) > ydbstore.IdentityMaintenanceArtifactBytesV1 {
		return nil, errArtifact
	}
	return raw, nil
}
func writeMaintenanceArtifact(ctx context.Context, name string, raw []byte) error {
	if ctx.Err() != nil || len(raw) == 0 || len(raw) > ydbstore.IdentityMaintenanceArtifactBytesV1 {
		return errArtifact
	}
	root, err := os.OpenRoot(filepath.Dir(name))
	if err != nil {
		return errArtifact
	}
	defer root.Close()
	file, err := root.OpenFile(filepath.Base(name), os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return errArtifact
	}
	info, statErr := file.Stat()
	if statErr != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		_ = file.Close()
		return errArtifact
	}
	// Never delete/overwrite an artifact after a persistence error. An incomplete
	// write remains private and fails strict parsing; a Sync/Close error may leave
	// complete bytes, but is still not acknowledged persistence or authority.
	return finishMaintenanceArtifact(ctx, file, raw)
}

// This private seam keeps the actual file ownership/open checks above intact
// while allowing deterministic partial-write, Sync and Close fault tests.
type maintenanceArtifactSink interface {
	Write([]byte) (int, error)
	Sync() error
	Close() error
}

func finishMaintenanceArtifact(ctx context.Context, file maintenanceArtifactSink, raw []byte) error {
	n, writeErr := file.Write(raw)
	syncErr := error(nil)
	if writeErr == nil && n == len(raw) {
		syncErr = file.Sync()
	}
	closeErr := file.Close()
	if writeErr != nil || n != len(raw) || syncErr != nil || closeErr != nil || ctx.Err() != nil {
		return errArtifact
	}
	return nil
}
func openMaintenance(ctx context.Context, dsn string) (maintenanceConnection, error) {
	client, err := ydbclient.Open(ctx, dsn)
	if err != nil {
		return maintenanceConnection{}, errDependency
	}
	return maintenanceConnection{run: func(ctx context.Context, r ydbstore.IdentityMaintenanceRequestV1) (ydbstore.IdentityMaintenanceResultV1, error) {
		return ydbstore.MaintainIdentityV1(ctx, client, r)
	}, close: client.Close}, nil
}
func maintenanceSafe(err error) error {
	for _, known := range []error{errArguments, errArtifact, errDependency, errOutput, ydbstore.ErrIdentityMaintenanceInvalidV1, ydbstore.ErrIdentityMaintenanceIncompleteV1, ydbstore.ErrIdentityMaintenanceAnomalyV1, ydbstore.ErrIdentityMaintenancePossiblyCommittedV1} {
		if errors.Is(err, known) {
			return known
		}
	}
	return errDependency
}
func runMaintenance(parent context.Context, args []string, getenv func(string) string, open maintenanceOpen, out io.Writer) error {
	c, err := parseMaintenance(args)
	if err != nil {
		return err
	}
	if parent == nil || getenv == nil || open == nil || out == nil {
		return errArguments
	}
	ctx, cancel := context.WithTimeout(parent, ydbstore.IdentityMaintenanceMaxDurationV1)
	defer cancel()
	if c.request.Mode == "apply" || c.request.Mode == "reconcile" {
		raw, e := readMaintenanceArtifact(ctx, c.artifact)
		if e != nil {
			return e
		}
		c.request.Candidate = raw
	}
	dsn := getenv("YDB_CONNECTION_STRING")
	if len(dsn) == 0 || len(dsn) > 4096 {
		return errDependency
	}
	conn, err := open(ctx, dsn)
	if err != nil {
		return maintenanceSafe(err)
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
	if conn.run == nil {
		return errDependency
	}
	result, runErr := conn.run(ctx, c.request)
	if runErr == nil && c.request.Mode == "prepare" {
		if e := writeMaintenanceArtifact(ctx, c.artifact, result.Candidate); e != nil {
			return e
		}
	}
	if e := closeOnce(); e != nil {
		if c.request.Mode == "apply" {
			return ydbstore.ErrIdentityMaintenancePossiblyCommittedV1
		}
		return errDependency
	}
	// Only the store's redacted report goes to stdout, never its candidate or a
	// raw error. Error reports are useful incomplete observations, not receipts.
	if len(result.Report) > 0 {
		if len(result.Report) > ydbstore.IdentityMaintenanceArtifactBytesV1 {
			return errOutput
		}
		n, e := out.Write(result.Report)
		if e != nil || n != len(result.Report) {
			if c.request.Mode == "apply" {
				return ydbstore.ErrIdentityMaintenancePossiblyCommittedV1
			}
			return errOutput
		}
	}
	if runErr != nil {
		return maintenanceSafe(runErr)
	}
	if len(result.Report) == 0 {
		return errOutput
	}
	return nil
}
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := runMaintenance(ctx, os.Args[1:], os.Getenv, openMaintenance, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, maintenanceSafe(err).Error())
		os.Exit(1)
	}
}
