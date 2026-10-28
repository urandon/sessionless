// local-profile-install is an explicit owner-local first-publication command.
// It never creates operator declarations/keys or starts a model or worker.
package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"gitcode.com/urandon/sessionless/internal/ydbclient"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
)

const (
	installConfirmation  = "INSTALL LOCAL PROFILE"
	maxConfirmationBytes = 64
)

// All failures use fixed, content-free text. Driver errors, connection strings,
// private paths, manifests, keys and receipt bytes never enter command output.
var errLocalProfileInstall = errors.New("local profile installation failed")

type installationConfig struct {
	root, executable, receipt, connection string
}

func installationFromEnvironment(args []string, getenv func(string) string) (installationConfig, error) {
	if len(args) != 0 || getenv == nil || getenv("SESSIONLESS_ENVIRONMENT") != "local" {
		return installationConfig{}, errLocalProfileInstall
	}
	attached := getenv("WEB_ATTACHED_EXECUTION_ENABLED")
	if (attached != "" && attached != "false") || getenv("WEB_ATTACHED_RESOURCE_PINS") != "" {
		return installationConfig{}, errLocalProfileInstall
	}
	c := installationConfig{root: getenv("WEB_LOCAL_INSTALLATION_ROOT"), executable: getenv("WEB_LOCAL_WORKER_EXECUTABLE"), receipt: getenv("WEB_LOCAL_PROFILE_RECEIPT_FILE"), connection: getenv("YDB_CONNECTION_STRING")}
	for _, path := range []string{c.root, c.executable, c.receipt} {
		if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path || path == string(filepath.Separator) {
			return installationConfig{}, errLocalProfileInstall
		}
	}
	if c.connection == "" || strings.TrimSpace(c.connection) != c.connection {
		return installationConfig{}, errLocalProfileInstall
	}
	return c, nil
}

func confirmInstallation(ctx context.Context, input io.Reader) error {
	if ctx == nil || ctx.Err() != nil || input == nil {
		return errLocalProfileInstall
	}
	// LimitReader bounds allocation and bytes consumed even without a newline.
	// It does not pretend an arbitrary blocking Reader is context-interruptible.
	// No database/private-file work begins until the explicit line is received.
	line, err := bufio.NewReader(io.LimitReader(input, maxConfirmationBytes+1)).ReadString('\n')
	if (err != nil && !errors.Is(err, io.EOF)) || len(line) > maxConfirmationBytes ||
		(line != installConfirmation+"\n" && line != installConfirmation) || ctx.Err() != nil {
		return errLocalProfileInstall
	}
	return nil
}

func run(ctx context.Context, args []string, getenv func(string) string, input io.Reader, output, prompt io.Writer) error {
	c, err := installationFromEnvironment(args, getenv)
	if err != nil || output == nil || prompt == nil {
		return errLocalProfileInstall
	}
	if _, err := fmt.Fprintln(prompt, "Type INSTALL LOCAL PROFILE to publish the configured owner-local profile:"); err != nil {
		return errLocalProfileInstall
	}
	if confirmInstallation(ctx, input) != nil {
		return errLocalProfileInstall
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	client, err := ydbclient.Open(ctx, c.connection)
	if err != nil {
		return errLocalProfileInstall
	}
	closed := false
	closeClient := func() error {
		if closed {
			return nil
		}
		closed = true
		closeCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		return client.Close(closeCtx)
	}
	defer func() { _ = closeClient() }()
	// Open alone constructs a driver. Establish actual SQL readiness before
	// the installer can open an original Source or publish its profile.
	if client.DB == nil || client.DB.PingContext(ctx) != nil || ctx.Err() != nil {
		return errLocalProfileInstall
	}
	if ydbstore.InstallLocalProfile(ctx, client.DB, c.root, c.executable, c.receipt) != nil || ctx.Err() != nil {
		return errLocalProfileInstall
	}
	if closeClient() != nil {
		return errLocalProfileInstall
	}
	if _, err := fmt.Fprintln(output, "local profile receipt ready"); err != nil {
		return errLocalProfileInstall
	}
	return nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:], os.Getenv, os.Stdin, os.Stdout, os.Stderr)
	stop()
	if err != nil {
		// run has returned: all acquired dependency cleanup has already run.
		fmt.Fprintln(os.Stderr, "local profile installation failed")
		os.Exit(1)
	}
}
