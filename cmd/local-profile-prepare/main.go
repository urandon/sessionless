// local-profile-prepare exclusively creates private operator declaration files.
// It never installs a profile, enables execution, or probes a model endpoint.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gitcode.com/urandon/sessionless/internal/localoperator"
)

const (
	prepareConfirmation = "PREPARE LOCAL PROFILE"
	maxTemplateBytes    = 256 << 10
)

var errPreparation = errors.New("local profile preparation failed; reconcile any existing or partial output before retrying")

type preparationConfig struct{ root, executable string }

func readInput(ctx context.Context, args []string, input io.Reader) (preparationConfig, []byte, error) {
	if ctx == nil || ctx.Err() != nil || input == nil {
		return preparationConfig{}, nil, errPreparation
	}
	flags := flag.NewFlagSet("local-profile-prepare", flag.ContinueOnError)
	flags.SetOutput(io.Discard) // Invalid arguments may themselves be private.
	root := flags.String("installation-root", "", "unused canonical private installation path")
	executable := flags.String("executable", "", "explicit installed worker path")
	confirmation := flags.String("confirm", "", "explicit preparation intent")
	if flags.Parse(args) != nil || flags.NArg() != 0 || *confirmation != prepareConfirmation {
		return preparationConfig{}, nil, errPreparation
	}
	for _, path := range []string{*root, *executable} {
		if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path || path == "/" || strings.IndexByte(path, 0) >= 0 {
			return preparationConfig{}, nil, errPreparation
		}
	}
	// Redirect a finite file. This bounds consumed bytes/allocation, not an
	// arbitrary blocking Reader; default OS interrupt handling remains enabled.
	raw, err := io.ReadAll(io.LimitReader(input, maxTemplateBytes+1))
	if err != nil || len(raw) == 0 || len(raw) > maxTemplateBytes || ctx.Err() != nil {
		clear(raw)
		return preparationConfig{}, nil, errPreparation
	}
	return preparationConfig{root: *root, executable: *executable}, raw, nil
}

func run(ctx context.Context, args []string, input io.Reader, output io.Writer) error {
	if output == nil {
		return errPreparation
	}
	config, raw, err := readInput(ctx, args, input)
	defer clear(raw)
	if err != nil || localoperator.PrepareInstallation(ctx, config.root, config.executable, raw) != nil {
		return errPreparation
	}
	if _, err := fmt.Fprintln(output, "local profile private files prepared; not installed or qualified"); err != nil {
		// Preparation may already be durable. An output error must never cause
		// automatic deletion, overwrite or key regeneration on a retry.
		return errPreparation
	}
	return nil
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	err := run(ctx, os.Args[1:], os.Stdin, os.Stdout)
	cancel()
	if err != nil {
		fmt.Fprintln(os.Stderr, errPreparation)
		os.Exit(1)
	}
}
