// local-profile-validate checks operator-authored template DATA only. It neither
// reads source.key nor installs, signs, publishes, executes or probes anything.
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

const maxTemplateBytes = 256 << 10

var errTemplate = errors.New("local profile template validation failed")

func readInput(ctx context.Context, args []string, input io.Reader) (string, []byte, error) {
	if ctx == nil || ctx.Err() != nil || input == nil {
		return "", nil, errTemplate
	}
	flags := flag.NewFlagSet("local-profile-validate", flag.ContinueOnError)
	flags.SetOutput(io.Discard) // Invalid arguments may contain private values.
	executable := flags.String("executable", "", "explicit installed worker path")
	if flags.Parse(args) != nil || flags.NArg() != 0 || *executable == "" ||
		!filepath.IsAbs(*executable) || filepath.Clean(*executable) != *executable || *executable == "/" ||
		strings.IndexByte(*executable, 0) >= 0 {
		return "", nil, errTemplate
	}
	// Bounded allocation/consumption, not a promise to interrupt an arbitrary
	// blocking Reader. The documented CLI takes redirected finite file input;
	// normal OS signals remain enabled rather than being swallowed by a handler.
	raw, err := io.ReadAll(io.LimitReader(input, maxTemplateBytes+1))
	if err != nil || len(raw) == 0 || len(raw) > maxTemplateBytes || ctx.Err() != nil {
		clear(raw)
		return "", nil, errTemplate
	}
	return *executable, raw, nil
}

func run(ctx context.Context, args []string, input io.Reader, output io.Writer) error {
	if output == nil {
		return errTemplate
	}
	executable, raw, err := readInput(ctx, args, input)
	defer clear(raw)
	if err != nil || localoperator.ValidateManifestTemplate(ctx, raw, executable) != nil {
		return errTemplate
	}
	if _, err := fmt.Fprintln(output, "local profile template structurally valid; not installed or qualified"); err != nil {
		return errTemplate
	}
	return nil
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	err := run(ctx, os.Args[1:], os.Stdin, os.Stdout)
	cancel()
	if err != nil {
		// Neither raw decoder/driver errors, paths, bodies nor arguments escape.
		fmt.Fprintln(os.Stderr, errTemplate)
		os.Exit(1)
	}
}
