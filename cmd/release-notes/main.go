package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"gitcode.com/urandon/sessionless/internal/releasenotes"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, arguments []string) error {
	flags := flag.NewFlagSet("release-notes", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	var tag, sourceSHA, previousTag, outputPath, repositoryRoot string
	flags.StringVar(&tag, "tag", "", "release tag")
	flags.StringVar(&sourceSHA, "source-sha", "", "optional exact 40-character source commit guard")
	flags.StringVar(&previousTag, "previous-tag", "", "optional exact previous release tag")
	flags.StringVar(&outputPath, "output", "", "Markdown output path")
	flags.StringVar(&repositoryRoot, "repository", ".", "local Git repository root")
	if err := flags.Parse(arguments); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments: %v", flags.Args())
	}
	if tag == "" || outputPath == "" {
		return errors.New("--tag and --output are required")
	}
	repository, err := releasenotes.NewLocalRepository(repositoryRoot)
	if err != nil {
		return err
	}
	notes, err := releasenotes.Generate(ctx, repository, releasenotes.Options{
		Tag: tag, SourceSHA: sourceSHA, PreviousTag: previousTag,
	})
	if err != nil {
		return err
	}
	return writeAtomic(outputPath, notes)
}

func writeAtomic(path string, data []byte) (result error) {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return fmt.Errorf("create release-note output directory: %w", err)
	}
	file, err := os.CreateTemp(directory, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create temporary release-note output: %w", err)
	}
	temporaryPath := file.Name()
	closed := false
	defer func() {
		var closeErr error
		if !closed {
			closeErr = file.Close()
		}
		removeErr := os.Remove(temporaryPath)
		if !errors.Is(removeErr, os.ErrNotExist) {
			result = errors.Join(result, removeErr)
		}
		result = errors.Join(result, closeErr)
	}()
	if err := file.Chmod(0o644); err != nil {
		return fmt.Errorf("set release-note output permissions: %w", err)
	}
	if _, err := file.Write(data); err != nil {
		return fmt.Errorf("write release notes: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync release notes: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close release notes: %w", err)
	}
	closed = true
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("publish release notes: %w", err)
	}
	return nil
}
