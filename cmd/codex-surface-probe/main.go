package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gitcode.com/urandon/sessionless/internal/codexsurface"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	var surface string
	var executable string
	var pythonExecutable string
	var output string
	var scratch string
	var iterations int
	var timeout time.Duration
	flag.StringVar(&surface, "surface", "", "credential-free surface: app-server, exec, or python-sdk")
	flag.StringVar(&executable, "codex-bin", "", "exact Codex executable (defaults to codex for app-server/exec)")
	flag.StringVar(&pythonExecutable, "python-bin", "python3", "Python executable containing the pinned Codex SDK")
	flag.StringVar(&output, "output", "", "sanitized JSON report path")
	flag.StringVar(&scratch, "scratch", "", "private temporary directory parent")
	flag.IntVar(&iterations, "iterations", 1, "cold credential-free iterations")
	flag.DurationVar(&timeout, "timeout", 20*time.Second, "per-operation timeout")
	flag.Parse()
	if output == "" || filepath.Base(output) == "." {
		return errors.New("--output is required")
	}
	report, err := codexsurface.Probe(context.Background(), codexsurface.Surface(surface), codexsurface.Config{
		Executable: executable, PythonExecutable: pythonExecutable,
		Iterations: iterations, Timeout: timeout, Scratch: scratch,
	})
	if err != nil {
		return err
	}
	encoded, err := report.Marshal()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(output), 0o700); err != nil {
		return errors.New("create report directory")
	}
	return writePrivateAtomic(output, encoded)
}

func writePrivateAtomic(path string, data []byte) (result error) {
	directory := filepath.Dir(path)
	file, err := os.CreateTemp(directory, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return errors.New("create temporary report")
	}
	temporaryPath := file.Name()
	closed := false
	defer func() {
		if !closed {
			result = errors.Join(result, file.Close())
		}
		if removeErr := os.Remove(temporaryPath); !errors.Is(removeErr, os.ErrNotExist) {
			result = errors.Join(result, removeErr)
		}
	}()
	if err := file.Chmod(0o600); err != nil {
		return errors.New("secure temporary report")
	}
	if _, err := file.Write(data); err != nil {
		return errors.New("write temporary report")
	}
	if err := file.Sync(); err != nil {
		return errors.New("sync temporary report")
	}
	if err := file.Close(); err != nil {
		return errors.New("close temporary report")
	}
	closed = true
	if err := os.Rename(temporaryPath, path); err != nil {
		return errors.New("publish report")
	}
	return nil
}
