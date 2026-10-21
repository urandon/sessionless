//go:build darwin || linux

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"gitcode.com/urandon/sessionless/internal/nativeharnessbootstrap"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Environ(), nativeharnessbootstrap.Run)
	stop()
	os.Exit(code)
}

func run(ctx context.Context, args, env []string, execute func(context.Context, []string, []string, *os.File, *os.File, *os.File) error) int {
	if execute(ctx, args, env, os.Stdin, os.Stdout, os.Stderr) != nil {
		fmt.Fprintln(os.Stderr, "native helper failed")
		return 1
	}
	return 0
}
