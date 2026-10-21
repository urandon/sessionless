//go:build darwin || linux

package main

import (
	"context"
	"errors"
	"os"
	"slices"
	"testing"
)

func TestCLIHandsOffExactInvocationAndExitStatus(t *testing.T) {
	args, env := []string{"bootstrap", "literal arg"}, []string{"PATH=", "FAKE_SECRET=never-log-this"}
	for _, fail := range []bool{false, true} {
		calls := 0
		got := run(t.Context(), args, env, func(ctx context.Context, a, e []string, in, out, errout *os.File) error {
			calls++
			if ctx != t.Context() || !slices.Equal(a, args) || !slices.Equal(e, env) || in != os.Stdin || out != os.Stdout || errout != os.Stderr {
				t.Fatal("CLI changed original invocation or stdio")
			}
			if fail {
				return errors.New("never-log-this")
			}
			return nil
		})
		want := 0
		if fail {
			want = 1
		}
		if got != want || calls != 1 {
			t.Fatalf("exit=%d calls=%d", got, calls)
		}
	}
}
