package main

import (
	"bytes"
	"context"
	"errors"
	"gitcode.com/urandon/sessionless/internal/ydbstore"
	"io"
	"strings"
	"testing"
	"testing/synctest"
)

type repairCLIFake struct {
	call func(context.Context, ydbstore.ReadyExpiryRepairRequest) ([]byte, error)
}

func (f repairCLIFake) RepairReadyExpiryV2(ctx context.Context, r ydbstore.ReadyExpiryRepairRequest) ([]byte, error) {
	return f.call(ctx, r)
}
func repairArgs(mode string) []string {
	return []string{mode, "--tenant", "tenant-test", "--kind", "quota", "--run", "run-test", "--id", "quota-test", "--timeout", "1m", "--max-source-bytes", "131072", "--max-output-bytes", "4096"}
}
func TestRepairCLIRejectsBeforeDependencies(t *testing.T) {
	cases := map[string][]string{}
	for _, name := range []string{"missing", "unknown", "duplicate", "bad-id", "timeout-zero", "timeout-over", "source-over", "output-over", "many", "large", "positional"} {
		a := repairArgs("plan")
		switch name {
		case "missing":
			a = a[:len(a)-2]
		case "unknown":
			a[1] = "--unknown"
		case "duplicate":
			a[3] = "--tenant"
		case "bad-id":
			a[8] = "private / secret"
		case "timeout-zero":
			a[10] = "0"
		case "timeout-over":
			a[10] = "10m1ns"
		case "source-over":
			a[12] = "131073"
		case "output-over":
			a[14] = "4097"
		case "many":
			a = append(a, "--extra")
		case "large":
			a[8] = strings.Repeat("x", 257)
		case "positional":
			a[1] = "tenant"
		}
		cases[name] = a
	}
	for name, args := range cases {
		t.Run(name, func(t *testing.T) {
			calls := 0
			var out bytes.Buffer
			err := runRepair(t.Context(), args, func(string) string { calls++; return "SECRET" }, func(context.Context, string) (repairConnection, error) {
				calls++
				return repairConnection{}, errors.New("SECRET")
			}, &out)
			if err == nil || calls != 0 || out.Len() != 0 || strings.Contains(err.Error(), "SECRET") {
				t.Fatalf("err=%v calls=%d output=%q", err, calls, out.String())
			}
		})
	}
}
func TestRepairCLIOriginalContextScopeAndClose(t *testing.T) {
	for _, mode := range []string{"plan", "apply"} {
		t.Run(mode, func(t *testing.T) {
			calls, closed := 0, 0
			var opened context.Context
			var out bytes.Buffer
			err := runRepair(t.Context(), repairArgs(mode), func(key string) string {
				if key != "YDB_CONNECTION_STRING" {
					t.Fatal("ambient credential read")
				}
				return "PRIVATE-DSN"
			},
				func(ctx context.Context, dsn string) (repairConnection, error) {
					calls++
					opened = ctx
					if dsn != "PRIVATE-DSN" {
						t.Fatal("config lost")
					}
					return repairConnection{backend: repairCLIFake{call: func(ctx context.Context, r ydbstore.ReadyExpiryRepairRequest) ([]byte, error) {
						if ctx != opened || r.TenantID != "tenant-test" || r.RunID != "run-test" || r.ID != "quota-test" || r.Kind != "quota" || r.Mode != mode {
							t.Fatal("original context/exact locator changed")
						}
						if _, ok := ctx.Deadline(); !ok {
							t.Fatal("open not under original timeout")
						}
						return []byte("{\"outcome\":\"" + map[string]string{"plan": "planned", "apply": "applied"}[mode] + "\"}\n"), nil
					}}, close: func(ctx context.Context) error {
						if ctx != opened {
							t.Fatal("new cleanup grace")
						}
						closed++
						return nil
					}}, nil
				}, &out)
			if err != nil || calls != 1 || closed != 1 || strings.Contains(out.String(), "PRIVATE") {
				t.Fatalf("err=%v open=%d close=%d", err, calls, closed)
			}
		})
	}
}

type failedRepairWriter struct{}

func (failedRepairWriter) Write([]byte) (int, error) { return 0, errors.New("SECRET output") }
func TestRepairCLIClosedFailuresAndAmbiguousOutput(t *testing.T) {
	for _, name := range []string{"open", "read", "possibly", "close", "write", "invalid-output", "short-output"} {
		t.Run(name, func(t *testing.T) {
			closed := 0
			var out bytes.Buffer
			var writer io.Writer = &out
			if name == "write" {
				writer = failedRepairWriter{}
			}
			err := runRepair(t.Context(), repairArgs("apply"), func(string) string { return "PRIVATE-DSN" }, func(context.Context, string) (repairConnection, error) {
				if name == "open" {
					return repairConnection{}, errors.New("SECRET DSN")
				}
				return repairConnection{backend: repairCLIFake{call: func(context.Context, ydbstore.ReadyExpiryRepairRequest) ([]byte, error) {
					switch name {
					case "read":
						return nil, errors.New("SECRET SQL")
					case "possibly":
						return []byte("{\"outcome\":\"possibly_applied_metadata\"}\n"), ydbstore.ErrReadyExpiryRepairPossiblyApplied
					case "invalid-output":
						return []byte("SECRET not JSON"), nil
					case "short-output":
						return nil, nil
					}
					return []byte("{\"outcome\":\"applied\"}\n"), nil
				}}, close: func(context.Context) error {
					closed++
					if name == "close" {
						return errors.New("SECRET close")
					}
					return nil
				}}, nil
			}, writer)
			if err == nil || strings.Contains(err.Error(), "SECRET") || strings.Contains(out.String(), "SECRET") || strings.Contains(out.String(), "PRIVATE") {
				t.Fatalf("unsafe failure: %v", err)
			}
			wantClose := 1
			if name == "open" {
				wantClose = 0
			}
			if closed != wantClose {
				t.Fatalf("cleanup=%d want%d", closed, wantClose)
			}
			if name == "possibly" && (!strings.Contains(out.String(), "possibly_applied_metadata") || !errors.Is(err, ydbstore.ErrReadyExpiryRepairPossiblyApplied)) {
				t.Fatal("unknown commit hidden or called rollback")
			}
			if name == "write" && !strings.Contains(err.Error(), "may already be committed") {
				t.Fatal("output failure called rollback")
			}
		})
	}
}
func TestRepairCLICancelBeforeOpen(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	calls := 0
	err := runRepair(ctx, repairArgs("plan"), func(string) string { calls++; return "PRIVATE" }, func(context.Context, string) (repairConnection, error) { calls++; return repairConnection{}, nil }, io.Discard)
	if !errors.Is(err, context.Canceled) || calls != 0 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
}
func TestRepairCLIDeadlineStartsBeforeOpen(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		args := repairArgs("plan")
		args[10] = "1s"
		called := false
		err := runRepair(t.Context(), args, func(string) string { return "PRIVATE" }, func(ctx context.Context, _ string) (repairConnection, error) {
			called = true
			<-ctx.Done()
			if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
				t.Fatal("original timeout missing")
			}
			return repairConnection{}, ctx.Err()
		}, io.Discard)
		if !called || err == nil {
			t.Fatal("dependency opening escaped deadline")
		}
	})
}
