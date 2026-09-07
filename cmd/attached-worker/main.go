package main

import (
	"context"
	"encoding/json"
	"flag"
	"io"
	"os"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerforeground"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
)

const commandTimeout = 15 * time.Second

type commandErrorV1 struct {
	Version uint32                         `json:"version"`
	Code    attachedworkerlocal.ResultCode `json:"code"`
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout))
}

func run(arguments []string, output io.Writer) int {
	if len(arguments) == 0 || output == nil {
		return writeResult(output, commandErrorV1{Version: 1, Code: attachedworkerlocal.CodeInvalid}, 2)
	}
	command := arguments[0]
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	stateRoot := flags.String("state-dir", "", "explicit absolute attached-worker state directory")
	expectedRevision := flags.Uint64("expected-revision", 0, "exact local manifest revision")
	idempotencyKey := flags.String("idempotency-key", "", "logout request idempotency key")
	if err := flags.Parse(arguments[1:]); err != nil || flags.NArg() != 0 || *stateRoot == "" {
		return writeResult(output, commandErrorV1{Version: 1, Code: attachedworkerlocal.CodeInvalid}, 2)
	}
	store, err := attachedworkerlocal.NewStore(*stateRoot, nil)
	if err != nil {
		return writeResult(output, commandErrorV1{Version: 1, Code: attachedworkerlocal.Code(err)}, 2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), commandTimeout)
	defer cancel()
	switch command {
	case "run":
		if *expectedRevision != 0 || *idempotencyKey != "" {
			return writeResult(output, commandErrorV1{Version: 1, Code: attachedworkerlocal.CodeInvalid}, 2)
		}
		foreground, foregroundErr := attachedworkerforeground.New(store, attachedworkerforeground.Config{})
		if foregroundErr != nil {
			return writeResult(output, commandErrorV1{Version: 1, Code: attachedworkerlocal.CodeInvalid}, 2)
		}
		result, operationErr := foreground.Run(ctx)
		return writeOperation(output, result, operationErr)
	case "check":
		if *expectedRevision != 0 || *idempotencyKey != "" {
			return writeResult(output, commandErrorV1{Version: 1, Code: attachedworkerlocal.CodeInvalid}, 2)
		}
		result, operationErr := store.Check(ctx)
		return writeOperation(output, result, operationErr)
	case "doctor":
		if *expectedRevision != 0 || *idempotencyKey != "" {
			return writeResult(output, commandErrorV1{Version: 1, Code: attachedworkerlocal.CodeInvalid}, 2)
		}
		result, operationErr := store.Doctor(ctx)
		return writeOperation(output, result, operationErr)
	case "status":
		if *expectedRevision != 0 || *idempotencyKey != "" {
			return writeResult(output, commandErrorV1{Version: 1, Code: attachedworkerlocal.CodeInvalid}, 2)
		}
		result, operationErr := store.Status(ctx)
		return writeOperation(output, result, operationErr)
	case "logout":
		if *expectedRevision == 0 || *idempotencyKey == "" {
			return writeResult(output, commandErrorV1{Version: 1, Code: attachedworkerlocal.CodeInvalid}, 2)
		}
		result, operationErr := store.Logout(ctx, attachedworkerlocal.LogoutInputV1{
			ExpectedRevision: *expectedRevision, RequestID: *idempotencyKey,
		})
		return writeOperation(output, result, operationErr)
	case "uninstall-plan":
		if *expectedRevision != 0 || *idempotencyKey != "" {
			return writeResult(output, commandErrorV1{Version: 1, Code: attachedworkerlocal.CodeInvalid}, 2)
		}
		result, operationErr := store.UninstallPlan(ctx)
		return writeOperation(output, result, operationErr)
	default:
		return writeResult(output, commandErrorV1{Version: 1, Code: attachedworkerlocal.CodeInvalid}, 2)
	}
}

func writeOperation(output io.Writer, value any, err error) int {
	if err != nil {
		return writeResult(output, value, 1)
	}
	return writeResult(output, value, 0)
}

func writeResult(output io.Writer, value any, exitCode int) int {
	if output == nil {
		return 2
	}
	encoder := json.NewEncoder(output)
	encoder.SetEscapeHTML(true)
	if err := encoder.Encode(value); err != nil {
		return 2
	}
	return exitCode
}
