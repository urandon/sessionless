package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerforeground"
	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkerpackage"
	"gitcode.com/urandon/sessionless/internal/attachedworkerservice"
)

const commandTimeout = 15 * time.Second

type commandErrorV1 struct {
	Version uint32                         `json:"version"`
	Code    attachedworkerlocal.ResultCode `json:"code"`
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "serve" {
		// The long-lived owner must not retain ambient HOME, API keys,
		// provider configuration, Docker context, or process tokens.
		os.Clearenv()
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	os.Exit(runWithContext(ctx, os.Args[1:], os.Stdout))
}

func run(arguments []string, output io.Writer) int {
	return runWithContext(context.Background(), arguments, output)
}

func runWithContext(parent context.Context, arguments []string, output io.Writer) int {
	if parent == nil || len(arguments) == 0 || output == nil {
		return writeResult(output, commandErrorV1{Version: 1, Code: attachedworkerlocal.CodeInvalid}, 2)
	}
	command := arguments[0]
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	stateRoot := flags.String("state-dir", "", "explicit absolute attached-worker state directory")
	expectedRevision := flags.Uint64("expected-revision", 0, "exact local manifest revision")
	idempotencyKey := flags.String("idempotency-key", "", "logout request idempotency key")
	packageMode := flags.String("package-mode", "", "launchd, systemd-user, or rootless-container")
	installDir := flags.String("install-dir", "", "explicit private package staging directory")
	binaryPath := flags.String("binary", "", "exact attached-worker binary path")
	binarySHA256 := flags.String("binary-sha256", "", "exact attached-worker binary digest")
	containerImage := flags.String("container-image", "", "pinned daemon-service image for rootless-container plan")
	expectedInstallRevision := flags.Uint64("expected-install-revision", 0, "exact prior package revision")
	planSHA256 := flags.String("plan-sha256", "", "digest of reviewed package plan")
	nativeAction := flags.String("native-action", "", "register or unregister an exact user service")
	expectedRegistrationRevision := flags.Uint64("expected-registration-revision", 0, "exact native registration revision")
	if err := flags.Parse(arguments[1:]); err != nil || flags.NArg() != 0 || *stateRoot == "" {
		return writeResult(output, commandErrorV1{Version: 1, Code: attachedworkerlocal.CodeInvalid}, 2)
	}
	store, err := attachedworkerlocal.NewStore(*stateRoot, nil)
	if err != nil {
		return writeResult(output, commandErrorV1{Version: 1, Code: attachedworkerlocal.Code(err)}, 2)
	}
	packageCommand := command == "package-plan" || command == "package-apply" ||
		command == "package-rollback-plan" || command == "package-rollback-apply"
	nativeCommand := command == "native-plan" || command == "native-apply" || command == "native-reconcile" ||
		command == "native-inspect" || command == "native-start"
	if !packageCommand && !nativeCommand && command != "serve" && (*packageMode != "" || *installDir != "" || *binaryPath != "" ||
		*binarySHA256 != "" || *containerImage != "" || *expectedInstallRevision != 0 || *planSHA256 != "" ||
		*nativeAction != "" || *expectedRegistrationRevision != 0) {
		return writeResult(output, commandErrorV1{Version: 1, Code: attachedworkerlocal.CodeInvalid}, 2)
	}
	if packageCommand {
		if *nativeAction != "" || *expectedRegistrationRevision != 0 {
			return writeResult(output, commandErrorV1{Version: 1, Code: attachedworkerlocal.CodeInvalid}, 2)
		}
		applyCommand := command == "package-apply" || command == "package-rollback-apply"
		if *expectedRevision != 0 || *idempotencyKey != "" || (*planSHA256 != "") != applyCommand {
			return writeResult(output, commandErrorV1{Version: 1, Code: attachedworkerlocal.CodeInvalid}, 2)
		}
		ctx, cancel := context.WithTimeout(parent, commandTimeout)
		defer cancel()
		config := attachedworkerpackage.Config{Mode: attachedworkerpackage.Mode(*packageMode), StateRoot: *stateRoot,
			InstallDir: *installDir, BinaryPath: *binaryPath, BinarySHA256: *binarySHA256, ContainerImage: *containerImage}
		if command == "package-rollback-plan" || command == "package-rollback-apply" {
			plan, planErr := attachedworkerpackage.RollbackPlan(ctx, config, *expectedInstallRevision)
			if planErr != nil {
				return writeResult(output, commandErrorV1{Version: 1, Code: packageCode(planErr)}, 1)
			}
			if command == "package-rollback-plan" {
				return writeResult(output, plan, 0)
			}
			if plan.PlanSHA256 != *planSHA256 {
				return writeResult(output, commandErrorV1{Version: 1, Code: attachedworkerlocal.CodeConflict}, 1)
			}
			receipt, applyErr := attachedworkerpackage.ApplyRollback(ctx, config, plan)
			if applyErr != nil {
				return writeResult(output, commandErrorV1{Version: 1, Code: packageCode(applyErr)}, 1)
			}
			return writeResult(output, receipt, 0)
		}
		plan, planErr := attachedworkerpackage.Plan(ctx, config, *expectedInstallRevision)
		if planErr != nil {
			return writeResult(output, commandErrorV1{Version: 1, Code: packageCode(planErr)}, 1)
		}
		if command == "package-plan" {
			return writeResult(output, plan, 0)
		}
		if plan.PlanSHA256 != *planSHA256 {
			return writeResult(output, commandErrorV1{Version: 1, Code: attachedworkerlocal.CodeConflict}, 1)
		}
		receipt, applyErr := attachedworkerpackage.Apply(ctx, config, plan)
		if applyErr != nil {
			return writeResult(output, commandErrorV1{Version: 1, Code: packageCode(applyErr)}, 1)
		}
		return writeResult(output, receipt, 0)
	}
	if nativeCommand {
		if *expectedRevision != 0 || *idempotencyKey != "" || *containerImage != "" ||
			(*planSHA256 != "") != (command == "native-apply" || command == "native-reconcile") ||
			(command != "native-start" && *expectedRegistrationRevision != 0) ||
			((command == "native-reconcile" || command == "native-inspect" || command == "native-start") && *nativeAction != "") ||
			((command == "native-reconcile" || command == "native-inspect") && *expectedInstallRevision != 0) ||
			(command == "native-start" && (*expectedInstallRevision == 0 || *expectedRegistrationRevision == 0)) {
			return writeResult(output, commandErrorV1{Version: 1, Code: attachedworkerlocal.CodeInvalid}, 2)
		}
		ctx, cancel := context.WithTimeout(parent, commandTimeout)
		defer cancel()
		config := attachedworkerpackage.Config{Mode: attachedworkerpackage.Mode(*packageMode), StateRoot: *stateRoot,
			InstallDir: *installDir, BinaryPath: *binaryPath, BinarySHA256: *binarySHA256}
		if command == "native-inspect" {
			result, inspectErr := attachedworkerpackage.NativeInspect(ctx, config)
			if inspectErr != nil {
				return writeResult(output, commandErrorV1{Version: 1, Code: packageCode(inspectErr)}, 1)
			}
			return writeResult(output, result, 0)
		}
		if command == "native-start" {
			result, startErr := attachedworkerpackage.NativeStart(ctx, config, *expectedInstallRevision, *expectedRegistrationRevision)
			if startErr != nil {
				return writeResult(output, commandErrorV1{Version: 1, Code: packageCode(startErr)}, 1)
			}
			return writeResult(output, result, 0)
		}
		if command == "native-reconcile" {
			result, reconcileErr := attachedworkerpackage.ReconcileNative(ctx, config, *planSHA256)
			if reconcileErr != nil {
				return writeResult(output, commandErrorV1{Version: 1, Code: packageCode(reconcileErr)}, 1)
			}
			return writeResult(output, result, 0)
		}
		plan, planErr := attachedworkerpackage.NativePlan(ctx, config, attachedworkerpackage.NativeAction(*nativeAction), *expectedInstallRevision)
		if planErr != nil {
			return writeResult(output, commandErrorV1{Version: 1, Code: packageCode(planErr)}, 1)
		}
		if command == "native-plan" {
			return writeResult(output, plan, 0)
		}
		if plan.PlanSHA256 != *planSHA256 {
			return writeResult(output, commandErrorV1{Version: 1, Code: attachedworkerlocal.CodeConflict}, 1)
		}
		receipt, applyErr := attachedworkerpackage.ApplyNative(ctx, config, plan)
		if applyErr != nil {
			return writeResult(output, commandErrorV1{Version: 1, Code: packageCode(applyErr)}, 1)
		}
		return writeResult(output, receipt, 0)
	}
	if command == "serve" {
		if *idempotencyKey != "" || *packageMode != "" || *installDir != "" || *containerImage != "" ||
			*expectedInstallRevision != 0 || *planSHA256 != "" || *nativeAction != "" || *expectedRegistrationRevision != 0 ||
			*expectedRevision == 0 || *binaryPath == "" || *binarySHA256 == "" {
			return writeResult(output, commandErrorV1{Version: 1, Code: attachedworkerlocal.CodeInvalid}, 2)
		}
		if *binaryPath != "" {
			if err := attachedworkerpackage.VerifyRunningBinary(*binaryPath, *binarySHA256); err != nil {
				return writeResult(output, commandErrorV1{Version: 1, Code: packageCode(err)}, 1)
			}
		}
		foreground, foregroundErr := attachedworkerforeground.New(store, attachedworkerforeground.Config{})
		if foregroundErr != nil {
			return writeResult(output, commandErrorV1{Version: 1, Code: attachedworkerlocal.CodeInvalid}, 2)
		}
		result, session, startErr := foreground.Start(parent)
		if startErr != nil {
			return writeOperation(output, result, startErr)
		}
		defer func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), commandTimeout)
			_ = session.Shutdown(cleanupCtx)
			cancel()
		}()
		if *expectedRevision != 0 && result.ManifestRevision != *expectedRevision {
			return writeResult(output, commandErrorV1{Version: 1, Code: attachedworkerlocal.CodeConflict}, 1)
		}
		controlDir, dirErr := attachedworkerservice.Directory(*stateRoot)
		if dirErr != nil {
			return writeOperation(output, session.Result(), dirErr)
		}
		serveErr := attachedworkerservice.Serve(parent, controlDir, session, store.Doctor)
		return writeOperation(output, session.Result(), serveErr)
	}
	ctx, cancel := context.WithTimeout(parent, commandTimeout)
	defer cancel()
	switch command {
	case "live-status", "live-doctor", "drain", "stop":
		mutation := command == "drain" || command == "stop"
		if *idempotencyKey != "" || mutation != (*expectedRevision != 0) {
			return writeResult(output, commandErrorV1{Version: 1, Code: attachedworkerlocal.CodeInvalid}, 2)
		}
		controlDir, dirErr := attachedworkerservice.Directory(*stateRoot)
		if dirErr != nil {
			return writeResult(output, commandErrorV1{Version: 1, Code: attachedworkerlocal.CodeInvalid}, 2)
		}
		action := map[string]attachedworkerservice.Action{
			"live-status": attachedworkerservice.ActionStatus,
			"live-doctor": attachedworkerservice.ActionDoctor,
			"drain":       attachedworkerservice.ActionDrain,
			"stop":        attachedworkerservice.ActionShutdown,
		}[command]
		result, callErr := attachedworkerservice.Call(ctx, controlDir, action, *expectedRevision)
		if callErr != nil {
			if errors.Is(callErr, attachedworkerservice.ErrAmbiguous) {
				return writeResult(output, commandErrorV1{Version: 1, Code: attachedworkerlocal.CodeAmbiguous}, 1)
			}
			return writeResult(output, commandErrorV1{Version: 1, Code: attachedworkerlocal.CodeIO}, 1)
		}
		if result.Code != "ok" {
			return writeResult(output, result, 1)
		}
		return writeResult(output, result, 0)
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

func packageCode(err error) attachedworkerlocal.ResultCode {
	switch {
	case errors.Is(err, attachedworkerpackage.ErrAmbiguous):
		return attachedworkerlocal.CodeAmbiguous
	case errors.Is(err, attachedworkerpackage.ErrConflict):
		return attachedworkerlocal.CodeConflict
	case errors.Is(err, attachedworkerpackage.ErrInvalid):
		return attachedworkerlocal.CodeInvalid
	case errors.Is(err, attachedworkerpackage.ErrIO):
		return attachedworkerlocal.CodeIO
	default:
		return attachedworkerlocal.Code(err)
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
