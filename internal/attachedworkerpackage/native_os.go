package attachedworkerpackage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

// osNativeManager deliberately uses only the current user's service manager.
// It never enables a service or starts a worker as a side effect of registration.
type osNativeManager struct{}

func (osNativeManager) inspect(ctx context.Context, mode Mode, name, unitPath string) (nativeState, error) {
	if err := nativeOSInput(mode, name, unitPath); err != nil {
		return nativeState{}, err
	}
	switch mode {
	case ModeLaunchd:
		output, err := nativeCommand(ctx, mode, "print", launchdTarget(name))
		absent := "Could not find service \"" + name + "\" in domain for user gui: " + strconv.Itoa(os.Getuid())
		if exitCode(err) == 113 && strings.Contains(output, absent) {
			return nativeState{}, nil
		}
		if err != nil {
			return nativeState{}, err
		}
		path := nativeProperty(output, "path")
		state := nativeProperty(output, "state")
		if !canonicalPath(path) || state == "" {
			return nativeState{}, ErrConflict
		}
		return nativeState{loaded: true, active: state == "running", path: path}, nil
	case ModeSystemdUser:
		output, err := nativeCommand(ctx, mode, "show", filepath.Base(unitPath),
			"--property=LoadState,FragmentPath,ActiveState", "--no-pager")
		if err != nil {
			return nativeState{}, err
		}
		properties, err := systemdProperties(output)
		if err != nil {
			return nativeState{}, err
		}
		if properties["LoadState"] == "not-found" {
			return nativeState{}, nil
		}
		if properties["LoadState"] != "loaded" || !canonicalPath(properties["FragmentPath"]) ||
			properties["ActiveState"] == "" {
			return nativeState{}, ErrConflict
		}
		return nativeState{loaded: true, active: properties["ActiveState"] != "inactive" &&
			properties["ActiveState"] != "failed", path: properties["FragmentPath"]}, nil
	default:
		return nativeState{}, ErrInvalid
	}
}

func (osNativeManager) register(ctx context.Context, mode Mode, name, unitPath string) error {
	if err := nativeOSInput(mode, name, unitPath); err != nil {
		return err
	}
	switch mode {
	case ModeLaunchd:
		_, err := nativeCommand(ctx, mode, "bootstrap", launchdDomain(), unitPath)
		return err
	case ModeSystemdUser:
		if _, err := nativeCommand(ctx, mode, "link", unitPath); err != nil {
			return err
		}
		_, err := nativeCommand(ctx, mode, "daemon-reload")
		return err
	default:
		return ErrInvalid
	}
}

func (osNativeManager) unregister(ctx context.Context, mode Mode, name, unitPath string) error {
	if err := nativeOSInput(mode, name, unitPath); err != nil {
		return err
	}
	switch mode {
	case ModeLaunchd:
		_, err := nativeCommand(ctx, mode, "bootout", launchdTarget(name))
		return err
	case ModeSystemdUser:
		if _, err := nativeCommand(ctx, mode, "disable", filepath.Base(unitPath)); err != nil {
			return err
		}
		_, err := nativeCommand(ctx, mode, "daemon-reload")
		return err
	default:
		return ErrInvalid
	}
}

func (osNativeManager) start(ctx context.Context, mode Mode, name, unitPath string) error {
	if err := nativeOSInput(mode, name, unitPath); err != nil {
		return err
	}
	switch mode {
	case ModeLaunchd:
		_, err := nativeCommand(ctx, mode, "kickstart", launchdTarget(name))
		return err
	case ModeSystemdUser:
		_, err := nativeCommand(ctx, mode, "start", filepath.Base(unitPath))
		return err
	default:
		return ErrInvalid
	}
}

// stop is reserved for bounded cleanup of an exact test-owned service. It
// never disables or removes registration by itself.
func (osNativeManager) stop(ctx context.Context, mode Mode, name, unitPath string) error {
	if err := nativeOSInput(mode, name, unitPath); err != nil {
		return err
	}
	switch mode {
	case ModeLaunchd:
		_, err := nativeCommand(ctx, mode, "stop", launchdTarget(name))
		return err
	case ModeSystemdUser:
		_, err := nativeCommand(ctx, mode, "stop", filepath.Base(unitPath))
		return err
	default:
		return ErrInvalid
	}
}

func nativeOSInput(mode Mode, name, unitPath string) error {
	if !canonicalPath(unitPath) || !strings.HasPrefix(name, "com.sessionless.attached-worker.") ||
		len(name) != len("com.sessionless.attached-worker.")+16 {
		return ErrInvalid
	}
	for _, digit := range name[len("com.sessionless.attached-worker."):] {
		if !strings.ContainsRune("0123456789abcdef", digit) {
			return ErrInvalid
		}
	}
	if (mode == ModeLaunchd && runtime.GOOS != "darwin") ||
		(mode == ModeSystemdUser && runtime.GOOS != "linux") ||
		(mode != ModeLaunchd && mode != ModeSystemdUser) {
		return ErrInvalid
	}
	return nil
}

func launchdDomain() string            { return "gui/" + strconv.Itoa(os.Getuid()) }
func launchdTarget(name string) string { return launchdDomain() + "/" + name }

// nativeCommand accepts only an already-validated mode and a fixed executable.
// Inherited environment is intentionally discarded. Output is bounded and is
// never returned in errors, because a service manager may print local paths.
func nativeCommand(ctx context.Context, mode Mode, args ...string) (string, error) {
	var binary string
	var env []string
	switch mode {
	case ModeLaunchd:
		binary = "/bin/launchctl"
		env = []string{"PATH=/usr/bin:/bin"}
	case ModeSystemdUser:
		binary = "/usr/bin/systemctl"
		args = append([]string{"--user"}, args...)
		runtimeDir := "/run/user/" + strconv.Itoa(os.Getuid())
		info, err := os.Lstat(runtimeDir)
		if err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 || !ownedByCurrentUser(info) {
			return "", ErrInvalid
		}
		env = []string{"PATH=/usr/bin:/bin", "XDG_RUNTIME_DIR=" + runtimeDir,
			"DBUS_SESSION_BUS_ADDRESS=unix:path=" + runtimeDir + "/bus"}
	default:
		return "", ErrInvalid
	}
	command := exec.CommandContext(ctx, binary, args...)
	command.Env = env
	var output cappedOutput
	command.Stdout = &output
	command.Stderr = &output
	err := command.Run()
	if err != nil {
		return output.String(), err
	}
	return output.String(), nil
}

type cappedOutput struct{ bytes.Buffer }

func (o *cappedOutput) Write(p []byte) (int, error) {
	if len(p) > 64<<10-o.Len() {
		return 0, ErrIO
	}
	return o.Buffer.Write(p)
}

func exitCode(err error) int {
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		return exit.ExitCode()
	}
	return -1
}

func nativeProperty(output, key string) string {
	prefix := key + " = "
	for _, line := range strings.Split(output, "\n") {
		if value, ok := strings.CutPrefix(strings.TrimSpace(line), prefix); ok {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func systemdProperties(output string) (map[string]string, error) {
	want := map[string]bool{"LoadState": true, "FragmentPath": true, "ActiveState": true}
	result := make(map[string]string, len(want))
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		key, value, ok := strings.Cut(line, "=")
		if !ok || !want[key] || result[key] != "" {
			return nil, ErrConflict
		}
		result[key] = value
	}
	if len(result) != len(want) {
		return nil, ErrConflict
	}
	return result, nil
}

var _ io.Writer = (*cappedOutput)(nil)
