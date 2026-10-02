package attachedworkerpackage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"gitcode.com/urandon/sessionless/internal/attachedworkerlocal"
	"gitcode.com/urandon/sessionless/internal/attachedworkerservice"
)

const rootlessDockerBinary = "/usr/bin/docker"

func rootlessDockerSocket() string {
	return "/run/user/" + strconv.Itoa(os.Getuid()) + "/docker.sock"
}

func rootlessDockerConfig(config Config) string {
	return filepath.Join(config.InstallDir, "docker-config")
}

// The user service is the only process supervisor. The rootless Docker
// daemon supplies a namespace, not another polling/attempt owner. The worker
// binary is an immutable read-only bind mount and still verifies itself on
// entry; the image is a separately pinned, preloaded execution base.
func renderRootless(config Config, manifest attachedworkerlocal.ManifestV1) ([]byte, error) {
	if !imageDigest.MatchString(config.ContainerImage) || !canonicalPath(config.BinaryPath) ||
		!canonicalPath(config.StateRoot) || !canonicalPath(config.InstallDir) {
		return nil, ErrInvalid
	}
	controlDir, err := attachedworkerservice.Directory(config.StateRoot)
	if err != nil || !canonicalPath(controlDir) {
		return nil, ErrInvalid
	}
	name := "sessionless-attached-worker-" + shortID(manifest)
	socket := rootlessDockerSocket()
	client := fmt.Sprintf("%s --host unix://%s --config %s", rootlessDockerBinary, socket, rootlessDockerConfig(config))
	unit := fmt.Sprintf(`[Unit]
Description=Sessionless rootless attached worker %s
[Service]
Type=exec
ExecStart=/usr/bin/env -i PATH=/usr/bin:/bin %s container run --rm --pull never --name %s --network none --read-only --cap-drop ALL --no-healthcheck --ipc private --cgroupns private --security-opt no-new-privileges=true --pids-limit 64 --memory 268435456 --memory-swap 268435456 --log-driver none --restart no --stop-signal SIGTERM --stop-timeout 10 --user 0:0 --mount type=bind,src=%s,dst=%s,bind-propagation=rprivate --mount type=bind,src=%s,dst=%s,bind-propagation=rprivate --mount type=bind,src=%s,dst=%s,bind-propagation=rprivate,readonly --entrypoint %s %s serve --state-dir %s --expected-revision %d --binary %s --binary-sha256 %s
ExecStop=/usr/bin/env -i PATH=/usr/bin:/bin %s container stop --time 10 %s
ExecStopPost=-/usr/bin/env -i PATH=/usr/bin:/bin %s container stop --time 10 %s
Restart=no
UMask=0077
NoNewPrivileges=yes
`, shortID(manifest), client, name, config.StateRoot, config.StateRoot,
		controlDir, controlDir, config.BinaryPath, config.BinaryPath, config.BinaryPath,
		config.ContainerImage, config.StateRoot, manifest.Revision, config.BinaryPath,
		config.BinarySHA256, client, name, client, name)
	return []byte(unit), nil
}

func prepareRootlessDirectories(config Config) error {
	controlDir, err := attachedworkerservice.Directory(config.StateRoot)
	if err != nil {
		return ErrInvalid
	}
	for _, directory := range []string{controlDir, rootlessDockerConfig(config)} {
		if err := ensurePrivateDir(directory, true); err != nil {
			return err
		}
	}
	return emptyRootlessDockerConfig(config)
}

func emptyRootlessDockerConfig(config Config) error {
	directory := rootlessDockerConfig(config)
	if err := ensurePrivateDir(directory, false); err != nil {
		return err
	}
	entries, err := os.ReadDir(directory)
	if err != nil || len(entries) != 0 {
		return errors.Join(ErrConflict, err)
	}
	return nil
}

// A rootless start never discovers an inherited Docker context or home. It
// accepts only the current user's Unix socket, a rootless engine, and the
// exact preloaded image digest. Docker run itself also uses --pull never.
func verifyRootlessEngine(ctx context.Context, config Config) error {
	if ctx == nil || ctx.Err() != nil || runtime.GOOS != "linux" || os.Getuid() == 0 ||
		config.Mode != ModeRootlessContainer || emptyRootlessDockerConfig(config) != nil {
		return ErrInvalid
	}
	socket := rootlessDockerSocket()
	info, err := os.Lstat(socket)
	if err != nil || info.Mode()&os.ModeSocket == 0 || !ownedByCurrentUser(info) || info.Mode().Perm()&0o007 != 0 {
		return errors.Join(ErrInvalid, err)
	}
	var security []string
	if err := rootlessDockerJSON(ctx, config, &security, "info", "--format", "{{json .SecurityOptions}}"); err != nil {
		return err
	}
	rootless := false
	for _, option := range security {
		rootless = rootless || option == "name=rootless"
	}
	if !rootless {
		return ErrConflict
	}
	var digests []string
	if err := rootlessDockerJSON(ctx, config, &digests, "image", "inspect", "--format", "{{json .RepoDigests}}", config.ContainerImage); err != nil {
		return err
	}
	for _, candidate := range digests {
		if canonicalImageDigest(candidate) == canonicalImageDigest(config.ContainerImage) {
			return nil
		}
	}
	return ErrConflict
}

// Docker RepoDigests omit the optional human tag even when pull used
// repository:tag@sha256:digest. The repository and digest remain exact pins.
func canonicalImageDigest(reference string) string {
	at := strings.LastIndex(reference, "@sha256:")
	if at < 0 {
		return reference
	}
	repository := reference[:at]
	if colon := strings.LastIndex(repository, ":"); colon > strings.LastIndex(repository, "/") {
		repository = repository[:colon]
	}
	return repository + reference[at:]
}

func rootlessDockerJSON(ctx context.Context, config Config, result any, arguments ...string) error {
	output, err := rootlessDockerOutput(ctx, config.InstallDir, arguments...)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(output, result); err != nil {
		return errors.Join(ErrConflict, err)
	}
	return nil
}

// The service manager supervises the client, not the daemon-owned container.
// An orphaned exact container therefore blocks unregister, update, and any
// claim that the worker stopped, even when systemd calls the unit inactive.
func rootlessContainerPresent(ctx context.Context, installDir, nativeName string) (bool, error) {
	if !strings.HasPrefix(nativeName, "com.sessionless.attached-worker.") ||
		len(nativeName) != len("com.sessionless.attached-worker.")+16 {
		return false, ErrInvalid
	}
	if err := emptyRootlessDockerConfig(Config{InstallDir: installDir}); err != nil {
		return false, err
	}
	name := "sessionless-attached-worker-" + strings.TrimPrefix(nativeName, "com.sessionless.attached-worker.")
	output, err := rootlessDockerOutput(ctx, installDir, "container", "ls", "--all", "--no-trunc",
		"--filter", "name=^/"+name+"$", "--format", "{{json .Names}}")
	if err != nil {
		return false, err
	}
	output = bytes.TrimSpace(output)
	if len(output) == 0 {
		return false, nil
	}
	var found string
	if json.Unmarshal(output, &found) != nil || strings.TrimPrefix(found, "/") != name {
		return false, ErrConflict
	}
	return true, nil
}

func rootlessDockerOutput(ctx context.Context, installDir string, arguments ...string) ([]byte, error) {
	args := append([]string{"--host", "unix://" + rootlessDockerSocket(), "--config", filepath.Join(installDir, "docker-config")}, arguments...)
	command := exec.CommandContext(ctx, rootlessDockerBinary, args...)
	command.Env = []string{"PATH=/usr/bin:/bin"}
	var output, diagnostic cappedOutput
	command.Stdout = &output
	command.Stderr = &diagnostic
	if err := command.Run(); err != nil {
		return nil, errors.Join(ErrIO, err)
	}
	return output.Bytes(), nil
}
