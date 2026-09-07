// Package attachedworkeroci implements the explicit OCI/Docker Engine
// isolation boundary for an owner-managed attached worker. The selected engine
// and its VM or user namespace are part of the trusted computing base.
package attachedworkeroci

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
)

const (
	profileName = "sessionless.oci.docker.v1"

	profileLabel      = "dev.sessionless.attached-worker.profile"
	installationLabel = "dev.sessionless.attached-worker.installation"
	engineLabel       = "dev.sessionless.attached-worker.engine"

	minimumDockerAPI  = "1.42"
	maxCommandOutput  = 256 << 10
	minimumDiskBytes  = 16 << 20
	minimumMemory     = 64 << 20
	minimumPIDs       = 8
	maximumPIDs       = 4096
	defaultStopSecond = 5
	boundaryCleanup   = 30 * time.Second
	sharedMemoryBytes = 1 << 20
)

var (
	ErrConfig      = errors.New("attached worker OCI launcher configuration is invalid")
	ErrUnsupported = errors.New("attached worker OCI isolation is unsupported")
	ErrBoundary    = errors.New("attached worker OCI boundary operation failed")

	installationIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{7,63}$`)
)

type BoundaryKind string

const (
	BoundaryDarwinVM      BoundaryKind = "darwin-vm"
	BoundaryLinuxRootless BoundaryKind = "linux-rootless"
)

// Config is explicit operator-owned state. DockerPath, CLIConfigDir and Host
// replace ambient PATH, HOME, Docker contexts and credential helpers.
type Config struct {
	DockerPath          string
	CLIConfigDir        string
	Host                string
	EngineID            string
	InstallationID      string
	Boundary            BoundaryKind
	Image               string
	UserID              uint32
	GroupID             uint32
	DiskBytes           int64
	CredentialFileBytes int64
	MemoryBytes         int64
	PIDsLimit           int64
	StopSeconds         int

	runner commandRunner
	hostOS string
}

type commandRunner interface {
	Run(context.Context, string, []string, []string) ([]byte, []byte, error)
	Command(string, []string, []string) *exec.Cmd
}

type execRunner struct {
	path string
}

func (runner execRunner) Run(ctx context.Context, directory string, environment, arguments []string) ([]byte, []byte, error) {
	command := exec.CommandContext(ctx, runner.path, arguments...)
	command.Dir = directory
	command.Env = append([]string(nil), environment...)
	stdout := &boundedOutput{limit: maxCommandOutput}
	stderr := &boundedOutput{limit: maxCommandOutput}
	command.Stdout = stdout
	command.Stderr = stderr
	err := command.Run()
	if stdout.overflow || stderr.overflow {
		return nil, nil, ErrBoundary
	}
	return stdout.Bytes(), stderr.Bytes(), err
}

func (runner execRunner) Command(directory string, environment, arguments []string) *exec.Cmd {
	command := exec.Command(runner.path, arguments...)
	command.Dir = directory
	command.Env = append([]string(nil), environment...)
	return command
}

type boundedOutput struct {
	buffer   bytes.Buffer
	limit    int
	overflow bool
}

func (output *boundedOutput) Write(value []byte) (int, error) {
	remaining := output.limit - output.buffer.Len()
	if remaining > 0 {
		prefix := value
		if len(prefix) > remaining {
			prefix = prefix[:remaining]
		}
		_, _ = output.buffer.Write(prefix)
	}
	if len(value) > remaining {
		output.overflow = true
	}
	return len(value), nil
}

func (output *boundedOutput) Bytes() []byte {
	return append([]byte(nil), output.buffer.Bytes()...)
}

// Launcher is usable only after NewLauncher has completed the fail-closed
// engine and image preflight.
type Launcher struct {
	client              dockerClient
	image               string
	user                string
	diskBytes           int64
	credentialFileBytes int64
	memoryBytes         int64
	pidsLimit           int64
	stopSeconds         int
	engineID            string
	installationID      string
}

type dockerClient struct {
	runner    commandRunner
	directory string
	prefix    []string
}

func NewLauncher(ctx context.Context, config Config) (*Launcher, error) {
	if ctx == nil || ctx.Err() != nil {
		return nil, ErrConfig
	}
	dockerPath, err := canonicalRegularFile(config.DockerPath)
	if err != nil || dockerPath != config.DockerPath {
		return nil, ErrConfig
	}
	cliConfig, err := canonicalDirectory(config.CLIConfigDir)
	if err != nil || cliConfig != config.CLIConfigDir {
		return nil, ErrConfig
	}
	if err := requireEmptyPrivateDirectory(cliConfig); err != nil {
		return nil, ErrConfig
	}
	if !validUnixHost(config.Host) || !validEngineID(config.EngineID) ||
		!installationIDPattern.MatchString(config.InstallationID) || !validPinnedImage(config.Image) ||
		config.UserID == 0 || config.GroupID == 0 || config.DiskBytes < minimumDiskBytes ||
		config.CredentialFileBytes <= 0 || config.CredentialFileBytes+sharedMemoryBytes >= config.DiskBytes ||
		config.MemoryBytes < minimumMemory || config.PIDsLimit < minimumPIDs || config.PIDsLimit > maximumPIDs {
		return nil, ErrConfig
	}
	if config.StopSeconds == 0 {
		config.StopSeconds = defaultStopSecond
	}
	if config.StopSeconds < 1 || config.StopSeconds > 30 {
		return nil, ErrConfig
	}
	runner := config.runner
	if runner == nil {
		runner = execRunner{path: dockerPath}
	}
	hostOS := config.hostOS
	if hostOS == "" {
		hostOS = runtime.GOOS
	}
	if hostOS != "darwin" && hostOS != "linux" {
		return nil, ErrUnsupported
	}
	if hostOS == "darwin" && config.Boundary != BoundaryDarwinVM ||
		hostOS == "linux" && config.Boundary != BoundaryLinuxRootless {
		return nil, ErrUnsupported
	}
	client := dockerClient{
		runner: runner, directory: cliConfig,
		prefix: []string{"--config", cliConfig, "--host", config.Host},
	}
	if err := preflight(ctx, client, config.Image, config.EngineID, config.Boundary); err != nil {
		return nil, err
	}
	return &Launcher{
		client: client, image: config.Image,
		user:      strconv.FormatUint(uint64(config.UserID), 10) + ":" + strconv.FormatUint(uint64(config.GroupID), 10),
		diskBytes: config.DiskBytes, credentialFileBytes: config.CredentialFileBytes,
		memoryBytes: config.MemoryBytes, pidsLimit: config.PIDsLimit,
		stopSeconds: config.StopSeconds,
		engineID:    config.EngineID, installationID: config.InstallationID,
	}, nil
}

func (launcher *Launcher) Profile() attachedworkerdaemon.IsolationProfile {
	if launcher == nil {
		return attachedworkerdaemon.IsolationProfile{}
	}
	return attachedworkerdaemon.IsolationProfile{
		Name: profileName, FilesystemReadBoundary: true, FilesystemWriteBoundary: true,
		NetworkDenied: true, ProcessBoundary: true, DiskBytesBounded: true,
	}
}

func (launcher *Launcher) Prepare(ctx context.Context, spec attachedworkerdaemon.LaunchSpec) (attachedworkerdaemon.IsolationBoundary, error) {
	if launcher == nil || ctx == nil || ctx.Err() != nil {
		return nil, ErrConfig
	}
	validated, err := validateLaunchSpec(spec)
	if err != nil {
		return nil, err
	}
	name, err := randomContainerName()
	if err != nil {
		return nil, ErrBoundary
	}
	var staged *credentialStage
	if len(spec.WriteFiles) == 1 {
		staged, err = stageCredentialFile(validated.attemptRoot, spec.WriteFiles[0], launcher.credentialFileBytes)
		if err != nil {
			return nil, err
		}
	}
	stageOwned := staged != nil
	defer func() {
		if stageOwned {
			_ = staged.remove()
		}
	}()
	arguments := launcher.createArguments(name, spec, validated, staged)
	stdout, _, err := launcher.client.run(ctx, spec.Directory, spec.Environment, arguments...)
	if err != nil {
		launcher.removeAfterFailedPrepare(name)
		return nil, ErrBoundary
	}
	id := strings.TrimSpace(string(stdout))
	if !validContainerID(id) {
		launcher.removeAfterFailedPrepare(name)
		return nil, ErrBoundary
	}
	cleanup := true
	defer func() {
		if cleanup {
			launcher.removeAfterFailedPrepare(name)
		}
	}()
	if err := launcher.verifyContainer(ctx, id, name, spec, validated, staged); err != nil {
		return nil, err
	}
	command := launcher.client.command(spec.Directory, spec.Environment,
		"container", "start", "--attach", "--interactive", id)
	cleanup = false
	stageOwned = false
	return &boundary{
		client: launcher.client, id: id, command: command,
		attestation: attachedworkerdaemon.WorkloadAttestation{
			Executable: spec.Executable, ExecutableDigest: spec.ExecutableDigest,
			Arguments: append([]string(nil), spec.Arguments...),
		},
		stage: staged, stopSeconds: launcher.stopSeconds,
	}, nil
}

func (launcher *Launcher) removeAfterFailedPrepare(reference string) {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), boundaryCleanup)
	defer cancel()
	_, _, _ = launcher.client.run(cleanupCtx, launcher.client.directory, nil,
		"container", "rm", "--force", "--volumes", reference)
}

type validatedLaunch struct {
	attemptRoot string
	readRoots   []string
}

func validateLaunchSpec(spec attachedworkerdaemon.LaunchSpec) (validatedLaunch, error) {
	if len(spec.WriteRoots) != 1 || len(spec.ReadFiles) != 1 || spec.ReadFiles[0] != spec.Executable ||
		len(spec.WriteFiles) > 1 || !filepath.IsAbs(spec.Directory) || !filepath.IsAbs(spec.Executable) {
		return validatedLaunch{}, ErrConfig
	}
	executable, err := canonicalRegularFile(spec.Executable)
	if err != nil || executable != spec.Executable || spec.ExecutableDigest == (attachedworkerdaemon.ExecutableDigest{}) {
		return validatedLaunch{}, ErrConfig
	}
	attemptRoot, err := canonicalDirectory(spec.WriteRoots[0])
	if err != nil || attemptRoot != spec.WriteRoots[0] || !pathWithin(spec.Directory, attemptRoot) || spec.Directory == attemptRoot {
		return validatedLaunch{}, ErrConfig
	}
	directory, err := canonicalDirectory(spec.Directory)
	if err != nil || directory != spec.Directory {
		return validatedLaunch{}, ErrConfig
	}
	seen := make(map[string]struct{}, len(spec.ReadRoots))
	readRoots := make([]string, 0, len(spec.ReadRoots))
	hasAttemptRoot := false
	for _, root := range spec.ReadRoots {
		canonical, err := canonicalDirectory(root)
		if err != nil || canonical != root || canonical == string(filepath.Separator) {
			return validatedLaunch{}, ErrConfig
		}
		if _, exists := seen[root]; exists {
			return validatedLaunch{}, ErrConfig
		}
		seen[root] = struct{}{}
		readRoots = append(readRoots, root)
		if root == attemptRoot {
			hasAttemptRoot = true
		}
	}
	if !hasAttemptRoot {
		return validatedLaunch{}, ErrConfig
	}
	for index, left := range readRoots {
		for _, right := range readRoots[index+1:] {
			if left != attemptRoot && right != attemptRoot && (pathWithin(left, right) || pathWithin(right, left)) {
				return validatedLaunch{}, ErrConfig
			}
		}
	}
	if len(spec.WriteFiles) == 1 {
		writeFile, err := canonicalRegularFile(spec.WriteFiles[0])
		if err != nil || writeFile != spec.WriteFiles[0] || filepath.Base(writeFile) != "auth.json" || pathWithin(writeFile, attemptRoot) {
			return validatedLaunch{}, ErrConfig
		}
		allowed := false
		for _, root := range readRoots {
			if root != attemptRoot && pathWithin(writeFile, root) {
				allowed = true
				break
			}
		}
		if !allowed {
			return validatedLaunch{}, ErrConfig
		}
	}
	for _, variable := range spec.Environment {
		name, value, ok := strings.Cut(variable, "=")
		if !ok || name == "" || strings.IndexByte(name, 0) >= 0 || strings.IndexByte(value, 0) >= 0 {
			return validatedLaunch{}, ErrConfig
		}
	}
	sort.Strings(readRoots)
	return validatedLaunch{attemptRoot: attemptRoot, readRoots: readRoots}, nil
}

func (launcher *Launcher) createArguments(name string, spec attachedworkerdaemon.LaunchSpec, validated validatedLaunch, staged *credentialStage) []string {
	scratchBytes := launcher.diskBytes - launcher.credentialFileBytes - sharedMemoryBytes
	userID, groupID, _ := strings.Cut(launcher.user, ":")
	arguments := []string{
		"container", "create", "--pull", "never", "--name", name,
		"--network", "none", "--read-only", "--cap-drop", "ALL", "--no-healthcheck",
		"--ipc", "private", "--cgroupns", "private",
		"--security-opt", "no-new-privileges=true", "--pids-limit", strconv.FormatInt(launcher.pidsLimit, 10),
		"--memory", strconv.FormatInt(launcher.memoryBytes, 10), "--memory-swap", strconv.FormatInt(launcher.memoryBytes, 10),
		"--shm-size", strconv.FormatInt(sharedMemoryBytes, 10), "--log-driver", "none", "--restart", "no",
		"--stop-signal", "SIGTERM", "--stop-timeout", strconv.Itoa(launcher.stopSeconds),
		"--init", "--user", launcher.user, "--workdir", spec.Directory,
		"--label", profileLabel + "=" + profileName,
		"--label", installationLabel + "=" + launcher.installationID,
		"--label", engineLabel + "=" + launcher.engineID,
		"--ulimit", fmt.Sprintf("fsize=%d:%d", launcher.credentialFileBytes, launcher.credentialFileBytes),
		"--tmpfs", fmt.Sprintf("%s:rw,nosuid,nodev,noexec,size=%d,mode=0700,uid=%s,gid=%s", validated.attemptRoot, scratchBytes, userID, groupID),
	}
	arguments = append(arguments, "--mount", bindMount(spec.Executable, spec.Executable, true))
	for _, root := range validated.readRoots {
		if root == validated.attemptRoot {
			continue
		}
		arguments = append(arguments, "--mount", bindMount(root, root, true))
	}
	if staged != nil {
		arguments = append(arguments, "--mount", bindMount(staged.path, staged.target, false))
	}
	for _, variable := range spec.Environment {
		name, _, _ := strings.Cut(variable, "=")
		arguments = append(arguments, "--env", name)
	}
	arguments = append(arguments, "--entrypoint", spec.Executable, launcher.image)
	arguments = append(arguments, spec.Arguments...)
	return arguments
}

func bindMount(source, target string, readOnly bool) string {
	value := "type=bind,src=" + source + ",dst=" + target + ",bind-propagation=rprivate"
	if readOnly {
		value += ",readonly"
	}
	return value
}

type versionResponse struct {
	Client struct {
		APIVersion string `json:"ApiVersion"`
	} `json:"Client"`
	Server struct {
		APIVersion string `json:"ApiVersion"`
		OS         string `json:"Os"`
	} `json:"Server"`
}

type infoResponse struct {
	ID              string   `json:"ID"`
	OSType          string   `json:"OSType"`
	OperatingSystem string   `json:"OperatingSystem"`
	SecurityOptions []string `json:"SecurityOptions"`
	CgroupVersion   string   `json:"CgroupVersion"`
	MemoryLimit     bool     `json:"MemoryLimit"`
	PidsLimit       bool     `json:"PidsLimit"`
	SwapLimit       bool     `json:"SwapLimit"`
}

type imageResponse struct {
	ID          string         `json:"Id"`
	RepoDigests []string       `json:"RepoDigests"`
	OS          string         `json:"Os"`
	Volumes     map[string]any `json:"Volumes"`
	Config      struct {
		Volumes      map[string]any     `json:"Volumes"`
		Env          []string           `json:"Env"`
		ExposedPorts map[string]any     `json:"ExposedPorts"`
		Healthcheck  *healthcheckConfig `json:"Healthcheck"`
	} `json:"Config"`
}

type healthcheckConfig struct {
	Test []string `json:"Test"`
}

type ulimitConfig struct {
	Name string `json:"Name"`
	Soft int64  `json:"Soft"`
	Hard int64  `json:"Hard"`
}

func preflight(ctx context.Context, client dockerClient, image, engineID string, boundary BoundaryKind) error {
	var version versionResponse
	if err := client.runJSON(ctx, &version, "version", "--format", "{{json .}}"); err != nil ||
		!apiAtLeast(version.Client.APIVersion, minimumDockerAPI) || !apiAtLeast(version.Server.APIVersion, minimumDockerAPI) ||
		version.Server.OS != "linux" {
		return ErrUnsupported
	}
	var info infoResponse
	if err := client.runJSON(ctx, &info, "info", "--format", "{{json .}}"); err != nil || info.ID != engineID || info.OSType != "linux" ||
		info.CgroupVersion != "2" || !info.MemoryLimit || !info.PidsLimit || !info.SwapLimit ||
		!seccompEnabled(info.SecurityOptions) {
		return ErrUnsupported
	}
	if boundary == BoundaryLinuxRootless {
		if !hasNamedSecurityOption(info.SecurityOptions, "rootless") && !hasNamedSecurityOption(info.SecurityOptions, "userns") {
			return ErrUnsupported
		}
	} else if boundary != BoundaryDarwinVM {
		return ErrUnsupported
	}
	var inspected imageResponse
	if err := client.runJSON(ctx, &inspected, "image", "inspect", "--format", "{{json .}}", image); err != nil ||
		inspected.OS != "linux" || len(inspected.Config.Volumes) != 0 || len(inspected.Volumes) != 0 ||
		len(inspected.Config.Env) != 0 || len(inspected.Config.ExposedPorts) != 0 || inspected.Config.Healthcheck != nil ||
		!containsExact(inspected.RepoDigests, image) {
		return ErrUnsupported
	}
	return nil
}

type containerResponse struct {
	ID     string        `json:"Id"`
	Name   string        `json:"Name"`
	State  stateResponse `json:"State"`
	Config struct {
		Image        string             `json:"Image"`
		User         string             `json:"User"`
		WorkingDir   string             `json:"WorkingDir"`
		StopSignal   string             `json:"StopSignal"`
		StopTimeout  *int               `json:"StopTimeout"`
		Entrypoint   []string           `json:"Entrypoint"`
		Cmd          []string           `json:"Cmd"`
		Env          []string           `json:"Env"`
		Labels       map[string]string  `json:"Labels"`
		ExposedPorts map[string]any     `json:"ExposedPorts"`
		Healthcheck  *healthcheckConfig `json:"Healthcheck"`
	} `json:"Config"`
	HostConfig struct {
		NetworkMode     string            `json:"NetworkMode"`
		ReadonlyRootfs  bool              `json:"ReadonlyRootfs"`
		Privileged      bool              `json:"Privileged"`
		AutoRemove      bool              `json:"AutoRemove"`
		PublishAllPorts bool              `json:"PublishAllPorts"`
		PidMode         string            `json:"PidMode"`
		IpcMode         string            `json:"IpcMode"`
		CgroupnsMode    string            `json:"CgroupnsMode"`
		UsernsMode      string            `json:"UsernsMode"`
		CapAdd          []string          `json:"CapAdd"`
		CapDrop         []string          `json:"CapDrop"`
		SecurityOpt     []string          `json:"SecurityOpt"`
		PidsLimit       int64             `json:"PidsLimit"`
		Memory          int64             `json:"Memory"`
		MemorySwap      int64             `json:"MemorySwap"`
		ShmSize         int64             `json:"ShmSize"`
		Tmpfs           map[string]string `json:"Tmpfs"`
		Init            *bool             `json:"Init"`
		Devices         []json.RawMessage `json:"Devices"`
		DeviceRequests  []json.RawMessage `json:"DeviceRequests"`
		PortBindings    map[string]any    `json:"PortBindings"`
		Ulimits         []ulimitConfig    `json:"Ulimits"`
		LogConfig       struct {
			Type string `json:"Type"`
		} `json:"LogConfig"`
		RestartPolicy struct {
			Name string `json:"Name"`
		} `json:"RestartPolicy"`
	} `json:"HostConfig"`
	Mounts []struct {
		Type        string `json:"Type"`
		Source      string `json:"Source"`
		Destination string `json:"Destination"`
		RW          bool   `json:"RW"`
	} `json:"Mounts"`
}

func (launcher *Launcher) verifyContainer(ctx context.Context, id, name string, spec attachedworkerdaemon.LaunchSpec, validated validatedLaunch, staged *credentialStage) error {
	var inspected containerResponse
	if err := launcher.client.runJSON(ctx, &inspected, "container", "inspect", "--format", "{{json .}}", id); err != nil {
		return ErrBoundary
	}
	scratchBytes := launcher.diskBytes - launcher.credentialFileBytes - sharedMemoryBytes
	tmpfs := inspected.HostConfig.Tmpfs[validated.attemptRoot]
	if inspected.ID != id || inspected.Name != "/"+name || inspected.State.Status != "created" || inspected.State.Running || inspected.State.Dead ||
		inspected.Config.Image != launcher.image || inspected.Config.User != launcher.user ||
		inspected.Config.WorkingDir != spec.Directory || inspected.Config.StopSignal != "SIGTERM" ||
		inspected.Config.StopTimeout == nil || *inspected.Config.StopTimeout != launcher.stopSeconds ||
		!equalStrings(inspected.Config.Entrypoint, []string{spec.Executable}) || !equalStrings(inspected.Config.Cmd, spec.Arguments) ||
		!equalStringSets(inspected.Config.Env, spec.Environment) || len(inspected.Config.ExposedPorts) != 0 ||
		inspected.Config.Healthcheck == nil || !equalStrings(inspected.Config.Healthcheck.Test, []string{"NONE"}) ||
		inspected.HostConfig.NetworkMode != "none" || !inspected.HostConfig.ReadonlyRootfs || inspected.HostConfig.Privileged ||
		inspected.HostConfig.AutoRemove || inspected.HostConfig.PublishAllPorts || inspected.HostConfig.PidMode != "" ||
		inspected.HostConfig.IpcMode != "private" || inspected.HostConfig.CgroupnsMode != "private" ||
		inspected.HostConfig.UsernsMode != "" || len(inspected.HostConfig.CapAdd) != 0 ||
		len(inspected.HostConfig.CapDrop) != 1 || !strings.EqualFold(inspected.HostConfig.CapDrop[0], "ALL") ||
		!noNewPrivilegesOnly(inspected.HostConfig.SecurityOpt) || inspected.HostConfig.PidsLimit != launcher.pidsLimit ||
		inspected.HostConfig.Memory != launcher.memoryBytes || inspected.HostConfig.MemorySwap != launcher.memoryBytes ||
		inspected.HostConfig.ShmSize != sharedMemoryBytes || len(inspected.HostConfig.Devices) != 0 ||
		len(inspected.HostConfig.DeviceRequests) != 0 || len(inspected.HostConfig.PortBindings) != 0 ||
		!exactUlimits(inspected.HostConfig.Ulimits, launcher.credentialFileBytes) ||
		inspected.HostConfig.LogConfig.Type != "none" || inspected.HostConfig.RestartPolicy.Name != "no" ||
		inspected.HostConfig.Init == nil || !*inspected.HostConfig.Init ||
		len(inspected.HostConfig.Tmpfs) != 1 || !exactTmpfs(tmpfs, scratchBytes, launcher.user) ||
		!exactLabels(inspected.Config.Labels, launcher.engineID, launcher.installationID) ||
		!exactMounts(inspected.Mounts, spec, validated, staged) {
		return ErrUnsupported
	}
	return nil
}

func exactTmpfs(value string, size int64, user string) bool {
	userID, groupID, ok := strings.Cut(user, ":")
	if !ok {
		return false
	}
	expected := map[string]struct{}{
		"rw": {}, "nosuid": {}, "nodev": {}, "noexec": {},
		"size=" + strconv.FormatInt(size, 10): {}, "mode=0700": {},
		"uid=" + userID: {}, "gid=" + groupID: {},
	}
	parts := strings.Split(value, ",")
	if len(parts) != len(expected) {
		return false
	}
	for _, part := range parts {
		if _, exists := expected[part]; !exists {
			return false
		}
		delete(expected, part)
	}
	return len(expected) == 0
}

func exactUlimits(values []ulimitConfig, fileBytes int64) bool {
	return len(values) == 1 && values[0].Name == "fsize" &&
		values[0].Soft == fileBytes && values[0].Hard == fileBytes
}

func exactLabels(labels map[string]string, engineID, installationID string) bool {
	return len(labels) == 3 && labels[profileLabel] == profileName &&
		labels[installationLabel] == installationID && labels[engineLabel] == engineID
}

// Reconcile removes only containers owned by this exact installation and
// pinned engine. It is intended for startup before the daemon admits work.
func (launcher *Launcher) Reconcile(ctx context.Context) (int, error) {
	if launcher == nil || ctx == nil || ctx.Err() != nil {
		return 0, ErrConfig
	}
	stdout, _, err := launcher.client.run(ctx, launcher.client.directory, nil,
		"container", "ls", "--all", "--quiet",
		"--filter", "label="+profileLabel+"="+profileName,
		"--filter", "label="+installationLabel+"="+launcher.installationID,
		"--filter", "label="+engineLabel+"="+launcher.engineID)
	if err != nil {
		return 0, ErrBoundary
	}
	var ids []string
	seenIDs := make(map[string]struct{})
	for _, line := range strings.Split(strings.TrimSpace(string(stdout)), "\n") {
		if line == "" {
			continue
		}
		if !validContainerID(line) {
			return 0, ErrBoundary
		}
		if _, exists := seenIDs[line]; exists {
			return 0, ErrBoundary
		}
		seenIDs[line] = struct{}{}
		ids = append(ids, line)
	}
	for _, id := range ids {
		var inspected containerResponse
		if err := launcher.client.runJSON(ctx, &inspected, "container", "inspect", "--format", "{{json .}}", id); err != nil ||
			inspected.ID != id || !validOwnedContainerName(inspected.Name) ||
			!exactLabels(inspected.Config.Labels, launcher.engineID, launcher.installationID) {
			return 0, ErrBoundary
		}
	}
	for _, id := range ids {
		if _, _, err := launcher.client.run(ctx, launcher.client.directory, nil,
			"container", "rm", "--force", "--volumes", id); err != nil {
			return 0, ErrBoundary
		}
	}
	return len(ids), nil
}

func validOwnedContainerName(value string) bool {
	suffix, ok := strings.CutPrefix(value, "/sessionless-aw-")
	if !ok || len(suffix) != 32 {
		return false
	}
	_, err := hex.DecodeString(suffix)
	return err == nil
}

type expectedMount struct {
	typeName string
	source   string
	writable bool
}

func exactMounts(mounts []struct {
	Type        string `json:"Type"`
	Source      string `json:"Source"`
	Destination string `json:"Destination"`
	RW          bool   `json:"RW"`
}, spec attachedworkerdaemon.LaunchSpec, validated validatedLaunch, staged *credentialStage) bool {
	expected := map[string]expectedMount{
		spec.Executable: {typeName: "bind", source: spec.Executable},
	}
	for _, root := range validated.readRoots {
		if root != validated.attemptRoot {
			expected[root] = expectedMount{typeName: "bind", source: root}
		}
	}
	if staged != nil {
		expected[staged.target] = expectedMount{typeName: "bind", source: staged.path, writable: true}
	}
	if len(mounts) != len(expected) {
		return false
	}
	for _, mount := range mounts {
		want, exists := expected[mount.Destination]
		if !exists || mount.Type != want.typeName || mount.RW != want.writable ||
			(want.source != "" && mount.Source != want.source) {
			return false
		}
		delete(expected, mount.Destination)
	}
	return len(expected) == 0
}

type stateResponse struct {
	Status  string `json:"Status"`
	Running bool   `json:"Running"`
	Dead    bool   `json:"Dead"`
}

type boundary struct {
	client       dockerClient
	id           string
	command      *exec.Cmd
	attestation  attachedworkerdaemon.WorkloadAttestation
	stage        *credentialStage
	stopSeconds  int
	releaseMu    sync.Mutex
	released     bool
	releaseError error
}

func (boundary *boundary) Command() *exec.Cmd { return boundary.command }

func (boundary *boundary) AttestedWorkload() attachedworkerdaemon.WorkloadAttestation {
	return attachedworkerdaemon.WorkloadAttestation{
		Executable: boundary.attestation.Executable, ExecutableDigest: boundary.attestation.ExecutableDigest,
		Arguments: append([]string(nil), boundary.attestation.Arguments...),
	}
}

func (boundary *boundary) GracefulStop(ctx context.Context) error {
	_, _, err := boundary.client.run(ctx, boundary.client.directory, nil,
		"container", "stop", "--time", strconv.Itoa(boundary.stopSeconds), boundary.id)
	if err != nil {
		return ErrBoundary
	}
	return nil
}

func (boundary *boundary) ForceStop(ctx context.Context) error {
	_, _, err := boundary.client.run(ctx, boundary.client.directory, nil,
		"container", "kill", "--signal", "KILL", boundary.id)
	if err != nil {
		return ErrBoundary
	}
	return nil
}

func (boundary *boundary) Alive(ctx context.Context) (bool, error) {
	var state stateResponse
	if err := boundary.client.runJSON(ctx, &state, "container", "inspect", "--format", "{{json .State}}", boundary.id); err != nil {
		return false, ErrBoundary
	}
	return state.Running, nil
}

func (boundary *boundary) Release(ctx context.Context) error {
	boundary.releaseMu.Lock()
	defer boundary.releaseMu.Unlock()
	if boundary.released {
		return boundary.releaseError
	}
	boundary.released = true
	var failures []error
	if boundary.stage != nil {
		alive, inspectErr := boundary.Alive(ctx)
		if inspectErr != nil || alive {
			failures = append(failures, ErrBoundary)
		} else if err := boundary.stage.commit(); err != nil {
			failures = append(failures, err)
		}
		if err := boundary.stage.remove(); err != nil {
			failures = append(failures, err)
		}
	}
	if _, _, err := boundary.client.run(ctx, boundary.client.directory, nil,
		"container", "rm", "--force", "--volumes", boundary.id); err != nil {
		failures = append(failures, ErrBoundary)
	}
	boundary.releaseError = errors.Join(failures...)
	return boundary.releaseError
}

type credentialStage struct {
	path     string
	target   string
	maxBytes int64
}

func (stage *credentialStage) remove() error {
	if stage == nil {
		return nil
	}
	if err := os.Remove(stage.path); err != nil {
		return ErrBoundary
	}
	return nil
}

func stageCredentialFile(attemptRoot, target string, maxBytes int64) (*credentialStage, error) {
	source, err := os.Open(target)
	if err != nil {
		return nil, ErrBoundary
	}
	defer source.Close()
	info, err := source.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > maxBytes {
		return nil, ErrBoundary
	}
	path := filepath.Join(attemptRoot, ".sessionless-auth-stage")
	destination, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, ErrBoundary
	}
	copyErr := copyBoundedSecret(destination, source, maxBytes)
	syncErr := destination.Sync()
	closeErr := destination.Close()
	if copyErr != nil || syncErr != nil || closeErr != nil {
		_ = os.Remove(path)
		return nil, ErrBoundary
	}
	return &credentialStage{path: path, target: target, maxBytes: maxBytes}, nil
}

func (stage *credentialStage) commit() error {
	source, err := os.Open(stage.path)
	if err != nil {
		return ErrBoundary
	}
	defer source.Close()
	info, err := source.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > stage.maxBytes {
		return ErrBoundary
	}
	temporary, err := os.CreateTemp(filepath.Dir(stage.target), ".sessionless-auth-writeback-*")
	if err != nil {
		return ErrBoundary
	}
	temporaryName := temporary.Name()
	committed := false
	defer func() {
		_ = temporary.Close()
		if !committed {
			_ = os.Remove(temporaryName)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil || copyBoundedSecret(temporary, source, stage.maxBytes) != nil ||
		temporary.Sync() != nil || temporary.Close() != nil || os.Rename(temporaryName, stage.target) != nil {
		return ErrBoundary
	}
	directory, err := os.Open(filepath.Dir(stage.target))
	if err != nil {
		return ErrBoundary
	}
	err = directory.Sync()
	_ = directory.Close()
	if err != nil {
		return ErrBoundary
	}
	committed = true
	return nil
}

func copyBoundedSecret(destination io.Writer, source io.Reader, maxBytes int64) error {
	buffer := make([]byte, 32<<10)
	defer func() {
		for index := range buffer {
			buffer[index] = 0
		}
	}()
	written, err := io.CopyBuffer(destination, io.LimitReader(source, maxBytes+1), buffer)
	if err != nil || written > maxBytes {
		return ErrBoundary
	}
	return nil
}

func (client dockerClient) arguments(arguments ...string) []string {
	result := append([]string(nil), client.prefix...)
	return append(result, arguments...)
}

func (client dockerClient) run(ctx context.Context, directory string, environment []string, arguments ...string) ([]byte, []byte, error) {
	return client.runner.Run(ctx, directory, append([]string(nil), environment...), client.arguments(arguments...))
}

func (client dockerClient) command(directory string, environment []string, arguments ...string) *exec.Cmd {
	return client.runner.Command(directory, append([]string(nil), environment...), client.arguments(arguments...))
}

func (client dockerClient) runJSON(ctx context.Context, result any, arguments ...string) error {
	stdout, _, err := client.run(ctx, client.directory, nil, arguments...)
	if err != nil || json.Unmarshal(stdout, result) != nil {
		return ErrBoundary
	}
	return nil
}

func canonicalRegularFile(path string) (string, error) {
	if !filepath.IsAbs(path) {
		return "", ErrConfig
	}
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(canonical)
	if err != nil || !info.Mode().IsRegular() {
		return "", ErrConfig
	}
	return canonical, nil
}

func canonicalDirectory(path string) (string, error) {
	if !filepath.IsAbs(path) {
		return "", ErrConfig
	}
	canonical, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(canonical)
	if err != nil || !info.IsDir() {
		return "", ErrConfig
	}
	return canonical, nil
}

func requireEmptyPrivateDirectory(path string) error {
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm()&0o077 != 0 {
		return ErrConfig
	}
	entries, err := os.ReadDir(path)
	if err != nil || len(entries) != 0 {
		return ErrConfig
	}
	return nil
}

func validUnixHost(value string) bool {
	path, ok := strings.CutPrefix(value, "unix://")
	return ok && filepath.IsAbs(path) && filepath.Clean(path) == path && path != string(filepath.Separator)
}

func validEngineID(value string) bool {
	return len(value) >= 16 && len(value) <= 128 && !strings.ContainsAny(value, " \t\r\n\x00")
}

func validPinnedImage(value string) bool {
	name, digest, ok := strings.Cut(value, "@sha256:")
	if !ok || name == "" || strings.ContainsAny(name, "@ \t\r\n") || len(digest) != 64 {
		return false
	}
	_, err := hex.DecodeString(digest)
	return err == nil
}

func validContainerID(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func randomContainerName() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return "sessionless-aw-" + hex.EncodeToString(value), nil
}

func apiAtLeast(value, minimum string) bool {
	parse := func(input string) (int, int, bool) {
		majorText, minorText, ok := strings.Cut(input, ".")
		if !ok {
			return 0, 0, false
		}
		major, errMajor := strconv.Atoi(majorText)
		minor, errMinor := strconv.Atoi(minorText)
		return major, minor, errMajor == nil && errMinor == nil
	}
	major, minor, ok := parse(value)
	minimumMajor, minimumMinor, minimumOK := parse(minimum)
	return ok && minimumOK && (major > minimumMajor || major == minimumMajor && minor >= minimumMinor)
}

func hasNamedSecurityOption(values []string, expected string) bool {
	for _, value := range values {
		for _, field := range strings.Split(strings.ToLower(value), ",") {
			if strings.TrimSpace(field) == "name="+expected {
				return true
			}
		}
	}
	return false
}

func noNewPrivilegesOnly(values []string) bool {
	if len(values) != 1 {
		return false
	}
	value := strings.ToLower(strings.TrimSpace(values[0]))
	return value == "no-new-privileges:true" || value == "no-new-privileges=true"
}

func seccompEnabled(values []string) bool {
	for _, value := range values {
		fields := strings.Split(strings.ToLower(value), ",")
		hasName := false
		hasBuiltinProfile := false
		for _, field := range fields {
			switch strings.TrimSpace(field) {
			case "name=seccomp":
				hasName = true
			case "profile=builtin":
				hasBuiltinProfile = true
			}
		}
		if hasName && hasBuiltinProfile {
			return true
		}
	}
	return false
}

func containsExact(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func equalStringSets(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	leftCopy := append([]string(nil), left...)
	rightCopy := append([]string(nil), right...)
	sort.Strings(leftCopy)
	sort.Strings(rightCopy)
	return equalStrings(leftCopy, rightCopy)
}

func pathWithin(path, root string) bool {
	relative, err := filepath.Rel(root, path)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}
