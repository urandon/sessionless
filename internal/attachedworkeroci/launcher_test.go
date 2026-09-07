package attachedworkeroci

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"gitcode.com/urandon/sessionless/internal/attachedworkerdaemon"
)

const testContainerID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

type fakeCall struct {
	directory   string
	environment []string
	arguments   []string
}

type fakeRunner struct {
	mu                  sync.Mutex
	path                string
	version             versionResponse
	info                infoResponse
	imageInspect        imageResponse
	containerInspect    containerResponse
	state               stateResponse
	failCommand         string
	malformedCreateID   bool
	preserveInspectName bool
	listIDs             []string
	calls               []fakeCall
}

func (runner *fakeRunner) Run(_ context.Context, directory string, environment, arguments []string) ([]byte, []byte, error) {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	call := fakeCall{directory: directory, environment: append([]string(nil), environment...), arguments: append([]string(nil), arguments...)}
	runner.calls = append(runner.calls, call)
	command := dockerCommand(arguments)
	if runner.failCommand != "" && strings.Join(command, " ") == runner.failCommand {
		return nil, []byte("redacted fake failure"), errors.New("fake docker failure")
	}
	switch {
	case reflect.DeepEqual(command, []string{"version", "--format", "{{json .}}"}):
		value, _ := json.Marshal(runner.version)
		return value, nil, nil
	case reflect.DeepEqual(command, []string{"info", "--format", "{{json .}}"}):
		value, _ := json.Marshal(runner.info)
		return value, nil, nil
	case len(command) == 5 && reflect.DeepEqual(command[:4], []string{"image", "inspect", "--format", "{{json .}}"}):
		value, _ := json.Marshal(runner.imageInspect)
		return value, nil, nil
	case len(command) > 2 && command[0] == "container" && command[1] == "create":
		if !runner.preserveInspectName {
			for index := range command {
				if command[index] == "--name" && index+1 < len(command) {
					runner.containerInspect.Name = "/" + command[index+1]
				}
			}
		}
		if runner.malformedCreateID {
			return []byte("not-a-container-id\n"), nil, nil
		}
		return []byte(testContainerID + "\n"), nil, nil
	case len(command) == 5 && reflect.DeepEqual(command[:4], []string{"container", "inspect", "--format", "{{json .State}}"}):
		value, _ := json.Marshal(runner.state)
		return value, nil, nil
	case len(command) == 5 && reflect.DeepEqual(command[:4], []string{"container", "inspect", "--format", "{{json .}}"}):
		inspected := runner.containerInspect
		if command[4] != testContainerID {
			inspected.ID = command[4]
		}
		value, _ := json.Marshal(inspected)
		return value, nil, nil
	case len(command) >= 4 && reflect.DeepEqual(command[:4], []string{"container", "ls", "--all", "--quiet"}):
		return []byte(strings.Join(runner.listIDs, "\n")), nil, nil
	case len(command) >= 3 && command[0] == "container" && (command[1] == "stop" || command[1] == "kill" || command[1] == "rm"):
		return nil, nil, nil
	default:
		return nil, nil, errors.New("unexpected fake docker command")
	}
}

func (runner *fakeRunner) Command(directory string, environment, arguments []string) *exec.Cmd {
	command := exec.Command(runner.path, arguments...)
	command.Dir = directory
	command.Env = append([]string(nil), environment...)
	return command
}

func (runner *fakeRunner) snapshot() []fakeCall {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	result := make([]fakeCall, len(runner.calls))
	copy(result, runner.calls)
	return result
}

func dockerCommand(arguments []string) []string {
	if len(arguments) < 4 || arguments[0] != "--config" || arguments[2] != "--host" {
		return nil
	}
	return arguments[4:]
}

type fixture struct {
	config     Config
	runner     *fakeRunner
	spec       attachedworkerdaemon.LaunchSpec
	attempt    string
	credential string
	image      string
}

func newFixture(t *testing.T, hostOS string, withCredential bool) fixture {
	t.Helper()
	root := canonicalTempDir(t)
	dockerPath := filepath.Join(root, "docker")
	if err := os.WriteFile(dockerPath, []byte("fixture"), 0o700); err != nil {
		t.Fatal(err)
	}
	cliConfig := filepath.Join(root, "docker-config")
	if err := os.Mkdir(cliConfig, 0o700); err != nil {
		t.Fatal(err)
	}
	binDir := filepath.Join(root, "bin")
	if err := os.Mkdir(binDir, 0o700); err != nil {
		t.Fatal(err)
	}
	executable := filepath.Join(binDir, "harness")
	if err := os.WriteFile(executable, []byte("pinned harness"), 0o700); err != nil {
		t.Fatal(err)
	}
	attempt := filepath.Join(root, "attempt")
	work := filepath.Join(attempt, "work")
	if err := os.MkdirAll(work, 0o700); err != nil {
		t.Fatal(err)
	}
	digest, err := attachedworkerdaemon.DigestExecutable(executable)
	if err != nil {
		t.Fatal(err)
	}
	readRoots := []string{attempt}
	var writeFiles []string
	credential := ""
	if withCredential {
		credentialRoot := filepath.Join(root, "credential")
		if err := os.Mkdir(credentialRoot, 0o700); err != nil {
			t.Fatal(err)
		}
		credential = filepath.Join(credentialRoot, "auth.json")
		if err := os.WriteFile(credential, []byte(`{"token":"old"}`), 0o600); err != nil {
			t.Fatal(err)
		}
		readRoots = append(readRoots, credentialRoot)
		writeFiles = []string{credential}
	}
	image := "registry.example/sessionless-worker@sha256:" + strings.Repeat("c", 64)
	runner := &fakeRunner{path: dockerPath, state: stateResponse{}}
	runner.version.Client.APIVersion = "1.45"
	runner.version.Server.APIVersion = "1.45"
	runner.version.Server.OS = "linux"
	runner.info = infoResponse{
		ID: "fixture-engine-0001", OSType: "linux", OperatingSystem: "Docker Engine",
		CgroupVersion: "2", MemoryLimit: true, PidsLimit: true, SwapLimit: true,
	}
	runner.imageInspect = imageResponse{
		ID: "sha256:" + strings.Repeat("b", 64), RepoDigests: []string{image}, OS: "linux",
	}
	if hostOS == "linux" {
		runner.info.SecurityOptions = []string{"name=seccomp,profile=builtin", "name=rootless"}
	} else {
		runner.info.OperatingSystem = "Docker Desktop"
		runner.info.SecurityOptions = []string{"name=seccomp,profile=builtin"}
	}
	spec := attachedworkerdaemon.LaunchSpec{
		Executable: executable, ExecutableDigest: digest, Arguments: []string{"exec", "--json"},
		Directory: work, Environment: []string{"HOME=" + filepath.Join(attempt, "home"), "TMPDIR=" + filepath.Join(attempt, "tmp")},
		ReadFiles: []string{executable}, ReadRoots: readRoots, WriteFiles: writeFiles, WriteRoots: []string{attempt},
	}
	result := fixture{
		config: Config{
			DockerPath: dockerPath, CLIConfigDir: cliConfig, Host: "unix:///private/tmp/sessionless-docker.sock",
			EngineID: "fixture-engine-0001", InstallationID: "fixture-installation-0001",
			Image: image, UserID: 65532, GroupID: 65532, DiskBytes: 32 << 20,
			CredentialFileBytes: 1 << 20, MemoryBytes: 128 << 20, PIDsLimit: 32,
			runner: runner, hostOS: hostOS,
		},
		runner: runner, spec: spec, attempt: attempt, credential: credential, image: image,
	}
	if hostOS == "darwin" {
		result.config.Boundary = BoundaryDarwinVM
	} else {
		result.config.Boundary = BoundaryLinuxRootless
	}
	return result
}

func canonicalTempDir(t *testing.T) string {
	t.Helper()
	value, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func prepareInspect(fixture fixture) containerResponse {
	truth := true
	stopSeconds := fixture.config.StopSeconds
	if stopSeconds == 0 {
		stopSeconds = defaultStopSecond
	}
	response := containerResponse{ID: testContainerID}
	response.Name = "/sessionless-aw-" + strings.Repeat("d", 32)
	response.State.Status = "created"
	response.Config.Image = fixture.image
	response.Config.User = "65532:65532"
	response.Config.WorkingDir = fixture.spec.Directory
	response.Config.StopSignal = "SIGTERM"
	response.Config.StopTimeout = &stopSeconds
	response.Config.Entrypoint = []string{fixture.spec.Executable}
	response.Config.Cmd = append([]string(nil), fixture.spec.Arguments...)
	response.Config.Env = append([]string(nil), fixture.spec.Environment...)
	response.Config.Healthcheck = &healthcheckConfig{Test: []string{"NONE"}}
	response.Config.Labels = map[string]string{
		profileLabel: profileName, installationLabel: "fixture-installation-0001", engineLabel: "fixture-engine-0001",
	}
	response.HostConfig.NetworkMode = "none"
	response.HostConfig.ReadonlyRootfs = true
	response.HostConfig.IpcMode = "private"
	response.HostConfig.CgroupnsMode = "private"
	response.HostConfig.CapDrop = []string{"ALL"}
	response.HostConfig.SecurityOpt = []string{"no-new-privileges:true"}
	response.HostConfig.PidsLimit = 32
	response.HostConfig.Memory = 128 << 20
	response.HostConfig.MemorySwap = 128 << 20
	response.HostConfig.ShmSize = sharedMemoryBytes
	response.HostConfig.Tmpfs = map[string]string{fixture.attempt: "rw,nosuid,nodev,noexec,size=31457280,mode=0700,uid=65532,gid=65532"}
	response.HostConfig.Ulimits = []ulimitConfig{{Name: "fsize", Soft: 1 << 20, Hard: 1 << 20}}
	response.HostConfig.Init = &truth
	response.HostConfig.LogConfig.Type = "none"
	response.HostConfig.RestartPolicy.Name = "no"
	response.Mounts = append(response.Mounts,
		struct {
			Type        string `json:"Type"`
			Source      string `json:"Source"`
			Destination string `json:"Destination"`
			RW          bool   `json:"RW"`
		}{Type: "bind", Source: fixture.spec.Executable, Destination: fixture.spec.Executable},
	)
	for _, root := range fixture.spec.ReadRoots {
		if root != fixture.attempt {
			response.Mounts = append(response.Mounts, struct {
				Type        string `json:"Type"`
				Source      string `json:"Source"`
				Destination string `json:"Destination"`
				RW          bool   `json:"RW"`
			}{Type: "bind", Source: root, Destination: root})
		}
	}
	if fixture.credential != "" {
		response.Mounts = append(response.Mounts, struct {
			Type        string `json:"Type"`
			Source      string `json:"Source"`
			Destination string `json:"Destination"`
			RW          bool   `json:"RW"`
		}{Type: "bind", Source: filepath.Join(fixture.attempt, ".sessionless-auth-stage"), Destination: fixture.credential, RW: true})
	}
	return response
}

func TestLauncherPreflightAndBoundaryLifecycle(t *testing.T) {
	fixture := newFixture(t, "linux", true)
	fixture.runner.containerInspect = prepareInspect(fixture)
	launcher, err := NewLauncher(context.Background(), fixture.config)
	if err != nil {
		t.Fatalf("NewLauncher() error = %v", err)
	}
	if err := launcher.Profile().Validate(); err != nil {
		t.Fatalf("Profile().Validate() error = %v", err)
	}
	prepared, err := launcher.Prepare(context.Background(), fixture.spec)
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	command := prepared.Command()
	if command.Dir != fixture.spec.Directory || !reflect.DeepEqual(command.Env, fixture.spec.Environment) {
		t.Fatalf("Command() directory/environment = %q/%#v", command.Dir, command.Env)
	}
	if got := dockerCommand(command.Args[1:]); !reflect.DeepEqual(got, []string{"container", "start", "--attach", "--interactive", testContainerID}) {
		t.Fatalf("Command() docker arguments = %#v", got)
	}
	attestation := prepared.AttestedWorkload()
	if attestation.Executable != fixture.spec.Executable || attestation.ExecutableDigest != fixture.spec.ExecutableDigest ||
		!reflect.DeepEqual(attestation.Arguments, fixture.spec.Arguments) {
		t.Fatalf("AttestedWorkload() = %#v", attestation)
	}
	alive, err := prepared.Alive(context.Background())
	if err != nil || alive {
		t.Fatalf("Alive() = %t, %v", alive, err)
	}
	if err := os.WriteFile(filepath.Join(fixture.attempt, ".sessionless-auth-stage"), []byte(`{"token":"new"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := prepared.Release(context.Background()); err != nil {
		t.Fatalf("Release() error = %v", err)
	}
	if err := prepared.Release(context.Background()); err != nil {
		t.Fatalf("second Release() error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(fixture.attempt, ".sessionless-auth-stage")); !os.IsNotExist(err) {
		t.Fatalf("credential stage survived release: %v", err)
	}
	contents, err := os.ReadFile(fixture.credential)
	if err != nil || string(contents) != `{"token":"new"}` {
		t.Fatalf("credential after Release() = %q, %v", contents, err)
	}
	calls := fixture.runner.snapshot()
	var create []string
	rmCount := 0
	for _, call := range calls {
		command := dockerCommand(call.arguments)
		if len(command) >= 2 && command[0] == "container" && command[1] == "create" {
			create = command
		}
		if len(command) >= 2 && command[0] == "container" && command[1] == "rm" {
			rmCount++
		}
	}
	for _, required := range []string{
		"--network", "none", "--read-only", "--cap-drop", "ALL", "--log-driver", "none", "--pull", "never",
		"--no-healthcheck", "--ipc", "private", "--cgroupns",
	} {
		if !containsExact(create, required) {
			t.Errorf("container create arguments missing %q: %#v", required, create)
		}
	}
	if rmCount != 1 {
		t.Errorf("container remove count = %d, want 1", rmCount)
	}
}

func TestLauncherAcceptsDockerDesktopBoundary(t *testing.T) {
	fixture := newFixture(t, "darwin", false)
	fixture.runner.containerInspect = prepareInspect(fixture)
	launcher, err := NewLauncher(context.Background(), fixture.config)
	if err != nil {
		t.Fatalf("NewLauncher() error = %v", err)
	}
	if _, err := launcher.Prepare(context.Background(), fixture.spec); err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
}

func TestLauncherRejectsUnsupportedPreflight(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*fixture)
	}{
		{name: "implicit image tag", mutate: func(value *fixture) { value.config.Image = "registry.example/sessionless-worker:latest" }},
		{name: "ambient docker host", mutate: func(value *fixture) { value.config.Host = "" }},
		{name: "changed engine identity", mutate: func(value *fixture) { value.config.EngineID = "different-engine-0001" }},
		{name: "old client API", mutate: func(value *fixture) { value.runner.version.Client.APIVersion = "1.41" }},
		{name: "old server API", mutate: func(value *fixture) { value.runner.version.Server.APIVersion = "1.41" }},
		{name: "non-linux server", mutate: func(value *fixture) { value.runner.version.Server.OS = "windows" }},
		{name: "non-linux engine", mutate: func(value *fixture) { value.runner.info.OSType = "windows" }},
		{name: "cgroup v1", mutate: func(value *fixture) { value.runner.info.CgroupVersion = "1" }},
		{name: "memory limit unavailable", mutate: func(value *fixture) { value.runner.info.MemoryLimit = false }},
		{name: "PID limit unavailable", mutate: func(value *fixture) { value.runner.info.PidsLimit = false }},
		{name: "swap limit unavailable", mutate: func(value *fixture) { value.runner.info.SwapLimit = false }},
		{name: "rootful linux daemon", mutate: func(value *fixture) { value.runner.info.SecurityOptions = []string{"name=seccomp,profile=builtin"} }},
		{name: "misleading rootless option", mutate: func(value *fixture) {
			value.runner.info.SecurityOptions = []string{"name=seccomp,profile=builtin", "name=notrootless"}
		}},
		{name: "unconfined seccomp", mutate: func(value *fixture) {
			value.runner.info.SecurityOptions = []string{"name=seccomp,profile=unconfined", "name=rootless"}
		}},
		{name: "custom seccomp", mutate: func(value *fixture) {
			value.runner.info.SecurityOptions = []string{"name=seccomp,profile=custom", "name=rootless"}
		}},
		{name: "non-linux image", mutate: func(value *fixture) { value.runner.imageInspect.OS = "windows" }},
		{name: "legacy image volume", mutate: func(value *fixture) { value.runner.imageInspect.Volumes = map[string]any{"/data": struct{}{}} }},
		{name: "image volume", mutate: func(value *fixture) { value.runner.imageInspect.Config.Volumes = map[string]any{"/data": struct{}{}} }},
		{name: "image environment", mutate: func(value *fixture) { value.runner.imageInspect.Config.Env = []string{"TOKEN=unexpected"} }},
		{name: "image exposed port", mutate: func(value *fixture) {
			value.runner.imageInspect.Config.ExposedPorts = map[string]any{"80/tcp": struct{}{}}
		}},
		{name: "image healthcheck", mutate: func(value *fixture) {
			value.runner.imageInspect.Config.Healthcheck = &healthcheckConfig{Test: []string{"CMD", "false"}}
		}},
		{name: "digest mismatch", mutate: func(value *fixture) {
			value.runner.imageInspect.RepoDigests = []string{"registry.example/other@sha256:" + strings.Repeat("d", 64)}
		}},
		{name: "version command failure", mutate: func(value *fixture) { value.runner.failCommand = "version --format {{json .}}" }},
		{name: "info command failure", mutate: func(value *fixture) { value.runner.failCommand = "info --format {{json .}}" }},
		{name: "image inspect failure", mutate: func(value *fixture) { value.runner.failCommand = "image inspect --format {{json .}} " + value.image }},
		{name: "nonempty CLI config", mutate: func(value *fixture) {
			if err := os.WriteFile(filepath.Join(value.config.CLIConfigDir, "config.json"), []byte("{}"), 0o600); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newFixture(t, "linux", false)
			test.mutate(&fixture)
			if _, err := NewLauncher(context.Background(), fixture.config); err == nil {
				t.Fatal("NewLauncher() error = nil")
			}
		})
	}
}

func TestReconcileRemovesOnlyValidatedOwnedContainerIDs(t *testing.T) {
	fixture := newFixture(t, "linux", false)
	fixture.runner.containerInspect = prepareInspect(fixture)
	fixture.runner.listIDs = []string{testContainerID, strings.Repeat("b", 64)}
	launcher, err := NewLauncher(context.Background(), fixture.config)
	if err != nil {
		t.Fatal(err)
	}
	removedCount, err := launcher.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if removedCount != 2 {
		t.Fatalf("Reconcile() count = %d, want 2", removedCount)
	}
	calls := fixture.runner.snapshot()
	var list []string
	var removed []string
	for _, call := range calls {
		command := dockerCommand(call.arguments)
		if len(command) >= 2 && command[0] == "container" && command[1] == "ls" {
			list = command
		}
		if len(command) >= 2 && command[0] == "container" && command[1] == "rm" {
			removed = append(removed, command[len(command)-1])
		}
	}
	for _, filter := range []string{
		"label=" + profileLabel + "=" + profileName,
		"label=" + installationLabel + "=fixture-installation-0001",
		"label=" + engineLabel + "=fixture-engine-0001",
	} {
		if !containsExact(list, filter) {
			t.Errorf("container ls missing filter %q: %#v", filter, list)
		}
	}
	if !reflect.DeepEqual(removed, fixture.runner.listIDs) {
		t.Fatalf("removed IDs = %#v, want %#v", removed, fixture.runner.listIDs)
	}
}

func TestReconcileRejectsMalformedListBeforeMutation(t *testing.T) {
	fixture := newFixture(t, "linux", false)
	fixture.runner.containerInspect = prepareInspect(fixture)
	fixture.runner.listIDs = []string{testContainerID, "not-an-id"}
	launcher, err := NewLauncher(context.Background(), fixture.config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := launcher.Reconcile(context.Background()); !errors.Is(err, ErrBoundary) {
		t.Fatalf("Reconcile() error = %v, want ErrBoundary", err)
	}
	for _, call := range fixture.runner.snapshot() {
		command := dockerCommand(call.arguments)
		if len(command) >= 2 && command[0] == "container" && command[1] == "rm" {
			t.Fatalf("Reconcile() mutated after malformed list: %#v", command)
		}
	}
}

func TestReconcileRejectsDuplicateListBeforeMutation(t *testing.T) {
	fixture := newFixture(t, "linux", false)
	fixture.runner.containerInspect = prepareInspect(fixture)
	fixture.runner.listIDs = []string{testContainerID, testContainerID}
	launcher, err := NewLauncher(context.Background(), fixture.config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := launcher.Reconcile(context.Background()); !errors.Is(err, ErrBoundary) {
		t.Fatalf("Reconcile() error = %v, want ErrBoundary", err)
	}
	for _, call := range fixture.runner.snapshot() {
		command := dockerCommand(call.arguments)
		if len(command) >= 2 && command[0] == "container" && command[1] == "rm" {
			t.Fatalf("Reconcile() mutated after duplicate list: %#v", command)
		}
	}
}

func TestPrepareRejectsUnsafeLaunchSpecs(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*attachedworkerdaemon.LaunchSpec)
	}{
		{name: "root read", mutate: func(spec *attachedworkerdaemon.LaunchSpec) {
			spec.ReadRoots = append(spec.ReadRoots, string(filepath.Separator))
		}},
		{name: "missing attempt root", mutate: func(spec *attachedworkerdaemon.LaunchSpec) { spec.ReadRoots = spec.ReadRoots[1:] }},
		{name: "extra write root", mutate: func(spec *attachedworkerdaemon.LaunchSpec) { spec.WriteRoots = append(spec.WriteRoots, spec.Directory) }},
		{name: "write outside read roots", mutate: func(spec *attachedworkerdaemon.LaunchSpec) { spec.ReadRoots = []string{spec.WriteRoots[0]} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newFixture(t, "linux", true)
			fixture.runner.containerInspect = prepareInspect(fixture)
			launcher, err := NewLauncher(context.Background(), fixture.config)
			if err != nil {
				t.Fatal(err)
			}
			test.mutate(&fixture.spec)
			if _, err := launcher.Prepare(context.Background(), fixture.spec); !errors.Is(err, ErrConfig) {
				t.Fatalf("Prepare() error = %v, want ErrConfig", err)
			}
		})
	}
}

func TestPrepareCleansCreatedContainerAfterInspectionFailure(t *testing.T) {
	fixture := newFixture(t, "linux", false)
	fixture.runner.containerInspect = prepareInspect(fixture)
	fixture.runner.containerInspect.HostConfig.NetworkMode = "bridge"
	launcher, err := NewLauncher(context.Background(), fixture.config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := launcher.Prepare(context.Background(), fixture.spec); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("Prepare() error = %v, want ErrUnsupported", err)
	}
	calls := fixture.runner.snapshot()
	last := dockerCommand(calls[len(calls)-1].arguments)
	if len(last) != 5 || !reflect.DeepEqual(last[:4], []string{"container", "rm", "--force", "--volumes"}) ||
		!strings.HasPrefix(last[4], "sessionless-aw-") {
		t.Fatalf("cleanup command = %#v", last)
	}
}

func TestPrepareRejectsReusedContainerIdentityWithoutDeletingItsID(t *testing.T) {
	fixture := newFixture(t, "linux", false)
	fixture.runner.containerInspect = prepareInspect(fixture)
	fixture.runner.containerInspect.Name = "/some-other-container"
	fixture.runner.preserveInspectName = true
	launcher, err := NewLauncher(context.Background(), fixture.config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := launcher.Prepare(context.Background(), fixture.spec); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("Prepare() error = %v, want ErrUnsupported", err)
	}
	calls := fixture.runner.snapshot()
	last := dockerCommand(calls[len(calls)-1].arguments)
	if len(last) == 0 || last[len(last)-1] == testContainerID || !strings.HasPrefix(last[len(last)-1], "sessionless-aw-") {
		t.Fatalf("cleanup used untrusted create output: %#v", last)
	}
}

func TestPrepareFailureRemovesCredentialStage(t *testing.T) {
	fixture := newFixture(t, "linux", true)
	fixture.runner.containerInspect = prepareInspect(fixture)
	fixture.runner.containerInspect.HostConfig.NetworkMode = "bridge"
	launcher, err := NewLauncher(context.Background(), fixture.config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := launcher.Prepare(context.Background(), fixture.spec); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("Prepare() error = %v, want ErrUnsupported", err)
	}
	if _, err := os.Stat(filepath.Join(fixture.attempt, ".sessionless-auth-stage")); !os.IsNotExist(err) {
		t.Fatalf("credential stage survived failed Prepare(): %v", err)
	}
}

func TestPrepareRejectsAlteredIsolationState(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*containerResponse)
	}{
		{name: "healthcheck", mutate: func(value *containerResponse) { value.Config.Healthcheck.Test = []string{"CMD", "false"} }},
		{name: "working directory", mutate: func(value *containerResponse) { value.Config.WorkingDir = "/" }},
		{name: "shared memory", mutate: func(value *containerResponse) { value.HostConfig.ShmSize++ }},
		{name: "host pid namespace", mutate: func(value *containerResponse) { value.HostConfig.PidMode = "host" }},
		{name: "host ipc namespace", mutate: func(value *containerResponse) { value.HostConfig.IpcMode = "host" }},
		{name: "host cgroup namespace", mutate: func(value *containerResponse) { value.HostConfig.CgroupnsMode = "host" }},
		{name: "host user namespace", mutate: func(value *containerResponse) { value.HostConfig.UsernsMode = "host" }},
		{name: "unexpected user namespace", mutate: func(value *containerResponse) { value.HostConfig.UsernsMode = "private" }},
		{name: "added capability", mutate: func(value *containerResponse) { value.HostConfig.CapAdd = []string{"SYS_ADMIN"} }},
		{name: "misleading no-new-privileges", mutate: func(value *containerResponse) {
			value.HostConfig.SecurityOpt = []string{"not-no-new-privileges:true"}
		}},
		{name: "device", mutate: func(value *containerResponse) { value.HostConfig.Devices = []json.RawMessage{json.RawMessage(`{}`)} }},
		{name: "published port", mutate: func(value *containerResponse) { value.HostConfig.PortBindings = map[string]any{"80/tcp": []any{}} }},
		{name: "file limit", mutate: func(value *containerResponse) { value.HostConfig.Ulimits[0].Hard++ }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newFixture(t, "linux", false)
			fixture.runner.containerInspect = prepareInspect(fixture)
			test.mutate(&fixture.runner.containerInspect)
			launcher, err := NewLauncher(context.Background(), fixture.config)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := launcher.Prepare(context.Background(), fixture.spec); !errors.Is(err, ErrUnsupported) {
				t.Fatalf("Prepare() error = %v, want ErrUnsupported", err)
			}
		})
	}
}

func TestPrepareCleansMalformedCreateByPreselectedName(t *testing.T) {
	fixture := newFixture(t, "linux", false)
	fixture.runner.containerInspect = prepareInspect(fixture)
	fixture.runner.malformedCreateID = true
	launcher, err := NewLauncher(context.Background(), fixture.config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := launcher.Prepare(context.Background(), fixture.spec); !errors.Is(err, ErrBoundary) {
		t.Fatalf("Prepare() error = %v, want ErrBoundary", err)
	}
	calls := fixture.runner.snapshot()
	var createdName string
	var removedReference string
	for _, call := range calls {
		command := dockerCommand(call.arguments)
		if len(command) >= 2 && command[0] == "container" && command[1] == "create" {
			for index := range command {
				if command[index] == "--name" && index+1 < len(command) {
					createdName = command[index+1]
				}
			}
		}
		if len(command) >= 3 && command[0] == "container" && command[1] == "rm" {
			removedReference = command[len(command)-1]
		}
	}
	if createdName == "" || removedReference != createdName || !strings.HasPrefix(createdName, "sessionless-aw-") {
		t.Fatalf("create/cleanup references = %q/%q", createdName, removedReference)
	}
}

func TestCredentialWritebackRejectsOversize(t *testing.T) {
	fixture := newFixture(t, "linux", true)
	fixture.runner.containerInspect = prepareInspect(fixture)
	launcher, err := NewLauncher(context.Background(), fixture.config)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := launcher.Prepare(context.Background(), fixture.spec)
	if err != nil {
		t.Fatal(err)
	}
	stage := filepath.Join(fixture.attempt, ".sessionless-auth-stage")
	file, err := os.OpenFile(stage, os.O_WRONLY|os.O_TRUNC, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(fixture.config.CredentialFileBytes + 1); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	if err := prepared.Release(context.Background()); !errors.Is(err, ErrBoundary) {
		t.Fatalf("Release() error = %v, want ErrBoundary", err)
	}
	contents, err := os.ReadFile(fixture.credential)
	if err != nil || string(contents) != `{"token":"old"}` {
		t.Fatalf("credential after rejected writeback = %q, %v", contents, err)
	}
	if _, err := os.Stat(stage); !os.IsNotExist(err) {
		t.Fatalf("oversize credential stage survived release: %v", err)
	}
}

func TestCredentialWritebackRequiresStoppedContainer(t *testing.T) {
	fixture := newFixture(t, "linux", true)
	fixture.runner.containerInspect = prepareInspect(fixture)
	launcher, err := NewLauncher(context.Background(), fixture.config)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := launcher.Prepare(context.Background(), fixture.spec)
	if err != nil {
		t.Fatal(err)
	}
	stage := filepath.Join(fixture.attempt, ".sessionless-auth-stage")
	if err := os.WriteFile(stage, []byte(`{"token":"new"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	fixture.runner.state.Running = true
	if err := prepared.Release(context.Background()); !errors.Is(err, ErrBoundary) {
		t.Fatalf("Release() error = %v, want ErrBoundary", err)
	}
	contents, err := os.ReadFile(fixture.credential)
	if err != nil || string(contents) != `{"token":"old"}` {
		t.Fatalf("credential changed while container was running: %q, %v", contents, err)
	}
	if _, err := os.Stat(stage); !os.IsNotExist(err) {
		t.Fatalf("credential stage survived rejected release: %v", err)
	}
}
