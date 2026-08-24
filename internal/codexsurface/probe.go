package codexsurface

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"gitcode.com/urandon/sessionless/internal/codexapp"
)

var versionPattern = regexp.MustCompile(`codex-cli ([0-9A-Za-z.+-]+)`)

const (
	expectedDirectVersion = "0.148.0-alpha.15"
	expectedSDKVersion    = "0.147.0"
)

type Config struct {
	Executable       string
	PythonExecutable string
	Iterations       int
	Timeout          time.Duration
	Scratch          string
}

func Probe(ctx context.Context, surface Surface, config Config) (Report, error) {
	if config.Executable == "" && surface != SurfaceSDK {
		config.Executable = "codex"
	}
	if config.Iterations < 1 || config.Iterations > 100 {
		return Report{}, errors.New("iterations must be between 1 and 100")
	}
	if config.Timeout <= 0 {
		config.Timeout = 20 * time.Second
	}
	if config.Scratch == "" {
		config.Scratch = os.TempDir()
	}
	absoluteScratch, err := filepath.Abs(config.Scratch)
	if err != nil {
		return Report{}, errors.New("resolve scratch directory")
	}
	config.Scratch = absoluteScratch
	if config.Executable != "" {
		config.Executable, err = resolveExecutable(config.Executable)
		if err != nil {
			return Report{}, err
		}
	}
	if config.PythonExecutable != "" {
		config.PythonExecutable, err = resolveExecutable(config.PythonExecutable)
		if err != nil {
			return Report{}, err
		}
	}
	switch surface {
	case SurfaceAppServer:
		return probeAppServer(ctx, config)
	case SurfaceExec:
		return probeExec(ctx, config)
	case SurfaceSDK:
		return probeSDK(ctx, config)
	default:
		return Report{}, errors.New("surface is not implemented by the Go probe")
	}
}

func resolveExecutable(value string) (string, error) {
	if value == "" {
		return value, nil
	}
	if strings.ContainsRune(value, os.PathSeparator) {
		absolute, err := filepath.Abs(value)
		if err != nil {
			return "", errors.New("resolve probe executable")
		}
		return absolute, nil
	}
	resolved, err := exec.LookPath(value)
	if err != nil {
		return "", errors.New("probe executable is unavailable")
	}
	if filepath.IsAbs(resolved) {
		return resolved, nil
	}
	absolute, err := filepath.Abs(resolved)
	if err != nil {
		return "", errors.New("resolve probe executable")
	}
	return absolute, nil
}

func probeAppServer(ctx context.Context, config Config) (Report, error) {
	version, err := readVersion(ctx, config)
	if err != nil {
		return Report{}, err
	}
	durations := make([]time.Duration, 0, config.Iterations)
	checks := []Check{
		{Name: "binary_version_pinned", Pass: version == expectedDirectVersion},
		{Name: "chatgpt_auth_required_without_ambient_credentials", Pass: true},
		{Name: "experimental_api_disabled", Pass: true},
		{Name: "isolated_process_environment", Pass: true},
		{Name: "production_command_supported", Pass: false},
		{Name: "restricted_read_roots_explicit", Pass: false},
	}
	for range config.Iterations {
		started := time.Now()
		client, startErr := codexapp.Start(ctx, codexapp.Config{
			Executable: config.Executable, ScratchRoot: config.Scratch,
			RequestTimeout: config.Timeout, ShutdownTimeout: 3 * time.Second,
		})
		if startErr != nil {
			return Report{}, fmt.Errorf("start app-server: %w", startErr)
		}
		_, initializeErr := client.Initialize(ctx)
		if initializeErr != nil {
			_ = client.Close()
			return Report{}, fmt.Errorf("initialize app-server: %w", initializeErr)
		}
		account, accountErr := client.ReadAccount(ctx)
		closeErr := client.Close()
		if accountErr != nil {
			return Report{}, fmt.Errorf("read isolated account state: %w", accountErr)
		}
		if closeErr != nil {
			return Report{}, fmt.Errorf("close app-server: %w", closeErr)
		}
		if account.Account != nil || !account.RequiresOpenAIAuth {
			return Report{}, errors.New("isolated app-server inherited an account")
		}
		durations = append(durations, time.Since(started))
	}
	return NewReport(SurfaceAppServer, version, version, durations, checks, []string{
		"production_command_unsupported", "read_only_defaults_to_full_filesystem_read",
	}), nil
}

func probeExec(ctx context.Context, config Config) (Report, error) {
	version, err := readVersion(ctx, config)
	if err != nil {
		return Report{}, err
	}
	durations := make([]time.Duration, 0, config.Iterations)
	checks := []Check{
		{Name: "ephemeral_flag", Pass: false}, {Name: "ignore_rules_flag", Pass: false},
		{Name: "ignore_user_config_flag", Pass: false}, {Name: "jsonl_flag", Pass: false},
		{Name: "read_only_flag", Pass: false},
		{Name: "binary_version_pinned", Pass: version == expectedDirectVersion},
	}
	for range config.Iterations {
		root, pathErr := makeIsolatedRoot(config.Scratch, "sessionless-codexexec-")
		if pathErr != nil {
			return Report{}, pathErr
		}
		started := time.Now()
		command := exec.Command(config.Executable, "exec", "--help")
		command.Dir = root
		command.Env = isolatedEnvironment(root)
		output, commandErr := boundedCommandOutput(ctx, config.Timeout, command, 128<<10)
		durations = append(durations, time.Since(started))
		_ = os.RemoveAll(root)
		if commandErr != nil {
			return Report{}, errors.New("codex exec contract probe failed")
		}
		help := string(output)
		checks[0].Pass = strings.Contains(help, "--ephemeral")
		checks[1].Pass = strings.Contains(help, "--ignore-rules")
		checks[2].Pass = strings.Contains(help, "--ignore-user-config")
		checks[3].Pass = strings.Contains(help, "--json")
		checks[4].Pass = strings.Contains(help, "read-only")
	}
	return NewReport(SurfaceExec, version, version, durations, checks, nil), nil
}

type sdkProbeResult struct {
	SDKVersion                    string `json:"sdk_version"`
	RuntimeVersion                string `json:"runtime_version"`
	Initialized                   bool   `json:"initialized"`
	AccountPresent                bool   `json:"account_present"`
	RequiresAuth                  bool   `json:"requires_auth"`
	ExperimentalDefault           bool   `json:"experimental_default"`
	InheritsAmbientEnvironment    bool   `json:"inherits_ambient_environment"`
	DefaultApprovalAccepts        bool   `json:"default_approval_accepts"`
	HighLevelApprovalHandler      bool   `json:"high_level_approval_handler"`
	RestrictedReadAccessSupported bool   `json:"restricted_read_access_supported"`
	TypedRateLimitRead            bool   `json:"typed_rate_limit_read"`
}

func probeSDK(ctx context.Context, config Config) (Report, error) {
	python := config.PythonExecutable
	if python == "" {
		python = config.Executable
	}
	if python == "" || python == "codex" {
		python = "python3"
	}
	durations := make([]time.Duration, 0, config.Iterations)
	var evidence sdkProbeResult
	for range config.Iterations {
		root, err := makeIsolatedRoot(config.Scratch, "sessionless-codexsdk-")
		if err != nil {
			return Report{}, err
		}
		started := time.Now()
		command := exec.Command(python, "-I", "-c", sdkCredentialFreeProbe, root)
		command.Dir = root
		command.Env = isolatedEnvironment(root)
		output, commandErr := boundedCommandOutput(ctx, config.Timeout, command, 8<<10)
		durations = append(durations, time.Since(started))
		_ = os.RemoveAll(root)
		if commandErr != nil {
			return Report{}, errors.New("python SDK contract probe failed")
		}
		var current sdkProbeResult
		if len(output) > 8<<10 || json.Unmarshal(output, &current) != nil {
			return Report{}, errors.New("python SDK returned invalid contract evidence")
		}
		if evidence.SDKVersion != "" && current != evidence {
			return Report{}, errors.New("python SDK contract evidence changed between iterations")
		}
		evidence = current
	}
	if evidence.SDKVersion == "" || evidence.RuntimeVersion == "" {
		return Report{}, errors.New("python SDK provenance is incomplete")
	}
	checks := []Check{
		{Name: "runtime_version_pinned", Pass: evidence.RuntimeVersion == expectedSDKVersion},
		{Name: "sdk_version_pinned", Pass: evidence.SDKVersion == expectedSDKVersion},
		{Name: "chatgpt_auth_required_without_ambient_credentials", Pass: evidence.Initialized && !evidence.AccountPresent && evidence.RequiresAuth},
		{Name: "experimental_api_disabled_by_default", Pass: !evidence.ExperimentalDefault},
		{Name: "isolated_process_environment_by_default", Pass: !evidence.InheritsAmbientEnvironment},
		{Name: "approvals_fail_closed_by_default", Pass: !evidence.DefaultApprovalAccepts},
		{Name: "high_level_custom_approval_handler", Pass: evidence.HighLevelApprovalHandler},
		{Name: "restricted_read_roots_explicit", Pass: evidence.RestrictedReadAccessSupported},
		{Name: "typed_rate_limit_read", Pass: evidence.TypedRateLimitRead},
	}
	return NewReport(SurfaceSDK, evidence.SDKVersion, evidence.RuntimeVersion, durations, checks, []string{
		"ambient_environment_inherited_by_sdk", "default_approval_handler_accepts_mutations",
		"experimental_api_enabled_by_default", "read_only_defaults_to_full_filesystem_read",
		"typed_quota_read_missing",
	}), nil
}

func makeIsolatedRoot(parent, prefix string) (string, error) {
	root, err := os.MkdirTemp(parent, prefix)
	if err != nil {
		return "", errors.New("create isolated probe root")
	}
	if err := os.Chmod(root, 0o700); err != nil {
		_ = os.RemoveAll(root)
		return "", errors.New("secure isolated probe root")
	}
	for _, name := range []string{"home", "codex-home", "tmp", "workspace"} {
		if err := os.Mkdir(filepath.Join(root, name), 0o700); err != nil {
			_ = os.RemoveAll(root)
			return "", errors.New("create isolated probe directory")
		}
	}
	return root, nil
}

const sdkCredentialFreeProbe = `
import inspect, json, os, subprocess, sys
import openai_codex
from openai_codex.api import Codex
from openai_codex.client import CodexClient, CodexConfig
from openai_codex._sandbox import Sandbox, _sandbox_policy
from codex_cli_bin import bundled_codex_path

root = sys.argv[1]
runtime = subprocess.run([str(bundled_codex_path()), "--version"], check=True,
    capture_output=True, text=True, env={"HOME": root+"/home", "CODEX_HOME": root+"/codex-home",
    "TMPDIR": root+"/tmp", "PATH": "/usr/local/bin:/usr/bin:/bin", "NO_COLOR": "1"}).stdout.strip()

def reject_request(method, params):
    raise RuntimeError("unexpected server request")

config = CodexConfig(cwd=root+"/workspace", experimental_api=False,
    env={"HOME": root+"/home", "CODEX_HOME": root+"/codex-home", "TMPDIR": root+"/tmp",
         "PATH": "/usr/local/bin:/usr/bin:/bin", "NO_COLOR": "1"})
client = CodexClient(config=config, approval_handler=reject_request)
try:
    client.start()
    initialized = client.initialize()
    account = client.account_read({"refreshToken": False})
finally:
    client.close()

start_source = inspect.getsource(CodexClient.start)
default_source = inspect.getsource(CodexClient._default_approval_handler)
high_signature = inspect.signature(Codex)
policy = _sandbox_policy(Sandbox.read_only).model_dump(by_alias=True, exclude_none=True, mode="json")
policy_root = policy.get("root", policy)
result = {
    "sdk_version": openai_codex.__version__,
    "runtime_version": runtime.removeprefix("codex-cli "),
    "initialized": bool(initialized.userAgent and initialized.platformFamily and initialized.platformOs),
    "account_present": account.account is not None,
    "requires_auth": account.requires_openai_auth,
    "experimental_default": CodexConfig().experimental_api,
    "inherits_ambient_environment": "os.environ.copy()" in start_source,
    "default_approval_accepts": "decision\": \"accept" in default_source,
    "high_level_approval_handler": "approval_handler" in high_signature.parameters,
    "restricted_read_access_supported": "access" in policy_root,
    "typed_rate_limit_read": hasattr(client, "account_rate_limits_read"),
}
print(json.dumps(result, sort_keys=True, separators=(",", ":")))
`

func readVersion(ctx context.Context, config Config) (string, error) {
	command := exec.Command(config.Executable, "--version")
	command.Env = []string{"HOME=/nonexistent", "CODEX_HOME=/nonexistent", "PATH=/usr/local/bin:/usr/bin:/bin", "NO_COLOR=1"}
	output, err := boundedCommandOutput(ctx, config.Timeout, command, 64<<10)
	if err != nil {
		return "", errors.New("read codex version")
	}
	match := versionPattern.FindStringSubmatch(string(output))
	if len(match) != 2 {
		return "", errors.New("unrecognized codex version")
	}
	return match[1], nil
}

type boundedOutput struct {
	mu        sync.Mutex
	limit     int
	data      []byte
	truncated bool
}

func (output *boundedOutput) Write(value []byte) (int, error) {
	output.mu.Lock()
	defer output.mu.Unlock()
	written := len(value)
	remaining := output.limit - len(output.data)
	if remaining > len(value) {
		remaining = len(value)
	}
	if remaining > 0 {
		output.data = append(output.data, value[:remaining]...)
	}
	if remaining < len(value) {
		output.truncated = true
	}
	return written, nil
}

func (output *boundedOutput) bytes() ([]byte, bool) {
	output.mu.Lock()
	defer output.mu.Unlock()
	return append([]byte(nil), output.data...), output.truncated
}

func boundedCommandOutput(parent context.Context, timeout time.Duration, command *exec.Cmd, limit int) ([]byte, error) {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	stdout := &boundedOutput{limit: limit}
	stderr := &boundedOutput{limit: limit}
	command.Stdout = stdout
	command.Stderr = stderr
	configureProbeProcessGroup(command)
	if err := command.Start(); err != nil {
		return nil, errors.New("start bounded probe command")
	}
	done := make(chan error, 1)
	go func() { done <- command.Wait() }()
	select {
	case err := <-done:
		data, truncated := stdout.bytes()
		_, stderrTruncated := stderr.bytes()
		if err != nil {
			return nil, errors.New("bounded probe command failed")
		}
		if truncated || stderrTruncated {
			return nil, errors.New("bounded probe command exceeded output limit")
		}
		return data, nil
	case <-ctx.Done():
		killProbeProcessGroup(command)
		<-done
		return nil, errors.New("bounded probe command timed out")
	}
}

var _ io.Writer = (*boundedOutput)(nil)

func isolatedEnvironment(root string) []string {
	return []string{
		"HOME=" + filepath.Join(root, "home"),
		"CODEX_HOME=" + filepath.Join(root, "codex-home"),
		"TMPDIR=" + filepath.Join(root, "tmp"),
		"PATH=/usr/local/bin:/usr/bin:/bin", "LANG=C.UTF-8", "LC_ALL=C.UTF-8", "NO_COLOR=1",
	}
}
