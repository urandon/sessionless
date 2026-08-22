package codexapp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestBoundedSubscriptionLifecycle(t *testing.T) {
	client := startHelper(t, "lifecycle", Config{})
	defer client.Close()

	initialized, err := client.Initialize(context.Background())
	if err != nil {
		client.stderr.mu.Lock()
		stderr := string(client.stderr.data)
		client.stderr.mu.Unlock()
		t.Fatalf("Initialize() error = %v; helper stderr=%q", err, stderr)
	}
	if initialized.UserAgent != "fake-codex/0.148.0-alpha.15" || initialized.CodexHome != client.Paths().CodexHome {
		t.Fatalf("Initialize() = %#v", initialized)
	}
	device, err := client.StartDeviceCodeLogin(context.Background())
	if err != nil {
		t.Fatalf("StartDeviceCodeLogin() error = %v", err)
	}
	if device.LoginID != "login-exact" || device.UserCode != "ABCD-EFGH" {
		t.Fatalf("device code = %#v", device)
	}
	login, err := client.WaitDeviceCodeLogin(context.Background(), device.LoginID)
	if err != nil || !login.Success {
		t.Fatalf("WaitDeviceCodeLogin() = %#v, %v", login, err)
	}
	account, err := client.ReadAccount(context.Background())
	if err != nil || account.Account == nil || account.Account.Type != "chatgpt" || account.Account.PlanType != "plus" {
		t.Fatalf("ReadAccount() = %#v, %v", account, err)
	}
	if got := account.Observation().State; got != ProviderStateAvailable {
		t.Fatalf("account observation = %q", got)
	}
	limits, err := client.ReadRateLimits(context.Background())
	if err != nil {
		t.Fatalf("ReadRateLimits() error = %v", err)
	}
	waitFor(t, func() bool {
		current := client.CurrentRateLimits().Current
		return current.Primary != nil && current.Primary.UsedPercent != nil && *current.Primary.UsedPercent == 20
	})
	limits = client.CurrentRateLimits()
	if limits.Current.Primary == nil || limits.Current.Primary.WindowDurationMins == nil ||
		*limits.Current.Primary.WindowDurationMins != 300 || limits.Current.LimitName == nil ||
		*limits.Current.LimitName != "Codex" {
		t.Fatalf("sparse update cleared authoritative values: %#v", limits.Current)
	}
	if got := limits.Observation(); got.State != ProviderStateAvailable || got.ResetAt != nil {
		t.Fatalf("rate-limit observation = %#v", got)
	}
	limits, err = client.ReadRateLimits(context.Background())
	if err != nil || limits.Current.IndividualLimit != nil {
		t.Fatalf("authoritative full read did not clear nullable field: %#v, %v", limits.Current, err)
	}

	thread, err := client.StartThread(context.Background())
	if err != nil || thread.ID != "thread-exact" {
		t.Fatalf("StartThread() = %#v, %v", thread, err)
	}
	turn, err := client.StartTurn(context.Background(), thread.ID, "bounded prompt", "message-exact")
	if err != nil || turn.ID != "turn-exact" || turn.ThreadID != thread.ID {
		t.Fatalf("StartTurn() = %#v, %v", turn, err)
	}
	result, err := client.WaitTurn(context.Background(), thread.ID, turn.ID)
	if err != nil || result.Status != "completed" || result.OutputText != "bounded answer" {
		t.Fatalf("WaitTurn() = %#v, %v", result, err)
	}
	if err := client.InterruptTurn(context.Background(), thread.ID, turn.ID); err != nil {
		t.Fatalf("InterruptTurn() error = %v", err)
	}
}

func TestRejectsAPIKeyAccountWithoutLeakingHostEnvironment(t *testing.T) {
	t.Setenv("OPENAI_API_KEY", "host-secret-must-not-reach-child")
	t.Setenv("SESSIONLESS_HOST_MARKER", "host-marker-must-not-reach-child")
	client := startHelper(t, "api-key", Config{})
	defer client.Close()
	if _, err := client.Initialize(context.Background()); err != nil {
		t.Fatalf("Initialize() error = %v", err)
	}
	_, err := client.ReadAccount(context.Background())
	if !errors.Is(err, ErrUnsupportedAuth) {
		t.Fatalf("ReadAccount() error = %v, want ErrUnsupportedAuth", err)
	}
	if strings.Contains(fmt.Sprint(err), "host-secret") {
		t.Fatal("error exposed inherited credential")
	}
}

func TestUnexpectedServerRequestFailsClosedWithoutResponse(t *testing.T) {
	client := startHelper(t, "server-request", Config{})
	defer client.Close()
	_, err := client.Initialize(context.Background())
	if !errors.Is(err, ErrUnexpectedServerRequest) {
		t.Fatalf("Initialize() error = %v, want ErrUnexpectedServerRequest", err)
	}
}

func TestFrameLimitAppliesBeforeJSONDecodeAndStderrIsBounded(t *testing.T) {
	client := startHelper(t, "oversized", Config{MaxFrameBytes: 512, MaxStderrBytes: 128})
	defer client.Close()
	_, err := client.Initialize(context.Background())
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("Initialize() error = %v, want ErrFrameTooLarge", err)
	}
	waitFor(t, func() bool {
		client.stderr.mu.Lock()
		defer client.stderr.mu.Unlock()
		return client.stderr.truncated
	})
	client.stderr.mu.Lock()
	defer client.stderr.mu.Unlock()
	if len(client.stderr.data) != 128 || !client.stderr.truncated {
		t.Fatalf("stderr = %d bytes, truncated=%v", len(client.stderr.data), client.stderr.truncated)
	}
}

func TestRequestDeadlineKillsProcessGroup(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("process group assertion is Unix-specific")
	}
	client := startHelper(t, "deadline", Config{RequestTimeout: 2 * time.Second})
	canonicalWorkDir, err := filepath.EvalSymlinks(client.Paths().WorkDir)
	if err != nil {
		t.Fatal(err)
	}
	pidPath := filepath.Join(canonicalWorkDir, "grandchild.pid")
	errorChannel := make(chan error, 1)
	go func() {
		_, err := client.Initialize(context.Background())
		errorChannel <- err
	}()
	var pid int
	pidDeadline := time.Now().Add(3 * time.Second)
	for pid == 0 && time.Now().Before(pidDeadline) {
		data, err := os.ReadFile(pidPath)
		if err == nil {
			pid, _ = strconv.Atoi(strings.TrimSpace(string(data)))
		}
		if pid == 0 {
			time.Sleep(10 * time.Millisecond)
		}
	}
	if pid == 0 {
		client.stderr.mu.Lock()
		stderr := string(client.stderr.data)
		client.stderr.mu.Unlock()
		select {
		case early := <-errorChannel:
			t.Fatalf("grandchild pid missing; Initialize()=%v stderr=%q", early, stderr)
		default:
			t.Fatalf("grandchild pid missing; stderr=%q", stderr)
		}
	}
	if err := <-errorChannel; !errors.Is(err, ErrDeadline) {
		t.Fatalf("Initialize() error = %v, want ErrDeadline", err)
	}
	_ = client.Close()
	waitFor(t, func() bool { return errors.Is(syscall.Kill(pid, 0), syscall.ESRCH) })
}

func TestTurnFailureClassificationIsStableAndRedacted(t *testing.T) {
	result, err := classifyTurn(TurnResult{Status: "failed", FailureCode: "usageLimitExceeded"})
	if result.FailureCode != "usageLimitExceeded" || !errors.Is(err, ErrQuotaExhausted) {
		t.Fatalf("quota classification = %#v, %v", result, err)
	}
	result, err = classifyTurn(TurnResult{Status: "failed", FailureCode: "unauthorized"})
	if !errors.Is(err, ErrReauthenticationRequired) {
		t.Fatalf("reauth classification = %#v, %v", result, err)
	}
	result, err = classifyTurn(TurnResult{Status: "failed", FailureCode: "serverOverloaded"})
	var turnError *TurnFailureError
	if !errors.As(err, &turnError) || strings.Contains(err.Error(), "provider said") {
		t.Fatalf("protocol classification = %#v, %v", result, err)
	}
}

func TestProviderObservationsDoNotInventExhaustionOrReset(t *testing.T) {
	falseValue := false
	trueValue := true
	used50 := int32(50)
	used100 := int32(100)
	reset := int64(1700)
	reached := "rate_limit_reached"
	tests := []struct {
		name      string
		limits    RateLimits
		wantState ProviderState
		wantReset bool
	}{
		{name: "no extra credits is unknown", limits: RateLimits{Current: RateLimitSnapshot{Credits: &CreditsSnapshot{HasCredits: &falseValue}}}, wantState: ProviderStateUnknown},
		{name: "generic reached type has no inferred window", limits: RateLimits{Current: RateLimitSnapshot{RateLimitReachedType: &reached, Primary: &RateLimitWindow{UsedPercent: &used50, ResetsAt: &reset}}}, wantState: ProviderStateExhausted},
		{name: "exhausted primary exposes its reset", limits: RateLimits{Current: RateLimitSnapshot{Primary: &RateLimitWindow{UsedPercent: &used100, ResetsAt: &reset}}}, wantState: ProviderStateExhausted, wantReset: true},
		{name: "spend control reached is exhausted without reset", limits: RateLimits{Current: RateLimitSnapshot{SpendControlReached: &trueValue}}, wantState: ProviderStateExhausted},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := test.limits.Observation()
			if got.State != test.wantState || (got.ResetAt != nil) != test.wantReset {
				t.Fatalf("Observation() = %#v", got)
			}
		})
	}
	if got := (AccountState{Account: &Account{Type: "chatgpt", PlanType: "plus"}, RequiresOpenAIAuth: true}).Observation(); got.State != ProviderStateAvailable {
		t.Fatalf("authenticated account with provider auth requirement = %#v", got)
	}
}

func TestTerminalNotificationRequiresExactIDsAndRejectsDuplicates(t *testing.T) {
	client := &Client{
		turns:    map[string]string{turnKey("thread-exact", "turn-exact"): "inProgress"},
		turnDone: make(map[string]TurnResult), turnWait: make(map[string][]chan TurnResult),
		turnErrors: make(map[string]string),
	}
	wrong := json.RawMessage(`{"threadId":"thread-other","turn":{"id":"turn-exact","status":"completed","items":[],"error":null}}`)
	if err := client.completeTurn(wrong); !errors.Is(err, ErrProtocol) {
		t.Fatalf("wrong terminal id error = %v", err)
	}
	providerError := json.RawMessage(`{"threadId":"thread-exact","turnId":"turn-exact","willRetry":false,"error":{"codexErrorInfo":"usageLimitExceeded"}}`)
	if err := client.handleNotification("error", providerError); err != nil {
		t.Fatalf("error notification = %v", err)
	}
	if len(client.turnDone) != 0 {
		t.Fatal("non-terminal error notification completed the turn")
	}
	terminal := json.RawMessage(`{"threadId":"thread-exact","turn":{"id":"turn-exact","status":"failed","items":[],"error":null}}`)
	if err := client.completeTurn(terminal); err != nil {
		t.Fatalf("terminal notification = %v", err)
	}
	if got := client.turnDone[turnKey("thread-exact", "turn-exact")].FailureCode; got != "usageLimitExceeded" {
		t.Fatalf("terminal failure code = %q", got)
	}
	if err := client.completeTurn(terminal); !errors.Is(err, ErrProtocol) {
		t.Fatalf("duplicate terminal error = %v", err)
	}
}

func TestToolItemNotificationFailsClosed(t *testing.T) {
	client := &Client{turns: map[string]string{turnKey("thread-exact", "turn-exact"): "inProgress"}}
	params := json.RawMessage(`{"threadId":"thread-exact","turnId":"turn-exact","item":{"type":"commandExecution"}}`)
	if err := client.handleNotification("item/started", params); !errors.Is(err, ErrUnexpectedCapability) {
		t.Fatalf("tool item error = %v", err)
	}
}

func TestTurnTimeoutInterruptsAndWaitsForTerminalBeforeReturning(t *testing.T) {
	client := startHelper(t, "turn-timeout", Config{ShutdownTimeout: time.Second})
	defer client.Close()
	if _, err := client.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	thread, err := client.StartThread(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	turn, err := client.StartTurn(context.Background(), thread.ID, "wait", "")
	if err != nil {
		t.Fatal(err)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	result, err := client.WaitTurn(waitCtx, thread.ID, turn.ID)
	if !errors.Is(err, ErrDeadline) || result.Status != "interrupted" {
		t.Fatalf("WaitTurn() = %#v, %v", result, err)
	}
	if client.failure() != ErrClosed {
		t.Fatal("successful interrupt unexpectedly killed the client")
	}
}

func TestLoginTimeoutCancelsBeforeReturning(t *testing.T) {
	client := startHelper(t, "login-timeout", Config{ShutdownTimeout: time.Second})
	defer client.Close()
	if _, err := client.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	device, err := client.StartDeviceCodeLogin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	result, err := client.WaitDeviceCodeLogin(waitCtx, device.LoginID)
	if !errors.Is(err, ErrDeadline) || result.LoginID != device.LoginID {
		t.Fatalf("WaitDeviceCodeLogin() = %#v, %v", result, err)
	}
}

func TestReadBoundedFrameRejectsIncompleteAndOversizedFrames(t *testing.T) {
	if _, err := readBoundedFrame(bufio.NewReader(strings.NewReader("{}")), 10); !errors.Is(err, io.EOF) {
		t.Fatalf("incomplete frame error = %v", err)
	}
	if _, err := readBoundedFrame(bufio.NewReader(strings.NewReader(strings.Repeat("x", 20)+"\n")), 10); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("oversized frame error = %v", err)
	}
}

func startHelper(t *testing.T, scenario string, overrides Config) *Client {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	overrides.Executable = executable
	overrides.ScratchRoot = t.TempDir()
	overrides.testArguments = []string{"-test.run=TestCodexAppHelperProcess", "--", scenario}
	client, err := Start(context.Background(), overrides)
	if err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	return client
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition did not become true")
}

func TestCodexAppHelperProcess(t *testing.T) {
	separator := -1
	for index, argument := range os.Args {
		if argument == "--" {
			separator = index
			break
		}
	}
	if separator < 0 || separator+1 >= len(os.Args) {
		return
	}
	scenario := os.Args[separator+1]
	if os.Getenv("OPENAI_API_KEY") != "" || os.Getenv("SESSIONLESS_HOST_MARKER") != "" {
		os.Exit(91)
	}
	for _, name := range []string{"HOME", "CODEX_HOME", "TMPDIR"} {
		info, err := os.Stat(os.Getenv(name))
		if err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
			os.Exit(92)
		}
	}
	cwd, _ := filepath.EvalSymlinks(mustCWD())
	wantCWD, _ := filepath.EvalSymlinks(filepath.Join(filepath.Dir(os.Getenv("HOME")), "workspace"))
	if cwd == "" || cwd != wantCWD {
		os.Exit(93)
	}
	if scenario == "oversized" {
		_, _ = fmt.Fprintln(os.Stderr, strings.Repeat("sensitive-stderr", 1024))
		_, _ = fmt.Fprintln(os.Stdout, strings.Repeat("{", 1024))
		for {
			time.Sleep(time.Hour)
		}
	}
	reader := bufio.NewReader(os.Stdin)
	writer := bufio.NewWriter(os.Stdout)
	defer writer.Flush()
	initialized := false
	rateReads := 0
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			return
		}
		var request map[string]json.RawMessage
		if json.Unmarshal(line, &request) != nil {
			os.Exit(94)
		}
		if _, exists := request["jsonrpc"]; exists {
			os.Exit(95)
		}
		var method string
		_ = json.Unmarshal(request["method"], &method)
		if method == "initialized" {
			initialized = true
			continue
		}
		id := request["id"]
		switch method {
		case "initialize":
			if scenario == "deadline" {
				child := exec.Command("/bin/sleep", "60")
				if child.Start() != nil {
					os.Exit(96)
				}
				_ = os.WriteFile(filepath.Join(mustCWD(), "grandchild.pid"), []byte(strconv.Itoa(child.Process.Pid)), 0o600)
				for {
					time.Sleep(time.Hour)
				}
			}
			if scenario == "server-request" {
				writeJSON(writer, map[string]any{"id": "server-exact", "method": "item/tool/call", "params": map[string]any{"secret": "must-not-echo"}})
				continue
			}
			writeJSON(writer, map[string]any{"id": rawID(id), "result": map[string]any{
				"userAgent": "fake-codex/0.148.0-alpha.15", "codexHome": os.Getenv("CODEX_HOME"),
				"platformFamily": "unix", "platformOs": runtime.GOOS,
			}})
		case "account/login/start":
			requireInitialized(initialized)
			writeJSON(writer, map[string]any{"id": rawID(id), "result": map[string]any{
				"type": "chatgptDeviceCode", "loginId": "login-exact",
				"verificationUrl": "https://example.invalid/device", "userCode": "ABCD-EFGH",
			}})
			if scenario != "login-timeout" {
				writeJSON(writer, map[string]any{"method": "account/login/completed", "params": map[string]any{
					"loginId": "login-exact", "success": true, "error": nil, "onboardingEntrypoint": nil,
				}})
			}
		case "account/login/cancel":
			writeJSON(writer, map[string]any{"id": rawID(id), "result": map[string]any{"status": "canceled"}})
			writeJSON(writer, map[string]any{"method": "account/login/completed", "params": map[string]any{
				"loginId": "login-exact", "success": false, "error": "redacted upstream text", "onboardingEntrypoint": nil,
			}})
		case "account/read":
			requireInitialized(initialized)
			accountType := "chatgpt"
			account := map[string]any{"type": accountType, "email": "redacted@example.invalid", "planType": "plus"}
			if scenario == "api-key" {
				account = map[string]any{"type": "apiKey"}
			}
			writeJSON(writer, map[string]any{"id": rawID(id), "result": map[string]any{"account": account, "requiresOpenaiAuth": scenario == "lifecycle"}})
		case "account/rateLimits/read":
			rateReads++
			if _, hasParams := request["params"]; hasParams {
				os.Exit(97)
			}
			snapshot := rateSnapshot(10, 300, 1700)
			if rateReads == 1 {
				snapshot["individualLimit"] = map[string]any{"limit": "10", "used": "1", "remainingPercent": 90, "resetsAt": 1800}
			}
			writeJSON(writer, map[string]any{"id": rawID(id), "result": map[string]any{
				"rateLimits": snapshot, "rateLimitsByLimitId": map[string]any{"codex": snapshot},
				"rateLimitResetCredits": nil,
			}})
			writeJSON(writer, map[string]any{"method": "account/rateLimits/updated", "params": map[string]any{
				"rateLimits": map[string]any{"limitId": "codex", "limitName": nil,
					"primary":   map[string]any{"usedPercent": 20, "windowDurationMins": nil, "resetsAt": nil},
					"secondary": nil, "credits": nil, "individualLimit": nil, "spendControlReached": nil,
					"planType": nil, "rateLimitReachedType": nil},
			}})
		case "thread/start":
			var params struct {
				CWD            string         `json:"cwd"`
				ApprovalPolicy string         `json:"approvalPolicy"`
				Sandbox        string         `json:"sandbox"`
				Ephemeral      bool           `json:"ephemeral"`
				Config         map[string]any `json:"config"`
			}
			if json.Unmarshal(request["params"], &params) != nil || !samePath(params.CWD, mustCWD()) ||
				params.ApprovalPolicy != "never" || params.Sandbox != "read-only" || !params.Ephemeral {
				os.Exit(98)
			}
			features, ok := params.Config["features"].(map[string]any)
			if !ok || len(features) < 15 {
				os.Exit(98)
			}
			for _, enabled := range features {
				if enabled != false {
					os.Exit(98)
				}
			}
			var rawParams map[string]json.RawMessage
			_ = json.Unmarshal(request["params"], &rawParams)
			for _, experimental := range []string{"runtimeWorkspaceRoots", "environments", "dynamicTools"} {
				if _, found := rawParams[experimental]; found {
					os.Exit(98)
				}
			}
			thread := map[string]any{"id": "thread-exact"}
			writeJSON(writer, map[string]any{"id": rawID(id), "result": map[string]any{"thread": thread}})
			writeJSON(writer, map[string]any{"method": "thread/started", "params": map[string]any{"thread": thread}})
		case "turn/start":
			var params struct {
				ThreadID       string `json:"threadId"`
				ApprovalPolicy string `json:"approvalPolicy"`
				SandboxPolicy  struct {
					Type          string `json:"type"`
					NetworkAccess bool   `json:"networkAccess"`
				} `json:"sandboxPolicy"`
			}
			if json.Unmarshal(request["params"], &params) != nil || params.ThreadID != "thread-exact" ||
				params.ApprovalPolicy != "never" || params.SandboxPolicy.Type != "readOnly" || params.SandboxPolicy.NetworkAccess {
				os.Exit(99)
			}
			turn := map[string]any{"id": "turn-exact", "status": "inProgress", "items": []any{}, "error": nil}
			writeJSON(writer, map[string]any{"id": rawID(id), "result": map[string]any{"turn": turn}})
			writeJSON(writer, map[string]any{"method": "turn/started", "params": map[string]any{"threadId": "thread-exact", "turn": turn}})
			if scenario == "turn-timeout" {
				continue
			}
			writeJSON(writer, map[string]any{"method": "item/completed", "params": map[string]any{
				"threadId": "thread-exact", "turnId": "turn-exact", "completedAtMs": 1,
				"item": map[string]any{"type": "agentMessage", "id": "item-exact", "text": "bounded answer", "phase": nil, "memoryCitation": nil},
			}})
			turn = map[string]any{"id": "turn-exact", "status": "completed", "error": nil,
				"items": []any{map[string]any{"type": "agentMessage", "id": "item-exact", "text": "bounded answer"}}}
			writeJSON(writer, map[string]any{"method": "turn/completed", "params": map[string]any{"threadId": "thread-exact", "turn": turn}})
		case "turn/interrupt":
			writeJSON(writer, map[string]any{"id": rawID(id), "result": map[string]any{}})
			if scenario == "turn-timeout" {
				turn := map[string]any{"id": "turn-exact", "status": "interrupted", "error": nil, "items": []any{}}
				writeJSON(writer, map[string]any{"method": "turn/completed", "params": map[string]any{"threadId": "thread-exact", "turn": turn}})
			}
		default:
			os.Exit(100)
		}
	}
}

func writeJSON(writer *bufio.Writer, value any) {
	encoded, _ := json.Marshal(value)
	_, _ = writer.Write(encoded)
	_ = writer.WriteByte('\n')
	_ = writer.Flush()
}

func rawID(raw json.RawMessage) any {
	var id any
	_ = json.Unmarshal(raw, &id)
	return id
}

func mustCWD() string {
	cwd, _ := os.Getwd()
	return cwd
}

func samePath(left, right string) bool {
	leftResolved, leftErr := filepath.EvalSymlinks(left)
	rightResolved, rightErr := filepath.EvalSymlinks(right)
	return leftErr == nil && rightErr == nil && leftResolved == rightResolved
}

func requireInitialized(initialized bool) {
	if !initialized {
		os.Exit(101)
	}
}

func rateSnapshot(used int32, duration, reset int64) map[string]any {
	return map[string]any{
		"limitId": "codex", "limitName": "Codex",
		"primary":   map[string]any{"usedPercent": used, "windowDurationMins": duration, "resetsAt": reset},
		"secondary": nil, "credits": map[string]any{"hasCredits": true, "unlimited": false, "balance": "5"},
		"individualLimit": nil, "spendControlReached": false, "planType": "plus", "rateLimitReachedType": nil,
	}
}
