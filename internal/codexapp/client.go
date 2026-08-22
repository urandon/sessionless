package codexapp

import (
	"context"
	"encoding/json"
	"strings"
	"time"
)

type TurnFailureError struct{ Code string }

func (err *TurnFailureError) Error() string { return "codex turn failed" }

func (client *Client) Initialize(ctx context.Context) (InitializeResult, error) {
	client.mu.Lock()
	alreadyInitialized := client.initialized
	client.mu.Unlock()
	if alreadyInitialized {
		return InitializeResult{}, ErrProtocol
	}
	params := map[string]any{
		"clientInfo": map[string]any{
			"name": "sessionless_worker", "title": "Sessionless Worker", "version": ClientVersion,
		},
		"capabilities": map[string]any{
			"experimentalApi": false, "requestAttestation": false,
			"optOutNotificationMethods": []string{}, "extensions": map[string]any{},
		},
	}
	var result InitializeResult
	if err := client.call(ctx, client.config.RequestTimeout, "initialize", params, &result); err != nil {
		return InitializeResult{}, err
	}
	if !strings.Contains(result.UserAgent, ExpectedAppServerVersion) || result.CodexHome != client.paths.CodexHome ||
		result.PlatformFamily == "" || result.PlatformOS == "" {
		client.fail(ErrProtocol)
		return InitializeResult{}, ErrProtocol
	}
	if err := client.notify("initialized", nil); err != nil {
		return InitializeResult{}, err
	}
	client.mu.Lock()
	client.initialized = true
	client.mu.Unlock()
	return result, nil
}

func (client *Client) StartDeviceCodeLogin(ctx context.Context) (DeviceCode, error) {
	if err := client.requireInitialized(); err != nil {
		return DeviceCode{}, err
	}
	var response struct {
		Type            string `json:"type"`
		LoginID         string `json:"loginId"`
		VerificationURL string `json:"verificationUrl"`
		UserCode        string `json:"userCode"`
	}
	if err := client.call(ctx, client.config.RequestTimeout, "account/login/start",
		map[string]string{"type": "chatgptDeviceCode"}, &response); err != nil {
		return DeviceCode{}, err
	}
	if response.Type != "chatgptDeviceCode" || response.LoginID == "" ||
		response.VerificationURL == "" || response.UserCode == "" {
		client.fail(ErrProtocol)
		return DeviceCode{}, ErrProtocol
	}
	return DeviceCode{
		LoginID: response.LoginID, VerificationURL: response.VerificationURL, UserCode: response.UserCode,
	}, nil
}

func (client *Client) WaitDeviceCodeLogin(ctx context.Context, loginID string) (LoginResult, error) {
	if loginID == "" {
		return LoginResult{}, ErrProtocol
	}
	ctx, cancel := boundedContext(ctx, client.config.TurnTimeout)
	defer cancel()
	waiter := make(chan LoginResult, 1)
	client.mu.Lock()
	if result, found := client.loginDone[loginID]; found {
		client.mu.Unlock()
		return classifyLogin(result)
	}
	if client.fatal != nil {
		err := client.fatal
		client.mu.Unlock()
		return LoginResult{}, err
	}
	client.loginWait[loginID] = append(client.loginWait[loginID], waiter)
	client.mu.Unlock()
	select {
	case result := <-waiter:
		return classifyLogin(result)
	case <-ctx.Done():
		cancelCtx, cancelLogin := context.WithTimeout(context.Background(), client.config.RequestTimeout)
		var response map[string]any
		_ = client.call(cancelCtx, client.config.RequestTimeout, "account/login/cancel",
			map[string]string{"loginId": loginID}, &response)
		cancelLogin()
		grace := time.NewTimer(client.config.ShutdownTimeout)
		defer grace.Stop()
		select {
		case result := <-waiter:
			return result, ErrDeadline
		case <-grace.C:
			client.fail(ErrDeadline)
			return LoginResult{}, ErrDeadline
		case <-client.done:
			return LoginResult{}, ErrDeadline
		}
	case <-client.done:
		return LoginResult{}, client.failure()
	}
}

func classifyLogin(result LoginResult) (LoginResult, error) {
	if !result.Success {
		return result, ErrReauthenticationRequired
	}
	return result, nil
}

func (client *Client) ReadAccount(ctx context.Context) (AccountState, error) {
	if err := client.requireInitialized(); err != nil {
		return AccountState{}, err
	}
	var response struct {
		Account *struct {
			Type     string `json:"type"`
			PlanType string `json:"planType"`
		} `json:"account"`
		RequiresOpenAIAuth bool `json:"requiresOpenaiAuth"`
	}
	if err := client.call(ctx, client.config.RequestTimeout, "account/read",
		map[string]bool{"refreshToken": false}, &response); err != nil {
		return AccountState{}, err
	}
	state := AccountState{RequiresOpenAIAuth: response.RequiresOpenAIAuth}
	if response.Account == nil {
		client.mu.Lock()
		client.authMode = ""
		client.authValid = false
		client.mu.Unlock()
		return state, nil
	}
	if response.Account.Type != "chatgpt" {
		client.fail(ErrUnsupportedAuth)
		return AccountState{}, ErrUnsupportedAuth
	}
	if response.Account.PlanType == "" {
		client.fail(ErrProtocol)
		return AccountState{}, ErrProtocol
	}
	state.Account = &Account{
		Type: response.Account.Type, PlanType: response.Account.PlanType,
	}
	client.mu.Lock()
	client.authMode = "chatgpt"
	client.authValid = true
	client.mu.Unlock()
	return state, nil
}

func (client *Client) ReadRateLimits(ctx context.Context) (RateLimits, error) {
	if err := client.requireInitialized(); err != nil {
		return RateLimits{}, err
	}
	client.rateReadMu.Lock()
	defer client.rateReadMu.Unlock()
	client.mu.Lock()
	client.rateReading = true
	client.rateUpdates = nil
	client.mu.Unlock()
	var response struct {
		RateLimits          RateLimitSnapshot            `json:"rateLimits"`
		RateLimitsByLimitID map[string]RateLimitSnapshot `json:"rateLimitsByLimitId"`
	}
	if err := client.call(ctx, client.config.RequestTimeout, "account/rateLimits/read", nil, &response); err != nil {
		client.mu.Lock()
		for _, update := range client.rateUpdates {
			client.applyRateUpdateLocked(update)
		}
		client.rateReading = false
		client.rateUpdates = nil
		client.mu.Unlock()
		return RateLimits{}, err
	}
	client.mu.Lock()
	client.rateLimits.Current = response.RateLimits
	client.rateLimits.ByLimitID = make(map[string]RateLimitSnapshot, len(response.RateLimitsByLimitID))
	for id, snapshot := range response.RateLimitsByLimitID {
		client.rateLimits.ByLimitID[id] = snapshot
	}
	for _, update := range client.rateUpdates {
		client.applyRateUpdateLocked(update)
	}
	client.rateReading = false
	client.rateUpdates = nil
	result := cloneRateLimits(client.rateLimits)
	client.mu.Unlock()
	return result, nil
}

func (client *Client) CurrentRateLimits() RateLimits {
	client.mu.Lock()
	defer client.mu.Unlock()
	return cloneRateLimits(client.rateLimits)
}

func (client *Client) StartThread(ctx context.Context) (Thread, error) {
	if err := client.requireInitialized(); err != nil {
		return Thread{}, err
	}
	client.mu.Lock()
	authValid := client.authValid && client.authMode == "chatgpt"
	client.mu.Unlock()
	if !authValid {
		return Thread{}, ErrReauthenticationRequired
	}
	params := map[string]any{
		"cwd":            client.paths.WorkDir,
		"approvalPolicy": "never", "sandbox": "read-only", "ephemeral": true,
		"config": boundedThreadConfig(),
	}
	var response struct {
		Thread struct {
			ID string `json:"id"`
		} `json:"thread"`
	}
	if err := client.call(ctx, client.config.RequestTimeout, "thread/start", params, &response); err != nil {
		return Thread{}, err
	}
	if response.Thread.ID == "" {
		client.fail(ErrProtocol)
		return Thread{}, ErrProtocol
	}
	client.mu.Lock()
	if client.threadID != "" && client.threadID != response.Thread.ID {
		client.mu.Unlock()
		client.fail(ErrProtocol)
		return Thread{}, ErrProtocol
	}
	client.threadID = response.Thread.ID
	client.threads[response.Thread.ID] = struct{}{}
	client.mu.Unlock()
	return Thread{ID: response.Thread.ID}, nil
}

func boundedThreadConfig() map[string]any {
	features := make(map[string]any)
	for _, name := range []string{
		"apps", "auth_elicitation", "browser_use", "browser_use_external", "browser_use_full_cdp_access",
		"code_mode_host", "computer_use", "hooks", "image_generation", "in_app_browser",
		"multi_agent", "multi_agent_v2", "plugins", "recommended_plugins", "remote_plugin",
		"shell_snapshot", "shell_tool", "skill_mcp_dependency_install", "skill_search",
		"tool_call_mcp_elicitation", "tool_suggest", "unified_exec", "view_image", "workspace_dependencies",
	} {
		features[name] = false
	}
	return map[string]any{
		"features": features, "mcp_servers": map[string]any{}, "web_search": "disabled",
	}
}

func (client *Client) StartTurn(
	ctx context.Context,
	threadID string,
	text string,
	clientUserMessageID string,
) (Turn, error) {
	if strings.TrimSpace(text) == "" || threadID == "" {
		return Turn{}, ErrProtocol
	}
	client.mu.Lock()
	validThread := client.threadID == threadID
	client.mu.Unlock()
	if !validThread {
		return Turn{}, ErrProtocol
	}
	params := map[string]any{
		"threadId":       threadID,
		"input":          []any{map[string]any{"type": "text", "text": text, "text_elements": []any{}}},
		"cwd":            client.paths.WorkDir,
		"approvalPolicy": "never", "sandboxPolicy": map[string]any{"type": "readOnly", "networkAccess": false},
	}
	if clientUserMessageID != "" {
		params["clientUserMessageId"] = clientUserMessageID
	}
	var response struct {
		Turn rawTurn `json:"turn"`
	}
	if err := client.call(ctx, client.config.RequestTimeout, "turn/start", params, &response); err != nil {
		return Turn{}, err
	}
	if response.Turn.ID == "" || response.Turn.Status != "inProgress" {
		client.fail(ErrProtocol)
		return Turn{}, ErrProtocol
	}
	key := turnKey(threadID, response.Turn.ID)
	client.mu.Lock()
	if existing, found := client.turns[key]; found {
		if existing != "inProgress" {
			if _, terminal := client.turnDone[key]; !terminal {
				client.mu.Unlock()
				client.fail(ErrProtocol)
				return Turn{}, ErrProtocol
			}
		}
	} else {
		client.turns[key] = "inProgress"
	}
	client.mu.Unlock()
	return Turn{ID: response.Turn.ID, ThreadID: threadID, Status: response.Turn.Status}, nil
}

func (client *Client) WaitTurn(ctx context.Context, threadID, turnID string) (TurnResult, error) {
	if threadID == "" || turnID == "" {
		return TurnResult{}, ErrProtocol
	}
	key := turnKey(threadID, turnID)
	ctx, cancel := boundedContext(ctx, client.config.TurnTimeout)
	defer cancel()
	waiter := make(chan TurnResult, 1)
	client.mu.Lock()
	if result, found := client.turnDone[key]; found {
		client.mu.Unlock()
		return client.revalidateTurn(ctx, result)
	}
	if _, found := client.turns[key]; !found {
		client.mu.Unlock()
		return TurnResult{}, ErrProtocol
	}
	if client.fatal != nil {
		err := client.fatal
		client.mu.Unlock()
		return TurnResult{}, err
	}
	client.turnWait[key] = append(client.turnWait[key], waiter)
	client.mu.Unlock()
	select {
	case result := <-waiter:
		return client.revalidateTurn(ctx, result)
	case <-ctx.Done():
		interruptCtx, cancelInterrupt := context.WithTimeout(context.Background(), client.config.RequestTimeout)
		_ = client.InterruptTurn(interruptCtx, threadID, turnID)
		cancelInterrupt()
		grace := time.NewTimer(client.config.ShutdownTimeout)
		defer grace.Stop()
		select {
		case result := <-waiter:
			return result, ErrDeadline
		case <-grace.C:
			client.fail(ErrDeadline)
			return TurnResult{}, ErrDeadline
		case <-client.done:
			return TurnResult{}, ErrDeadline
		}
	case <-client.done:
		return TurnResult{}, client.failure()
	}
}

func (client *Client) revalidateTurn(ctx context.Context, result TurnResult) (TurnResult, error) {
	account, err := client.ReadAccount(ctx)
	if err != nil {
		return TurnResult{}, err
	}
	if account.Account == nil || account.Account.Type != "chatgpt" {
		return TurnResult{}, ErrReauthenticationRequired
	}
	return classifyTurn(result)
}

func classifyTurn(result TurnResult) (TurnResult, error) {
	switch result.FailureCode {
	case "":
		return result, nil
	case "usageLimitExceeded":
		return result, ErrQuotaExhausted
	case "unauthorized":
		return result, ErrReauthenticationRequired
	default:
		return result, &TurnFailureError{Code: result.FailureCode}
	}
}

func (client *Client) InterruptTurn(ctx context.Context, threadID, turnID string) error {
	if threadID == "" || turnID == "" {
		return ErrProtocol
	}
	client.mu.Lock()
	_, known := client.turns[turnKey(threadID, turnID)]
	client.mu.Unlock()
	if !known {
		return ErrProtocol
	}
	var response map[string]any
	return client.call(ctx, client.config.RequestTimeout, "turn/interrupt",
		map[string]string{"threadId": threadID, "turnId": turnID}, &response)
}

func (client *Client) call(ctx context.Context, timeout time.Duration, method string, params any, output any) error {
	ctx, cancel := boundedContext(ctx, timeout)
	defer cancel()
	client.mu.Lock()
	if client.fatal != nil {
		err := client.fatal
		client.mu.Unlock()
		return err
	}
	client.nextID++
	id := "sessionless-" + uintString(client.nextID)
	waiter := make(chan rpcResponse, 1)
	client.pending[id] = waiter
	client.mu.Unlock()
	request := map[string]any{"id": id, "method": method}
	if params != nil {
		request["params"] = params
	}
	if err := client.write(request); err != nil {
		client.removePending(id)
		client.fail(err)
		return err
	}
	select {
	case response := <-waiter:
		if response.err != nil {
			return response.err
		}
		if err := json.Unmarshal(response.result, output); err != nil {
			client.fail(ErrProtocol)
			return ErrProtocol
		}
		return nil
	case <-ctx.Done():
		client.removePending(id)
		client.fail(ErrDeadline)
		return ErrDeadline
	case <-client.done:
		return client.failure()
	}
}

func (client *Client) notify(method string, params any) error {
	request := map[string]any{"method": method}
	if params != nil {
		request["params"] = params
	}
	return client.write(request)
}

func (client *Client) write(value any) error {
	encoded, err := json.Marshal(value)
	if err != nil || len(encoded)+1 > client.config.MaxFrameBytes {
		return ErrProtocol
	}
	encoded = append(encoded, '\n')
	client.writeMu.Lock()
	defer client.writeMu.Unlock()
	if _, err := client.stdin.Write(encoded); err != nil {
		return ErrProcessExited
	}
	return nil
}

func (client *Client) removePending(id string) {
	client.mu.Lock()
	delete(client.pending, id)
	client.mu.Unlock()
}

func (client *Client) requireInitialized() error {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.fatal != nil {
		return client.fatal
	}
	if !client.initialized {
		return ErrProtocol
	}
	return nil
}

func (client *Client) failure() error {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.fatal != nil {
		return client.fatal
	}
	return ErrClosed
}

func boundedContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if deadline, ok := parent.Deadline(); ok && time.Until(deadline) <= timeout {
		return context.WithCancel(parent)
	}
	return context.WithTimeout(parent, timeout)
}

func uintString(value uint64) string {
	if value == 0 {
		return "0"
	}
	var digits [20]byte
	index := len(digits)
	for value > 0 {
		index--
		digits[index] = byte('0' + value%10)
		value /= 10
	}
	return string(digits[index:])
}

func turnKey(threadID, turnID string) string { return threadID + "\x00" + turnID }

func cloneRateLimits(source RateLimits) RateLimits {
	result := RateLimits{Current: cloneRateLimit(source.Current), ByLimitID: make(map[string]RateLimitSnapshot, len(source.ByLimitID))}
	for id, snapshot := range source.ByLimitID {
		result.ByLimitID[id] = cloneRateLimit(snapshot)
	}
	return result
}

func cloneRateLimit(source RateLimitSnapshot) RateLimitSnapshot {
	result := source
	result.LimitID = clonePointer(source.LimitID)
	result.LimitName = clonePointer(source.LimitName)
	result.SpendControlReached = clonePointer(source.SpendControlReached)
	result.PlanType = clonePointer(source.PlanType)
	result.RateLimitReachedType = clonePointer(source.RateLimitReachedType)
	if source.Primary != nil {
		copy := *source.Primary
		copy.UsedPercent = clonePointer(source.Primary.UsedPercent)
		copy.WindowDurationMins = clonePointer(source.Primary.WindowDurationMins)
		copy.ResetsAt = clonePointer(source.Primary.ResetsAt)
		result.Primary = &copy
	}
	if source.Secondary != nil {
		copy := *source.Secondary
		copy.UsedPercent = clonePointer(source.Secondary.UsedPercent)
		copy.WindowDurationMins = clonePointer(source.Secondary.WindowDurationMins)
		copy.ResetsAt = clonePointer(source.Secondary.ResetsAt)
		result.Secondary = &copy
	}
	if source.Credits != nil {
		copy := *source.Credits
		copy.HasCredits = clonePointer(source.Credits.HasCredits)
		copy.Unlimited = clonePointer(source.Credits.Unlimited)
		copy.Balance = clonePointer(source.Credits.Balance)
		result.Credits = &copy
	}
	if source.IndividualLimit != nil {
		copy := *source.IndividualLimit
		copy.Limit = clonePointer(source.IndividualLimit.Limit)
		copy.Used = clonePointer(source.IndividualLimit.Used)
		copy.RemainingPercent = clonePointer(source.IndividualLimit.RemainingPercent)
		copy.ResetsAt = clonePointer(source.IndividualLimit.ResetsAt)
		result.IndividualLimit = &copy
	}
	return result
}

func clonePointer[T any](source *T) *T {
	if source == nil {
		return nil
	}
	copy := *source
	return &copy
}

func mergeRateLimit(old, update RateLimitSnapshot) RateLimitSnapshot {
	if update.LimitID != nil {
		old.LimitID = update.LimitID
	}
	if update.LimitName != nil {
		old.LimitName = update.LimitName
	}
	if update.Primary != nil {
		old.Primary = mergeWindow(old.Primary, update.Primary)
	}
	if update.Secondary != nil {
		old.Secondary = mergeWindow(old.Secondary, update.Secondary)
	}
	if update.Credits != nil {
		old.Credits = mergeCredits(old.Credits, update.Credits)
	}
	if update.IndividualLimit != nil {
		old.IndividualLimit = mergeSpend(old.IndividualLimit, update.IndividualLimit)
	}
	if update.SpendControlReached != nil {
		old.SpendControlReached = update.SpendControlReached
	}
	if update.PlanType != nil {
		old.PlanType = update.PlanType
	}
	if update.RateLimitReachedType != nil {
		old.RateLimitReachedType = update.RateLimitReachedType
	}
	return old
}

// applyRateUpdateLocked applies one rolling sparse update in wire order.
// client.mu must be held.
func (client *Client) applyRateUpdateLocked(update RateLimitSnapshot) {
	client.rateLimits.Current = mergeRateLimit(client.rateLimits.Current, update)
	if update.LimitID != nil {
		id := *update.LimitID
		client.rateLimits.ByLimitID[id] = mergeRateLimit(client.rateLimits.ByLimitID[id], update)
	}
}

func mergeWindow(old, update *RateLimitWindow) *RateLimitWindow {
	if old == nil {
		old = &RateLimitWindow{}
	} else {
		copy := *old
		old = &copy
	}
	if update.UsedPercent != nil {
		old.UsedPercent = update.UsedPercent
	}
	if update.WindowDurationMins != nil {
		old.WindowDurationMins = update.WindowDurationMins
	}
	if update.ResetsAt != nil {
		old.ResetsAt = update.ResetsAt
	}
	return old
}

func mergeCredits(old, update *CreditsSnapshot) *CreditsSnapshot {
	if old == nil {
		old = &CreditsSnapshot{}
	} else {
		copy := *old
		old = &copy
	}
	if update.HasCredits != nil {
		old.HasCredits = update.HasCredits
	}
	if update.Unlimited != nil {
		old.Unlimited = update.Unlimited
	}
	if update.Balance != nil {
		old.Balance = update.Balance
	}
	return old
}

func mergeSpend(old, update *SpendControlSnapshot) *SpendControlSnapshot {
	if old == nil {
		old = &SpendControlSnapshot{}
	} else {
		copy := *old
		old = &copy
	}
	if update.Limit != nil {
		old.Limit = update.Limit
	}
	if update.Used != nil {
		old.Used = update.Used
	}
	if update.RemainingPercent != nil {
		old.RemainingPercent = update.RemainingPercent
	}
	if update.ResetsAt != nil {
		old.ResetsAt = update.ResetsAt
	}
	return old
}

func (limits RateLimits) Observation() ProviderObservation {
	snapshot := limits.Current
	observation := ProviderObservation{State: ProviderStateUnknown, LimitID: snapshot.LimitID, ReachedType: snapshot.RateLimitReachedType}
	primaryExhausted := snapshot.Primary != nil && snapshot.Primary.UsedPercent != nil && *snapshot.Primary.UsedPercent >= 100
	if snapshot.RateLimitReachedType != nil ||
		(snapshot.SpendControlReached != nil && *snapshot.SpendControlReached) || primaryExhausted {
		observation.State = ProviderStateExhausted
		if primaryExhausted && snapshot.Primary.ResetsAt != nil {
			observation.ResetAt = snapshot.Primary.ResetsAt
		}
		return observation
	}
	if snapshot.Primary != nil || snapshot.Secondary != nil {
		observation.State = ProviderStateAvailable
	}
	return observation
}

func (state AccountState) Observation() ProviderObservation {
	if state.Account == nil {
		return ProviderObservation{State: ProviderStateReauth}
	}
	return ProviderObservation{State: ProviderStateAvailable}
}

func (client *Client) handleNotification(method string, params json.RawMessage) error {
	lower := strings.ToLower(method)
	if strings.Contains(lower, "approval") || strings.Contains(lower, "permissions") ||
		strings.Contains(lower, "mcpserver") || strings.Contains(lower, "/tool/") {
		return ErrUnexpectedCapability
	}
	switch method {
	case "account/updated":
		var event struct {
			AuthMode *string `json:"authMode"`
		}
		if json.Unmarshal(params, &event) != nil {
			return ErrProtocol
		}
		client.mu.Lock()
		hadValidAuth := client.authValid
		runActive := len(client.turns) != 0
		if event.AuthMode == nil || *event.AuthMode == "" {
			client.authMode = ""
			client.authValid = false
			client.mu.Unlock()
			if hadValidAuth || runActive {
				return ErrReauthenticationRequired
			}
			return nil
		}
		if *event.AuthMode != "chatgpt" {
			client.authMode = *event.AuthMode
			client.authValid = false
			client.mu.Unlock()
			return ErrUnsupportedAuth
		}
		client.authMode = "chatgpt"
		client.mu.Unlock()
	case "account/login/completed":
		var event struct {
			LoginID *string `json:"loginId"`
			Success bool    `json:"success"`
		}
		if json.Unmarshal(params, &event) != nil || event.LoginID == nil || *event.LoginID == "" {
			return ErrProtocol
		}
		result := LoginResult{LoginID: *event.LoginID, Success: event.Success}
		client.mu.Lock()
		client.loginDone[result.LoginID] = result
		waiters := client.loginWait[result.LoginID]
		delete(client.loginWait, result.LoginID)
		client.mu.Unlock()
		for _, waiter := range waiters {
			waiter <- result
		}
	case "account/rateLimits/updated":
		var event struct {
			RateLimits RateLimitSnapshot `json:"rateLimits"`
		}
		if json.Unmarshal(params, &event) != nil {
			return ErrProtocol
		}
		client.mu.Lock()
		if client.rateReading {
			client.rateUpdates = append(client.rateUpdates, event.RateLimits)
		} else {
			client.applyRateUpdateLocked(event.RateLimits)
		}
		client.mu.Unlock()
	case "thread/started":
		var event struct {
			Thread struct {
				ID string `json:"id"`
			} `json:"thread"`
		}
		if json.Unmarshal(params, &event) != nil || event.Thread.ID == "" {
			return ErrProtocol
		}
		client.mu.Lock()
		if client.threadID != "" && client.threadID != event.Thread.ID {
			client.mu.Unlock()
			return ErrProtocol
		}
		client.threadID = event.Thread.ID
		client.threads[event.Thread.ID] = struct{}{}
		client.mu.Unlock()
	case "turn/started":
		var event struct {
			ThreadID string  `json:"threadId"`
			Turn     rawTurn `json:"turn"`
		}
		if json.Unmarshal(params, &event) != nil || event.ThreadID == "" ||
			event.Turn.ID == "" || event.Turn.Status != "inProgress" {
			return ErrProtocol
		}
		client.mu.Lock()
		_, threadKnown := client.threads[event.ThreadID]
		key := turnKey(event.ThreadID, event.Turn.ID)
		existing, turnKnown := client.turns[key]
		if !threadKnown || (turnKnown && existing != "inProgress") {
			client.mu.Unlock()
			return ErrProtocol
		}
		client.turns[key] = event.Turn.Status
		client.mu.Unlock()
	case "turn/completed":
		return client.completeTurn(params)
	case "item/started", "item/completed":
		var event struct {
			ThreadID string `json:"threadId"`
			TurnID   string `json:"turnId"`
			Item     struct {
				Type string `json:"type"`
			} `json:"item"`
		}
		if json.Unmarshal(params, &event) != nil || event.ThreadID == "" || event.TurnID == "" || event.Item.Type == "" {
			return ErrProtocol
		}
		client.mu.Lock()
		status, known := client.turns[turnKey(event.ThreadID, event.TurnID)]
		client.mu.Unlock()
		if !known || status != "inProgress" {
			return ErrProtocol
		}
		switch event.Item.Type {
		case "userMessage", "agentMessage", "plan", "reasoning", "contextCompaction":
		default:
			return ErrUnexpectedCapability
		}
	case "error":
		var event struct {
			ThreadID  string       `json:"threadId"`
			TurnID    string       `json:"turnId"`
			WillRetry bool         `json:"willRetry"`
			Error     rawTurnError `json:"error"`
		}
		if json.Unmarshal(params, &event) != nil || event.ThreadID == "" || event.TurnID == "" {
			return ErrProtocol
		}
		if !event.WillRetry {
			code := codexErrorCode(event.Error.CodexErrorInfo)
			if code == "" {
				code = "providerError"
			}
			key := turnKey(event.ThreadID, event.TurnID)
			client.mu.Lock()
			status, known := client.turns[key]
			existing := client.turnErrors[key]
			if !known || status != "inProgress" || (existing != "" && existing != code) {
				client.mu.Unlock()
				return ErrProtocol
			}
			client.turnErrors[key] = code
			client.mu.Unlock()
		}
	}
	return nil
}

func (client *Client) completeTurn(params json.RawMessage) error {
	var event struct {
		ThreadID string  `json:"threadId"`
		Turn     rawTurn `json:"turn"`
	}
	if json.Unmarshal(params, &event) != nil || event.ThreadID == "" || event.Turn.ID == "" {
		return ErrProtocol
	}
	if event.Turn.Status != "completed" && event.Turn.Status != "interrupted" && event.Turn.Status != "failed" {
		return ErrProtocol
	}
	key := turnKey(event.ThreadID, event.Turn.ID)
	client.mu.Lock()
	status, known := client.turns[key]
	_, duplicate := client.turnDone[key]
	pendingFailure := client.turnErrors[key]
	client.mu.Unlock()
	if !known || status != "inProgress" || duplicate {
		return ErrProtocol
	}
	result := TurnResult{ThreadID: event.ThreadID, TurnID: event.Turn.ID, Status: event.Turn.Status}
	if event.Turn.Error != nil {
		result.FailureCode = codexErrorCode(event.Turn.Error.CodexErrorInfo)
		if result.FailureCode == "" {
			result.FailureCode = "providerError"
		}
	}
	if pendingFailure != "" {
		if result.Status != "failed" || (result.FailureCode != "" && result.FailureCode != pendingFailure) {
			return ErrProtocol
		}
		result.FailureCode = pendingFailure
	}
	if len(event.Turn.Items) != 0 {
		var items []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		}
		if json.Unmarshal(event.Turn.Items, &items) != nil {
			return ErrProtocol
		}
		var messages []string
		for _, item := range items {
			if item.Type == "agentMessage" {
				messages = append(messages, item.Text)
			}
		}
		result.OutputText = strings.Join(messages, "\n")
	}
	return client.publishTurn(result)
}

func (client *Client) publishTurn(result TurnResult) error {
	key := turnKey(result.ThreadID, result.TurnID)
	client.mu.Lock()
	if status, known := client.turns[key]; !known || status != "inProgress" {
		client.mu.Unlock()
		return ErrProtocol
	}
	if _, duplicate := client.turnDone[key]; duplicate {
		client.mu.Unlock()
		return ErrProtocol
	}
	client.turns[key] = result.Status
	client.turnDone[key] = result
	delete(client.turnErrors, key)
	waiters := client.turnWait[key]
	delete(client.turnWait, key)
	client.mu.Unlock()
	for _, waiter := range waiters {
		waiter <- result
	}
	return nil
}

func codexErrorCode(raw json.RawMessage) string {
	var code string
	if json.Unmarshal(raw, &code) != nil {
		return ""
	}
	switch code {
	case "contextWindowExceeded", "sessionBudgetExceeded", "usageLimitExceeded", "serverOverloaded",
		"cyberPolicy", "internalServerError", "unauthorized", "badRequest", "threadRollbackFailed", "sandboxError", "other":
		return code
	default:
		return ""
	}
}
