// Package codexapp drives the bounded subset of the Codex App Server protocol
// used by the subscription feasibility adapter.
package codexapp

import (
	"encoding/json"
	"errors"
	"time"
)

var (
	ErrProtocol                 = errors.New("codex app-server protocol failure")
	ErrFrameTooLarge            = errors.New("codex app-server frame exceeds limit")
	ErrUnexpectedServerRequest  = errors.New("unexpected codex app-server request")
	ErrUnexpectedCapability     = errors.New("unexpected codex app-server capability use")
	ErrProcessExited            = errors.New("codex app-server exited")
	ErrProcessUnavailable       = errors.New("codex app-server process unavailable")
	ErrDeadline                 = errors.New("codex app-server deadline exceeded")
	ErrClosed                   = errors.New("codex app-server client closed")
	ErrUnsupportedAuth          = errors.New("unsupported codex account authentication")
	ErrReauthenticationRequired = errors.New("codex account reauthentication required")
	ErrQuotaExhausted           = errors.New("codex subscription quota exhausted")
)

const (
	// ExpectedAppServerVersion is the locally generated stable-schema and
	// known-client policy gate for this Phase A adapter.
	ExpectedAppServerVersion = "0.148.0-alpha.15"
	ClientVersion            = "phase-a"
)

// Config deliberately has no HOME, CODEX_HOME or environment override. The
// child receives a fixed allowlist and invocation-private directories.
type Config struct {
	Executable      string
	ScratchRoot     string
	RequestTimeout  time.Duration
	TurnTimeout     time.Duration
	ShutdownTimeout time.Duration
	MaxFrameBytes   int
	MaxStderrBytes  int

	// testArguments is intentionally unavailable to production callers. The
	// production command line is always exactly "app-server --stdio".
	testArguments []string
}

type Paths struct {
	Root      string
	Home      string
	CodexHome string
	WorkDir   string
	TempDir   string
}

type InitializeResult struct {
	UserAgent      string `json:"userAgent"`
	CodexHome      string `json:"codexHome"`
	PlatformFamily string `json:"platformFamily"`
	PlatformOS     string `json:"platformOs"`
}

type DeviceCode struct {
	LoginID         string
	VerificationURL string
	UserCode        string
}

type LoginResult struct {
	LoginID string
	Success bool
}

type Account struct {
	Type     string
	PlanType string
}

type AccountState struct {
	Account            *Account
	RequiresOpenAIAuth bool
}

// Optional fields are pointers because a rolling update is sparse: nil means
// unavailable and must not clear a previously observed value.
type RateLimitWindow struct {
	UsedPercent        *int32 `json:"usedPercent"`
	WindowDurationMins *int64 `json:"windowDurationMins"`
	ResetsAt           *int64 `json:"resetsAt"`
}

type CreditsSnapshot struct {
	HasCredits *bool   `json:"hasCredits"`
	Unlimited  *bool   `json:"unlimited"`
	Balance    *string `json:"balance"`
}

type SpendControlSnapshot struct {
	Limit            *string `json:"limit"`
	Used             *string `json:"used"`
	RemainingPercent *int32  `json:"remainingPercent"`
	ResetsAt         *int64  `json:"resetsAt"`
}

type RateLimitSnapshot struct {
	LimitID              *string               `json:"limitId"`
	LimitName            *string               `json:"limitName"`
	Primary              *RateLimitWindow      `json:"primary"`
	Secondary            *RateLimitWindow      `json:"secondary"`
	Credits              *CreditsSnapshot      `json:"credits"`
	IndividualLimit      *SpendControlSnapshot `json:"individualLimit"`
	SpendControlReached  *bool                 `json:"spendControlReached"`
	PlanType             *string               `json:"planType"`
	RateLimitReachedType *string               `json:"rateLimitReachedType"`
}

type RateLimits struct {
	Current   RateLimitSnapshot
	ByLimitID map[string]RateLimitSnapshot
}

type ProviderState string

const (
	ProviderStateUnknown   ProviderState = "unknown"
	ProviderStateAvailable ProviderState = "available"
	ProviderStateExhausted ProviderState = "exhausted"
	ProviderStateReauth    ProviderState = "reauth"
)

type ProviderObservation struct {
	State       ProviderState
	ResetAt     *int64
	LimitID     *string
	ReachedType *string
}

type Thread struct {
	ID string
}

type Turn struct {
	ID       string
	ThreadID string
	Status   string
}

type TurnResult struct {
	ThreadID    string
	TurnID      string
	Status      string
	OutputText  string
	FailureCode string
}

type rawTurn struct {
	ID     string          `json:"id"`
	Status string          `json:"status"`
	Items  json.RawMessage `json:"items"`
	Error  *rawTurnError   `json:"error"`
}

type rawTurnError struct {
	CodexErrorInfo json.RawMessage `json:"codexErrorInfo"`
}
