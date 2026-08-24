// Package codexsurface implements the credential-free half of the Codex
// integration-surface comparator. It deliberately records timings and policy
// booleans only; prompts, output, credentials and account identity are outside
// its public evidence contract.
package codexsurface

import (
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"time"
)

const SchemaVersion = 1

type Surface string

const (
	SurfaceAppServer Surface = "app-server"
	SurfaceExec      Surface = "exec"
	SurfaceSDK       Surface = "python-sdk"
)

type Check struct {
	Name string `json:"name"`
	Pass bool   `json:"pass"`
}

type Report struct {
	SchemaVersion int      `json:"schema_version"`
	Phase         string   `json:"phase"`
	Surface       Surface  `json:"surface"`
	Status        string   `json:"status"`
	Version       string   `json:"version"`
	Runtime       string   `json:"runtime,omitempty"`
	OS            string   `json:"os"`
	Arch          string   `json:"arch"`
	Iterations    int      `json:"iterations"`
	DurationMS    []int64  `json:"duration_ms"`
	Checks        []Check  `json:"checks"`
	FindingCodes  []string `json:"finding_codes,omitempty"`
}

func NewReport(surface Surface, version, runtimeVersion string, durations []time.Duration, checks []Check, findings []string) Report {
	durationMS := make([]int64, len(durations))
	for i, duration := range durations {
		durationMS[i] = duration.Milliseconds()
	}
	sort.Slice(checks, func(i, j int) bool { return checks[i].Name < checks[j].Name })
	sort.Strings(findings)
	status := "pass"
	for _, check := range checks {
		if !check.Pass {
			status = "no_go"
			break
		}
	}
	return Report{
		SchemaVersion: SchemaVersion,
		Phase:         "credential-free",
		Surface:       surface,
		Status:        status,
		Version:       version,
		Runtime:       runtimeVersion,
		OS:            runtime.GOOS,
		Arch:          runtime.GOARCH,
		Iterations:    len(durations),
		DurationMS:    durationMS,
		Checks:        checks,
		FindingCodes:  findings,
	}
}

func (report Report) Validate() error {
	if report.SchemaVersion != SchemaVersion || report.Phase != "credential-free" {
		return errors.New("unsupported codex surface report contract")
	}
	if report.Surface != SurfaceAppServer && report.Surface != SurfaceExec && report.Surface != SurfaceSDK {
		return errors.New("unsupported codex surface")
	}
	if report.Status != "pass" && report.Status != "no_go" && report.Status != "unsupported" {
		return errors.New("unsupported codex surface status")
	}
	if strings.TrimSpace(report.Version) == "" || report.OS == "" || report.Arch == "" {
		return errors.New("incomplete codex surface provenance")
	}
	if report.Iterations < 1 || report.Iterations != len(report.DurationMS) {
		return errors.New("invalid codex surface iteration evidence")
	}
	for _, duration := range report.DurationMS {
		if duration < 0 {
			return errors.New("negative codex surface duration")
		}
	}
	seen := make(map[string]struct{}, len(report.Checks))
	hasFailedCheck := false
	if len(report.Checks) == 0 {
		return errors.New("codex surface report has no checks")
	}
	for _, check := range report.Checks {
		if !validCode(check.Name) {
			return errors.New("invalid codex surface check code")
		}
		if _, found := seen[check.Name]; found {
			return errors.New("duplicate codex surface check code")
		}
		seen[check.Name] = struct{}{}
		if !check.Pass {
			hasFailedCheck = true
		}
	}
	if report.Status == "pass" && hasFailedCheck {
		return errors.New("passing codex surface report contains a failed check")
	}
	if report.Status == "no_go" && !hasFailedCheck {
		return errors.New("no-go codex surface report has no failed check")
	}
	seenFindings := make(map[string]struct{}, len(report.FindingCodes))
	for _, code := range report.FindingCodes {
		if !validCode(code) {
			return errors.New("invalid codex surface finding code")
		}
		if _, found := seenFindings[code]; found {
			return errors.New("duplicate codex surface finding code")
		}
		seenFindings[code] = struct{}{}
	}
	return nil
}

func (report Report) Marshal() ([]byte, error) {
	if err := report.Validate(); err != nil {
		return nil, err
	}
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode codex surface report: %w", err)
	}
	return append(encoded, '\n'), nil
}

func validCode(value string) bool {
	if value == "" {
		return false
	}
	for _, character := range value {
		if (character < 'a' || character > 'z') && (character < '0' || character > '9') && character != '_' {
			return false
		}
	}
	return true
}
