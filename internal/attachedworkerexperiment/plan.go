// Package attachedworkerexperiment prepares offline AW-03 measurement plans.
// It never creates a connection, reads credentials, or authorizes execution.
package attachedworkerexperiment

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"time"

	"gitcode.com/urandon/sessionless/internal/attachedworkertransport"
)

const maximumManifestBytes = 64 * 1024

var ErrInvalidManifest = errors.New("invalid transport experiment draft")

// Manifest is a proposal. Stop limits require independent review and operator
// approval; successful decoding does not turn them into deployment authority.
type Manifest struct {
	Version         string   `json:"version"`
	SourceSHA       string   `json:"source_sha"`
	CloudExecution  bool     `json:"cloud_execution"`
	WindowSeconds   int64    `json:"window_seconds"`
	Cohorts         []Cohort `json:"cohorts"`
	StopLimits      Limits   `json:"stop_limits"`
	RestartAttempts uint64   `json:"restart_attempts"`
}

type Cohort struct {
	IntervalMinutes int64  `json:"interval_minutes"`
	Workers         uint64 `json:"workers"`
}

type Limits struct {
	Requests         uint64 `json:"requests"`
	YDBRequestUnits  uint64 `json:"ydb_request_units"`
	BilledSeconds    uint64 `json:"billed_seconds"`
	EgressBytes      uint64 `json:"egress_bytes"`
	RubKopecks       uint64 `json:"rub_kopecks"`
	WallClockSeconds uint64 `json:"wall_clock_seconds"`
}

type CohortPlan struct {
	IntervalMinutes        int64  `json:"interval_minutes"`
	Workers                uint64 `json:"workers"`
	ScheduledExchangeBound uint64 `json:"scheduled_exchange_bound"`
	WithWakeExchangeBound  uint64 `json:"with_wake_exchange_bound"`
}

type Plan struct {
	Status     string       `json:"status"`
	SourceSHA  string       `json:"source_sha"`
	Cohorts    []CohortPlan `json:"cohorts"`
	StopLimits Limits       `json:"proposed_stop_limits"`
	Unknown    []string     `json:"unknown"`
	Excluded   []string     `json:"excluded_from_exchange_bounds"`
}

var sourceSHA = regexp.MustCompile(`^[0-9a-f]{40}$`)

// ReadDraft accepts one bounded JSON object. Errors intentionally do not echo
// input, paths, endpoints or arbitrary unknown field names into reports.
func ReadDraft(reader io.Reader) (Manifest, error) {
	if reader == nil {
		return Manifest{}, ErrInvalidManifest
	}
	data, err := io.ReadAll(io.LimitReader(reader, maximumManifestBytes+1))
	if err != nil || len(data) > maximumManifestBytes {
		return Manifest{}, ErrInvalidManifest
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var manifest Manifest
	if err := decoder.Decode(&manifest); err != nil {
		return Manifest{}, ErrInvalidManifest
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return Manifest{}, ErrInvalidManifest
	}
	if _, err := Prepare(manifest); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func Prepare(manifest Manifest) (Plan, error) {
	if manifest.Version != "aw03-experiment-draft-v1" || !sourceSHA.MatchString(manifest.SourceSHA) ||
		manifest.CloudExecution || manifest.WindowSeconds != 86400 || len(manifest.Cohorts) != 3 ||
		manifest.RestartAttempts == 0 || manifest.RestartAttempts > 5 {
		return Plan{}, ErrInvalidManifest
	}
	limits := manifest.StopLimits
	if limits.Requests == 0 || limits.YDBRequestUnits == 0 || limits.BilledSeconds == 0 ||
		limits.EgressBytes == 0 || limits.RubKopecks == 0 ||
		limits.WallClockSeconds < 86400 || limits.WallClockSeconds > 48*60*60 {
		return Plan{}, ErrInvalidManifest
	}
	plan := Plan{
		Status: "draft_not_authorized", SourceSHA: manifest.SourceSHA, StopLimits: limits,
		Unknown:  []string{"deployment_digest_and_config", "experiment_resource_ids", "telemetry_attribution", "ydb_ru_and_writes", "billed_duration_and_invocations", "egress", "rub_charge_and_tariff", "approved_manifest", "verified_teardown"},
		Excluded: []string{"bootstrap_and_manifest", "reconnect", "exact_replay", "active_controls", "experiment_setup_and_teardown"},
	}
	seen := make(map[int64]bool)
	var wakeTotal uint64
	for _, cohort := range manifest.Cohorts {
		if (cohort.IntervalMinutes != 15 && cohort.IntervalMinutes != 30 && cohort.IntervalMinutes != 60) ||
			seen[cohort.IntervalMinutes] || cohort.Workers != 1 {
			return Plan{}, ErrInvalidManifest
		}
		seen[cohort.IntervalMinutes] = true
		interval := time.Duration(cohort.IntervalMinutes) * time.Minute
		scheduled, err := attachedworkertransport.PollCountUpperBound(cohort.Workers, 24*time.Hour, interval)
		if err != nil {
			return Plan{}, ErrInvalidManifest
		}
		wake, err := attachedworkertransport.PollCountUpperBoundWithWake(cohort.Workers, 24*time.Hour, interval)
		if err != nil {
			return Plan{}, ErrInvalidManifest
		}
		wakeTotal += wake
		plan.Cohorts = append(plan.Cohorts, CohortPlan{
			IntervalMinutes: cohort.IntervalMinutes, Workers: cohort.Workers,
			ScheduledExchangeBound: scheduled, WithWakeExchangeBound: wake,
		})
	}
	// Other traffic is deliberately not estimated here. There must be request
	// headroom; the operator must separately reserve it in the execution manifest.
	if limits.Requests <= wakeTotal {
		return Plan{}, ErrInvalidManifest
	}
	return plan, nil
}
