package observabilityconfig

import (
	"encoding/json"
	"errors"
	"sort"
	"time"
)

type CanaryMetadata struct {
	SchemaVersion    int    `json:"schema_version"`
	GeneratorVersion string `json:"generator_version"`
	InputSHA256      string `json:"input_sha256"`
	BindingRevision  string `json:"binding_revision"`
	BindingSHA256    string `json:"binding_sha256"`
	EvaluatedAt      string `json:"evaluated_at"`
	Mode             string `json:"mode"`
	Authority        string `json:"authority"`
	Evidence         string `json:"evidence"`
}
type CanaryIdentity struct {
	FolderID  string `json:"folder_id"`
	ProjectID string `json:"project_id"`
	ChannelID string `json:"channel_id"`
	RuleName  string `json:"rule_name"`
	Service   string `json:"service"`
	Metric    string `json:"metric"`
	Instance  string `json:"instance"`
	Selector  string `json:"selector"`
}
type CanaryConsole struct {
	Metadata     CanaryMetadata  `json:"metadata"`
	Status       string          `json:"status"`
	Metric       CanaryMetric    `json:"metric"`
	Rule         CanaryRule      `json:"rule"`
	Identity     *CanaryIdentity `json:"identity,omitempty"`
	ClearMessage string          `json:"clear_message"`
	Checklist    []string        `json:"checklist"`
}
type CanaryPlanPhase struct {
	Allocation               CanaryPhase `json:"allocation"`
	Actor                    string      `json:"actor"`
	CumulativeDurationBudget string      `json:"cumulative_duration_budget"`
	AdvancePolicy            string      `json:"advance_policy"`
	Action                   string      `json:"action"`
	RequiredObservation      string      `json:"required_observation"`
}
type CanaryPlan struct {
	Metadata        CanaryMetadata    `json:"metadata"`
	Identity        *CanaryIdentity   `json:"identity,omitempty"`
	Budget          CanaryBudget      `json:"budget"`
	TimingSemantics string            `json:"timing_semantics"`
	Phases          []CanaryPlanPhase `json:"phases"`
	StopConditions  []string          `json:"stop_conditions"`
	Limitations     []string          `json:"limitations"`
}
type CanaryEnvelope struct {
	Metadata CanaryMetadata  `json:"metadata"`
	Policy   CanaryDocument  `json:"policy"`
	Bindings *CanaryBindings `json:"bindings,omitempty"`
}
type CanaryBundle struct {
	Envelope CanaryEnvelope
	Console  CanaryConsole
	Plan     CanaryPlan
}

// RenderCanary creates desired-policy evidence only. Neither mode performs a
// lookup, applies settings, writes samples, schedules work or proves delivery.
func RenderCanary(c *CanaryDocument, b *CanaryBindingsDocument, now time.Time) (*CanaryBundle, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	if (b == nil) != now.IsZero() {
		return nil, errors.New("canary bindings and explicit now must be supplied together")
	}
	encoded, err := json.Marshal(c)
	if err != nil {
		return nil, errors.New("cannot normalize canary")
	}
	var normalized CanaryDocument
	if err := json.Unmarshal(encoded, &normalized); err != nil {
		return nil, errors.New("cannot normalize canary")
	}
	sort.Strings(normalized.Rule.Notifications.States)
	inputDigest, err := canaryDigest(normalized)
	if err != nil {
		return nil, err
	}
	meta := CanaryMetadata{SchemaVersion: 1, GeneratorVersion: CanaryGeneratorVersion, InputSHA256: inputDigest, Mode: "unbound_plan", Authority: "PLAN_ONLY_NO_LIVE_AUTHORITY", Evidence: "planned_not_observed"}
	var identity *CanaryIdentity
	var bindingCopy *CanaryBindings
	if b != nil {
		if err := validateCanaryBindings(b, now); err != nil {
			return nil, err
		}
		bd := b.Bindings
		bindingCopy = &bd
		meta.BindingSHA256, err = canaryDigest(bd)
		if err != nil {
			return nil, err
		}
		meta.BindingRevision = bd.Revision
		meta.EvaluatedAt = now.UTC().Format(time.RFC3339)
		meta.Mode = "bound_private_plan"
		identity = &CanaryIdentity{FolderID: bd.FolderID, ProjectID: bd.ProjectID, ChannelID: bd.ChannelID, RuleName: "sessionless-obs228-" + bd.Instance, Service: normalized.Metric.Service, Metric: normalized.Metric.Name, Instance: bd.Instance, Selector: selector(bd.FolderID, map[string]string{"service": normalized.Metric.Service, "name": normalized.Metric.Name, "instance": bd.Instance})}
	}
	console := CanaryConsole{Metadata: meta, Status: "PLAN_ONLY_REQUIRES_SUPPORTED_SURFACE_READBACK_AND_AUTHORITY", Metric: normalized.Metric, Rule: normalized.Rule, Identity: identity, ClearMessage: "Synthetic test metric condition cleared; not application or canonical Run recovery.", Checklist: []string{
		"Use only the supported native console; this sheet is not a CRUD request or executable policy.",
		"Private preflight fields are operator assertions, not independently acquired cloud observations.",
		"Before arming, inspect/read back exact target, fresh collision/ownership, one existing Cloud push recipient, no default severity subscription, states and repeat policy; any channel correction requires separate exact authority.",
		"Read back Maximum >0.5, 2m window, 1m delay, one series, no gap filling, and both missing-series and missing-points NoData.",
		"NoData or stale success must not establish OK; fresh zero samples and the delayed MAX window must establish clear.",
		"Immediately before arming and again before breach, require fresh zero in the delayed evaluation window and native normal; configuration delay that loses freshness stops the attempt without extra unbudgeted samples.",
		"Initial OK consumes the notification budget; no escalation, idle NoData page or periodic renotification.",
		"Freeze absolute phase deadlines and admitted IAM, tariff and supported controls before separately authorized live execution.",
		"Use exactly the same bound metric identity for baseline, reads, rule and sample instructions; never broaden a selector.",
	}}
	actions := []string{
		"Obtain separate exact live authority; inspect target, complete rule/series collisions, sole ownership, existing channel controls and admitted incremental cost at most five RUB including native push pricing; unknown price is not free.",
		"Write up to the allocated fresh zero samples, separated by exactly one minute; advance immediately once the normal baseline is established, do not wait out the phase budget.",
		"Configure/read back only the owned rule with notifications unarmed; immediately before arming require fresh zero and native normal, then arm only the intended existing channel and account for any initial OK. If the supported surface cannot inspect normal without notification effects, stop for revised authority.",
		"Immediately before the first breach write, recheck fresh zero and native normal; then write up to the allocated benign one samples and observe the exact query, rule Alarm and operator push receipt.",
		"Write up to the allocated fresh zero samples; wait for breach to leave the delayed MAX window, then observe native clear and recovery receipt.",
		"Stop all test writes and observe the full twenty-minute idle interval without an idle notification; NoData is not OK.",
		"Disable only owned test notifications/rule under exact authority; read back final state and preserve incomplete stages and private receipts.",
	}
	observations := []string{
		"Fresh scoped assertions, supported controls, cost/IAM admission and approved absolute deadlines; otherwise incomplete.",
		"Exactly one zero series with a point no older than three minutes and usable in the delayed evaluation window; matching series with no points is not a baseline.",
		"Actual configuration readback matches this sheet, fresh zero/native normal immediately precedes arming, and initial OK is accounted; desired-vs-desired is not a readback.",
		"Fresh zero/native normal immediately precedes breach, then fresh threshold breach, actual native Alarm and human receipt; vendor status alone is insufficient.",
		"Fresh zero after delayed-window clearance, native metric-clear state and human recovery receipt; no semantic recovery claim.",
		"Full idle observation with no writes, no idle page and explicit missing-series/no-points disposition.",
		"Owned test disabled and read back, writer stopped, no unrelated changes; queued messages may still arrive.",
	}
	plan := CanaryPlan{Metadata: meta, Identity: identity, Budget: normalized.Budget, TimingSemantics: "Upper duration budgets, not scheduled starts: advance immediately on required observations; only idle requires its full twenty minutes. Cumulative duration budgets are latest aggregate accounting bounds, not instructions to wait.", Phases: []CanaryPlanPhase{}, StopConditions: []string{
		"Stop as incomplete on any missing or ambiguous write acknowledgement; never blindly retry an uncertain effect.",
		"Stop on wrong scope, extra series or recipient, selector expansion, stale clear, missing operator receipt, unavailable controls or concurrent editor.",
		"Stop before any aggregate write/read/value/planned-notification/time bound is exhausted or tariff/credential/budget uncertainty appears.",
		"Reserve all remaining idle and cleanup time; missing evidence never extends a phase or the absolute experiment deadline.",
		"If configuration or waiting consumes baseline freshness before arming or breach, stop incomplete or revise the plan within the original write/time caps; do not silently add refresh samples or restart the budget.",
		"No user material, raw logs, model/tool call, application probe, shared-resource fault, redrive, IAM grant or new credential.",
	}, Limitations: []string{
		"PLAN_ONLY: no writer payload, executor, cloud client, credential discovery, Terraform or activation authority.",
		"These are aggregate offline ceilings, not per-phase fresh allowances, live quota enforcement or a hard delivery/spend guarantee.",
		"UI-internal polling is not accounted as one request per page; the later live manifest must reconcile its actual read surface.",
		"Platform notification retries, duplicates and queued deliveries may exceed intended transitions; disabling cannot retract them.",
		"Only synthetic metric-condition recovery is tested; semantic ingestion and both real MVP execution routes remain unqualified.",
		"No deletion API or immediate metric erasure is implied; only explicitly owned test controls may be changed under separate authority.",
	}}
	var offset time.Duration
	for i, p := range normalized.Phases {
		d, _ := time.ParseDuration(p.Duration)
		actor := "authorized_operator_and_bounded_test_executor"
		if oneOf(p.ID, "preflight", "arming", "cleanup") {
			actor = "authorized_operator"
		}
		advance := "immediately_on_required_observation_within_upper_budget"
		if p.ID == "idle" {
			advance = "only_after_full_twenty_minute_idle_observation"
		}
		plan.Phases = append(plan.Phases, CanaryPlanPhase{Allocation: p, Actor: actor, CumulativeDurationBudget: (offset + d).String(), AdvancePolicy: advance, Action: actions[i], RequiredObservation: observations[i]})
		offset += d
	}
	return &CanaryBundle{Envelope: CanaryEnvelope{Metadata: meta, Policy: normalized, Bindings: bindingCopy}, Console: console, Plan: plan}, nil
}
