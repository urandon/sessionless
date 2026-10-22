package observabilityconfig

import (
	"encoding/json"
	"errors"
	"reflect"
	"regexp"
	"strings"
	"time"
	"unicode"
)

// CanaryGeneratorVersion is separate from the production dashboard generator.
// Canary policy is an offline synthetic plan, not a qualified production query.
const CanaryGeneratorVersion = "observability-canary/1.0.0"

type CanaryMetric struct {
	Service            string `yaml:"service" json:"service"`
	Name               string `yaml:"name" json:"name"`
	MetricType         string `yaml:"metric_type" json:"metric_type"`
	Unit               string `yaml:"unit" json:"unit"`
	InstanceLabel      string `yaml:"instance_label" json:"instance_label"`
	MaxSeries          int    `yaml:"max_series" json:"max_series"`
	MaxPointsPerSeries int    `yaml:"max_points_per_series" json:"max_points_per_series"`
	GapFilling         string `yaml:"gap_filling" json:"gap_filling"`
}
type CanaryRule struct {
	Aggregation     string        `yaml:"aggregation" json:"aggregation"`
	Operator        string        `yaml:"operator" json:"operator"`
	Threshold       float64       `yaml:"threshold" json:"threshold"`
	Window          string        `yaml:"window" json:"window"`
	EvaluationDelay string        `yaml:"evaluation_delay" json:"evaluation_delay"`
	Freshness       string        `yaml:"freshness" json:"freshness"`
	NoData          NoData        `yaml:"no_data" json:"no_data"`
	Notifications   Notifications `yaml:"notifications" json:"notifications"`
	Recovery        string        `yaml:"recovery" json:"recovery"`
}
type CanaryBudget struct {
	MetricWrites            int     `yaml:"metric_writes" json:"metric_writes"`
	ReadRequests            int     `yaml:"read_requests" json:"read_requests"`
	ReturnedValues          int     `yaml:"returned_values" json:"returned_values"`
	PlannedNotifications    int     `yaml:"planned_notifications" json:"planned_notifications"`
	WallTime                string  `yaml:"wall_time" json:"wall_time"`
	EstimatedIncrementalRUB float64 `yaml:"estimated_incremental_rub" json:"estimated_incremental_rub"`
}
type CanaryPhase struct {
	ID                   string  `yaml:"id" json:"id"`
	Duration             string  `yaml:"duration" json:"duration"`
	MaxSamples           int     `yaml:"max_samples" json:"max_samples"`
	SampleValue          float64 `yaml:"sample_value" json:"sample_value"`
	MinSampleSpacing     string  `yaml:"min_sample_spacing" json:"min_sample_spacing"`
	ReadRequests         int     `yaml:"read_requests" json:"read_requests"`
	ReturnedValues       int     `yaml:"returned_values" json:"returned_values"`
	PlannedNotifications int     `yaml:"planned_notifications" json:"planned_notifications"`
}
type CanaryDocument struct {
	SchemaVersion int           `yaml:"schema_version" json:"schema_version"`
	Kind          string        `yaml:"kind" json:"kind"`
	ID            string        `yaml:"id" json:"id"`
	Evidence      string        `yaml:"evidence" json:"evidence"`
	Metric        CanaryMetric  `yaml:"metric" json:"metric"`
	Rule          CanaryRule    `yaml:"rule" json:"rule"`
	Budget        CanaryBudget  `yaml:"budget" json:"budget"`
	Phases        []CanaryPhase `yaml:"phases" json:"phases"`
}

// All private preflight fields are supplied operator assertions. Loading them
// acquires no cloud evidence and can never change planned_not_observed.
type CanaryPreflight struct {
	ObservedAt                    string `yaml:"observed_at" json:"observed_at"`
	SoleOwner                     bool   `yaml:"sole_owner" json:"sole_owner"`
	CompleteRuleList              bool   `yaml:"complete_rule_list" json:"complete_rule_list"`
	CompleteSeriesList            bool   `yaml:"complete_series_list" json:"complete_series_list"`
	RuleCollision                 bool   `yaml:"rule_collision" json:"rule_collision"`
	SeriesCollision               bool   `yaml:"series_collision" json:"series_collision"`
	CloudPush                     bool   `yaml:"cloud_push" json:"cloud_push"`
	ChannelScopeConfirmed         bool   `yaml:"channel_scope_confirmed" json:"channel_scope_confirmed"`
	OneConfirmedRecipient         bool   `yaml:"one_confirmed_recipient" json:"one_confirmed_recipient"`
	NoDefaultSeveritySubscription bool   `yaml:"no_default_severity_subscription" json:"no_default_severity_subscription"`
	NotificationStatesChecked     bool   `yaml:"notification_states_checked" json:"notification_states_checked"`
	RepeatChecked                 bool   `yaml:"repeat_checked" json:"repeat_checked"`
}
type CanaryBindings struct {
	Revision  string          `yaml:"revision" json:"revision"`
	FolderID  string          `yaml:"folder_id" json:"folder_id"`
	ProjectID string          `yaml:"project_id" json:"project_id"`
	ChannelID string          `yaml:"channel_id" json:"channel_id"`
	Instance  string          `yaml:"instance" json:"instance"`
	Preflight CanaryPreflight `yaml:"preflight" json:"preflight"`
}
type CanaryBindingsDocument struct {
	SchemaVersion int            `yaml:"schema_version" json:"schema_version"`
	Bindings      CanaryBindings `yaml:"bindings" json:"bindings"`
}

func LoadCanary(path string) (*CanaryDocument, error) {
	var c CanaryDocument
	if err := readDocument(path, &c); err != nil {
		return nil, err
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}
func ReadCanaryBindings(path string) (*CanaryBindingsDocument, error) {
	if err := privateFile(path); err != nil {
		return nil, err
	}
	var b CanaryBindingsDocument
	if err := readDocument(path, &b); err != nil {
		return nil, err
	}
	return &b, nil
}
func (c *CanaryDocument) Validate() error {
	if c == nil || c.SchemaVersion != 1 || c.Kind != "synthetic_metric_canary" || c.ID != "obs228" || c.Evidence != "planned_not_observed" {
		return errors.New("closed synthetic canary identity required")
	}
	m, r, b := c.Metric, c.Rule, c.Budget
	if m.Service != "custom" || m.Name != "sessionless_obs228_canary" || m.MetricType != "DGAUGE" || m.Unit != "dimensionless" || m.InstanceLabel != "instance" || m.MaxSeries != 1 || m.MaxPointsPerSeries != 60 || m.GapFilling != "none" {
		return errors.New("closed synthetic metric required")
	}
	if r.Aggregation != "max" || r.Operator != "gt" || r.Threshold != 0.5 || r.Window != "2m" || r.EvaluationDelay != "1m" || r.Freshness != "3m" || r.NoData.NoMatchingMetrics != "no_data" || r.NoData.NoPoints != "no_data" || r.Notifications.Repeat || !sameSet(r.Notifications.ChannelRefs, []string{"operator_primary"}) || !sameSet(r.Notifications.States, []string{"alarm", "ok"}) || r.Recovery != "synthetic_metric_clear_only" {
		return errors.New("closed synthetic rule and notification policy required")
	}
	if b.MetricWrites < 1 || b.MetricWrites > 12 || b.ReadRequests < 1 || b.ReadRequests > 40 || b.ReturnedValues < 1 || b.ReturnedValues > 10000 || b.PlannedNotifications < 1 || b.PlannedNotifications > 4 || !(b.EstimatedIncrementalRUB > 0 && b.EstimatedIncrementalRUB <= 5) || b.WallTime != "90m" {
		return errors.New("aggregate canary budget rejected")
	}
	ids := []string{"preflight", "baseline", "arming", "breach", "clear", "idle", "cleanup"}
	durations := []string{"10m", "10m", "10m", "15m", "15m", "20m", "10m"}
	if len(c.Phases) != len(ids) {
		return errors.New("complete ordered canary phases required")
	}
	writes, reads, values, notifications := 0, 0, 0, 0
	var total time.Duration
	for i, p := range c.Phases {
		if p.ID != ids[i] || p.Duration != durations[i] || p.ReadRequests < 1 || p.ReturnedValues < 0 || p.PlannedNotifications < 0 || p.ReadRequests > b.ReadRequests || p.ReturnedValues > b.ReturnedValues || p.PlannedNotifications > b.PlannedNotifications {
			return errors.New("canary phase identity, allocation or duration rejected")
		}
		d, err := duration(p.Duration)
		if err != nil {
			return err
		}
		total += d
		sampling := oneOf(p.ID, "baseline", "breach", "clear")
		wantValue := 0.0
		if p.ID == "breach" {
			wantValue = 1
		}
		if p.SampleValue != wantValue {
			return errors.New("canary sample value rejected")
		}
		if sampling {
			spacing, err := duration(p.MinSampleSpacing)
			if err != nil || p.MinSampleSpacing != "1m" || spacing != time.Minute || p.MaxSamples < 1 || p.MaxSamples > 4 || time.Duration(p.MaxSamples-1)*spacing > d || p.ReturnedValues == 0 {
				return errors.New("bounded fresh canary sampling required")
			}
		} else if p.MaxSamples != 0 || p.MinSampleSpacing != "0s" {
			return errors.New("non-sampling phase cannot write")
		}
		if (oneOf(p.ID, "preflight", "baseline", "idle", "cleanup") && p.PlannedNotifications != 0) || (oneOf(p.ID, "arming", "breach", "clear") && p.PlannedNotifications != 1) {
			return errors.New("closed per-phase planned notifications required")
		}
		writes += p.MaxSamples
		reads += p.ReadRequests
		values += p.ReturnedValues
		notifications += p.PlannedNotifications
	}
	wall, _ := time.ParseDuration(b.WallTime)
	if total != wall || writes > b.MetricWrites || reads > b.ReadRequests || values > b.ReturnedValues || notifications > b.PlannedNotifications {
		return errors.New("phase allocations exceed aggregate canary budget")
	}
	return nil
}

var canaryNonce = regexp.MustCompile(`^[a-f0-9]{32}$`)
var canaryFolder = regexp.MustCompile(`^[a-z][a-z0-9]{2,62}$`)

func validateCanaryBindings(doc *CanaryBindingsDocument, now time.Time) error {
	if doc == nil || now.IsZero() || doc.SchemaVersion != 1 {
		return errors.New("explicit canary binding and validation time required")
	}
	b := doc.Bindings
	// The observed native channel prefix is not a documented provider ID grammar.
	// Treat this as opaque local data; scope is a separate operator assertion.
	channelSafe := len(b.ChannelID) <= 256 && safePrivateValue(b.ChannelID) && strings.IndexFunc(b.ChannelID, func(r rune) bool { return unicode.IsControl(r) || unicode.IsSpace(r) }) < 0
	if !identifier.MatchString(b.Revision) || !canaryFolder.MatchString(b.FolderID) || b.ProjectID != "folder__"+b.FolderID || !channelSafe || !canaryNonce.MatchString(b.Instance) {
		return errors.New("private synthetic target identity rejected")
	}
	p := b.Preflight
	if !freshTimestamp(p.ObservedAt, now, time.Hour) || !p.SoleOwner || !p.CompleteRuleList || !p.CompleteSeriesList || p.RuleCollision || p.SeriesCollision || !p.CloudPush || !p.ChannelScopeConfirmed || !p.OneConfirmedRecipient || !p.NoDefaultSeveritySubscription || !p.NotificationStatesChecked || !p.RepeatChecked {
		return errors.New("fresh complete collision-free operator canary assertions required")
	}
	return nil
}

func CanarySchemas() map[string]any {
	out := map[string]any{}
	for name, typ := range map[string]reflect.Type{"canary": reflect.TypeOf(CanaryDocument{}), "canary-bindings": reflect.TypeOf(CanaryBindingsDocument{})} {
		schema := shapeSchema(typ)
		schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
		out[name] = schema
	}
	return out
}

func canaryDigest(value any) (string, error) {
	b, err := json.Marshal(value)
	if err != nil {
		return "", errors.New("cannot encode canary manifest")
	}
	return digest(b), nil
}
