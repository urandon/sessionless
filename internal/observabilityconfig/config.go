// Package observabilityconfig validates and exports the offline observability
// configuration. It has no cloud client, credential discovery or apply operation.
package observabilityconfig

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode"

	"gopkg.in/yaml.v3"
)

const (
	SchemaVersion    = 1
	GeneratorVersion = "observability-config/1.0.0"
	MaxDocumentBytes = 1 << 20
	maxNodes         = 10000
)

type Query struct {
	ID                string            `yaml:"id" json:"id"`
	Source            string            `yaml:"source" json:"source"`
	Service           string            `yaml:"service" json:"service"`
	Metric            string            `yaml:"metric" json:"metric"`
	MetricType        string            `yaml:"metric_type" json:"metric_type"`
	Unit              string            `yaml:"unit" json:"unit"`
	Aggregation       string            `yaml:"aggregation" json:"aggregation"`
	SeriesAggregation string            `yaml:"series_aggregation" json:"series_aggregation"`
	Evidence          string            `yaml:"evidence" json:"evidence"`
	Qualifications    []string          `yaml:"qualifications" json:"qualifications"`
	LabelKeys         []string          `yaml:"label_keys" json:"label_keys"`
	FixedLabels       map[string]string `yaml:"fixed_labels,omitempty" json:"fixed_labels,omitempty"`
	MaxSeries         int               `yaml:"max_series" json:"max_series"`
	Sparse            bool              `yaml:"sparse" json:"sparse"`
	Freshness         string            `yaml:"freshness" json:"freshness"`
}
type Panel struct {
	ID         string `yaml:"id" json:"id"`
	Title      string `yaml:"title" json:"title"`
	QueryRef   string `yaml:"query_ref,omitempty" json:"query_ref,omitempty"`
	Gap        string `yaml:"gap,omitempty" json:"gap,omitempty"`
	RunbookRef string `yaml:"runbook_ref" json:"runbook_ref"`
}
type Section struct {
	ID     string  `yaml:"id" json:"id"`
	Title  string  `yaml:"title" json:"title"`
	Panels []Panel `yaml:"panels" json:"panels"`
}
type Dashboard struct {
	ID       string    `yaml:"id" json:"id"`
	Name     string    `yaml:"name" json:"name"`
	Title    string    `yaml:"title" json:"title"`
	Sections []Section `yaml:"sections" json:"sections"`
}
type Condition struct {
	Aggregation string  `yaml:"aggregation" json:"aggregation"`
	Operator    string  `yaml:"operator" json:"operator"`
	Value       float64 `yaml:"value" json:"value"`
	Unit        string  `yaml:"unit" json:"unit"`
	Window      string  `yaml:"window" json:"window"`
}
type NoData struct {
	NoMatchingMetrics string `yaml:"no_matching_metrics" json:"no_matching_metrics"`
	NoPoints          string `yaml:"no_points" json:"no_points"`
}
type Notifications struct {
	ChannelRefs []string `yaml:"channel_refs" json:"channel_refs"`
	States      []string `yaml:"states" json:"states"`
	Repeat      bool     `yaml:"repeat" json:"repeat"`
}
type Alert struct {
	ID              string        `yaml:"id" json:"id"`
	QueryRef        string        `yaml:"query_ref" json:"query_ref"`
	Class           string        `yaml:"class" json:"class"`
	Condition       Condition     `yaml:"condition" json:"condition"`
	EvaluationDelay string        `yaml:"evaluation_delay" json:"evaluation_delay"`
	Demand          string        `yaml:"demand" json:"demand"`
	Freshness       string        `yaml:"freshness" json:"freshness"`
	NoData          NoData        `yaml:"no_data" json:"no_data"`
	Notifications   Notifications `yaml:"notifications" json:"notifications"`
	Recovery        string        `yaml:"recovery" json:"recovery"`
	Owner           string        `yaml:"owner" json:"owner"`
	RunbookRef      string        `yaml:"runbook_ref" json:"runbook_ref"`
}
type Profile struct {
	ID                 string   `yaml:"id" json:"id"`
	DashboardRef       string   `yaml:"dashboard_ref" json:"dashboard_ref"`
	EnabledQueries     []string `yaml:"enabled_queries" json:"enabled_queries"`
	EnabledAlerts      []string `yaml:"enabled_alerts" json:"enabled_alerts"`
	ChannelRefs        []string `yaml:"channel_refs" json:"channel_refs"`
	RunbookRefs        []string `yaml:"runbook_refs" json:"runbook_refs"`
	MaxLeafRules       int      `yaml:"max_leaf_rules" json:"max_leaf_rules"`
	MaxSemanticSeries  int      `yaml:"max_semantic_series" json:"max_semantic_series"`
	MaxReadValues      int      `yaml:"max_read_values" json:"max_read_values"`
	MaxSeriesPerLoad   int      `yaml:"max_series_per_load" json:"max_series_per_load"`
	MaxPointsPerSeries int      `yaml:"max_points_per_series" json:"max_points_per_series"`
	AutoRefresh        bool     `yaml:"auto_refresh" json:"auto_refresh"`
}
type Config struct {
	SchemaVersion int         `json:"schema_version"`
	Queries       []Query     `json:"queries"`
	Dashboards    []Dashboard `json:"dashboards"`
	Alerts        []Alert     `json:"alerts"`
	Profile       Profile     `json:"profile"`
	InputDigest   string      `json:"-"`
}
type queriesDoc struct {
	SchemaVersion int     `yaml:"schema_version"`
	Queries       []Query `yaml:"queries"`
}
type dashboardsDoc struct {
	SchemaVersion int         `yaml:"schema_version"`
	Dashboards    []Dashboard `yaml:"dashboards"`
}
type alertsDoc struct {
	SchemaVersion int     `yaml:"schema_version"`
	Alerts        []Alert `yaml:"alerts"`
}
type profileDoc struct {
	SchemaVersion int     `yaml:"schema_version"`
	Profile       Profile `yaml:"profile"`
}

var identifier = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,79}$`)
var metricName = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_.-]{0,119}$`)
var suspicious = regexp.MustCompile(`(?i)(https?://|bearer[ ]|secret[=:]|token[=:]|eyJ[A-Za-z0-9_-]{10}|\b(?:b1g|b1f|etn)[a-z0-9]{15,}\b)`)
var ambiguous = regexp.MustCompile(`(?i)^(yes|no|on|off|null|~|\.inf|[-+]\.inf|\.nan)$`)

// Decode accepts exactly one bounded document. Errors deliberately omit source
// snippets and values: private files must not leak through parser diagnostics.
func Decode(data []byte, target any) error {
	if len(data) == 0 || len(data) > MaxDocumentBytes {
		return errors.New("document size outside allowed bound")
	}
	var node yaml.Node
	d := yaml.NewDecoder(bytes.NewReader(data))
	if err := d.Decode(&node); err != nil {
		return errors.New("invalid YAML syntax")
	}
	var extra yaml.Node
	if err := d.Decode(&extra); err != io.EOF {
		return errors.New("exactly one YAML document required")
	}
	nodes := 0
	if err := inspectNode(&node, 0, &nodes); err != nil {
		return err
	}
	t := reflect.TypeOf(target)
	if t == nil || t.Kind() != reflect.Pointer {
		return errors.New("decode target must be a pointer")
	}
	if err := inspectShape(node.Content[0], t.Elem()); err != nil {
		return err
	}
	d = yaml.NewDecoder(bytes.NewReader(data))
	d.KnownFields(true)
	if err := d.Decode(target); err != nil {
		return errors.New("YAML does not match the closed typed schema")
	}
	return nil
}
func inspectNode(n *yaml.Node, depth int, nodes *int, allowNull ...bool) error {
	*nodes++
	if depth > 32 || *nodes > maxNodes {
		return errors.New("YAML complexity bound exceeded")
	}
	if n.Anchor != "" || n.Kind == yaml.AliasNode {
		return errors.New("YAML anchors and aliases are forbidden")
	}
	if n.Style&yaml.TaggedStyle != 0 {
		return errors.New("explicit YAML tags are forbidden")
	}
	if n.Kind == yaml.ScalarNode {
		if len(n.Value) > 2048 || strings.IndexFunc(n.Value, func(r rune) bool { return unicode.IsControl(r) && r != '\n' }) >= 0 {
			return errors.New("scalar length or control character rejected")
		}
		if n.Tag != "!!str" && n.Tag != "!!int" && n.Tag != "!!float" && n.Tag != "!!bool" && !(n.Tag == "!!null" && len(allowNull) > 0 && allowNull[0]) {
			return errors.New("unsupported scalar type")
		}
		if n.Tag == "!!str" && n.Style == 0 && ambiguous.MatchString(n.Value) {
			return errors.New("ambiguous scalar must be quoted")
		}
	}
	if n.Kind == yaml.MappingNode {
		seen := map[string]bool{}
		for i := 0; i < len(n.Content); i += 2 {
			k := n.Content[i]
			if k.Kind != yaml.ScalarNode || k.Tag != "!!str" || k.Value == "<<" || seen[k.Value] {
				return errors.New("duplicate, merge or non-string key rejected")
			}
			seen[k.Value] = true
		}
	}
	for _, child := range n.Content {
		if err := inspectNode(child, depth+1, nodes, allowNull...); err != nil {
			return err
		}
	}
	return nil
}

// Required fields are all struct fields except those explicitly marked omitempty.
// Checking nodes before decode avoids YAML's string/number/bool coercions.
func inspectShape(n *yaml.Node, t reflect.Type) error {
	return inspectTypedShape(n, t, "yaml")
}
func inspectTypedShape(n *yaml.Node, t reflect.Type, tagName string) error {
	if t.Kind() == reflect.Pointer {
		if n.Tag == "!!null" {
			return nil
		}
		return inspectTypedShape(n, t.Elem(), tagName)
	}
	if t.Kind() == reflect.Struct {
		if n.Kind != yaml.MappingNode {
			return errors.New("expected mapping")
		}
		fields := map[string]reflect.StructField{}
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			tag := f.Tag.Get(tagName)
			if tag == "-" {
				continue
			}
			fields[strings.Split(tag, ",")[0]] = f
		}
		seen := map[string]bool{}
		for i := 0; i < len(n.Content); i += 2 {
			key := n.Content[i].Value
			f, ok := fields[key]
			if !ok {
				return errors.New("unknown schema field")
			}
			seen[key] = true
			if err := inspectTypedShape(n.Content[i+1], f.Type, tagName); err != nil {
				return err
			}
		}
		for key, f := range fields {
			if !seen[key] && !strings.Contains(f.Tag.Get(tagName), ",omitempty") {
				return errors.New("required schema field missing")
			}
		}
		return nil
	}
	if t.Kind() == reflect.Slice {
		if n.Kind != yaml.SequenceNode || len(n.Content) > 128 {
			return errors.New("expected bounded sequence")
		}
		for _, c := range n.Content {
			if err := inspectTypedShape(c, t.Elem(), tagName); err != nil {
				return err
			}
		}
		return nil
	}
	if t.Kind() == reflect.Map {
		if n.Kind != yaml.MappingNode || len(n.Content) > 64 {
			return errors.New("expected bounded mapping")
		}
		for i := 0; i < len(n.Content); i += 2 {
			if err := inspectTypedShape(n.Content[i+1], t.Elem(), tagName); err != nil {
				return err
			}
		}
		return nil
	}
	if n.Kind != yaml.ScalarNode {
		return errors.New("expected scalar")
	}
	switch t.Kind() {
	case reflect.String:
		if n.Tag != "!!str" {
			return errors.New("string scalar required")
		}
	case reflect.Bool:
		if n.Tag != "!!bool" || (n.Value != "true" && n.Value != "false") {
			return errors.New("explicit true or false required")
		}
	case reflect.Int:
		if n.Tag != "!!int" || !regexp.MustCompile(`^(0|[1-9][0-9]*)$`).MatchString(n.Value) {
			return errors.New("decimal integer required")
		}
	case reflect.Float64:
		if (n.Tag != "!!float" && n.Tag != "!!int") || !regexp.MustCompile(`^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][-+]?[0-9]+)?$`).MatchString(n.Value) {
			return errors.New("number required")
		}
	default:
		return errors.New("unsupported schema type")
	}
	return nil
}
func readDocument(path string, target any) error {
	info, err := os.Lstat(path)
	if err != nil {
		return errors.New("cannot inspect configuration file")
	}
	if !info.Mode().IsRegular() || info.Size() > MaxDocumentBytes {
		return errors.New("configuration file must be bounded and regular")
	}
	f, err := os.Open(path)
	if err != nil {
		return errors.New("cannot open configuration file")
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, MaxDocumentBytes+1))
	if err != nil {
		return errors.New("cannot read configuration file")
	}
	return Decode(data, target)
}

// Load reads only the four named files, without environment substitutions.
func Load(root, profile string) (*Config, error) {
	if !identifier.MatchString(profile) {
		return nil, errors.New("invalid profile name")
	}
	var q queriesDoc
	var d dashboardsDoc
	var a alertsDoc
	var p profileDoc
	for _, v := range []struct {
		path string
		dst  any
	}{{"queries.yaml", &q}, {"dashboards.yaml", &d}, {"alerts.yaml", &a}, {filepath.Join("profiles", profile+".yaml"), &p}} {
		if err := readDocument(filepath.Join(root, v.path), v.dst); err != nil {
			return nil, fmt.Errorf("%s: %w", v.path, err)
		}
	}
	if q.SchemaVersion != 1 || d.SchemaVersion != 1 || a.SchemaVersion != 1 || p.SchemaVersion != 1 {
		return nil, errors.New("unsupported schema version")
	}
	c := &Config{SchemaVersion: 1, Queries: q.Queries, Dashboards: d.Dashboards, Alerts: a.Alerts, Profile: p.Profile}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	if c.Profile.ID != profile {
		return nil, errors.New("profile identity mismatch")
	}
	c.Normalize()
	encoded, _ := json.Marshal(c)
	c.InputDigest = digest(encoded)
	return c, nil
}
func digest(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }
func oneOf(v string, values ...string) bool {
	for _, s := range values {
		if v == s {
			return true
		}
	}
	return false
}
func duration(s string) (time.Duration, error) {
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 || d > 24*time.Hour {
		return 0, errors.New("duration must be positive, explicit and at most 24h")
	}
	return d, nil
}
func uniqueIDs(ids []string) bool {
	seen := map[string]bool{}
	for _, id := range ids {
		if !identifier.MatchString(id) || seen[id] {
			return false
		}
		seen[id] = true
	}
	return true
}
func contains(v []string, s string) bool {
	for _, x := range v {
		if x == s {
			return true
		}
	}
	return false
}
func sameSet(a, b []string) bool {
	aa := append([]string(nil), a...)
	bb := append([]string(nil), b...)
	sort.Strings(aa)
	sort.Strings(bb)
	return reflect.DeepEqual(aa, bb)
}
func safeText(s string) bool {
	return len(s) > 0 && len(s) <= 320 && !suspicious.MatchString(s) && !strings.ContainsAny(s, "${}%") && strings.IndexFunc(s, unicode.IsControl) < 0
}
func (c *Config) Validate() error {
	if c.SchemaVersion != 1 || len(c.Queries) == 0 || len(c.Queries) > 64 || len(c.Dashboards) != 1 || len(c.Alerts) > 48 {
		return errors.New("configuration cardinality/version rejected")
	}
	p := c.Profile
	if !identifier.MatchString(p.ID) || !uniqueIDs(p.EnabledQueries) || !uniqueIDs(p.EnabledAlerts) || !uniqueIDs(p.RunbookRefs) || !uniqueIDs(p.ChannelRefs) || len(p.ChannelRefs) > 1 || p.AutoRefresh || p.MaxLeafRules < 1 || p.MaxLeafRules > 12 || p.MaxSemanticSeries < 1 || p.MaxSemanticSeries > 24 || p.MaxReadValues < 1 || p.MaxReadValues > 2000000 || p.MaxSeriesPerLoad < 1 || p.MaxSeriesPerLoad > 30 || p.MaxPointsPerSeries < 1 || p.MaxPointsPerSeries > 60 {
		return errors.New("profile safety budget or references rejected")
	}
	queries := map[string]Query{}
	series, semantic := 0, 0
	for _, q := range c.Queries {
		if !identifier.MatchString(q.ID) || queries[q.ID].ID != "" || !metricName.MatchString(q.Metric) || !metricName.MatchString(q.Service) || !oneOf(q.Source, "native", "semantic") || !oneOf(q.MetricType, "DGAUGE", "RATE", "IGAUGE") || !oneOf(q.Unit, "requests_per_second", "invocations_per_second", "messages", "bytes", "seconds", "milliseconds", "percent", "events") || !oneOf(q.Aggregation, "max", "avg", "sum", "p95") || !oneOf(q.Evidence, "VERIFIED", "CATALOG_ONLY", "PROPOSED", "DISABLED", "UNSUPPORTED") || q.MaxSeries < 1 || q.MaxSeries > 30 || !uniqueIDs(q.Qualifications) || !uniqueIDs(q.LabelKeys) {
			return errors.New("query contract rejected")
		}
		for _, x := range q.Qualifications {
			if !oneOf(x, "histogram_count", "queue_attributes", "compatible_limit", "semantic_bridge") {
				return errors.New("unsupported qualification")
			}
		}
		for _, key := range q.LabelKeys {
			if !allowedLabel(key) {
				return errors.New("private selector label is not allowed")
			}
		}
		if !oneOf(q.Service, "serverless-apigateway", "serverless-containers", "message-queue", "ydb", "storage", "logging_aggregates") || suspicious.MatchString(q.Metric) {
			return errors.New("query service or metric rejected")
		}
		if (q.Source == "semantic") != (q.Service == "logging_aggregates") {
			return errors.New("query source and service must agree on semantic delivery")
		}
		if !oneOf(q.SeriesAggregation, "none", "sum") {
			return errors.New("explicit closed cross-series aggregation required")
		}
		if q.Service == "serverless-apigateway" && q.Metric == "api_gateway.requests_count_per_second" && (q.SeriesAggregation != "sum" || q.MaxSeries != 1) {
			return errors.New("gateway activity requires the reviewed single series_sum aggregate")
		}
		if q.SeriesAggregation == "sum" && (q.Service != "serverless-apigateway" || q.Metric != "api_gateway.requests_count_per_second" || q.MaxSeries != 1) {
			return errors.New("cross-series sum is limited to the reviewed gateway rate aggregate")
		}
		if expected, ok := map[string]string{"api_gateway.requests_count_per_second": "requests_per_second", "api_gateway.errors_count_per_second": "requests_per_second", "serverless.containers.started_per_second": "invocations_per_second", "queue.messages.stored_count": "messages", "database_size": "bytes", "space_usage": "bytes"}[q.Metric]; ok && (q.Unit != expected || q.MetricType != "DGAUGE") {
			return errors.New("known source metric unit or type mismatch")
		}
		if q.Source == "native" && (contains(q.LabelKeys, "project") || contains(q.LabelKeys, "cluster") || contains(q.LabelKeys, "signal_id")) {
			return errors.New("native query cannot claim semantic project labels")
		}
		for key, value := range q.FixedLabels {
			if !oneOf(key, "resource_type", "storage_type", "signal_id") || !contains(q.LabelKeys, key) || !safeText(value) {
				return errors.New("fixed selector label rejected")
			}
		}
		if q.Metric == "space_usage" && (q.FixedLabels["storage_type"] != "All" || q.FixedLabels["resource_type"] != "bucket") {
			return errors.New("bucket space requires non-overlapping All total")
		}
		if q.Metric == "database_size" && q.FixedLabels["resource_type"] != "database.serverless" {
			return errors.New("YDB size requires serverless resource type")
		}
		if !contains(q.LabelKeys, "service") || !contains(q.LabelKeys, "name") {
			return errors.New("query requires exact service and name labels")
		}
		if q.Sparse {
			if q.Freshness != "last_observed" {
				return errors.New("sparse streams cannot be health heartbeats")
			}
		} else {
			if _, err := duration(q.Freshness); err != nil {
				return err
			}
		}
		if q.Aggregation == "sum" && (q.Unit == "requests_per_second" || q.Unit == "invocations_per_second" || q.Metric == "space_usage") {
			return errors.New("overlapping totals or rate samples cannot be summed")
		}
		if q.Aggregation == "p95" && !contains(q.Qualifications, "histogram_count") {
			return errors.New("percentile requires histogram and count qualification")
		}
		if q.Unit == "percent" && !contains(q.Qualifications, "compatible_limit") {
			return errors.New("ratio requires compatible used and positive limit")
		}
		if strings.Contains(q.Metric, "oldest_age") && (q.Unit != "seconds" || !contains(q.Qualifications, "queue_attributes")) {
			return errors.New("queue age requires seconds and full queue attributes")
		}
		if q.Source == "semantic" && (!contains(q.Qualifications, "semantic_bridge") || q.Unit != "events" || q.Service != "logging_aggregates" || !sameSet(q.LabelKeys, []string{"project", "cluster", "service", "name", "signal_id"}) || !allowedSignal(q.FixedLabels["signal_id"])) {
			return errors.New("semantic bridge is unqualified")
		}
		if contains(p.EnabledQueries, q.ID) {
			if q.Evidence != "VERIFIED" {
				return errors.New("unqualified query cannot be enabled")
			}
			if q.Source == "semantic" || q.Aggregation != "max" || q.Unit == "percent" {
				return errors.New("classic adapter cannot render this semantic, composed or percentile query")
			}
			series += q.MaxSeries
			if q.Source == "semantic" {
				semantic += q.MaxSeries
			}
		}
		queries[q.ID] = q
	}
	if series > p.MaxSeriesPerLoad || semantic > p.MaxSemanticSeries {
		return errors.New("rendered series budget exceeded")
	}
	for _, id := range p.EnabledQueries {
		if queries[id].ID == "" {
			return errors.New("enabled query reference missing")
		}
	}
	d := c.Dashboards[0]
	if d.ID != p.DashboardRef || !identifier.MatchString(d.ID) || !identifier.MatchString(d.Name) || !safeText(d.Title) || len(d.Sections) != 5 {
		return errors.New("one five-section owned overview required")
	}
	sections := []string{}
	panels := []string{}
	shown := map[string]bool{}
	for _, s := range d.Sections {
		sections = append(sections, s.ID)
		if !safeText(s.Title) || len(s.Panels) == 0 || len(s.Panels) > 12 {
			return errors.New("dashboard section rejected")
		}
		for _, panel := range s.Panels {
			panels = append(panels, panel.ID)
			if !safeText(panel.Title) || !contains(p.RunbookRefs, panel.RunbookRef) || (panel.QueryRef == "") == (panel.Gap == "") {
				return errors.New("panel needs one query or explicit safe gap and runbook")
			}
			if panel.Gap != "" && !safeText(panel.Gap) {
				return errors.New("unsafe gap text")
			}
			if panel.QueryRef != "" {
				if queries[panel.QueryRef].ID == "" || shown[panel.QueryRef] {
					return errors.New("panel query missing or duplicated")
				}
				shown[panel.QueryRef] = true
			}
		}
	}
	if !uniqueIDs(sections) || !uniqueIDs(panels) {
		return errors.New("duplicate dashboard identifiers")
	}
	alerts := map[string]Alert{}
	for _, a := range c.Alerts {
		q, ok := queries[a.QueryRef]
		if !identifier.MatchString(a.ID) || alerts[a.ID].ID != "" || !ok || !oneOf(a.Class, "platform", "latency", "queue", "capacity", "semantic") || a.Condition.Aggregation != q.Aggregation || a.Condition.Unit != q.Unit || a.Condition.Operator != "gt" || math.IsNaN(a.Condition.Value) || math.IsInf(a.Condition.Value, 0) || a.Condition.Value < 0 || a.NoData.NoMatchingMetrics != "no_data" || a.NoData.NoPoints != "no_data" || a.Notifications.Repeat || len(a.Notifications.ChannelRefs) != 1 || !sameSet(a.Notifications.ChannelRefs, p.ChannelRefs) || !uniqueIDs(a.Notifications.States) || !contains(a.Notifications.States, "alarm") || !oneOf(a.Recovery, "manual_positive_evidence", "metric_clear_only") || a.Owner != "deployment_operator" || !contains(p.RunbookRefs, a.RunbookRef) {
			return errors.New("alert contract or unsafe policy rejected")
		}
		for _, state := range a.Notifications.States {
			if !oneOf(state, "alarm", "ok") {
				return errors.New("idle NoData notifications forbidden")
			}
		}
		for _, v := range []string{a.Condition.Window, a.EvaluationDelay, a.Freshness} {
			if _, err := duration(v); err != nil {
				return err
			}
		}
		if !oneOf(a.Demand, "positive_activity", "expected_work", "positive_limit", "nonzero_occurrence") {
			return errors.New("explicit demand predicate required")
		}
		if (q.Service == "message-queue" && a.Class != "queue") || (q.Source == "semantic" && a.Class != "semantic") {
			return errors.New("alert class cannot relabel queue or semantic query safety")
		}
		if oneOf(a.Class, "queue", "semantic") || q.Source == "semantic" || q.Service == "message-queue" {
			if contains(a.Notifications.States, "ok") || a.Recovery != "manual_positive_evidence" {
				return errors.New("semantic automatic OK forbidden")
			}
		}
		if contains(a.Notifications.States, "ok") && (a.Recovery != "metric_clear_only" || a.Demand == "nonzero_occurrence") {
			return errors.New("positive fresh demand required for metric-clear notification")
		}
		if a.Class == "capacity" && (q.Unit != "percent" || a.Condition.Value != 85 || a.Condition.Window != "15m" || a.Demand != "positive_limit") {
			return errors.New("capacity policy requires compatible percent ratio and accepted threshold")
		}
		if a.Class == "latency" && (q.Aggregation != "p95" || q.Unit != "milliseconds" || a.Condition.Value != 2000 || a.Demand != "positive_activity") {
			return errors.New("latency policy requires qualified histogram/count and accepted threshold")
		}
		if contains(p.EnabledAlerts, a.ID) {
			if !contains(p.EnabledQueries, q.ID) {
				return errors.New("enabled alert requires enabled query")
			}
			if a.Class == "capacity" || a.Class == "latency" || q.Source == "semantic" {
				return errors.New("this adapter cannot activate composed, percentile or semantic rules")
			}
		}
		alerts[a.ID] = a
	}
	if len(p.EnabledAlerts) > p.MaxLeafRules {
		return errors.New("evaluated leaf-rule budget exceeded")
	}
	for _, id := range p.EnabledAlerts {
		if alerts[id].ID == "" {
			return errors.New("enabled alert reference missing")
		}
	}
	return nil
}
func allowedLabel(s string) bool {
	return oneOf(s, "service", "name", "gateway", "release", "container", "revision", "queue", "resource_type", "resource_id", "database_path", "storage_type", "location_id", "code", "bin", "operation", "project", "cluster", "signal_id")
}
func allowedSignal(s string) bool {
	return oneOf(s, "web.auth_failure", "web.session_failure", "web.upload_failure", "web.artifact_access_failure", "web.auth_success", "control.wake_deferred", "control.dispatch_failure", "control.queue_failure", "managed.execution_failure", "managed.publication_failure", "managed.cleanup_failure", "managed.deadline_exceeded", "managed.canonical_unknown", "managed.canonical_success", "managed.cleanup_success", "attached.transport_failure", "attached.execution_failure", "attached.publication_failure", "attached.cleanup_failure", "attached.deadline_exceeded", "attached.canonical_unknown", "attached.canonical_success")
}

// Normalize preserves visual panel/section order, sorting unordered catalogs and refs.
func (c *Config) Normalize() {
	sort.Slice(c.Queries, func(i, j int) bool { return c.Queries[i].ID < c.Queries[j].ID })
	sort.Slice(c.Alerts, func(i, j int) bool { return c.Alerts[i].ID < c.Alerts[j].ID })
	for i := range c.Queries {
		sort.Strings(c.Queries[i].Qualifications)
		sort.Strings(c.Queries[i].LabelKeys)
	}
	for i := range c.Alerts {
		sort.Strings(c.Alerts[i].Notifications.ChannelRefs)
		sort.Strings(c.Alerts[i].Notifications.States)
	}
	sort.Strings(c.Profile.EnabledQueries)
	sort.Strings(c.Profile.EnabledAlerts)
	sort.Strings(c.Profile.ChannelRefs)
	sort.Strings(c.Profile.RunbookRefs)
}

// PublicSchemas returns machine-readable closed shape schemas. Business invariants
// (units, references, budgets, qualification and recovery) remain in Validate.
func PublicSchemas() map[string]any {
	out := map[string]any{}
	for name, t := range map[string]reflect.Type{"queries": reflect.TypeOf(queriesDoc{}), "dashboards": reflect.TypeOf(dashboardsDoc{}), "alerts": reflect.TypeOf(alertsDoc{}), "profile": reflect.TypeOf(profileDoc{})} {
		s := shapeSchema(t)
		s["$schema"] = "https://json-schema.org/draft/2020-12/schema"
		out[name] = s
	}
	return out
}
func shapeSchema(t reflect.Type) map[string]any {
	s := map[string]any{}
	switch t.Kind() {
	case reflect.Struct:
		p := map[string]any{}
		required := []string{}
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			tag := f.Tag.Get("yaml")
			key := strings.Split(tag, ",")[0]
			p[key] = shapeSchema(f.Type)
			if !strings.Contains(tag, ",omitempty") {
				required = append(required, key)
			}
		}
		s["type"] = "object"
		s["properties"] = p
		s["required"] = required
		s["additionalProperties"] = false
	case reflect.Slice:
		s["type"] = "array"
		s["items"] = shapeSchema(t.Elem())
		s["maxItems"] = 128
	case reflect.Map:
		s["type"] = "object"
		s["additionalProperties"] = shapeSchema(t.Elem())
		s["maxProperties"] = 16
	case reflect.String:
		s["type"] = "string"
		s["maxLength"] = 2048
	case reflect.Int:
		s["type"] = "integer"
		s["minimum"] = 0
	case reflect.Float64:
		s["type"] = "number"
	case reflect.Bool:
		s["type"] = "boolean"
	}
	return s
}
