package observabilityconfig

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type QueryBinding struct {
	ID             string            `yaml:"id" json:"id"`
	ObservedAt     string            `yaml:"observed_at" json:"observed_at"`
	MetricType     string            `yaml:"metric_type" json:"metric_type"`
	Unit           string            `yaml:"unit" json:"unit"`
	Labels         map[string]string `yaml:"labels" json:"labels"`
	Qualifications []string          `yaml:"qualifications" json:"qualifications"`
}
type Bindings struct {
	Revision string         `yaml:"revision" json:"revision"`
	FolderID string         `yaml:"folder_id" json:"folder_id"`
	Queries  []QueryBinding `yaml:"queries" json:"queries"`
}
type BindingsDocument struct {
	SchemaVersion int      `yaml:"schema_version" json:"schema_version"`
	Bindings      Bindings `yaml:"bindings" json:"bindings"`
}
type ExistingDashboard struct {
	ID    string `yaml:"id" json:"id"`
	Name  string `yaml:"name" json:"name"`
	Owner string `yaml:"owner" json:"owner"`
}
type Ownership struct {
	FolderID       string              `yaml:"folder_id" json:"folder_id"`
	Name           string              `yaml:"name" json:"name"`
	ObservedAt     string              `yaml:"observed_at" json:"observed_at"`
	Complete       bool                `yaml:"complete" json:"complete"`
	SoleOwner      bool                `yaml:"sole_owner" json:"sole_owner"`
	Existing       []ExistingDashboard `yaml:"existing" json:"existing"`
	ImportID       string              `yaml:"import_id" json:"import_id"`
	SnapshotSHA256 string              `yaml:"snapshot_sha256" json:"snapshot_sha256"`
}
type OwnershipDocument struct {
	SchemaVersion int       `yaml:"schema_version" json:"schema_version"`
	Ownership     Ownership `yaml:"ownership" json:"ownership"`
}
type Metadata struct {
	SchemaVersion    int    `json:"schema_version"`
	GeneratorVersion string `json:"generator_version"`
	InputSHA256      string `json:"input_sha256"`
	BindingRevision  string `json:"binding_revision"`
	BindingSHA256    string `json:"binding_sha256"`
	OwnershipSHA256  string `json:"ownership_sha256"`
	EvaluatedAt      string `json:"evaluated_at"`
	Mode             string `json:"mode"`
	Authority        string `json:"authority"`
}
type ConsoleRule struct {
	Policy    Alert    `json:"policy"`
	Selected  bool     `json:"selected"`
	Status    string   `json:"status"`
	Selector  string   `json:"selector,omitempty"`
	Checklist []string `json:"checklist"`
}
type PanelPreview struct {
	ID         string `json:"id"`
	Title      string `json:"title"`
	State      string `json:"state"`
	Evidence   string `json:"evidence,omitempty"`
	Freshness  string `json:"freshness,omitempty"`
	RunbookRef string `json:"runbook_ref"`
}
type Envelope struct {
	Metadata      Metadata       `json:"metadata"`
	Policy        Config         `json:"policy"`
	QueryBindings []QueryBinding `json:"query_bindings"`
	Ownership     *Ownership     `json:"ownership"`
	Panels        []PanelPreview `json:"panels"`
	Console       []ConsoleRule  `json:"console"`
}
type Bundle struct {
	Envelope  Envelope
	Terraform map[string]any
}

func privateFile(path string) error {
	i, err := os.Lstat(path)
	if err != nil || !i.Mode().IsRegular() || i.Mode().Perm()&0077 != 0 {
		return errors.New("private input must be a bounded owner-only regular file")
	}
	return nil
}
func ReadBindings(path string) (*BindingsDocument, error) {
	var b BindingsDocument
	if err := privateFile(path); err != nil {
		return nil, err
	}
	if err := readDocument(path, &b); err != nil {
		return nil, err
	}
	return &b, nil
}
func ReadOwnership(path string) (*OwnershipDocument, error) {
	var o OwnershipDocument
	if err := privateFile(path); err != nil {
		return nil, err
	}
	if err := readDocument(path, &o); err != nil {
		return nil, err
	}
	return &o, nil
}
func freshTimestamp(value string, now time.Time, maxAge time.Duration) bool {
	at, err := time.Parse(time.RFC3339, value)
	return err == nil && !at.After(now) && now.Sub(at) <= maxAge
}
func safePrivateValue(s string) bool {
	return len(s) > 0 && len(s) <= 512 && !strings.ContainsAny(s, "\x00\n\r\t*?{}$%\\\"")
}

// Render never applies configuration. Bound mode requires explicit dated private
// receipts. Their contents are operator assertions, not independently acquired
// cloud evidence or authority to deploy. Preview mode emits no Terraform.
func Render(c *Config, b *BindingsDocument, o *OwnershipDocument, now time.Time) (*Bundle, error) {
	if c == nil {
		return nil, errors.New("configuration required")
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	copyBytes, err := json.Marshal(c)
	if err != nil {
		return nil, errors.New("cannot normalize policy")
	}
	var normalized Config
	if err = json.Unmarshal(copyBytes, &normalized); err != nil {
		return nil, errors.New("cannot normalize policy")
	}
	normalized.Normalize()
	normalizedBytes, _ := json.Marshal(normalized)
	inputDigest := digest(normalizedBytes)
	meta := Metadata{SchemaVersion: 1, GeneratorVersion: GeneratorVersion, InputSHA256: inputDigest, Mode: "preview", Authority: "offline_only_no_apply_authority"}
	queries := map[string]Query{}
	for _, q := range normalized.Queries {
		queries[q.ID] = q
	}
	resolved := map[string]QueryBinding{}
	bound := b != nil || o != nil
	if bound {
		if b == nil || o == nil || now.IsZero() {
			return nil, errors.New("bound export requires bindings, ownership and explicit now")
		}
		if b.SchemaVersion != 1 || o.SchemaVersion != 1 || !identifier.MatchString(b.Bindings.Revision) || !safePrivateValue(b.Bindings.FolderID) {
			return nil, errors.New("private binding identity rejected")
		}
		for _, binding := range b.Bindings.Queries {
			q, exists := queries[binding.ID]
			if !exists || resolved[binding.ID].ID != "" || binding.MetricType != q.MetricType || binding.Unit != q.Unit || !uniqueIDs(binding.Qualifications) || !freshTimestamp(binding.ObservedAt, now, 24*time.Hour) {
				return nil, errors.New("private query evidence mismatch, duplicate or stale")
			}
			keys := []string{}
			for key, value := range binding.Labels {
				keys = append(keys, key)
				if !allowedLabel(key) || !safePrivateValue(value) || (key == "operation" && !metricName.MatchString(value)) {
					return nil, errors.New("private query label rejected")
				}
			}
			if !sameSet(keys, q.LabelKeys) || binding.Labels["service"] != q.Service || binding.Labels["name"] != q.Metric {
				return nil, errors.New("private selector may not override or broaden reviewed query")
			}
			for key, value := range q.FixedLabels {
				if binding.Labels[key] != value {
					return nil, errors.New("private selector changed fixed policy label")
				}
			}
			if !sameSet(binding.Qualifications, q.Qualifications) {
				return nil, errors.New("private qualification evidence mismatch")
			}
			resolved[binding.ID] = binding
		}
		for _, id := range normalized.Profile.EnabledQueries {
			if resolved[id].ID == "" {
				return nil, errors.New("enabled query has no fresh exact private binding")
			}
		}
		if err := GuardOwnership(normalized.Dashboards[0], b.Bindings.FolderID, o.Ownership, now); err != nil {
			return nil, err
		}
		bd := b.Bindings
		bd.Queries = append([]QueryBinding(nil), bd.Queries...)
		sort.Slice(bd.Queries, func(i, j int) bool { return bd.Queries[i].ID < bd.Queries[j].ID })
		for i := range bd.Queries {
			bd.Queries[i].Qualifications = append([]string{}, bd.Queries[i].Qualifications...)
			sort.Strings(bd.Queries[i].Qualifications)
		}
		bb, _ := json.Marshal(bd)
		oo, _ := json.Marshal(o.Ownership)
		meta.Mode = "bound_private"
		meta.BindingRevision = b.Bindings.Revision
		meta.BindingSHA256 = digest(bb)
		meta.OwnershipSHA256 = digest(oo)
		meta.EvaluatedAt = now.UTC().Format(time.RFC3339)
	}
	result := &Bundle{Envelope: Envelope{Metadata: meta, Policy: normalized, QueryBindings: []QueryBinding{}, Panels: []PanelPreview{}, Console: []ConsoleRule{}}}
	if bound {
		own := o.Ownership
		result.Envelope.Ownership = &own
		for _, q := range normalized.Queries {
			if binding, ok := resolved[q.ID]; ok {
				result.Envelope.QueryBindings = append(result.Envelope.QueryBindings, binding)
			}
		}
	}
	for _, section := range normalized.Dashboards[0].Sections {
		for _, panel := range section.Panels {
			p := PanelPreview{ID: panel.ID, Title: panel.Title, State: "gap", RunbookRef: panel.RunbookRef}
			if q, ok := queries[panel.QueryRef]; ok {
				p.Evidence = q.Evidence
				p.Freshness = q.Freshness
				p.State = "disabled_unqualified"
				if contains(normalized.Profile.EnabledQueries, q.ID) {
					if bound {
						p.State = "qualified_dated_query_not_health"
					} else {
						p.State = "unbound_not_deployable"
					}
				}
			}
			result.Envelope.Panels = append(result.Envelope.Panels, p)
		}
	}
	for _, alert := range normalized.Alerts {
		selected := contains(normalized.Profile.EnabledAlerts, alert.ID)
		rule := ConsoleRule{Policy: alert, Selected: selected, Status: "disabled_candidate", Checklist: []string{"Supported native console only; no alert CRUD API generated.", "Confirm exact metric type/unit, window, delay, demand and freshness against dated readback.", "No matching metrics and no points both remain NoData; no idle notification or periodic repeat.", "Resolve one operator-confirmed channel privately; no recipients or credentials in repository.", "Record actual policy readback and compare; configuration is not notification receipt.", "Require separately scoped live authority; full saved plan, snapshot and no concurrent edits."}}
		if selected {
			if bound {
				rule.Status = "selected_requires_console_readback_and_authority"
				rule.Selector = queryExpression(queries[alert.QueryRef], b.Bindings.FolderID, resolved[alert.QueryRef].Labels)
			} else {
				rule.Status = "unbound_not_deployable"
			}
		}
		result.Envelope.Console = append(result.Envelope.Console, rule)
	}
	if bound {
		result.Terraform = terraformDashboard(normalized, b.Bindings.FolderID, resolved)
	}
	return result, nil
}

// GuardOwnership blocks the pinned provider's same-name auto-adoption. A caller
// must still inspect the full saved plan; no offline assertion guarantees that
// the cloud has not changed after the supplied complete listing.
func GuardOwnership(d Dashboard, folder string, o Ownership, now time.Time) error {
	if o.FolderID != folder || o.Name != d.Name || !o.Complete || !o.SoleOwner || !freshTimestamp(o.ObservedAt, now, time.Hour) {
		return errors.New("fresh complete exact-folder sole-ownership preflight required")
	}
	seen := map[string]bool{}
	collisions := 0
	for _, existing := range o.Existing {
		if !safePrivateValue(existing.ID) || !safePrivateValue(existing.Name) || !safePrivateValue(existing.Owner) || seen[existing.ID] {
			return errors.New("ownership listing rejected")
		}
		seen[existing.ID] = true
		if existing.Name == d.Name {
			collisions++
			if existing.Owner != "sessionless" || o.ImportID != existing.ID || !regexp.MustCompile(`^[a-f0-9]{64}$`).MatchString(o.SnapshotSHA256) {
				return errors.New("same-name dashboard collision: explicit owned import and snapshot required")
			}
		}
	}
	if collisions > 1 {
		return errors.New("ambiguous dashboard name collision")
	}
	if collisions == 0 && (o.ImportID != "" || o.SnapshotSHA256 != "") {
		return errors.New("import assertion has no matching owned dashboard")
	}
	return nil
}
func selector(folder string, labels map[string]string) string {
	all := map[string]string{"folderId": folder}
	for key, value := range labels {
		if key != "name" {
			all[key] = value
		}
	}
	keys := []string{}
	for key := range all {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := []string{}
	for _, key := range keys {
		v, _ := json.Marshal(all[key])
		parts = append(parts, key+"="+string(v))
	}
	metric, _ := json.Marshal(labels["name"])
	return string(metric) + "{" + strings.Join(parts, ",") + "}"
}

// Cross-series aggregation combines matching label dimensions at each instant;
// it is deliberately separate from grid/time aggregation of a rate's samples.
func queryExpression(q Query, folder string, labels map[string]string) string {
	expression := selector(folder, labels)
	if q.SeriesAggregation == "sum" {
		return "series_sum(" + expression + ")"
	}
	return expression
}

// terraformDashboard is intentionally limited to the pinned classic resource.
// Semantic/ratio/percentile adapters are not silently approximated.
func terraformDashboard(c Config, folder string, bindings map[string]QueryBinding) map[string]any {
	widgets := []any{}
	y := 0
	queries := map[string]Query{}
	for _, q := range c.Queries {
		queries[q.ID] = q
	}
	for _, section := range c.Dashboards[0].Sections {
		widgets = append(widgets, map[string]any{"text": []any{map[string]any{"text": section.Title}}, "position": []any{map[string]any{"x": 0, "y": y, "w": 36, "h": 2}}})
		y += 2
		for _, panel := range section.Panels {
			q, exists := queries[panel.QueryRef]
			binding, hasBinding := bindings[panel.QueryRef]
			if exists && hasBinding && contains(c.Profile.EnabledQueries, q.ID) {
				widgets = append(widgets, chartWidget(panel.Title, queryExpression(q, folder, binding.Labels), q, y, c.Profile.MaxPointsPerSeries))
			} else {
				text := panel.Title + ": " + panel.Gap
				if exists {
					text = panel.Title + ": DISABLED / unqualified query; not zero or healthy."
				}
				widgets = append(widgets, map[string]any{"text": []any{map[string]any{"text": text}}, "position": []any{map[string]any{"x": 0, "y": y, "w": 36, "h": 4}}})
			}
			y += 4
		}
	}
	d := c.Dashboards[0]
	return map[string]any{"terraform": map[string]any{"required_providers": map[string]any{"yandex": map[string]any{"source": "yandex-cloud/yandex", "version": "= 0.220.0"}}}, "resource": map[string]any{"yandex_monitoring_dashboard": map[string]any{"overview": map[string]any{"name": d.Name, "folder_id": folder, "title": d.Title, "widgets": widgets, "lifecycle": map[string]any{"prevent_destroy": true}}}}}
}
func chartWidget(title, query string, q Query, y, points int) map[string]any {
	return map[string]any{"chart": []any{map[string]any{"title": title + " (" + q.Unit + "; " + q.Freshness + "; not health)", "queries": []any{map[string]any{"target": []any{map[string]any{"query": query, "text_mode": true}}, "downsampling": []any{map[string]any{"gap_filling": "GAP_FILLING_NONE", "grid_aggregation": "GRID_AGGREGATION_MAX", "max_points": points}}}}, "visualization_settings": []any{map[string]any{"type": "VISUALIZATION_TYPE_POINTS"}}}}, "position": []any{map[string]any{"x": 0, "y": y, "w": 36, "h": 4}}}
}

type Drift struct {
	Equal           bool     `json:"equal"`
	ChangedSections []string `json:"changed_sections"`
}

// Compare checks an operator-normalized readback envelope, not a native API
// response. Native readback acquisition and identity/receipt checks belong to #228.
func Compare(expected, actual []byte) (Drift, error) {
	decode := func(data []byte) (Envelope, error) {
		var e Envelope
		if len(data) == 0 || len(data) > 4*MaxDocumentBytes {
			return e, errors.New("readback outside size bound")
		}
		if !json.Valid(data) {
			return e, errors.New("readback must be one JSON document")
		}
		var node yaml.Node
		if err := yaml.Unmarshal(data, &node); err != nil || len(node.Content) != 1 {
			return e, errors.New("invalid normalized readback")
		}
		nodes := 0
		if err := inspectNode(&node, 0, &nodes, true); err != nil {
			return e, errors.New("readback syntax or complexity rejected")
		}
		if err := inspectTypedShape(node.Content[0], reflect.TypeOf(e), "json"); err != nil {
			return e, errors.New("complete closed typed readback required")
		}
		var n any
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&e); err != nil {
			return e, errors.New("invalid normalized readback")
		}
		if err := decoder.Decode(&n); err != io.EOF {
			return e, errors.New("multiple readback documents")
		}
		if e.Metadata.SchemaVersion != 1 || e.Metadata.GeneratorVersion != GeneratorVersion || e.Metadata.InputSHA256 == "" {
			return e, errors.New("readback identity missing")
		}
		var b *BindingsDocument
		var o *OwnershipDocument
		var now time.Time
		if e.Metadata.Mode == "bound_private" {
			if e.Ownership == nil {
				return e, errors.New("bound readback ownership missing")
			}
			var err error
			now, err = time.Parse(time.RFC3339, e.Metadata.EvaluatedAt)
			if err != nil {
				return e, errors.New("bound readback timestamp missing")
			}
			b = &BindingsDocument{SchemaVersion: 1, Bindings: Bindings{Revision: e.Metadata.BindingRevision, FolderID: e.Ownership.FolderID, Queries: e.QueryBindings}}
			o = &OwnershipDocument{SchemaVersion: 1, Ownership: *e.Ownership}
		} else if e.Metadata.Mode != "preview" {
			return e, errors.New("readback mode rejected")
		}
		regenerated, err := Render(&e.Policy, b, o, now)
		if err != nil {
			return e, errors.New("readback policy, bindings or ownership invalid")
		}
		if !reflect.DeepEqual(regenerated.Envelope, e) {
			return e, errors.New("readback identity or generated sections inconsistent")
		}
		return e, nil
	}
	e, err := decode(expected)
	if err != nil {
		return Drift{}, err
	}
	a, err := decode(actual)
	if err != nil {
		return Drift{}, err
	}
	d := Drift{Equal: true, ChangedSections: []string{}}
	for _, pair := range []struct {
		name string
		e, a any
	}{{"metadata", e.Metadata, a.Metadata}, {"policy", e.Policy, a.Policy}, {"query_bindings", e.QueryBindings, a.QueryBindings}, {"ownership", e.Ownership, a.Ownership}, {"panels", e.Panels, a.Panels}, {"console", e.Console, a.Console}} {
		if !reflect.DeepEqual(pair.e, pair.a) {
			d.Equal = false
			d.ChangedSections = append(d.ChangedSections, pair.name)
		}
	}
	return d, nil
}

// Encode is deterministic: encoding/json sorts string map keys.
func Encode(value any) ([]byte, error) {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("cannot encode artifact")
	}
	return append(data, '\n'), nil
}
