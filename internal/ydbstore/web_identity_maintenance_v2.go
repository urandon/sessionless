package ydbstore

// Operator-only identity-maintenance.v1. IAM/explicit environment authority and
// the externally maintained all-ingress writer freeze are prerequisites, NOT
// facts this command can establish. Nothing here installs a serving V2 mode.
import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ydbclient"
	"gitcode.com/urandon/sessionless/internal/ydbpartition"
	ydbmigrations "gitcode.com/urandon/sessionless/migrations/ydb"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

const IdentityMaintenanceVersionV1 = "identity-maintenance.v1"
const IdentityMaintenanceMaxDurationV1 = 10 * time.Minute
const IdentityMaintenanceArtifactBytesV1 = 16 << 10

var (
	ErrIdentityMaintenanceInvalidV1           = errors.New("identity maintenance invalid input")
	ErrIdentityMaintenanceIncompleteV1        = errors.New("identity maintenance incomplete")
	ErrIdentityMaintenanceAnomalyV1           = errors.New("identity maintenance anomaly")
	ErrIdentityMaintenancePossiblyCommittedV1 = errors.New("identity maintenance possibly committed")
)

// Selectors are private operator DATA, never public authorization or a freeze
// receipt. Candidate bytes contain fingerprints/selectors, never record bodies.
type IdentityMaintenanceRequestV1 struct {
	Mode              string
	Kind              string
	Selector          string
	EnvironmentDigest domain.SecretDigest
	ManifestDigest    domain.SecretDigest
	SourceCommit      string
	Candidate         []byte
}
type IdentityMaintenanceResultV1 struct{ Report, Candidate []byte }
type identityMaintenancePins struct {
	Environment domain.SecretDigest `json:"environment"`
	Manifest    domain.SecretDigest `json:"manifest"`
	Source      string              `json:"source"`
}
type identityMaintenanceCandidate struct {
	Contract           string                  `json:"contract"`
	Pins               identityMaintenancePins `json:"pins"`
	Kind               string                  `json:"kind"`
	Selector           string                  `json:"selector"`
	PreparedAt         time.Time               `json:"prepared_at"`
	Preimage           []string                `json:"preimage"`
	Postimage          []string                `json:"postimage"`
	PrecisionProjected uint64                  `json:"precision_projected"`
}
type identityMaintenanceReport struct {
	Contract           string                  `json:"contract"`
	Pins               identityMaintenancePins `json:"pins"`
	Outcome            string                  `json:"outcome"`
	Complete           bool                    `json:"complete"`
	Ready              bool                    `json:"ready"`
	Rows               uint64                  `json:"rows"`
	Pages              uint64                  `json:"pages"`
	Statements         uint64                  `json:"statements"`
	Transactions       uint64                  `json:"transactions"`
	Material           uint64                  `json:"material_bytes"`
	PrecisionProjected uint64                  `json:"precision_projected"`
	CandidateDigest    string                  `json:"candidate_digest,omitempty"`
}
type identityMaintenanceBudget struct {
	tx, statements, inTx, pages, rows, material uint64
	failed, closeFailed, writeAttempted         bool
}

func (b *identityMaintenanceBudget) charge(n uint64) error {
	if b.failed || n > (128<<20)-b.material {
		b.failed = true
		return ErrIdentityMaintenanceIncompleteV1
	}
	b.material += n
	return nil
}
func (b *identityMaintenanceBudget) statement() error {
	if b.failed || b.statements >= 160 || b.inTx >= 16 {
		b.failed = true
		return ErrIdentityMaintenanceIncompleteV1
	}
	b.statements++
	b.inTx++
	return nil
}
func (b *identityMaintenanceBudget) begin(ctx context.Context, db *sql.DB) (*sql.Tx, context.Context, context.CancelFunc, error) {
	if b.failed || b.tx >= 134 || ctx.Err() != nil {
		b.failed = true
		return nil, nil, nil, ErrIdentityMaintenanceIncompleteV1
	}
	b.tx++
	b.inTx = 0
	call, cancel := context.WithTimeout(ctx, 5*time.Second)
	// sql.Conn avoids sql.DB.BeginTx's automatic bad-connection operation retry.
	// Connection/session acquisition can still have SDK transport behavior; it
	// cannot repeat a maintenance DML or original transaction callback.
	conn, err := db.Conn(call)
	if err != nil {
		cancel()
		return nil, nil, nil, ErrIdentityMaintenanceIncompleteV1
	}
	tx, err := conn.BeginTx(call, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		_ = conn.Close()
		cancel()
		return nil, nil, nil, ErrIdentityMaintenanceIncompleteV1
	}
	done := func() {
		if conn.Close() != nil {
			b.closeFailed = true
		}
		cancel()
	}
	return tx, call, done, nil
}
func identityMaintenanceHash(v any) string {
	raw, _ := json.Marshal(v)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
func identityMaintenanceDigest(v string) bool {
	return domain.SecretDigest(v).Validate("maintenance.digest") == nil
}
func (r IdentityMaintenanceRequestV1) pins() identityMaintenancePins {
	return identityMaintenancePins{r.EnvironmentDigest, r.ManifestDigest, r.SourceCommit}
}
func (r IdentityMaintenanceRequestV1) validate() error {
	if !identityMaintenanceDigest(string(r.EnvironmentDigest)) || !identityMaintenanceDigest(string(r.ManifestDigest)) || len(r.SourceCommit) != 40 {
		return ErrIdentityMaintenanceInvalidV1
	}
	if _, err := hex.DecodeString(r.SourceCommit); err != nil || strings.ToLower(r.SourceCommit) != r.SourceCommit {
		return ErrIdentityMaintenanceInvalidV1
	}
	switch r.Mode {
	case "inventory":
		if r.Kind != "" || r.Selector != "" || len(r.Candidate) != 0 {
			return ErrIdentityMaintenanceInvalidV1
		}
	case "prepare":
		if len(r.Candidate) != 0 || !identityMaintenanceSelector(r.Kind, r.Selector) {
			return ErrIdentityMaintenanceInvalidV1
		}
	case "apply", "reconcile":
		if r.Kind != "" || r.Selector != "" || len(r.Candidate) == 0 || len(r.Candidate) > IdentityMaintenanceArtifactBytesV1 {
			return ErrIdentityMaintenanceInvalidV1
		}
	default:
		return ErrIdentityMaintenanceInvalidV1
	}
	return nil
}
func identityMaintenanceSelector(kind, key string) bool {
	if kind == "identity" {
		return domain.UserID(key).Validate() == nil
	}
	return (kind == "cookie" || kind == "challenge") && identityMaintenanceDigest(key)
}

// No caller-selectable production namespace/table. The actual client fixes SQL
// and DescribeTable to its own database. The private core permits test-only
// fixed-table namespace adapters without a public serving configuration hook.
func MaintainIdentityV1(ctx context.Context, client *ydbclient.Client, r IdentityMaintenanceRequestV1) (IdentityMaintenanceResultV1, error) {
	if client == nil || client.DB == nil {
		return IdentityMaintenanceResultV1{}, ErrIdentityMaintenanceInvalidV1
	}
	return maintainIdentityV1(ctx, client.DB, client.Table(), client.DatabasePath(), r)
}

type identityMaintenanceColumn struct {
	name, kind string
	cap        int
}
type identityMaintenanceTable struct {
	name string
	cols []identityMaintenanceColumn
	keys []string
}

func identityMaintenanceTables() []identityMaintenanceTable {
	c := func(name, kind string, cap int) identityMaintenanceColumn {
		return identityMaintenanceColumn{name, kind, cap}
	}
	return []identityMaintenanceTable{
		{"external_identities", []identityMaintenanceColumn{c("shard_bucket", "Uint32", 0), c("provider", "Utf8", 160), c("subject", "Utf8", 160), c("user_id", "Utf8", 160), c("created_at", "Timestamp", 0), c("updated_at", "Timestamp", 0), c("record", "JsonDocument", 8192)}, []string{"shard_bucket", "provider", "subject"}},
		{"external_identities_by_user", []identityMaintenanceColumn{c("user_bucket", "Uint32", 0), c("user_id", "Utf8", 160), c("provider", "Utf8", 160), c("subject", "Utf8", 160), c("created_at", "Timestamp", 0)}, []string{"user_bucket", "user_id", "provider", "subject"}},
		{"oidc_login_challenges", []identityMaintenanceColumn{c("shard_bucket", "Uint32", 0), c("state_digest", "Utf8", 64), c("browser_binding_digest", "Utf8", 64), c("pkce_verifier", "Utf8", 128), c("nonce", "Utf8", 160), c("redirect_path", "Utf8", 8192), c("created_at", "Timestamp", 0), c("expires_at", "Timestamp", 0), c("consumed_at", "Timestamp", 0), c("record", "JsonDocument", 8192), c("delete_after", "Timestamp", 0)}, []string{"shard_bucket", "state_digest"}},
		{"web_sessions", []identityMaintenanceColumn{c("shard_bucket", "Uint32", 0), c("session_digest", "Utf8", 64), c("csrf_token_digest", "Utf8", 64), c("user_id", "Utf8", 160), c("active_tenant_id", "Utf8", 160), c("identity_provider", "Utf8", 160), c("external_subject", "Utf8", 160), c("membership_security_version", "Uint64", 0), c("issued_at", "Timestamp", 0), c("last_seen_at", "Timestamp", 0), c("idle_expires_at", "Timestamp", 0), c("absolute_expires_at", "Timestamp", 0), c("revoked_at", "Timestamp", 0), c("record", "JsonDocument", 8192)}, []string{"shard_bucket", "session_digest"}},
	}
}
func identityMaintenanceSchema(ctx context.Context, d ydbpartition.TableDescriber, base string) error {
	if d == nil || !strings.HasPrefix(base, "/") || path.Clean(base) != base {
		return ErrIdentityMaintenanceInvalidV1
	}
	for _, t := range identityMaintenanceTables() {
		desc, err := d.DescribeTable(ctx, path.Join(base, t.name))
		if err != nil || desc == nil || !slices.Equal(desc.PrimaryKey, t.keys) || len(desc.Columns) != len(t.cols) {
			return ErrIdentityMaintenanceAnomalyV1
		}
		for _, c := range t.cols {
			var expected types.Type
			switch c.kind {
			case "Uint32":
				expected = types.TypeUint32
			case "Uint64":
				expected = types.TypeUint64
			case "Timestamp":
				expected = types.TypeTimestamp
			case "Utf8":
				expected = types.TypeUTF8
			case "JsonDocument":
				expected = types.TypeJSONDocument
			}
			found := false
			for _, actual := range desc.Columns {
				if actual.Name == c.name {
					if found || !types.Equal(actual.Type, types.Optional(expected)) {
						return ErrIdentityMaintenanceAnomalyV1
					}
					found = true
				}
			}
			if !found {
				return ErrIdentityMaintenanceAnomalyV1
			}
		}
		ttl := desc.TimeToLiveSettings
		if t.name == "oidc_login_challenges" || t.name == "web_sessions" {
			column := "delete_after"
			if t.name == "web_sessions" {
				column = "absolute_expires_at"
			}
			if ttl == nil || ttl.Mode != options.TimeToLiveModeDateType || ttl.ColumnName != column || ttl.ExpireAfterSeconds != 0 || ttl.ColumnUnit != nil {
				return ErrIdentityMaintenanceAnomalyV1
			}
		} else if ttl != nil {
			return ErrIdentityMaintenanceAnomalyV1
		}
	}
	return nil
}

const identityMaintenanceChecksumsSQL = `SELECT version, SUBSTRING(CAST(filename AS String),0,257), SUBSTRING(CAST(sha256 AS String),0,65), SUBSTRING(CAST(state AS String),0,17), recorded_at, applied_at FROM schema_migration_checksums WHERE version IN (130,131) ORDER BY version LIMIT 3`
const identityMaintenanceGooseSQL = `SELECT version_id,is_applied,tstamp FROM sessionless_goose_versions WHERE version_id IN (130,131) ORDER BY version_id LIMIT 3`

func identityMaintenanceMetadata(ctx context.Context, tx *sql.Tx, b *identityMaintenanceBudget) (time.Time, error) {
	var at time.Time
	if err := b.charge(8 << 10); err != nil {
		return at, err
	}
	if err := b.statement(); err != nil {
		return at, err
	}
	if tx.QueryRowContext(ctx, `SELECT CurrentUtcTimestamp()`).Scan(&at) != nil || !identityMaintenanceTime(at) {
		return at, ErrIdentityMaintenanceIncompleteV1
	}
	for _, statement := range []string{identityMaintenanceChecksumsSQL, identityMaintenanceGooseSQL} {
		if err := b.statement(); err != nil {
			return at, err
		}
		rows, err := tx.QueryContext(ctx, statement)
		if err != nil {
			return at, ErrIdentityMaintenanceIncompleteV1
		}
		n := 0
		bad := false
		for rows.Next() {
			n++
			if n > 3 {
				bad = true
				break
			}
			if statement == identityMaintenanceChecksumsSQL {
				values, valid := identityMaintenanceMetadataCells(rows, []identityMaintenanceColumn{
					{"version", "Uint64", 0}, {"filename", "Utf8", 256},
					{"sha256", "Utf8", 64}, {"state", "Utf8", 16},
					{"recorded_at", "Timestamp", 0}, {"applied_at", "Timestamp", 0},
				})
				version, _ := values[0].(uint64)
				name, _ := values[1].(string)
				hash, _ := values[2].(string)
				state, _ := values[3].(string)
				recorded, _ := values[4].(time.Time)
				applied, _ := values[5].(time.Time)
				file := map[uint64]string{130: "00130_add_identity_challenge_delete_after.sql", 131: "00131_set_identity_challenge_delete_after_ttl.sql"}[version]
				raw, readErr := fs.ReadFile(ydbmigrations.Files, file)
				sum := sha256.Sum256(raw)
				if !valid || readErr != nil || len(hash) != 64 || version != uint64(129+n) || name != file || hash != hex.EncodeToString(sum[:]) || state != "applied" || recorded.After(applied) || applied.After(at) {
					bad = true
				}
			} else {
				values, valid := identityMaintenanceMetadataCells(rows, []identityMaintenanceColumn{
					{"version_id", "Uint64", 0}, {"is_applied", "Bool", 0}, {"tstamp", "Timestamp", 0},
				})
				version, _ := values[0].(uint64)
				applied, _ := values[1].(bool)
				stamp, _ := values[2].(time.Time)
				if !valid || version != uint64(129+n) || !applied || stamp.After(at) {
					bad = true
				}
			}
		}
		rowErr := rows.Err()
		closeErr := rows.Close()
		if bad || n != 2 || rowErr != nil || closeErr != nil {
			return at, ErrIdentityMaintenanceAnomalyV1
		}
	}
	return at, nil
}

// All metadata cells have already been prospectively charged to the one 8 KiB
// allowance. Scan every cell through its closed physical type, including after
// an earlier malformed cell; sql.Scan conversions are not correspondence.
func identityMaintenanceMetadataCells(rows *sql.Rows, columns []identityMaintenanceColumn) ([]any, bool) {
	cells := make([]identityMaintenanceCell, len(columns))
	dest := make([]any, len(columns))
	values := make([]any, len(columns))
	for i, column := range columns {
		cells[i].column = column
		dest[i] = &cells[i]
	}
	valid := rows.Scan(dest...) == nil
	for i, cell := range cells {
		values[i] = cell.value
		if cell.bad || cell.value == nil {
			valid = false
		}
	}
	return values, valid
}
func identityMaintenanceTime(at time.Time) bool {
	return !at.IsZero() && at.Nanosecond()%1000 == 0 && at.Year() >= 1970 && at.Year() <= 2105
}

// Strict null/type scanner: SQL string/numeric coercion is never authority.
type identityMaintenanceCell struct {
	column identityMaintenanceColumn
	value  any
	bad    bool
}

func (c *identityMaintenanceCell) Scan(v any) error {
	if v == nil {
		c.value = nil
		return nil
	}
	switch c.column.kind {
	case "Utf8", "JsonDocument":
		var s string
		switch x := v.(type) {
		case string:
			s = x
		case []byte:
			s = string(x)
		default:
			c.bad = true
			return nil
		}
		c.value = s
		if len(s) > c.column.cap || !utf8.ValidString(s) {
			c.bad = true
		}
	case "Timestamp":
		at, ok := v.(time.Time)
		if !ok || !identityMaintenanceTime(at) {
			c.bad = true
			return nil
		}
		c.value = at.UTC()
	case "Bool":
		value, ok := v.(bool)
		if !ok {
			c.bad = true
			return nil
		}
		c.value = value
	case "Uint32", "Uint64":
		var n uint64
		switch x := v.(type) {
		case uint32:
			// The pinned table SDK preserves Uint32 in driver.Value. This
			// exact physical type is valid for bucket columns, not a numeric
			// coercion for a differently declared column.
			if c.column.kind != "Uint32" {
				c.bad = true
				return nil
			}
			n = uint64(x)
		case int64:
			if x < 0 {
				c.bad = true
				return nil
			}
			n = uint64(x)
		case uint64:
			n = x
		default:
			c.bad = true
			return nil
		}
		c.value = n
		if c.column.kind == "Uint32" && n > uint64(^uint32(0)) {
			c.bad = true
		}
	default:
		c.bad = true
	}
	return nil
}

type identityMaintenanceRow struct {
	table    identityMaintenanceTable
	cells    []identityMaintenanceCell
	legacy   bool
	identity *domain.ExternalIdentityV2
	cookie   *domain.WebSession
}

func (r *identityMaintenanceRow) value(name string) any {
	for _, c := range r.cells {
		if c.column.name == name {
			return c.value
		}
	}
	return nil
}
func (r *identityMaintenanceRow) text(name string) string { v, _ := r.value(name).(string); return v }
func (r *identityMaintenanceRow) instant(name string) time.Time {
	v, _ := r.value(name).(time.Time)
	return v
}
func (r *identityMaintenanceRow) set(name string, v any) {
	for i := range r.cells {
		if r.cells[i].column.name == name {
			r.cells[i].value = v
			return
		}
	}
}
func (r *identityMaintenanceRow) hash() string {
	values := []any{r.table.name}
	for _, c := range r.cells {
		values = append(values, c.value)
	}
	return identityMaintenanceHash(values)
}
func (r *identityMaintenanceRow) key() []string {
	var key []string
	for _, name := range r.table.keys[1:] {
		key = append(key, r.text(name))
	}
	return key
}
func identityMaintenanceProjection(t identityMaintenanceTable) string {
	var fields []string
	for _, c := range t.cols {
		if c.cap > 0 {
			fields = append(fields, fmt.Sprintf("SUBSTRING(CAST(%s AS String),0,%d)", c.name, c.cap+1))
		} else {
			fields = append(fields, c.name)
		}
	}
	return strings.Join(fields, ",")
}
func identityMaintenanceReadRow(rows *sql.Rows, t identityMaintenanceTable, at time.Time) (*identityMaintenanceRow, error) {
	r := &identityMaintenanceRow{table: t, cells: make([]identityMaintenanceCell, len(t.cols))}
	dest := make([]any, len(t.cols))
	for i, c := range t.cols {
		r.cells[i].column = c
		dest[i] = &r.cells[i]
	}
	err := rows.Scan(dest...)
	for _, c := range r.cells {
		if c.bad {
			err = ErrIdentityMaintenanceAnomalyV1
		}
	}
	if err != nil {
		return nil, ErrIdentityMaintenanceAnomalyV1
	}
	for _, c := range r.cells {
		if c.value == nil && !(t.name == "oidc_login_challenges" && c.column.name == "delete_after") {
			return nil, ErrIdentityMaintenanceAnomalyV1
		}
	}
	for _, key := range t.keys[1:] {
		if domain.ValidateOpaqueID("maintenance.key", r.text(key)) != nil {
			return nil, ErrIdentityMaintenanceAnomalyV1
		}
	}
	var hashInput string
	switch t.name {
	case "external_identities":
		hashInput = r.text("provider") + ":" + r.text("subject")
	case "external_identities_by_user":
		hashInput = r.text("user_id")
	case "oidc_login_challenges":
		hashInput = r.text("state_digest")
	case "web_sessions":
		hashInput = r.text("session_digest")
	}
	bucket, _ := ydbpartition.BucketV1(hashInput)
	if r.value(t.keys[0]) != uint64(bucket) {
		return nil, ErrIdentityMaintenanceAnomalyV1
	}
	if t.name == "external_identities_by_user" {
		return r, nil
	}
	if err := r.decode(at); err != nil {
		return nil, err
	}
	return r, nil
}

// A recursive, exact contextual-key preflight for legacy structs and private
// artifacts. time.Time is a JSON string; omitempty alone permits field absence.
func identityMaintenanceStrict(raw []byte, target any, cap int) error {
	if len(raw) == 0 || len(raw) > cap || !identityAuthUnicodeValidV2ForMaintenance(raw) {
		return ErrIdentityMaintenanceInvalidV1
	}
	typ := reflect.TypeOf(target)
	if typ == nil || typ.Kind() != reflect.Pointer {
		return ErrIdentityMaintenanceInvalidV1
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if identityMaintenanceObject(d, typ.Elem()) != nil {
		return ErrIdentityMaintenanceInvalidV1
	}
	if _, err := d.Token(); !errors.Is(err, io.EOF) {
		return ErrIdentityMaintenanceInvalidV1
	}
	d = json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(target) != nil {
		return ErrIdentityMaintenanceInvalidV1
	}
	return nil
}
func identityAuthUnicodeValidV2ForMaintenance(raw []byte) bool {
	if !utf8.Valid(raw) {
		return false
	}
	// encoding/json replaces invalid escaped surrogates; prevent that loss before
	// any semantic comparison. Decode every escaped string, including key names.
	for i := 0; i < len(raw); i++ {
		if raw[i] != '"' {
			continue
		}
		start := i
		i++
		for ; i < len(raw); i++ {
			if raw[i] == '\\' {
				i++
				continue
			}
			if raw[i] == '"' {
				break
			}
		}
		if i >= len(raw) {
			return false
		}
		s := raw[start : i+1]
		for j := 0; j+5 < len(s); j++ {
			if s[j] != '\\' {
				continue
			}
			j++
			if s[j] != 'u' {
				continue
			}
			n, err := strconv.ParseUint(string(s[j+1:j+5]), 16, 16)
			if err != nil {
				return false
			}
			j += 4
			if n >= 0xd800 && n <= 0xdbff {
				if j+6 >= len(s) || s[j+1] != '\\' || s[j+2] != 'u' {
					return false
				}
				lo, err := strconv.ParseUint(string(s[j+3:j+7]), 16, 16)
				if err != nil || lo < 0xdc00 || lo > 0xdfff {
					return false
				}
				j += 6
			} else if n >= 0xdc00 && n <= 0xdfff {
				return false
			}
		}
	}
	return json.Valid(raw)
}
func identityMaintenanceObject(d *json.Decoder, t reflect.Type) error {
	if t.Kind() == reflect.Pointer {
		return identityMaintenanceObject(d, t.Elem())
	}
	if t == reflect.TypeOf(time.Time{}) {
		v, e := d.Token()
		if _, ok := v.(string); e != nil || !ok {
			return ErrIdentityMaintenanceInvalidV1
		}
		return nil
	}
	if t.Kind() == reflect.Struct {
		v, e := d.Token()
		if e != nil || v != json.Delim('{') {
			return ErrIdentityMaintenanceInvalidV1
		}
		fields := map[string]reflect.StructField{}
		required := map[string]bool{}
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			tag := strings.Split(f.Tag.Get("json"), ",")
			if tag[0] == "" || tag[0] == "-" {
				continue
			}
			fields[tag[0]] = f
			if !slices.Contains(tag[1:], "omitempty") {
				required[tag[0]] = true
			}
		}
		seen := map[string]bool{}
		for d.More() {
			v, e := d.Token()
			key, ok := v.(string)
			f, found := fields[key]
			if e != nil || !ok || !found || seen[key] {
				return ErrIdentityMaintenanceInvalidV1
			}
			seen[key] = true
			if identityMaintenanceObject(d, f.Type) != nil {
				return ErrIdentityMaintenanceInvalidV1
			}
		}
		for k := range required {
			if !seen[k] {
				return ErrIdentityMaintenanceInvalidV1
			}
		}
		v, e = d.Token()
		if e != nil || v != json.Delim('}') {
			return ErrIdentityMaintenanceInvalidV1
		}
		return nil
	}
	if t.Kind() == reflect.Slice {
		v, e := d.Token()
		if e != nil || v != json.Delim('[') {
			return ErrIdentityMaintenanceInvalidV1
		}
		n := 0
		for d.More() {
			n++
			if n > 8 || identityMaintenanceObject(d, t.Elem()) != nil {
				return ErrIdentityMaintenanceInvalidV1
			}
		}
		v, e = d.Token()
		if e != nil || v != json.Delim(']') {
			return ErrIdentityMaintenanceInvalidV1
		}
		return nil
	}
	v, e := d.Token()
	if e != nil || v == nil {
		return ErrIdentityMaintenanceInvalidV1
	}
	switch t.Kind() {
	case reflect.String:
		if _, ok := v.(string); ok {
			return nil
		}
	case reflect.Uint64:
		if n, ok := v.(json.Number); ok {
			_, e := strconv.ParseUint(string(n), 10, 64)
			if e == nil {
				return nil
			}
		}
	}
	return ErrIdentityMaintenanceInvalidV1
}

func (r *identityMaintenanceRow) decode(at time.Time) error {
	fail := func() error { return ErrIdentityMaintenanceAnomalyV1 }
	raw := []byte(r.text("record"))
	if len(raw) == 0 || len(raw) > 8192 || !identityAuthUnicodeValidV2ForMaintenance(raw) {
		return fail()
	}
	var tag map[string]json.RawMessage
	if json.Unmarshal(raw, &tag) != nil {
		return fail()
	}
	_, v2 := tag["version"]
	r.legacy = !v2
	equalTime := func(name string, t time.Time) bool {
		if r.legacy {
			t = t.UTC().Truncate(time.Microsecond)
		}
		return r.instant(name).Equal(t)
	}
	canonical := func(v any) error {
		out, e := json.Marshal(v)
		if e != nil || len(out) > 8192 {
			return fail()
		}
		r.set("record", string(out))
		return nil
	}
	switch r.table.name {
	case "external_identities":
		var value domain.ExternalIdentityV2
		if v2 {
			var e error
			value, e = domain.DecodeExternalIdentityV2(raw)
			if e != nil {
				return fail()
			}
		} else {
			old, e := decodeExternalIdentityV1(raw)
			if e != nil {
				return fail()
			}
			value = domain.ExternalIdentityV2{Version: 2, Subject: old.Subject, UserID: old.UserID, CreatedAt: old.CreatedAt, UpdatedAt: old.UpdatedAt, State: domain.ExternalIdentityActiveV2, Revision: 1}
			if canonical(old) != nil {
				return fail()
			}
		}
		if value.Validate() != nil || value.CreatedAt.After(at) || value.UpdatedAt.After(at) || string(value.Subject.Provider) != r.text("provider") || value.Subject.Subject != r.text("subject") || string(value.UserID) != r.text("user_id") || !equalTime("created_at", value.CreatedAt) || !equalTime("updated_at", value.UpdatedAt) {
			return fail()
		}
		r.identity = &value
		if v2 {
			return canonical(value)
		}
	case "web_sessions":
		var s domain.WebSession
		if v2 {
			value, e := domain.DecodeWebSessionV2(raw)
			if e != nil {
				return fail()
			}
			s = value.Session
			if canonical(value) != nil {
				return fail()
			}
		} else {
			if identityMaintenanceStrict(raw, &s, 8192) != nil || s.Validate() != nil {
				return fail()
			}
			if canonical(s) != nil {
				return fail()
			}
		}
		if string(s.SessionDigest) != r.text("session_digest") || string(s.CSRFTokenDigest) != r.text("csrf_token_digest") || string(s.UserID) != r.text("user_id") || string(s.ActiveTenantID) != r.text("active_tenant_id") || string(s.AuthenticatedSubject.Provider) != r.text("identity_provider") || s.AuthenticatedSubject.Subject != r.text("external_subject") || s.MembershipSecurityVersion != r.value("membership_security_version") || !equalTime("issued_at", s.IssuedAt) || !equalTime("last_seen_at", s.LastSeenAt) || !equalTime("idle_expires_at", s.IdleExpiresAt) || !equalTime("absolute_expires_at", s.AbsoluteExpiresAt) || s.IssuedAt.After(at) || s.LastSeenAt.After(at) {
			return fail()
		}
		revoked := time.Unix(0, 0).UTC()
		if s.RevokedAt != nil {
			revoked = *s.RevokedAt
			if revoked.After(at) {
				return fail()
			}
		}
		if !equalTime("revoked_at", revoked) {
			return fail()
		}
		r.cookie = &s
	case "oidc_login_challenges":
		var digest, browser, pkce, nonce, redirect string
		var created, expires, consumed, deleteAfter time.Time
		consumed = time.Unix(0, 0).UTC()
		if v2 {
			if _, operation := tag["operation_digest"]; operation {
				v, e := domain.DecodeIdentityOperationV2(raw)
				if e != nil {
					return fail()
				}
				digest = string(v.OperationDigest)
				created = v.CreatedAt
				expires = v.ExpiresAt
				deleteAfter = v.DeleteAfter
				if v.Pending != nil {
					browser = string(v.Pending.Scope.BrowserBindingDigest)
				} else {
					consumed = v.Receipt.CompletedAt
				}
				if canonical(v) != nil {
					return fail()
				}
			} else {
				v, e := domain.DecodeIdentityChallengeV2(raw)
				if e != nil {
					return fail()
				}
				digest = string(v.StateDigest)
				browser = string(v.BrowserBindingDigest)
				created = v.CreatedAt
				expires = v.ExpiresAt
				deleteAfter = v.DeleteAfter
				if v.ConsumedAt != nil {
					consumed = *v.ConsumedAt
				}
				if v.Secrets != nil {
					pkce = v.Secrets.PKCEVerifier
					nonce = v.Secrets.Nonce
				}
				if v.Login != nil {
					redirect = v.Login.ReturnPath
				}
				if canonical(v) != nil {
					return fail()
				}
			}
		} else {
			var v domain.OIDCLoginChallenge
			if identityMaintenanceStrict(raw, &v, 8192) != nil || v.Validate() != nil {
				return fail()
			}
			digest = string(v.StateDigest)
			browser = string(v.BrowserBindingDigest)
			created = v.CreatedAt
			expires = v.ExpiresAt
			deleteAfter = v.ExpiresAt
			pkce = v.PKCEVerifier
			nonce = v.Nonce
			redirect = v.RedirectPath
			if v.ConsumedAt != nil {
				consumed = *v.ConsumedAt
			}
			if canonical(v) != nil {
				return fail()
			}
		}
		if digest != r.text("state_digest") || browser != r.text("browser_binding_digest") || pkce != r.text("pkce_verifier") || nonce != r.text("nonce") || redirect != r.text("redirect_path") || created.After(at) || consumed.After(at) || !equalTime("created_at", created) || !equalTime("expires_at", expires) || !equalTime("consumed_at", consumed) {
			return fail()
		}
		if r.value("delete_after") == nil {
			if v2 {
				return fail()
			}
		} else if !equalTime("delete_after", deleteAfter) {
			return fail()
		}
	}
	return nil
}

func identityMaintenanceInventory(ctx context.Context, db *sql.DB, b *identityMaintenanceBudget, at time.Time) ([]*identityMaintenanceRow, error) {
	var all []*identityMaintenanceRow
	for _, t := range identityMaintenanceTables() {
		tx, call, cancel, err := b.begin(ctx, db)
		if err != nil {
			return nil, err
		}
		if b.charge(48<<10) != nil || b.statement() != nil {
			_ = tx.Rollback()
			cancel()
			return nil, ErrIdentityMaintenanceIncompleteV1
		}
		var nulls []string
		for _, k := range t.keys {
			nulls = append(nulls, k+" IS NULL")
		}
		predicate := strings.Join(nulls, " OR ") + " OR " + t.keys[0] + ">=16"
		var found int
		err = tx.QueryRowContext(call, "SELECT 1 FROM "+t.name+" WHERE "+predicate+" LIMIT 1").Scan(&found)
		rollback := tx.Rollback()
		cancel()
		if err == nil {
			return nil, ErrIdentityMaintenanceAnomalyV1
		}
		if !errors.Is(err, sql.ErrNoRows) || rollback != nil {
			return nil, ErrIdentityMaintenanceIncompleteV1
		}
	}
	for _, t := range identityMaintenanceTables() {
		for bucket := uint32(0); bucket < 16; bucket++ {
			var cursor []string
			for {
				if b.pages >= 128 {
					return nil, ErrIdentityMaintenanceIncompleteV1
				}
				b.pages++
				if b.charge(17*48<<10) != nil {
					return nil, ErrIdentityMaintenanceIncompleteV1
				}
				tx, call, cancel, err := b.begin(ctx, db)
				if err != nil {
					return nil, err
				}
				if b.statement() != nil {
					_ = tx.Rollback()
					cancel()
					return nil, ErrIdentityMaintenanceIncompleteV1
				}
				statement := "SELECT " + identityMaintenanceProjection(t) + " FROM " + t.name + " WHERE " + t.keys[0] + "=$1"
				args := []any{bucket}
				if len(cursor) > 0 {
					var placeholders []string
					for i, key := range cursor {
						args = append(args, key)
						placeholders = append(placeholders, "$"+strconv.Itoa(i+2))
					}
					statement += " AND (" + strings.Join(t.keys[1:], ",") + ") > (" + strings.Join(placeholders, ",") + ")"
				}
				statement += " ORDER BY " + strings.Join(t.keys[1:], ",") + " LIMIT 17"
				rows, err := tx.QueryContext(call, statement, args...)
				if err != nil {
					_ = tx.Rollback()
					cancel()
					return nil, ErrIdentityMaintenanceIncompleteV1
				}
				n := 0
				last := cursor
				var scanErr error
				for rows.Next() {
					n++
					if n > 17 {
						scanErr = ErrIdentityMaintenanceAnomalyV1
						break
					}
					r, e := identityMaintenanceReadRow(rows, t, at)
					if e != nil {
						scanErr = e
						break
					}
					if r.value(t.keys[0]) != uint64(bucket) || slices.Compare(r.key(), last) <= 0 && len(last) > 0 {
						scanErr = ErrIdentityMaintenanceAnomalyV1
						break
					}
					if n <= 16 {
						if b.rows >= 2048 {
							scanErr = ErrIdentityMaintenanceIncompleteV1
							break
						}
						b.rows++
						all = append(all, r)
						last = r.key()
					}
				}
				rowErr := rows.Err()
				closeErr := rows.Close()
				rollback := tx.Rollback()
				cancel()
				if scanErr != nil {
					return nil, scanErr
				}
				if rowErr != nil || closeErr != nil || rollback != nil {
					return nil, ErrIdentityMaintenanceIncompleteV1
				}
				if n <= 16 {
					break
				}
				cursor = last
			}
		}
	}
	return all, identityMaintenanceCorrespondence(all)
}
func identityMaintenanceCorrespondence(all []*identityMaintenanceRow) error {
	forwards := map[string]*identityMaintenanceRow{}
	reverse := map[string]*identityMaintenanceRow{}
	groups := map[string][]*identityMaintenanceRow{}
	for _, r := range all {
		switch r.table.name {
		case "external_identities":
			k := r.identity.Subject.String()
			if forwards[k] != nil {
				return ErrIdentityMaintenanceAnomalyV1
			}
			forwards[k] = r
			groups[string(r.identity.UserID)] = append(groups[string(r.identity.UserID)], r)
		case "external_identities_by_user":
			k := r.text("provider") + ":" + r.text("subject")
			if reverse[k] != nil {
				return ErrIdentityMaintenanceAnomalyV1
			}
			reverse[k] = r
		}
	}
	for key, r := range forwards {
		rev := reverse[key]
		if r.identity.State == domain.ExternalIdentityUnlinkedV2 {
			if rev != nil {
				return ErrIdentityMaintenanceAnomalyV1
			}
			continue
		}
		if rev == nil || rev.text("user_id") != string(r.identity.UserID) || !rev.instant("created_at").Equal(r.instant("created_at")) {
			return ErrIdentityMaintenanceAnomalyV1
		}
		delete(reverse, key)
	}
	if len(reverse) != 0 {
		return ErrIdentityMaintenanceAnomalyV1
	}
	for _, g := range groups {
		legacy := g[0].legacy
		var active []domain.ExternalIdentityV2
		for _, r := range g {
			if r.legacy != legacy {
				return ErrIdentityMaintenanceAnomalyV1
			}
			if r.identity.State == domain.ExternalIdentityActiveV2 {
				active = append(active, *r.identity)
			}
		}
		if !legacy && len(active) == 0 {
			continue
		}
		if _, err := domain.ValidateExternalIdentitySetV2(g[0].identity.UserID, active); err != nil {
			return ErrIdentityMaintenanceAnomalyV1
		}
	}
	return nil
}
func identityMaintenanceReady(all []*identityMaintenanceRow) bool {
	for _, r := range all {
		if r.legacy && (r.identity != nil || r.table.name == "oidc_login_challenges" || r.cookie != nil && r.cookie.RevokedAt == nil) {
			return false
		}
		if r.table.name == "oidc_login_challenges" && r.value("delete_after") == nil {
			return false
		}
	}
	return true
}
func identityMaintenanceTargets(all []*identityMaintenanceRow, kind, key string) []*identityMaintenanceRow {
	var target []*identityMaintenanceRow
	for _, r := range all {
		if kind == "identity" && (r.identity != nil && string(r.identity.UserID) == key || r.table.name == "external_identities_by_user" && r.text("user_id") == key) || kind == "cookie" && r.table.name == "web_sessions" && r.text("session_digest") == key || kind == "challenge" && r.table.name == "oidc_login_challenges" && r.text("state_digest") == key {
			target = append(target, r)
		}
	}
	slices.SortFunc(target, func(a, b *identityMaintenanceRow) int {
		return strings.Compare(a.table.name+"/"+strings.Join(a.key(), "/"), b.table.name+"/"+strings.Join(b.key(), "/"))
	})
	return target
}
func identityMaintenanceFingerprints(target []*identityMaintenanceRow) []string {
	out := make([]string, 0, len(target))
	for _, r := range target {
		out = append(out, r.hash())
	}
	return out
}
func identityMaintenancePostimage(target []*identityMaintenanceRow, kind string, at time.Time) ([]*identityMaintenanceRow, uint64, error) {
	var post []*identityMaintenanceRow
	var projected uint64
	if len(target) == 0 || len(target) > 4 {
		return nil, 0, ErrIdentityMaintenanceAnomalyV1
	}
	for _, old := range target {
		r := *old
		r.cells = slices.Clone(old.cells)
		switch kind {
		case "identity":
			if old.identity != nil {
				if !old.legacy {
					return nil, 0, ErrIdentityMaintenanceAnomalyV1
				}
				v := *old.identity
				v.CreatedAt = r.instant("created_at")
				v.UpdatedAt = r.instant("updated_at")
				if !v.CreatedAt.Equal(old.identity.CreatedAt) || !v.UpdatedAt.Equal(old.identity.UpdatedAt) {
					projected++
				}
				raw, e := domain.EncodeExternalIdentityV2(v)
				if e != nil {
					return nil, 0, ErrIdentityMaintenanceAnomalyV1
				}
				r.set("record", string(raw))
				r.legacy = false
				r.identity = &v
			}
		case "cookie":
			if !old.legacy || old.cookie == nil || old.cookie.RevokedAt != nil || at.Before(old.cookie.IssuedAt) {
				return nil, 0, ErrIdentityMaintenanceAnomalyV1
			}
			v := *old.cookie
			v.RevokedAt = &at
			raw, e := json.Marshal(v)
			if e != nil || len(raw) > 8192 {
				return nil, 0, ErrIdentityMaintenanceAnomalyV1
			}
			r.set("record", string(raw))
			r.set("revoked_at", at)
			r.cookie = &v
		case "challenge":
			if !old.legacy {
				return nil, 0, ErrIdentityMaintenanceAnomalyV1
			}
			continue
		default:
			return nil, 0, ErrIdentityMaintenanceInvalidV1
		}
		post = append(post, &r)
	}
	return post, projected, nil
}

func identityMaintenanceExact(ctx context.Context, tx *sql.Tx, b *identityMaintenanceBudget, all []*identityMaintenanceRow, c identityMaintenanceCandidate, at time.Time) ([]*identityMaintenanceRow, error) {
	if c.Kind == "identity" {
		// The final exact user range is itself part of the serializable write set.
		if b.statement() != nil {
			return nil, ErrIdentityMaintenanceIncompleteV1
		}
		bucket, _ := webBucket(c.Selector)
		rows, e := tx.QueryContext(ctx, webIdentityActiveSetSQLV2, bucket, c.Selector)
		if e != nil {
			return nil, ErrIdentityMaintenanceIncompleteV1
		}
		n := 0
		var reverse []*identityMaintenanceRow
		for rows.Next() {
			n++
			var provider, subject string
			var created time.Time
			if rows.Scan(&provider, &subject, &created) != nil || n > 2 || len(provider) > 160 || len(subject) > 160 {
				_ = rows.Close()
				return nil, ErrIdentityMaintenanceAnomalyV1
			}
			found := false
			for _, r := range all {
				if r.table.name == "external_identities_by_user" && r.text("user_id") == c.Selector && r.text("provider") == provider && r.text("subject") == subject && r.instant("created_at").Equal(created) {
					reverse = append(reverse, r)
					found = true
				}
			}
			if !found {
				_ = rows.Close()
				return nil, ErrIdentityMaintenanceAnomalyV1
			}
		}
		rowErr := rows.Err()
		closeErr := rows.Close()
		if rowErr != nil || closeErr != nil {
			return nil, ErrIdentityMaintenanceIncompleteV1
		}
		expected := 0
		for _, r := range all {
			if r.table.name == "external_identities_by_user" && r.text("user_id") == c.Selector {
				expected++
			}
		}
		if len(reverse) != expected {
			return nil, ErrIdentityMaintenanceAnomalyV1
		}
	}
	var out []*identityMaintenanceRow
	targets := identityMaintenanceTargets(all, c.Kind, c.Selector)
	if len(targets) == 0 && c.Kind != "identity" {
		tables := identityMaintenanceTables()
		index := 2
		if c.Kind == "cookie" {
			index = 3
		}
		t := tables[index]
		stub := &identityMaintenanceRow{table: t, cells: make([]identityMaintenanceCell, len(t.cols))}
		for i, col := range t.cols {
			stub.cells[i].column = col
		}
		bucket, _ := webBucket(c.Selector)
		stub.set(t.keys[0], uint64(bucket))
		stub.set(t.keys[1], c.Selector)
		targets = []*identityMaintenanceRow{stub}
	}
	for _, old := range targets {
		if old.table.name == "external_identities_by_user" {
			out = append(out, old)
			continue
		}
		if b.statement() != nil {
			return nil, ErrIdentityMaintenanceIncompleteV1
		}
		t := old.table
		var predicates []string
		args := make([]any, 0, len(t.keys))
		for i, k := range t.keys {
			predicates = append(predicates, k+"=$"+strconv.Itoa(i+1))
			value := old.value(k)
			if i == 0 {
				value = uint32(value.(uint64))
			}
			args = append(args, value)
		}
		rows, e := tx.QueryContext(ctx, "SELECT "+identityMaintenanceProjection(t)+" FROM "+t.name+" WHERE "+strings.Join(predicates, " AND ")+" LIMIT 2", args...)
		if e != nil {
			return nil, ErrIdentityMaintenanceIncompleteV1
		}
		if rows.Next() {
			r, e := identityMaintenanceReadRow(rows, t, at)
			if e != nil {
				_ = rows.Close()
				return nil, e
			}
			out = append(out, r)
			if rows.Next() {
				_ = rows.Close()
				return nil, ErrIdentityMaintenanceAnomalyV1
			}
		}
		rowErr := rows.Err()
		closeErr := rows.Close()
		if rowErr != nil || closeErr != nil {
			return nil, ErrIdentityMaintenanceIncompleteV1
		}
	}
	return out, nil
}

func maintainIdentityV1(ctx context.Context, db *sql.DB, d ydbpartition.TableDescriber, base string, r IdentityMaintenanceRequestV1) (result IdentityMaintenanceResultV1, err error) {
	if ctx == nil || db == nil || r.validate() != nil {
		return result, ErrIdentityMaintenanceInvalidV1
	}
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > IdentityMaintenanceMaxDurationV1 || ctx.Err() != nil {
		return result, ErrIdentityMaintenanceInvalidV1
	}
	b := &identityMaintenanceBudget{}
	report := identityMaintenanceReport{Contract: IdentityMaintenanceVersionV1, Pins: r.pins(), Outcome: "incomplete"}
	defer func() {
		if b.closeFailed && err == nil {
			err = ErrIdentityMaintenanceIncompleteV1
			if b.writeAttempted {
				report.Outcome = "possiblyCommitted"
				err = ErrIdentityMaintenancePossiblyCommittedV1
			}
			result.Candidate = nil
		}
		report.Statements = b.statements
		report.Transactions = b.tx
		report.Pages = b.pages
		report.Rows = b.rows
		report.Material = b.material
		raw, e := json.Marshal(report)
		if e == nil && len(raw) < IdentityMaintenanceArtifactBytesV1 {
			result.Report = append(raw, '\n')
		} else {
			result = IdentityMaintenanceResultV1{}
			if err == nil {
				err = ErrIdentityMaintenanceIncompleteV1
			}
		}
	}()
	var candidate identityMaintenanceCandidate
	if r.Mode == "apply" || r.Mode == "reconcile" {
		if identityMaintenanceStrict(r.Candidate, &candidate, IdentityMaintenanceArtifactBytesV1) != nil || candidate.Contract != IdentityMaintenanceVersionV1 || candidate.Pins != r.pins() || !identityMaintenanceSelector(candidate.Kind, candidate.Selector) || !identityMaintenanceTime(candidate.PreparedAt) || len(candidate.Preimage) == 0 || len(candidate.Preimage) > 4 || len(candidate.Postimage) > 4 {
			return result, ErrIdentityMaintenanceInvalidV1
		}
		for _, h := range append(slices.Clone(candidate.Preimage), candidate.Postimage...) {
			if !identityMaintenanceDigest(h) {
				return result, ErrIdentityMaintenanceInvalidV1
			}
		}
	}
	if e := identityMaintenanceSchema(ctx, d, base); e != nil {
		return result, e
	}
	tx, call, cancel, e := b.begin(ctx, db)
	if e != nil {
		return result, e
	}
	at, e := identityMaintenanceMetadata(call, tx, b)
	rollback := tx.Rollback()
	cancel()
	if e != nil {
		return result, e
	}
	if rollback != nil {
		return result, ErrIdentityMaintenanceIncompleteV1
	}
	all, e := identityMaintenanceInventory(ctx, db, b, at)
	if e != nil {
		return result, e
	}
	report.Complete = true
	report.Ready = identityMaintenanceReady(all)
	if r.Mode == "inventory" {
		report.Outcome = "inventoryComplete"
		return result, nil
	}
	if b.charge(1<<20) != nil {
		return result, ErrIdentityMaintenanceIncompleteV1
	}
	tx, call, cancel, e = b.begin(ctx, db)
	if e != nil {
		return result, e
	}
	defer cancel()
	defer tx.Rollback()
	if b.statement() != nil {
		return result, ErrIdentityMaintenanceIncompleteV1
	}
	var now time.Time
	if tx.QueryRowContext(call, `SELECT CurrentUtcTimestamp()`).Scan(&now) != nil || !identityMaintenanceTime(now) || now.Before(at) {
		return result, ErrIdentityMaintenanceIncompleteV1
	}
	if r.Mode == "prepare" {
		candidate = identityMaintenanceCandidate{Contract: IdentityMaintenanceVersionV1, Pins: r.pins(), Kind: r.Kind, Selector: r.Selector, PreparedAt: now}
	}
	if now.Before(candidate.PreparedAt) {
		return result, ErrIdentityMaintenanceAnomalyV1
	}
	target, e := identityMaintenanceExact(call, tx, b, all, candidate, now)
	if e != nil {
		return result, e
	}
	actual := identityMaintenanceFingerprints(target)
	if r.Mode == "prepare" {
		post, projected, e := identityMaintenancePostimage(target, r.Kind, now)
		if e != nil {
			return result, e
		}
		candidate.Preimage = actual
		candidate.Postimage = identityMaintenanceFingerprints(post)
		candidate.PrecisionProjected = projected
		raw, e := json.Marshal(candidate)
		if e != nil || len(raw) > IdentityMaintenanceArtifactBytesV1 {
			return result, ErrIdentityMaintenanceIncompleteV1
		}
		if tx.Rollback() != nil {
			return result, ErrIdentityMaintenanceIncompleteV1
		}
		result.Candidate = raw
		report.CandidateDigest = identityMaintenanceHash(candidate)
		report.Outcome = "prepared"
		report.PrecisionProjected = projected
		return result, nil
	}
	report.CandidateDigest = identityMaintenanceHash(candidate)
	if len(target) == 0 {
		if candidate.Kind != "identity" {
			report.Outcome = "desiredAbsentObservedActorUnknown"
			return result, nil
		}
		return result, ErrIdentityMaintenanceAnomalyV1
	}
	if slices.Equal(actual, candidate.Postimage) {
		report.Outcome = "alreadyDesiredObserved"
		if r.Mode == "reconcile" {
			report.Outcome = "exactPostimageObserved"
		}
		return result, nil
	}
	if !slices.Equal(actual, candidate.Preimage) {
		report.Outcome = "diverged"
		return result, ErrIdentityMaintenanceAnomalyV1
	}
	if r.Mode == "reconcile" {
		report.Outcome = "exactPreimageObserved"
		return result, nil
	}
	post, projected, e := identityMaintenancePostimage(target, candidate.Kind, candidate.PreparedAt)
	if e != nil || projected != candidate.PrecisionProjected || !slices.Equal(identityMaintenanceFingerprints(post), candidate.Postimage) {
		return result, ErrIdentityMaintenanceAnomalyV1
	}
	attempted := false
	for _, old := range target {
		if old.table.name == "external_identities_by_user" {
			continue
		}
		if call.Err() != nil {
			return result, ErrIdentityMaintenanceIncompleteV1
		}
		if b.statement() != nil {
			return result, ErrIdentityMaintenanceIncompleteV1
		}
		var statement string
		var args []any
		switch candidate.Kind {
		case "identity":
			var next *identityMaintenanceRow
			for _, p := range post {
				if p.table.name == old.table.name && slices.Equal(p.key(), old.key()) {
					next = p
				}
			}
			if next == nil {
				return result, ErrIdentityMaintenanceAnomalyV1
			}
			statement = `UPDATE external_identities SET record=CAST($1 AS JsonDocument) WHERE shard_bucket=$2 AND provider=$3 AND subject=$4`
			args = []any{next.text("record"), uint32(old.value("shard_bucket").(uint64)), old.text("provider"), old.text("subject")}
		case "cookie":
			statement = `UPDATE web_sessions SET record=CAST($1 AS JsonDocument),revoked_at=$2 WHERE shard_bucket=$3 AND session_digest=$4`
			args = []any{post[0].text("record"), candidate.PreparedAt, uint32(old.value("shard_bucket").(uint64)), old.text("session_digest")}
		case "challenge":
			statement = `DELETE FROM oidc_login_challenges WHERE shard_bucket=$1 AND state_digest=$2`
			args = []any{uint32(old.value("shard_bucket").(uint64)), old.text("state_digest")}
		}
		attempted = true
		b.writeAttempted = true
		if _, e := tx.ExecContext(call, statement, args...); e != nil {
			report.Outcome = "possiblyCommitted"
			return result, ErrIdentityMaintenancePossiblyCommittedV1
		}
	}
	if !attempted {
		return result, ErrIdentityMaintenanceAnomalyV1
	}
	if tx.Commit() != nil {
		report.Outcome = "possiblyCommitted"
		return result, ErrIdentityMaintenancePossiblyCommittedV1
	}
	report.Outcome = "applied"
	report.Ready = false
	report.PrecisionProjected = projected
	return result, nil
}
