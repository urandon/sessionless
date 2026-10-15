package ydbmigrate

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"regexp"
	"strings"

	ydbmigrations "gitcode.com/urandon/sessionless/migrations/ydb"
)

type retirementDefinition struct {
	Table              string `json:"table"`
	CreateMigration    string `json:"createMigration"`
	Successor          string `json:"successor"`
	SuccessorMigration string `json:"successorMigration"`
	Migration          string `json:"migration"`
	AfterVersion       int64  `json:"afterVersion"`
}

var allowedSchemaOperation = regexp.MustCompile(`(?i)^(CREATE|ALTER) TABLE\b`)

func retirementDefinitions() ([]retirementDefinition, error) {
	var manifest struct {
		Schema      string                 `json:"schema"`
		Retirements []retirementDefinition `json:"retirements"`
	}
	decoder := json.NewDecoder(bytes.NewReader(ydbmigrations.RetirementDefinitions))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return nil, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("retirement manifest has trailing content")
	}
	if manifest.Schema != "sessionless-ydb-retirements/v1" || len(manifest.Retirements) != 4 {
		return nil, fmt.Errorf("unsupported retirement manifest")
	}
	seenTables, seenFiles := map[string]bool{}, map[string]bool{}
	for _, r := range manifest.Retirements {
		if r.Table == "" || r.Successor == "" || r.Table == r.Successor || r.AfterVersion <= 0 ||
			seenTables[r.Table] || seenFiles[r.Migration] {
			return nil, fmt.Errorf("invalid duplicate/empty retirement identity")
		}
		seenTables[r.Table], seenFiles[r.Migration] = true, true
	}
	return manifest.Retirements, nil
}

// validateRetirementOperations runs before opening a database. It validates
// source ownership/order only: operator/environment rollout authority, deployed
// v2 coverage and old-binary drain are separate preconditions.
func validateRetirementOperations(files fs.FS, migrations []Migration) error {
	definitions, err := retirementDefinitions()
	if err != nil {
		return err
	}
	byName := map[string]Migration{}
	up := map[string]string{}
	for _, m := range migrations {
		body, err := fs.ReadFile(files, m.Filename)
		if err != nil {
			return err
		}
		byName[m.Filename] = m
		beforeDown, _, _ := strings.Cut(string(body), "-- +goose Down")
		var lines []string
		for _, line := range strings.Split(beforeDown, "\n") {
			code, _, _ := strings.Cut(line, "--")
			lines = append(lines, code)
		}
		up[m.Filename] = strings.Join(strings.Fields(strings.Join(lines, "\n")), " ")
	}
	owned := map[string]retirementDefinition{}
	for _, r := range definitions {
		owned[r.Migration] = r
	}
	anyRetirement := false
	for _, m := range migrations {
		for _, r := range definitions {
			anyRetirement = anyRetirement || m.Version == r.AfterVersion
		}
	}
	for name := range owned {
		_, found := byName[name]
		anyRetirement = anyRetirement || found
	}
	if anyRetirement {
		for name := range owned {
			if _, found := byName[name]; !found {
				return fmt.Errorf("missing declared retirement %s", name)
			}
		}
	}
	retired := map[string]bool{}
	operationTarget := regexp.MustCompile("(?i)^(?:CREATE TABLE(?: IF NOT EXISTS)?|ALTER TABLE) (?:`([a-z][a-z0-9_]*)`|([a-z][a-z0-9_]*))(?: |\\()")
	for _, m := range migrations {
		statement := up[m.Filename]
		r, isRetirement := owned[m.Filename]
		if !isRetirement {
			if !allowedSchemaOperation.MatchString(statement) {
				return fmt.Errorf("%s: unsupported unowned schema operation", m.Filename)
			}
			match := operationTarget.FindStringSubmatch(statement)
			if len(match) != 3 {
				return fmt.Errorf("%s: unsupported CREATE/ALTER target syntax", m.Filename)
			}
			target := match[1] + match[2]
			if retired[strings.ToLower(target)] {
				return fmt.Errorf("%s: cannot recreate or ALTER retired table %s", m.Filename, target)
			}
			continue
		}
		pattern := "^(?i:DROP TABLE IF EXISTS) `" + regexp.QuoteMeta(r.Table) + "`;$"
		if !regexp.MustCompile(pattern).MatchString(statement) {
			return fmt.Errorf("%s: expected exactly the owned idempotent DROP of %s", m.Filename, r.Table)
		}
		body, err := fs.ReadFile(files, m.Filename)
		if err != nil {
			return err
		}
		_, down, hasDown := strings.Cut(string(body), "-- +goose Down")
		for _, line := range strings.Split(down, "\n") {
			code, _, _ := strings.Cut(line, "--")
			if !hasDown || strings.TrimSpace(code) != "" {
				return fmt.Errorf("%s: retirement Down must contain comments only", m.Filename)
			}
		}
		if m.Version <= r.AfterVersion {
			return fmt.Errorf("%s: retirement precedes its compatible source baseline", m.Filename)
		}
		baselinePresent := false
		for _, prior := range migrations {
			baselinePresent = baselinePresent || prior.Version == r.AfterVersion
		}
		if !baselinePresent {
			return fmt.Errorf("%s: missing predecessor migration %d", m.Filename, r.AfterVersion)
		}
		for _, source := range []struct{ filename, table string }{{r.CreateMigration, r.Table}, {r.SuccessorMigration, r.Successor}} {
			prior, found := byName[source.filename]
			createPattern := "(?i)^CREATE TABLE IF NOT EXISTS `" + regexp.QuoteMeta(source.table) + "` \\("
			if !found || prior.Version >= m.Version || !regexp.MustCompile(createPattern).MatchString(up[source.filename]) {
				return fmt.Errorf("%s: missing/early/mismatched CREATE lineage %s", m.Filename, source.filename)
			}
		}
		retired[r.Table] = true
	}
	return nil
}
