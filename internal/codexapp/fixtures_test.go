package codexapp

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

type fixtureRecord struct {
	Kind       string          `json:"kind"`
	Direction  string          `json:"direction"`
	Message    json.RawMessage `json:"message"`
	Data       string          `json:"data"`
	Prefix     string          `json:"prefix"`
	Unit       string          `json:"unit"`
	Count      int             `json:"count"`
	Suffix     string          `json:"suffix"`
	DurationMS int             `json:"durationMs"`
}

func TestSyntheticProtocolFixturesAreBoundedJSONLRecipes(t *testing.T) {
	t.Parallel()
	paths, err := filepath.Glob(filepath.Join("..", "..", "test", "fixtures", "codex-app-server", "*.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"api-key-rejected.jsonl", "happy.jsonl", "malformed-oversized.jsonl",
		"reauth-required.jsonl", "sparse-quota.jsonl", "timeout.jsonl",
		"unexpected-approval-tool.jsonl", "usage-limit.jsonl",
	}
	var got []string
	for _, path := range paths {
		got = append(got, filepath.Base(path))
		validateFixture(t, path)
	}
	sort.Strings(got)
	if !equalStrings(got, want) {
		t.Fatalf("fixture inventory = %v, want %v", got, want)
	}
}

func TestHappyFixtureStartsWithStableHandshake(t *testing.T) {
	t.Parallel()
	path := filepath.Join("..", "..", "test", "fixtures", "codex-app-server", "happy.jsonl")
	records := readFixture(t, path)
	if len(records) < 3 {
		t.Fatalf("happy fixture has %d records", len(records))
	}
	want := []struct {
		direction string
		method    string
		hasResult bool
	}{
		{direction: "client", method: "initialize"},
		{direction: "server", hasResult: true},
		{direction: "client", method: "initialized"},
	}
	for index, expected := range want {
		var envelope struct {
			Method string          `json:"method"`
			Result json.RawMessage `json:"result"`
		}
		if err := json.Unmarshal(records[index].Message, &envelope); err != nil {
			t.Fatal(err)
		}
		if records[index].Direction != expected.direction || envelope.Method != expected.method ||
			(len(envelope.Result) != 0) != expected.hasResult {
			t.Fatalf("handshake record %d = direction %q method %q result=%v", index, records[index].Direction, envelope.Method, len(envelope.Result) != 0)
		}
	}
}

func validateFixture(t *testing.T, path string) {
	t.Helper()
	for index, record := range readFixture(t, path) {
		if record.Direction != "client" && record.Direction != "server" {
			t.Fatalf("%s:%d invalid direction %q", path, index+1, record.Direction)
		}
		switch record.Kind {
		case "frame":
			if len(record.Message) == 0 || record.Message[0] != '{' || bytes.Contains(record.Message, []byte(`"jsonrpc"`)) {
				t.Fatalf("%s:%d invalid stable protocol frame", path, index+1)
			}
		case "raw":
			if record.Direction != "server" || record.Data == "" {
				t.Fatalf("%s:%d invalid raw parser recipe", path, index+1)
			}
		case "repeat":
			if record.Direction != "server" || record.Prefix == "" || record.Unit == "" || record.Count <= 0 || record.Suffix == "" {
				t.Fatalf("%s:%d invalid repeated parser recipe", path, index+1)
			}
		case "stall":
			if record.Direction != "server" || record.DurationMS <= 0 {
				t.Fatalf("%s:%d invalid deadline recipe", path, index+1)
			}
		default:
			t.Fatalf("%s:%d unsupported recipe kind %q", path, index+1, record.Kind)
		}
	}
}

func readFixture(t *testing.T, path string) []fixtureRecord {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	var records []fixtureRecord
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 4096), 256<<10)
	for scanner.Scan() {
		var record fixtureRecord
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatalf("%s:%d: %v", path, len(records)+1, err)
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if len(records) == 0 {
		t.Fatalf("%s is empty", path)
	}
	return records
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
