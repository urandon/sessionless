package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
)

type countedInput struct{ bytes int }

func (r *countedInput) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	r.bytes += len(p)
	return len(p), nil
}

func TestReadInputPreservesExplicitTemplate(t *testing.T) {
	raw := []byte("{\n  \"version\": 1\n}\n")
	executable, got, err := readInput(t.Context(), []string{"--executable", "/explicit/worker-runtime"}, bytes.NewReader(raw))
	if err != nil || executable != "/explicit/worker-runtime" || !bytes.Equal(got, raw) {
		t.Fatalf("bounded DATA intake: executable=%q exact=%t err=%v", executable, bytes.Equal(got, raw), err)
	}
	// Parsing is deliberately separate from actual canonical template validity.
	var output bytes.Buffer
	if err := run(t.Context(), []string{"--executable", "/explicit/worker-runtime"}, bytes.NewReader(raw), &output); !errors.Is(err, errTemplate) || output.Len() != 0 {
		t.Fatalf("incomplete template became success: err=%v outputBytes=%d", err, output.Len())
	}
}

func TestReadInputRejectsBeforeReading(t *testing.T) {
	for name, args := range map[string][]string{
		"missing": nil, "relative": {"--executable", "relative/worker"},
		"unclean": {"--executable", "/explicit/../worker"}, "root": {"--executable", "/"},
		"unknown":  {"--private-token", "must-not-escape"},
		"trailing": {"--executable", "/explicit/worker", "extra"},
	} {
		t.Run(name, func(t *testing.T) {
			input := &countedInput{}
			_, raw, err := readInput(t.Context(), args, input)
			if !errors.Is(err, errTemplate) || raw != nil || input.bytes != 0 {
				t.Fatalf("invalid arguments read template: err=%v bytes=%d", err, input.bytes)
			}
		})
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	input := &countedInput{}
	_, _, err := readInput(ctx, []string{"--executable", "/explicit/worker"}, input)
	if !errors.Is(err, errTemplate) || input.bytes != 0 {
		t.Fatalf("cancelled intake consumed bytes: err=%v bytes=%d", err, input.bytes)
	}
}

func TestReadInputBoundsAndFixedErrors(t *testing.T) {
	input := &countedInput{}
	_, raw, err := readInput(t.Context(), []string{"--executable", "/explicit/worker"}, input)
	if !errors.Is(err, errTemplate) || raw != nil || input.bytes != maxTemplateBytes+1 {
		t.Fatalf("oversized intake: err=%v retained=%d read=%d", err, len(raw), input.bytes)
	}
	for _, data := range []string{"", "synthetic-private-body-do-not-echo"} {
		var output bytes.Buffer
		err := run(t.Context(), []string{"--executable", "/explicit/worker"}, strings.NewReader(data), &output)
		if err != errTemplate || output.Len() != 0 {
			t.Fatalf("validation leaked details: fixedError=%t outputBytes=%d", err == errTemplate, output.Len())
		}
	}
	if err := run(nil, nil, io.LimitReader(strings.NewReader("x"), 1), io.Discard); err != errTemplate {
		t.Fatal("nil context was accepted")
	}
}
