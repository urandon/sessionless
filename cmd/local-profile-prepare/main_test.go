package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
)

func preparationArgs() []string {
	return []string{"--installation-root", "/explicit/private/new-installation", "--executable", "/explicit/worker-runtime", "--confirm", prepareConfirmation}
}

type countedInput struct{ bytes int }

func (r *countedInput) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	r.bytes += len(p)
	return len(p), nil
}

func TestPreparationInputPreservesDeclaredBytes(t *testing.T) {
	raw := []byte("{\n  \"version\": 1\n}\n")
	config, got, err := readInput(t.Context(), preparationArgs(), bytes.NewReader(raw))
	if err != nil || config.root != "/explicit/private/new-installation" || config.executable != "/explicit/worker-runtime" || !bytes.Equal(got, raw) {
		t.Fatalf("explicit preparation DATA changed: config=%+v exact=%t err=%v", config, bytes.Equal(got, raw), err)
	}
	// This intentionally incomplete envelope is rejected before executable or
	// output access. Unit tests never prepare an owner installation at this path.
	var output bytes.Buffer
	if err := run(t.Context(), preparationArgs(), bytes.NewReader(raw), &output); err != errPreparation || output.Len() != 0 {
		t.Fatalf("incomplete template prepared: fixedError=%t outputBytes=%d", err == errPreparation, output.Len())
	}
}

func TestPreparationRequiresIntentBeforeReading(t *testing.T) {
	for name, args := range map[string][]string{
		"missing":       nil,
		"no-confirm":    {"--installation-root", "/explicit/private/new", "--executable", "/explicit/worker"},
		"wrong-confirm": {"--installation-root", "/explicit/private/new", "--executable", "/explicit/worker", "--confirm", "prepare local profile"},
		"relative":      {"--installation-root", "relative", "--executable", "/explicit/worker", "--confirm", prepareConfirmation},
		"root":          {"--installation-root", "/", "--executable", "/explicit/worker", "--confirm", prepareConfirmation},
		"unclean":       {"--installation-root", "/explicit/../new", "--executable", "/explicit/worker", "--confirm", prepareConfirmation},
		"unknown":       {"--private-argument", "must-not-escape"},
		"trailing":      append(preparationArgs(), "extra"),
	} {
		t.Run(name, func(t *testing.T) {
			input := &countedInput{}
			_, raw, err := readInput(t.Context(), args, input)
			if !errors.Is(err, errPreparation) || raw != nil || input.bytes != 0 {
				t.Fatalf("invalid preparation intent read input: err=%v read=%d", err, input.bytes)
			}
		})
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	input := &countedInput{}
	if _, _, err := readInput(ctx, preparationArgs(), input); err != errPreparation || input.bytes != 0 {
		t.Fatal("cancelled preparation consumed input")
	}
}

func TestPreparationInputIsBoundedAndRedacted(t *testing.T) {
	input := &countedInput{}
	_, raw, err := readInput(t.Context(), preparationArgs(), input)
	if err != errPreparation || raw != nil || input.bytes != maxTemplateBytes+1 {
		t.Fatalf("unbounded template read: err=%v retained=%d read=%d", err, len(raw), input.bytes)
	}
	for _, raw := range []string{"", "synthetic-private-body-must-not-escape"} {
		var output bytes.Buffer
		err := run(t.Context(), preparationArgs(), strings.NewReader(raw), &output)
		if err != errPreparation || output.Len() != 0 {
			t.Fatalf("private diagnostic escaped: fixedError=%t outputBytes=%d", err == errPreparation, output.Len())
		}
	}
	input = &countedInput{}
	if err := run(t.Context(), preparationArgs(), input, nil); err != errPreparation || input.bytes != 0 {
		t.Fatal("nil output could start preparation")
	}
}
