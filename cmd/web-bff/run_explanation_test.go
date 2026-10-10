package main

import (
	"context"
	"errors"
	"testing"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type explanationStartupFixture struct {
	calls  int
	commit string
	err    error
}

func (fixture *explanationStartupFixture) RequireRunExplanationCutover(_ context.Context, commit string) error {
	fixture.calls++
	fixture.commit = commit
	return fixture.err
}

func (*explanationStartupFixture) ReadRatedRunExplanationV1(context.Context, domain.SecretDigest, string, domain.RunID) (ports.RunExplanationReadOutcomeV1, error) {
	panic("startup must not read a resource")
}

func TestRunExplanationExplicitEnablementRequiresCheckedCutover(t *testing.T) {
	for _, test := range []struct {
		name, flag string
		failure    bool
		checked    bool
	}{
		{"default absent", "", false, false}, {"explicit disabled", "false", false, false},
		{"invalid flag", "yes", true, false}, {"enabled", "true", false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := &explanationStartupFixture{}
			got, err := runExplanationsFromEnvironment(context.Background(), func(key string) string {
				if key == "WEB_RUN_EXPLANATION_ENABLED" {
					return test.flag
				}
				return "writer-pin"
			}, fixture)
			if (err != nil) != test.failure || (fixture.calls == 1) != test.checked || (got != nil) != test.checked {
				t.Fatalf("failure=%v calls=%d mounted=%t", err, fixture.calls, got != nil)
			}
			if test.checked && fixture.commit != "writer-pin" {
				t.Fatal("deployment writer pin not checked")
			}
		})
	}
	fixture := &explanationStartupFixture{err: errors.New("cutover missing")}
	got, err := runExplanationsFromEnvironment(context.Background(), func(string) string { return "true" }, fixture)
	if err == nil || got != nil || fixture.calls != 1 {
		t.Fatal("enabled reader bypassed absent cutover")
	}
	got, err = runExplanationsFromEnvironment(context.Background(), func(string) string { return "true" }, nil)
	if err == nil || got != nil {
		t.Fatal("enabled reader bypassed absent store")
	}
}
