package attachedworkerdaemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type preparedExecutorFunc func(context.Context, PreparedInvocationV1) (AttemptResult, error)

func (execute preparedExecutorFunc) RunPrepared(ctx context.Context, invocation PreparedInvocationV1) (AttemptResult, error) {
	return execute(ctx, invocation)
}

type ordinaryExecutorFunc func(context.Context, AttemptSpec) (AttemptResult, error)

func (execute ordinaryExecutorFunc) Run(ctx context.Context, spec AttemptSpec) (AttemptResult, error) {
	return execute(ctx, spec)
}

type preparedCredentialFixture struct {
	*fakeCredentialLifecycle
	mutateHandle          func(*ports.CredentialHandle)
	mutateMaterialization func(*ports.CredentialMaterialization)
	issueErr              error
	materializeErr        error
}

func (fixture *preparedCredentialFixture) Issue(ctx context.Context, request ports.CredentialIssueRequest) (ports.CredentialHandle, error) {
	if fixture.issueErr != nil {
		fixture.record("issue")
		return ports.CredentialHandle{}, fixture.issueErr
	}
	handle, err := fixture.fakeCredentialLifecycle.Issue(ctx, request)
	if fixture.mutateHandle != nil {
		fixture.mutateHandle(&handle)
	}
	return handle, err
}

func (fixture *preparedCredentialFixture) Materialize(ctx context.Context, handle ports.CredentialHandle) (ports.CredentialMaterialization, error) {
	if fixture.materializeErr != nil {
		fixture.record("materialize")
		return ports.CredentialMaterialization{}, fixture.materializeErr
	}
	materialization, err := fixture.fakeCredentialLifecycle.Materialize(ctx, handle)
	if fixture.mutateMaterialization != nil {
		fixture.mutateMaterialization(&materialization)
	}
	return materialization, err
}

func TestPreparedInvocationSharesTheExistingCredentialOwner(t *testing.T) {
	for _, prepared := range []bool{false, true} {
		t.Run(fmt.Sprintf("prepared=%t", prepared), func(t *testing.T) {
			credentials := &fakeCredentialLifecycle{base: credentialFixtureBase(t)}
			invocation := validCredentialInvocation(t)
			invocation.Process.Arguments = []string{"pinned-argument"}
			invocation.Process.Environment = []EnvironmentVariable{{Name: "PINNED", Value: "fixture"}}
			invocation.Process.Stdin = []byte("private fixture input")
			calls := 0
			var borrowed []byte
			execute := func(spec AttemptSpec) (AttemptResult, error) {
				calls++
				credentials.record("execute")
				if len(spec.Environment) != 2 || spec.Environment[1].Name != invocation.Credential.HomeEnvironment ||
					spec.Environment[1].Value != credentials.root || spec.credentialWriteFile == "" ||
					!reflect.DeepEqual(spec.AdditionalReadRoots, []string{credentials.root}) {
					t.Errorf("prepared process did not receive exact root/file/environment authority")
				}
				return completeProcessRunner{}.Run(context.Background(), spec)
			}
			var runner *InvocationRunner
			var err error
			if prepared {
				runner, err = NewPreparedInvocationRunner(InvocationRunnerConfig{}, preparedExecutorFunc(func(_ context.Context, input PreparedInvocationV1) (AttemptResult, error) {
					if input.Identity != invocation.Identity || input.Credential.OwnerUserID != invocation.Identity.OwnerUserID ||
						input.Credential.ProviderResource != invocation.Credential.IssueRequest.ProviderResource ||
						input.Credential.LeaseFence != invocation.Identity.FenceToken ||
						input.Materialization.RootDir != credentials.root || input.Materialization.FilePath != input.Process.credentialWriteFile {
						t.Errorf("prepared projection differs from validated invocation authority")
					}
					borrowed = input.Process.Stdin
					result, runErr := execute(input.Process)
					// Only the callback's copies change. Lifecycle finalization uses
					// the outer owner's original handle and materialization.
					input.Process.Arguments[0] = "changed"
					input.Process.Environment[0].Value = "changed"
					input.Process.Stdin[0] = 'X'
					input.Credential.OwnerUserID = "other-owner"
					input.Materialization.RootDir = "/not-authorized"
					return result, runErr
				}), credentials)
			} else {
				runner, err = NewInvocationRunner(InvocationRunnerConfig{}, ordinaryExecutorFunc(func(_ context.Context, spec AttemptSpec) (AttemptResult, error) {
					return execute(spec)
				}), credentials)
			}
			if err != nil {
				t.Fatalf("construct runner: %v", err)
			}
			result, runErr := runner.Run(context.Background(), invocation)
			if calls != 1 || !result.Succeeded(runErr) || !result.CredentialReleaseRequired || !result.CredentialReleased {
				t.Errorf("one prepared execution: calls=%d result=%+v err=%v", calls, result, runErr)
			}
			credentials.mu.Lock()
			order := append([]string(nil), credentials.order...)
			credentials.mu.Unlock()
			if !reflect.DeepEqual(order, []string{"issue", "materialize", "execute", "writeback", "release"}) {
				t.Errorf("single lifecycle order=%v", order)
			}
			if string(invocation.Process.Stdin) != "private fixture input" || invocation.Process.Arguments[0] != "pinned-argument" ||
				invocation.Process.Environment[0].Value != "fixture" {
				t.Error("executor mutated caller-owned invocation")
			}
			for _, value := range borrowed {
				if value != 0 {
					t.Fatal("prepared borrowed stdin survived callback")
				}
			}
		})
	}
}

func TestPreparedInvocationRejectsAuthorityBeforeExecution(t *testing.T) {
	for _, tc := range []struct {
		name        string
		mutate      func(*Invocation, *preparedCredentialFixture)
		wantOrder   []string
		wantFailure string
	}{
		{name: "credentialless", mutate: func(input *Invocation, _ *preparedCredentialFixture) { input.Credential = nil }},
		{name: "wrong invocation owner", mutate: func(input *Invocation, _ *preparedCredentialFixture) { input.Identity.OwnerUserID = "other-owner" }},
		{name: "issue failed", mutate: func(_ *Invocation, fixture *preparedCredentialFixture) {
			fixture.issueErr = errors.New("private issue error")
		}, wantOrder: []string{"issue"}, wantFailure: "credential_issue_failed"},
		{name: "handle owner", mutate: func(_ *Invocation, fixture *preparedCredentialFixture) {
			fixture.mutateHandle = func(handle *ports.CredentialHandle) { handle.OwnerUserID = "other-owner" }
		}, wantOrder: []string{"issue", "release"}, wantFailure: "credential_handle_mismatch"},
		{name: "handle worker", mutate: func(_ *Invocation, fixture *preparedCredentialFixture) {
			fixture.mutateHandle = func(handle *ports.CredentialHandle) { handle.WorkerID = "other-worker" }
		}, wantOrder: []string{"issue", "release"}, wantFailure: "credential_handle_mismatch"},
		{name: "handle generation", mutate: func(_ *Invocation, fixture *preparedCredentialFixture) {
			fixture.mutateHandle = func(handle *ports.CredentialHandle) { handle.BindingGeneration++ }
		}, wantOrder: []string{"issue", "release"}, wantFailure: "credential_handle_mismatch"},
		{name: "resource revision", mutate: func(_ *Invocation, fixture *preparedCredentialFixture) {
			fixture.mutateHandle = func(handle *ports.CredentialHandle) { handle.ProviderResource.Revision++ }
		}, wantOrder: []string{"issue", "release"}, wantFailure: "credential_handle_mismatch"},
		{name: "materialize failed", mutate: func(_ *Invocation, fixture *preparedCredentialFixture) {
			fixture.materializeErr = errors.New("private materialize error")
		}, wantOrder: []string{"issue", "materialize", "release"}, wantFailure: "credential_materialization_failed"},
		{name: "materialization path", mutate: func(_ *Invocation, fixture *preparedCredentialFixture) {
			fixture.mutateMaterialization = func(value *ports.CredentialMaterialization) { value.AuthFile = "/not-authorized/auth.json" }
		}, wantOrder: []string{"issue", "materialize", "release"}, wantFailure: "credential_materialization_failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			credentials := &preparedCredentialFixture{fakeCredentialLifecycle: &fakeCredentialLifecycle{base: credentialFixtureBase(t)}}
			invocation := validCredentialInvocation(t)
			tc.mutate(&invocation, credentials)
			calls := 0
			runner, err := NewPreparedInvocationRunner(InvocationRunnerConfig{}, preparedExecutorFunc(func(context.Context, PreparedInvocationV1) (AttemptResult, error) {
				calls++
				return AttemptResult{}, nil
			}), credentials)
			if err != nil {
				t.Fatal(err)
			}
			result, runErr := runner.Run(context.Background(), invocation)
			if runErr == nil || calls != 0 || result.FailureCode != tc.wantFailure || result.Succeeded(runErr) {
				t.Errorf("guard: executor calls=%d result=%+v err=%v want failure=%q", calls, result, runErr, tc.wantFailure)
			}
			credentials.mu.Lock()
			order := append([]string(nil), credentials.order...)
			credentials.mu.Unlock()
			if !reflect.DeepEqual(order, tc.wantOrder) {
				t.Errorf("guard lifecycle order=%v want=%v", order, tc.wantOrder)
			}
			if tc.wantFailure == "credential_handle_mismatch" || tc.wantFailure == "credential_materialization_failed" {
				if !result.CredentialReleaseRequired || !result.CredentialReleased {
					t.Errorf("acquired authority was not retired: %+v", result)
				}
			}
		})
	}
}

func TestPreparedInvocationFinalizationCannotBecomeSuccess(t *testing.T) {
	for _, tc := range []struct {
		name         string
		executorErr  error
		writeBackErr error
		releaseErr   error
		wantFailure  string
	}{
		{name: "executor failed", executorErr: errors.New("ambiguous process outcome")},
		{name: "writeback failed", writeBackErr: errors.New("private writeback error"), wantFailure: "credential_writeback_failed"},
		{name: "release failed", releaseErr: errors.New("private release error"), wantFailure: "credential_release_failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			credentials := &fakeCredentialLifecycle{base: credentialFixtureBase(t), writeBackErr: tc.writeBackErr, releaseErr: tc.releaseErr}
			calls := 0
			runner, err := NewPreparedInvocationRunner(InvocationRunnerConfig{}, preparedExecutorFunc(func(context.Context, PreparedInvocationV1) (AttemptResult, error) {
				calls++
				credentials.record("execute")
				return AttemptResult{DescendantsReaped: true, BoundaryReleased: true, CleanupSucceeded: true}, tc.executorErr
			}), credentials)
			if err != nil {
				t.Fatal(err)
			}
			result, runErr := runner.Run(context.Background(), validCredentialInvocation(t))
			if runErr == nil || calls != 1 || result.Succeeded(runErr) || result.FailureCode != tc.wantFailure ||
				result.CredentialReleased != (tc.releaseErr == nil) {
				t.Errorf("finalization result=%+v err=%v calls=%d", result, runErr, calls)
			}
			credentials.mu.Lock()
			defer credentials.mu.Unlock()
			if !reflect.DeepEqual(credentials.order, []string{"issue", "materialize", "execute", "writeback", "release"}) ||
				!credentials.writeBackActive || !credentials.releaseActive {
				t.Errorf("bounded finalization order=%v writeback active=%t release active=%t", credentials.order, credentials.writeBackActive, credentials.releaseActive)
			}
		})
	}
}

func TestPreparedInvocationCancellationStillFinalizes(t *testing.T) {
	credentials := &fakeCredentialLifecycle{base: credentialFixtureBase(t)}
	started := make(chan struct{})
	runner, err := NewPreparedInvocationRunner(InvocationRunnerConfig{}, preparedExecutorFunc(func(ctx context.Context, _ PreparedInvocationV1) (AttemptResult, error) {
		credentials.record("execute")
		close(started)
		<-ctx.Done()
		return AttemptResult{Cancelled: true, DescendantsReaped: true, BoundaryReleased: true, CleanupSucceeded: true}, ctx.Err()
	}), credentials)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	invocation := validCredentialInvocation(t)
	type completion struct {
		result InvocationResult
		err    error
	}
	done := make(chan completion, 1)
	finished := make(chan struct{})
	t.Cleanup(func() {
		cancel()
		select {
		case <-finished:
		case <-time.After(10 * time.Second):
			t.Error("prepared invocation goroutine survived test cleanup")
		}
	})
	go func() {
		defer close(finished)
		result, runErr := runner.Run(ctx, invocation)
		done <- completion{result, runErr}
	}()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("prepared executor never started")
	}
	cancel()
	select {
	case observed := <-done:
		if !errors.Is(observed.err, context.Canceled) || !observed.result.CredentialReleased || !observed.result.Process.Cancelled {
			t.Errorf("cancel finalization result=%+v err=%v", observed.result, observed.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("prepared executor did not finish cancellation/finalization")
	}
	credentials.mu.Lock()
	defer credentials.mu.Unlock()
	if !reflect.DeepEqual(credentials.order, []string{"issue", "materialize", "execute", "writeback", "release"}) ||
		!credentials.writeBackActive || !credentials.releaseActive {
		t.Errorf("cancel finalization order=%v writeback active=%t release active=%t", credentials.order, credentials.writeBackActive, credentials.releaseActive)
	}
}

func TestPreparedAndOrdinaryInvocationRejectResourceRevisionDrift(t *testing.T) {
	for _, prepared := range []bool{false, true} {
		t.Run(fmt.Sprintf("prepared=%t", prepared), func(t *testing.T) {
			credentials := &preparedCredentialFixture{
				fakeCredentialLifecycle: &fakeCredentialLifecycle{base: credentialFixtureBase(t)},
				mutateHandle:            func(handle *ports.CredentialHandle) { handle.ProviderResource.Revision++ },
			}
			calls := 0
			var runner *InvocationRunner
			var err error
			if prepared {
				runner, err = NewPreparedInvocationRunner(InvocationRunnerConfig{}, preparedExecutorFunc(func(context.Context, PreparedInvocationV1) (AttemptResult, error) {
					calls++
					return AttemptResult{}, nil
				}), credentials)
			} else {
				runner, err = NewInvocationRunner(InvocationRunnerConfig{}, ordinaryExecutorFunc(func(context.Context, AttemptSpec) (AttemptResult, error) {
					calls++
					return AttemptResult{}, nil
				}), credentials)
			}
			if err != nil {
				t.Fatal(err)
			}
			result, runErr := runner.Run(context.Background(), validCredentialInvocation(t))
			if !errors.Is(runErr, ErrCredentialUnavailable) || calls != 0 || !result.CredentialReleased ||
				result.FailureCode != "credential_handle_mismatch" {
				t.Errorf("resource drift: calls=%d result=%+v err=%v", calls, result, runErr)
			}
			credentials.mu.Lock()
			defer credentials.mu.Unlock()
			if !reflect.DeepEqual(credentials.order, []string{"issue", "release"}) {
				t.Errorf("resource drift retirement order=%v", credentials.order)
			}
		})
	}
}

func TestPreparedInvocationConstructorAndProjectionFailClosed(t *testing.T) {
	executor := preparedExecutorFunc(func(context.Context, PreparedInvocationV1) (AttemptResult, error) { return AttemptResult{}, nil })
	credentials := &fakeCredentialLifecycle{base: credentialFixtureBase(t)}
	for _, tc := range []struct {
		name        string
		config      InvocationRunnerConfig
		executor    PreparedProcessRunner
		credentials ports.CredentialLifecycle
	}{
		{name: "missing executor", credentials: credentials},
		{name: "missing lifecycle", executor: executor},
		{name: "unbounded finalization", config: InvocationRunnerConfig{CredentialFinalizeGrace: time.Minute + time.Second}, executor: executor, credentials: credentials},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewPreparedInvocationRunner(tc.config, tc.executor, tc.credentials); !errors.Is(err, ErrInvocationInvalid) {
				t.Errorf("invalid prepared constructor err=%v", err)
			}
		})
	}
	for _, runner := range []*InvocationRunner{nil, {}} {
		if _, err := runner.Run(context.Background(), validCredentialInvocation(t)); !errors.Is(err, ErrInvocationInvalid) {
			t.Errorf("zero/nil runner err=%v", err)
		}
	}
	projection := PreparedInvocationV1{
		Identity:        InvocationIdentity{OwnerUserID: domain.UserID("private-owner")},
		Process:         AttemptSpec{Executable: "/private/executable", Stdin: []byte("private prompt")},
		Materialization: ports.ProviderCredentialMaterializationV1{RootDir: "/private/root", FilePath: "/private/root/auth.json"},
	}
	encoded, err := json.Marshal(projection)
	if err != nil || string(encoded) != "{}" {
		t.Fatalf("prepared projection is not wire-excluded: %s err=%v", encoded, err)
	}
	for _, rendered := range []string{fmt.Sprint(projection), fmt.Sprintf("%#v", projection)} {
		if strings.Contains(rendered, "private") || strings.Contains(rendered, "auth.json") {
			t.Errorf("prepared projection formatting leaked authority: %s", rendered)
		}
	}
}
