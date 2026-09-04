// Package deterministicharness provides a credential-free worker harness used
// to prove lifecycle semantics before binding a real subscription CLI.
package deterministicharness

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

type Config struct {
	Turns                 uint64
	Artifacts             uint64
	FailBeforeFirstTurn   bool
	FailAtTurn            uint64
	RetryableFail         bool
	CaptureContextHistory bool
}

type Driver struct {
	config Config
}

func (driver *Driver) Preflight(_ context.Context, identity ports.ExecutionIdentity) error {
	return identity.Validate()
}

func New(config Config) (*Driver, error) {
	if config.Turns == 0 {
		config.Turns = 2
	}
	if config.Artifacts == 0 {
		config.Artifacts = 1
	}
	if config.FailAtTurn > config.Turns {
		return nil, fmt.Errorf("fail turn must not exceed configured turns")
	}
	return &Driver{config: config}, nil
}

func (driver *Driver) Execute(
	ctx context.Context,
	request ports.ExecutionRequest,
	sink ports.ExecutionEventSink,
) (ports.ExecutionResult, error) {
	if err := request.Validate(); err != nil {
		return ports.ExecutionResult{}, err
	}
	if sink == nil {
		return ports.ExecutionResult{}, fmt.Errorf("execution event sink is required")
	}
	if driver.config.FailBeforeFirstTurn && request.ResumeCheckpoint == nil {
		return ports.ExecutionResult{}, driver.failure()
	}
	start := uint64(1)
	if request.ResumeCheckpoint != nil {
		start = request.ResumeCheckpoint.Sequence + 1
	}
	var totalInput, totalOutput uint64
	for turn := start; turn <= driver.config.Turns; turn++ {
		if err := ctx.Err(); err != nil {
			return ports.ExecutionResult{}, err
		}
		input, output := turn*10, turn*5
		totalInput += input
		totalOutput += output
		state, err := json.Marshal(map[string]any{
			"schema": "sessionless.deterministic-checkpoint.v1",
			"turn":   turn, "run_id": request.RunID,
		})
		if err != nil {
			return ports.ExecutionResult{}, err
		}
		if err := sink.Emit(ctx, ports.ExecutionEvent{
			Sequence: turn, Boundary: fmt.Sprintf("turn-%d", turn),
			CheckpointState: state, InputTokens: &input, OutputTokens: &output,
		}); err != nil {
			return ports.ExecutionResult{}, err
		}
		if driver.config.FailAtTurn == turn {
			return ports.ExecutionResult{}, driver.failure()
		}
	}
	result := ports.ExecutionResult{
		Summary: fmt.Sprintf("Deterministic run completed after %d turns.", driver.config.Turns),
	}
	evidence, err := (domain.ProviderExecutionEvidenceV1{
		AcceptanceClass:     domain.ProviderAcceptanceAcceptedV1,
		FinishClass:         domain.ProviderFinishCompletedV1,
		RouteState:          domain.ProviderEvidenceSupportedV1,
		ActualModelVendorID: "sessionless",
		ActualModelID:       request.HarnessBinding.ModelID,
		TransportKind:       domain.ProviderTransportLocalCLIV1,
		TransportProvider:   "sessionless",
		UpstreamProviderID:  "local",
		EndpointID:          "deterministic-fixture",
		PolicyVerdict:       domain.ProviderPolicyGoV1,
		UsageProvenance:     domain.ProviderUsageHarnessMeasuredV1,
		InputTokens:         &totalInput,
		OutputTokens:        &totalOutput,
	}).SealForBinding(request.HarnessBinding)
	if err != nil {
		return ports.ExecutionResult{}, err
	}
	result.ProviderEvidence = &evidence
	for index := uint64(1); index <= driver.config.Artifacts; index++ {
		name := fmt.Sprintf("result-%02d.txt", index)
		relative := filepath.Join("outputs", name)
		path := filepath.Join(request.WorkDir, relative)
		if err := os.WriteFile(
			path,
			[]byte(fmt.Sprintf("run=%s artifact=%d\n", request.RunID, index)),
			0o600,
		); err != nil {
			return ports.ExecutionResult{}, err
		}
		result.Outputs = append(result.Outputs, ports.ExecutionOutput{
			Name: name, MediaType: "text/plain", RelativePath: name,
		})
	}
	if driver.config.CaptureContextHistory {
		const name = "context-history.jsonl"
		history, err := os.ReadFile(filepath.Join(request.WorkDir, "context", "history.jsonl"))
		if err != nil {
			return ports.ExecutionResult{}, fmt.Errorf("read captured context history: %w", err)
		}
		if err := os.WriteFile(filepath.Join(request.WorkDir, "outputs", name), history, 0o600); err != nil {
			return ports.ExecutionResult{}, fmt.Errorf("write captured context history: %w", err)
		}
		result.Outputs = append(result.Outputs, ports.ExecutionOutput{
			Name: name, MediaType: "application/x-ndjson", RelativePath: name,
		})
	}
	return result, nil
}

func (driver *Driver) failure() error {
	kind := domain.ErrorTerminal
	if driver.config.RetryableFail {
		kind = domain.ErrorRetryable
	}
	return &domain.ClassifiedError{
		Kind: kind, Code: "deterministic_failure",
		Operation: "deterministic_harness.execute",
	}
}

func (*Driver) Cancel(context.Context, ports.ExecutionIdentity) error {
	return nil
}

var _ ports.HarnessDriver = (*Driver)(nil)
