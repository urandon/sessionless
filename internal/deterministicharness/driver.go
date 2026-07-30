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
	Turns         uint64
	Artifacts     uint64
	FailAtTurn    uint64
	RetryableFail bool
}

type Driver struct {
	config Config
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
	start := uint64(1)
	if request.ResumeCheckpoint != nil {
		start = request.ResumeCheckpoint.Sequence + 1
	}
	for turn := start; turn <= driver.config.Turns; turn++ {
		if err := ctx.Err(); err != nil {
			return ports.ExecutionResult{}, err
		}
		input, output := turn*10, turn*5
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
			kind := domain.ErrorTerminal
			if driver.config.RetryableFail {
				kind = domain.ErrorRetryable
			}
			return ports.ExecutionResult{}, &domain.ClassifiedError{
				Kind: kind, Code: "deterministic_failure",
				Operation: "deterministic_harness.execute",
			}
		}
	}
	result := ports.ExecutionResult{
		Summary: fmt.Sprintf("Deterministic run completed after %d turns.", driver.config.Turns),
	}
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
	return result, nil
}

func (*Driver) Cancel(context.Context, ports.ExecutionIdentity) error {
	return nil
}

var _ ports.HarnessDriver = (*Driver)(nil)
