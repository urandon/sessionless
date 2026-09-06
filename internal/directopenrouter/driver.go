package directopenrouter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/harnessconformance"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
	"gitcode.com/urandon/sessionless/internal/sessionlessharness"
)

type Driver struct {
	profile    ProfileV1
	descriptor domain.HarnessBackendDescriptorV1
	boundary   HTTPBoundaryV1
}

func NewDriver(profile ProfileV1, boundary HTTPBoundaryV1) (*Driver, error) {
	if boundary == nil {
		return nil, ErrContract
	}
	descriptor, err := profile.descriptor()
	if err != nil {
		return nil, err
	}
	return &Driver{profile: profile, descriptor: descriptor, boundary: boundary}, nil
}

func (driver *Driver) DescriptorV1() domain.HarnessBackendDescriptorV1 {
	if driver == nil {
		return domain.HarnessBackendDescriptorV1{}
	}
	return driver.descriptor
}

// DisabledRegistrationV1 is the only production-facing registration builder.
func DisabledRegistrationV1(driver *Driver) (sessionlessharness.Registration, error) {
	return registrationV1(driver, false)
}

func registrationV1(driver *Driver, enabled bool) (sessionlessharness.Registration, error) {
	if driver == nil || enabled != driver.profile.Enabled {
		return sessionlessharness.Registration{}, ErrContract
	}
	return sessionlessharness.Registration{
		Descriptor: driver.descriptor, Enabled: enabled, Driver: driver, ValidateBinding: driver.validateBinding,
	}, nil
}

func (driver *Driver) validateBinding(binding domain.HarnessBindingV1) sessionlessharness.FailureCode {
	if driver == nil || binding.Validate() != nil {
		return sessionlessharness.FailureHarnessBindingInvalid
	}
	if binding.Backend != driver.descriptor {
		return sessionlessharness.FailureHarnessBackendMismatch
	}
	if binding.Resource.Kind != domain.ProviderResourceRouterAccountV1 ||
		binding.Resource.CredentialMode != domain.ProviderCredentialInvocationV1 {
		return sessionlessharness.FailureProviderResourceMismatch
	}
	if binding.ModelVendorID != ModelVendorIDV1 || binding.ModelID != ModelIDV1 {
		return sessionlessharness.FailureProviderCatalogExpired
	}
	if binding.InputDataClass != domain.ProviderDataExternallyShareableV1 {
		return sessionlessharness.FailureEffectivePolicyMismatch
	}
	return ""
}

func (driver *Driver) Preflight(ctx context.Context, identity ports.ExecutionIdentity) error {
	if driver == nil || !driver.profile.Enabled {
		return ErrDisabled
	}
	if ctx == nil || ctx.Err() != nil || identity.Validate() != nil || driver.validateBinding(identity.HarnessBinding) != "" {
		return backendError(domain.ErrorTerminal)
	}
	return nil
}

func (driver *Driver) Execute(ctx context.Context, request ports.ExecutionRequest, sink ports.ExecutionEventSink) (ports.ExecutionResult, error) {
	if driver == nil || !driver.profile.Enabled {
		return ports.ExecutionResult{}, ErrDisabled
	}
	if ctx == nil || ctx.Err() != nil || sink == nil || request.Validate() != nil ||
		driver.validateBinding(request.HarnessBinding) != "" || len(request.InputArtifacts) != 0 ||
		request.ContextWindow == nil || request.ResumeCheckpoint != nil || len(request.AllowedMCPServers) != 0 ||
		request.CredentialMaterialization.Kind != domain.ProviderCredentialDeliveryDirectV1 ||
		request.CredentialMaterialization.RootDir != "" || request.CredentialMaterialization.FilePath != "" ||
		request.CredentialMaterialization.EnvironmentName != "" ||
		request.HarnessBinding.EvidenceExpiresAt == nil ||
		request.Credential.ExpiresAt.After(request.HarnessBinding.EvidenceExpiresAt.UTC()) {
		return ports.ExecutionResult{}, backendError(domain.ErrorTerminal)
	}
	prompt, err := compilePrompt(request)
	if err != nil {
		return ports.ExecutionResult{}, backendError(domain.ErrorTerminal)
	}
	defer clear(prompt)
	body, err := encodeRequest(prompt, driver.profile.MaxCompletionTokens)
	if err != nil {
		return ports.ExecutionResult{}, backendError(domain.ErrorTerminal)
	}
	defer clear(body)
	identity := ports.ExecutionIdentity{
		TenantID: request.TenantID, OwnerUserID: request.OwnerUserID, RunID: request.RunID, AttemptID: request.AttemptID,
		ExecutionPlacementV2: request.ExecutionPlacementV2, HarnessBinding: request.HarnessBinding.Clone(),
		SubstrateBinding: request.SubstrateBinding, AdmissionCostCeiling: request.AdmissionCostCeiling,
	}
	invocation := InvocationV1{
		Identity: identity, Credential: request.Credential, CredentialMaterialization: request.CredentialMaterialization,
		URL: EndpointURLV1, Method: "POST", Headers: requestHeaders(), Body: append([]byte(nil), body...),
		TimeoutMS: driver.profile.RequestTimeoutMS, MaxResponseBytes: maxResponseBytes,
		AuthorizationHeaderName: "Authorization", AuthorizationScheme: "Bearer",
		RequireTLSValidation: true, DenyAmbientCredentials: true, DenyRedirects: true,
		DenyAmbientProxy: true, DenyCookies: true, DenyCompression: true,
		RequireProviderEffectFence: true, MaxProviderEffects: maxProviderEffects,
	}
	transport, invokeErr := driver.boundary.Invoke(ctx, invocation)
	clear(invocation.Body)
	parsed, parseErr := parseResponse(transport.Body)
	clear(transport.Body)
	defer clear(parsed.Final)
	evidence, evidenceErr := reduceEvidence(request.HarnessBinding, uint64(len(body)), parsed, parseErr, transport, invokeErr)
	if evidenceErr != nil {
		return ports.ExecutionResult{}, backendError(domain.ErrorTerminal)
	}
	result := ports.ExecutionResult{ProviderEvidence: &evidence}
	if evidence.FinishClass != domain.ProviderFinishCompletedV1 {
		return result, backendError(domain.ErrorTerminal)
	}
	result.Summary = string(parsed.Final)
	input, output := parsed.InputTokens, parsed.OutputTokens
	if err := sink.Emit(ctx, ports.ExecutionEvent{Sequence: 1, Boundary: "openrouter.chat_completed", InputTokens: &input, OutputTokens: &output}); err != nil {
		result.Summary = ""
		return result, backendError(domain.ErrorTerminal)
	}
	return result, nil
}

func (driver *Driver) Cancel(ctx context.Context, identity ports.ExecutionIdentity) error {
	if driver == nil || ctx == nil || identity.Validate() != nil || driver.validateBinding(identity.HarnessBinding) != "" {
		return backendError(domain.ErrorTerminal)
	}
	if err := driver.boundary.Cancel(ctx, identity); err != nil {
		return backendError(domain.ErrorRetryable)
	}
	return nil
}

func (driver *Driver) BackendProtocolState() harnessconformance.BackendProtocolStateV1 {
	if driver != nil && driver.profile.Enabled {
		return harnessconformance.BackendProtocolSupportedV1
	}
	return harnessconformance.BackendProtocolUnsupportedV1
}

func reduceEvidence(binding domain.HarnessBindingV1, requestBytes uint64, parsed responseObservation, parseErr error, transport ResultV1, invokeErr error) (domain.ProviderExecutionEvidenceV1, error) {
	evidence := domain.ProviderExecutionEvidenceV1{
		AcceptanceClass: domain.ProviderAcceptanceUnknownV1, FinishClass: domain.ProviderFinishFailedV1,
		RouteState: domain.ProviderEvidenceUnknownV1, PolicyVerdict: domain.ProviderPolicyConditionalV1,
		UsageProvenance: domain.ProviderUsageUnknownV1, FailureCode: domain.ProviderExecutionFailureBackendV1,
	}
	if transport.AcceptanceClass == domain.ProviderAcceptanceAcceptedV1 || transport.AcceptanceClass == domain.ProviderAcceptancePreAcceptanceV1 {
		evidence.AcceptanceClass = transport.AcceptanceClass
	}
	switch {
	case evidence.AcceptanceClass == domain.ProviderAcceptancePreAcceptanceV1:
		evidence.FinishClass = domain.ProviderFinishFailedV1
		evidence.FailureCode = domain.ProviderExecutionFailurePreAcceptanceV1
	case transport.Cancelled || transport.Deadline:
		evidence.FinishClass = domain.ProviderFinishCancelledV1
		evidence.FailureCode = domain.ProviderExecutionFailureCancelledV1
	case evidence.AcceptanceClass != domain.ProviderAcceptanceAcceptedV1:
		evidence.FinishClass = domain.ProviderFinishUnknownV1
		evidence.FailureCode = domain.ProviderExecutionFailureAcceptedUnknownV1
	case !transport.CredentialFinalized:
		evidence.FinishClass = domain.ProviderFinishFailedV1
		evidence.FailureCode = domain.ProviderExecutionFailureCredentialFinalizeV1
	case !transport.CleanupSucceeded:
		evidence.FinishClass = domain.ProviderFinishFailedV1
		evidence.FailureCode = domain.ProviderExecutionFailureTeardownV1
	case parseErr != nil && transport.StatusCode == 200:
		evidence.FinishClass = domain.ProviderFinishFailedV1
		evidence.FailureCode = domain.ProviderExecutionFailureProtocolDriftV1
	case invokeErr != nil || transport.StatusCode != 200:
		evidence.FinishClass = domain.ProviderFinishFailedV1
		evidence.FailureCode = domain.ProviderExecutionFailureProviderFailedV1
	case transport.ContentType != "application/json" || transport.ContentEncoding != "" || transport.Redirected || transport.CookiesObserved ||
		transport.RequestBytesWritten != requestBytes || transport.ResponseBytes == 0 || transport.ResponseBytes > maxResponseBytes ||
		transport.ResponseBytes != uint64(len(transport.Body)) || !transport.ProviderEffectFenceSatisfied ||
		transport.ProviderEffects != maxProviderEffects || transport.FailureCode != "" || parseErr != nil:
		evidence.FinishClass = domain.ProviderFinishFailedV1
		evidence.FailureCode = domain.ProviderExecutionFailureProtocolDriftV1
	default:
		input, output := parsed.InputTokens, parsed.OutputTokens
		evidence.FinishClass = domain.ProviderFinishCompletedV1
		evidence.FailureCode = ""
		evidence.RouteState = domain.ProviderEvidenceSupportedV1
		evidence.ActualModelVendorID, evidence.ActualModelID = ModelVendorIDV1, ModelIDV1
		evidence.TransportKind, evidence.TransportProvider = domain.ProviderTransportRouterAPIV1, TransportProviderV1
		evidence.UpstreamProviderID, evidence.EndpointID = ProviderIDV1, EndpointIDV1
		evidence.UsageProvenance = domain.ProviderUsageProviderReportedV1
		evidence.InputTokens, evidence.OutputTokens = &input, &output
	}
	return evidence.SealForBinding(binding)
}

func compilePrompt(request ports.ExecutionRequest) ([]byte, error) {
	file, err := openContextHistory(request.WorkDir, maxPromptBytes)
	if err != nil {
		return nil, ErrContract
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, maxPromptBytes+1))
	if err != nil || len(raw) == 0 || len(raw) > maxPromptBytes {
		clear(raw)
		return nil, ErrContract
	}
	defer clear(raw)
	records, err := sessioncontext.DecodeJSONL(raw, request.TenantID, request.SessionID)
	if err != nil || len(records) == 0 || records[len(records)-1].Event.ID != request.TriggerEventID ||
		records[len(records)-1].Event.Sequence != request.ContextWindow.ThroughSequence {
		return nil, ErrContract
	}
	var output bytes.Buffer
	output.WriteString("Sessionless canonical transcript v1\n")
	for _, record := range records {
		var role, text string
		switch record.Event.Kind {
		case domain.SessionEventUserMessage:
			var payload struct {
				Version     uint32            `json:"version"`
				Origin      json.RawMessage   `json:"origin"`
				Text        string            `json:"text"`
				Metadata    map[string]string `json:"metadata"`
				Attachments []json.RawMessage `json:"attachments"`
			}
			if decodeExact(record.Payload, &payload) != nil || payload.Version != 1 || len(payload.Attachments) != 0 {
				return nil, ErrContract
			}
			role, text = "user", payload.Text
		case domain.SessionEventAssistantMessage:
			var payload struct {
				Schema             string `json:"schema"`
				Summary            string `json:"summary"`
				ArtifactManifestID string `json:"artifact_manifest_id"`
			}
			if decodeExact(record.Payload, &payload) != nil || payload.Schema != "sessionless.assistant-message.v1" {
				return nil, ErrContract
			}
			role, text = "assistant", payload.Summary
		default:
			return nil, ErrContract
		}
		if !validText(text, maxPromptBytes) {
			return nil, ErrContract
		}
		fmt.Fprintf(&output, "[%s]\n%s\n", role, text)
		if output.Len() > maxPromptBytes {
			return nil, ErrContract
		}
	}
	return append([]byte(nil), output.Bytes()...), nil
}

func decodeExact(value []byte, target any) error {
	if validateJSONShape(value, maxJSONDepth, maxJSONMembers) != nil {
		return ErrContract
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return ErrContract
	}
	return nil
}

func validText(value string, max int) bool {
	return strings.TrimSpace(value) != "" && len(value) <= max && utf8.ValidString(value) && !strings.ContainsRune(value, 0)
}

func backendError(kind domain.ErrorKind) error {
	return &domain.ClassifiedError{Kind: kind, Code: string(sessionlessharness.FailureHarnessBackendFailed), Operation: "direct_openrouter.adapter"}
}

var _ ports.HarnessDriver = (*Driver)(nil)
var _ harnessconformance.BackendProtocolObserver = (*Driver)(nil)
