package operationbody

import (
	"bytes"
	"context"
	"reflect"

	"gitcode.com/urandon/sessionless/internal/canonicaloutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessionmaterial"
)

// WriteCanonicalAssistant uses the original physical I/O gate for the shared
// canonical uploader AND immutable readback. The trusted original composer
// supplies the exact registered draft; DATA here does not establish current
// registration, provenance, cleanup or terminal success. Canonical event bytes
// keep their existing independent allowance, not the artifact/body debit.
// Typed tool publication remains a separate required original producer.
func (t *TerminalOutputV1) WriteCanonicalAssistant(ctx context.Context, run domain.Run, draft domain.SessionEventDraft, body []byte) error {
	if t.OperationBodyLimit() == 0 || run.Validate() != nil || run.ID != t.state.scope.RunID ||
		run.TenantID != t.state.scope.TenantID || run.SessionID != t.state.scope.SessionID ||
		run.TerminalCustody == nil || run.TerminalCustody.Plan.Scope != t.state.scope ||
		draft.Kind != domain.SessionEventAssistantMessage || draft.ValidateForRun(run) != nil ||
		len(body) == 0 || len(body) > sessionmaterial.MaxPayloadBytesV1 || int64(len(body)) != draft.Payload.Size {
		return ErrBody
	}
	s := StoreV1{state: t.state}
	call, finish, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer finish()
	st := t.state
	if call.Err() != nil || st.writeFailed || !t.owner.capsule.used || t.owner.assistantUsed {
		return ErrBody
	}
	// Burn before copies/possible I/O, and never clear uncertainty on failure.
	t.owner.assistantUsed = true
	st.writeFailed = true
	sealed := bytes.Clone(body)
	defer clear(sealed)
	expected, err := canonicaloutput.PlanEventDraftV1(run, draft.ID, draft.Kind, sealed, draft.CreatedAt)
	expected.DisplayText = draft.DisplayText
	if err != nil || !reflect.DeepEqual(expected, draft) || call.Err() != nil {
		return ErrBody
	}
	actual, prepared, err := canonicaloutput.PutEventDraftV1(call, st.blobs, run, draft.ID, draft.Kind, sealed, draft.CreatedAt)
	actual.DisplayText = draft.DisplayText
	preparedOK := prepared != nil
	if prepared != nil {
		prepared.Release()
	}
	if err != nil || !preparedOK || !reflect.DeepEqual(actual, draft) || call.Err() != nil {
		return ErrBody
	}
	confirmed, err := st.readVerified(call, draft.Payload)
	equal := bytes.Equal(confirmed, sealed)
	clear(confirmed)
	if err != nil || !equal || call.Err() != nil {
		return ErrBody
	}
	st.writeFailed = false
	return nil
}
