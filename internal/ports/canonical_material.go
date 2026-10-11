package ports

import (
	"errors"
	"fmt"
	"sync"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/sessionmaterial"
)

const (
	MaxCanonicalPreparedEventsV1 = 64
	MaxCanonicalPreparedBytesV1  = 4 << 20
)

// CanonicalEventMaterialSetV1 owns transient preparations through all callback
// retries of one canonical finalization. It is NOT durable receipt content,
// authority or an alternate event contract. Ordinary pointer aliases share
// its release lifetime; neither JSON nor formatting exposes its entries.
// Events outside its inspection budget still append unknown material facts.
type CanonicalEventMaterialSetV1 struct{ state *canonicalMaterialState }

type canonicalMaterialState struct {
	mu      sync.Mutex
	entries map[domain.SessionEventID]*sessionmaterial.PreparedV1
	bytes   uint64
	live    bool
}

func NewCanonicalEventMaterialSetV1() *CanonicalEventMaterialSetV1 {
	return &CanonicalEventMaterialSetV1{state: &canonicalMaterialState{entries: make(map[domain.SessionEventID]*sessionmaterial.PreparedV1), live: true}}
}

// Adopt transfers ownership, including on rejection. Budgets use actual owned
// bytes, never caller-supplied BlobRef sizes. Duplicate IDs cannot replace an
// earlier preparation. The final store still binds every stable identity.
func (set *CanonicalEventMaterialSetV1) Adopt(id domain.SessionEventID, prepared *sessionmaterial.PreparedV1) bool {
	if set == nil || set.state == nil || prepared == nil || id.Validate() != nil {
		prepared.Release()
		return false
	}
	state := set.state
	state.mu.Lock()
	defer state.mu.Unlock()
	if previous, exists := state.entries[id]; exists && previous == prepared && state.live {
		return true
	}
	size, verified := prepared.OwnedBytesV1()
	_, duplicate := state.entries[id]
	if !state.live || !verified || duplicate || len(state.entries) >= MaxCanonicalPreparedEventsV1 ||
		size > MaxCanonicalPreparedBytesV1-state.bytes {
		prepared.Release()
		return false
	}
	state.entries[id] = prepared
	state.bytes += size
	return true
}

// ForEvent borrows an alias; it does not extend ownership beyond Release.
func (set *CanonicalEventMaterialSetV1) ForEvent(id domain.SessionEventID) *sessionmaterial.PreparedV1 {
	if set == nil || set.state == nil {
		return nil
	}
	state := set.state
	state.mu.Lock()
	defer state.mu.Unlock()
	if !state.live {
		return nil
	}
	return state.entries[id]
}

func (set *CanonicalEventMaterialSetV1) Release() {
	if set == nil || set.state == nil {
		return
	}
	state := set.state
	state.mu.Lock()
	defer state.mu.Unlock()
	for _, prepared := range state.entries {
		prepared.Release()
	}
	clear(state.entries)
	state.bytes, state.live = 0, false
}

func (CanonicalEventMaterialSetV1) MarshalJSON() ([]byte, error) {
	return nil, errors.New("canonical prepared event set cannot be serialized")
}

func (CanonicalEventMaterialSetV1) Format(state fmt.State, verb rune) {
	_, _ = fmt.Fprint(state, "ports.CanonicalEventMaterialSetV1")
}

// PrepareCanonicalDraftV1 uses the exact canonical envelope and Run binding.
// Unrecognized/oversize material remains unknown; valid canonical output is
// not forbidden merely because this projection cannot inspect it.
func PrepareCanonicalDraftV1(run domain.Run, draft domain.SessionEventDraft, body []byte) *sessionmaterial.PreparedV1 {
	runID := run.ID
	prepared, _ := sessionmaterial.PrepareV1(domain.SessionEvent{
		ID: draft.ID, TenantID: run.TenantID, SessionID: run.SessionID,
		Kind: draft.Kind, RunID: &runID, IdempotencyKey: draft.IdempotencyKey, Payload: draft.Payload,
	}, body, nil)
	return prepared
}
