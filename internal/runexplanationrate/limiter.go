// Package runexplanationrate evaluates bounded rate metadata from one caller-owned
// coherent transaction snapshot. It provides neither storage nor authorization.
package runexplanationrate

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

const (
	Version     = 1
	MaxSlots    = 4096
	ProbeCount  = 4
	MaxRowBytes = 8192
	MaxReceipts = 16
	Interval    = 5 * time.Second
	ReceiptTTL  = 30 * time.Second
	IdleTTL     = 10 * time.Minute
)

// ErrInvalidState deliberately contains no identity, selector, or row content.
var ErrInvalidState = errors.New("invalid run explanation rate state")

type Receipt struct {
	RequestID      string    `json:"request_id"`
	SelectorDigest string    `json:"selector_digest"`
	DebitedAt      time.Time `json:"debited_at"`
}

type Slot struct {
	Version              uint32    `json:"version"`
	IdentityDigest       string    `json:"identity_digest"`
	TheoreticalArrivalAt time.Time `json:"theoretical_arrival_at"`
	LastDebitAt          time.Time `json:"last_debit_at"`
	ExpiresAt            time.Time `json:"expires_at"`
	Receipts             []Receipt `json:"receipts"`
}

type Decision struct {
	Allowed    bool
	Debited    bool
	RetryAfter time.Duration
	SlotID     uint32
	Slot       *Slot
}

func IdentityKeys(tenantID domain.TenantID, userID domain.UserID) (string, [ProbeCount]uint32, error) {
	if tenantID.Validate() != nil || userID.Validate() != nil {
		return "", [ProbeCount]uint32{}, ErrInvalidState
	}
	digest := framedDigest("sessionless.run-explanation-rate.identity.v1", string(tenantID), string(userID))
	keys, _ := identityKeys(digest)
	return digest, keys, nil
}

func SelectorDigest(runID domain.RunID) (string, error) {
	if runID.Validate() != nil {
		return "", ErrInvalidState
	}
	return framedDigest("sessionless.run-explanation-rate.resource.v1", string(runID)), nil
}

func framedDigest(prefix string, values ...string) string {
	h := sha256.New()
	h.Write([]byte(prefix))
	h.Write([]byte{0})
	var length [8]byte
	for _, value := range values {
		binary.BigEndian.PutUint64(length[:], uint64(len(value)))
		h.Write(length[:])
		h.Write([]byte(value))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func identityKeys(identity string) ([ProbeCount]uint32, error) {
	var keys [ProbeCount]uint32
	if !validDigest(identity) {
		return keys, ErrInvalidState
	}
	decoded, _ := hex.DecodeString(identity)
	base := uint32(binary.BigEndian.Uint64(decoded[:8]) % MaxSlots)
	for i := range keys {
		keys[i] = (base + uint32(i)) % MaxSlots
	}
	return keys, nil
}

// Evaluate returns a detached replacement row only for a debit. The caller must
// freshly authorize every request (including replays), then commit the outcome
// and optional replacement atomically with its transaction-owned resource read.
func Evaluate(now time.Time, identity string, candidates [ProbeCount]uint32, slots [ProbeCount]*Slot, requestID string, selectorDigest string) (Decision, error) {
	keys, err := identityKeys(identity)
	if err != nil || keys != candidates || !validTime(now) || domain.ValidateOpaqueID("request_id", requestID) != nil || !validDigest(selectorDigest) {
		return Decision{}, ErrInvalidState
	}
	eligibility, ok := addTime(now, Interval)
	if !ok {
		return Decision{}, ErrInvalidState
	}
	own, available := -1, -1
	for i, slot := range slots {
		if slot == nil {
			if available < 0 {
				available = i
			}
			continue
		}
		// Check every supplied row before expiration, allocation, or replay.
		if _, err := EncodeSlot(*slot); err != nil || slot.LastDebitAt.After(now) {
			return Decision{}, ErrInvalidState
		}
		occupantKeys, err := identityKeys(slot.IdentityDigest)
		if err != nil {
			return Decision{}, ErrInvalidState
		}
		placementValid := false
		for _, key := range occupantKeys {
			placementValid = placementValid || key == candidates[i]
		}
		// A valid row at an impossible key is still corruption, even if expired.
		if !placementValid {
			return Decision{}, ErrInvalidState
		}
		if slot.IdentityDigest == identity {
			if own >= 0 {
				return Decision{}, ErrInvalidState
			}
			own = i
		}
		if !now.Before(slot.ExpiresAt) && available < 0 {
			available = i
		}
	}
	selected := own
	if selected < 0 {
		selected = available
	}
	if selected < 0 {
		return Decision{RetryAfter: Interval}, nil
	}

	current := slots[selected]
	tat := now
	receipts := make([]Receipt, 0, MaxReceipts)
	if own >= 0 && now.Before(current.ExpiresAt) {
		tat = current.TheoreticalArrivalAt
		for _, receipt := range current.Receipts {
			if now.Sub(receipt.DebitedAt) >= ReceiptTTL {
				continue
			}
			if receipt.RequestID == requestID {
				if receipt.SelectorDigest != selectorDigest {
					return Decision{}, ErrInvalidState
				}
				return Decision{Allowed: true, SlotID: candidates[selected]}, nil
			}
			receipts = append(receipts, receipt)
		}
		if tat.After(eligibility) {
			remaining := tat.Sub(eligibility)
			seconds := remaining / time.Second
			if remaining%time.Second != 0 {
				seconds++
			}
			return Decision{RetryAfter: seconds * time.Second, SlotID: candidates[selected]}, nil
		}
	}
	if len(receipts) >= MaxReceipts {
		return Decision{}, ErrInvalidState
	}
	if tat.Before(now) {
		tat = now
	}
	nextTAT, ok := addTime(tat, Interval)
	if !ok {
		return Decision{}, ErrInvalidState
	}
	expires, ok := addTime(now, IdleTTL)
	if !ok {
		return Decision{}, ErrInvalidState
	}
	receipts = append(receipts, Receipt{RequestID: requestID, SelectorDigest: selectorDigest, DebitedAt: now})
	next := Slot{Version: Version, IdentityDigest: identity, TheoreticalArrivalAt: nextTAT, LastDebitAt: now, ExpiresAt: expires, Receipts: receipts}
	if _, err := EncodeSlot(next); err != nil {
		return Decision{}, ErrInvalidState
	}
	return Decision{Allowed: true, Debited: true, SlotID: candidates[selected], Slot: &next}, nil
}

func validDigest(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	for _, c := range value {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func validTime(value time.Time) bool {
	return !value.IsZero() && value.Location() == time.UTC && value.Year() >= 1 && value.Year() <= 9999
}

func addTime(value time.Time, duration time.Duration) (time.Time, bool) {
	result := value.Add(duration)
	return result, validTime(result) && result.After(value)
}

func validateSlot(slot Slot) error {
	if slot.Version != Version || !validDigest(slot.IdentityDigest) || !validTime(slot.LastDebitAt) || !validTime(slot.TheoreticalArrivalAt) || !validTime(slot.ExpiresAt) || len(slot.Receipts) == 0 || len(slot.Receipts) > MaxReceipts {
		return ErrInvalidState
	}
	minimum, minOK := addTime(slot.LastDebitAt, Interval)
	maximum, maxOK := addTime(slot.LastDebitAt, 2*Interval)
	expires, expiryOK := addTime(slot.LastDebitAt, IdleTTL)
	if !minOK || !maxOK || !expiryOK || slot.TheoreticalArrivalAt.Before(minimum) || slot.TheoreticalArrivalAt.After(maximum) || !slot.ExpiresAt.Equal(expires) {
		return ErrInvalidState
	}
	lastPresent := false
	for i, receipt := range slot.Receipts {
		if domain.ValidateOpaqueID("request_id", receipt.RequestID) != nil || !validDigest(receipt.SelectorDigest) || !validTime(receipt.DebitedAt) || receipt.DebitedAt.After(slot.LastDebitAt) || slot.LastDebitAt.Sub(receipt.DebitedAt) >= ReceiptTTL || i > 0 && receipt.DebitedAt.Before(slot.Receipts[i-1].DebitedAt) {
			return ErrInvalidState
		}
		for j := 0; j < i; j++ {
			if receipt.RequestID == slot.Receipts[j].RequestID {
				return ErrInvalidState
			}
		}
		lastPresent = lastPresent || receipt.DebitedAt.Equal(slot.LastDebitAt)
	}
	if !lastPresent {
		return ErrInvalidState
	}
	return nil
}

func EncodeSlot(slot Slot) ([]byte, error) {
	if validateSlot(slot) != nil {
		return nil, ErrInvalidState
	}
	data, err := json.Marshal(slot)
	if err != nil || len(data) > MaxRowBytes {
		return nil, ErrInvalidState
	}
	return data, nil
}

// decodeFields requires each exact canonical key once; encoding/json's normal
// case-insensitive matching and duplicate-key overwrite are not row contracts.
func decodeFields(data []byte, names []string) ([]json.RawMessage, error) {
	d := json.NewDecoder(bytes.NewReader(data))
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return nil, ErrInvalidState
	}
	values := make([]json.RawMessage, len(names))
	for d.More() {
		token, err := d.Token()
		if err != nil {
			return nil, ErrInvalidState
		}
		name, ok := token.(string)
		if !ok {
			return nil, ErrInvalidState
		}
		index := -1
		for i, expected := range names {
			if name == expected {
				index = i
				break
			}
		}
		if index < 0 || values[index] != nil {
			return nil, ErrInvalidState
		}
		if d.Decode(&values[index]) != nil || bytes.Equal(values[index], []byte("null")) {
			return nil, ErrInvalidState
		}
	}
	if token, err = d.Token(); err != nil || token != json.Delim('}') {
		return nil, ErrInvalidState
	}
	if _, err = d.Token(); err != io.EOF {
		return nil, ErrInvalidState
	}
	for _, value := range values {
		if value == nil {
			return nil, ErrInvalidState
		}
	}
	return values, nil
}

func DecodeSlot(data []byte) (Slot, error) {
	if len(data) == 0 || len(data) > MaxRowBytes {
		return Slot{}, ErrInvalidState
	}
	fields, err := decodeFields(data, []string{"version", "identity_digest", "theoretical_arrival_at", "last_debit_at", "expires_at", "receipts"})
	if err != nil {
		return Slot{}, ErrInvalidState
	}
	var slot Slot
	outputs := []any{&slot.Version, &slot.IdentityDigest, &slot.TheoreticalArrivalAt, &slot.LastDebitAt, &slot.ExpiresAt}
	for i, output := range outputs {
		if json.Unmarshal(fields[i], output) != nil {
			return Slot{}, ErrInvalidState
		}
	}
	var receipts []json.RawMessage
	if json.Unmarshal(fields[5], &receipts) != nil || len(receipts) == 0 || len(receipts) > MaxReceipts {
		return Slot{}, ErrInvalidState
	}
	for _, encoded := range receipts {
		fields, err := decodeFields(encoded, []string{"request_id", "selector_digest", "debited_at"})
		if err != nil {
			return Slot{}, ErrInvalidState
		}
		var receipt Receipt
		if json.Unmarshal(fields[0], &receipt.RequestID) != nil || json.Unmarshal(fields[1], &receipt.SelectorDigest) != nil || json.Unmarshal(fields[2], &receipt.DebitedAt) != nil {
			return Slot{}, ErrInvalidState
		}
		slot.Receipts = append(slot.Receipts, receipt)
	}
	if _, err := EncodeSlot(slot); err != nil {
		return Slot{}, ErrInvalidState
	}
	return slot, nil
}
