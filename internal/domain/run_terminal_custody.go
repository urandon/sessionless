package domain

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

const (
	MaxRunTerminalCustodyBytesV1 = 32 << 10
	MaxTerminalCapsuleBytesV1    = 512 << 10
)

// RunTerminalCustodyV1 is retained DATA, never evidence of an upload, physical
// erasure, current permission or original producer ownership. Failed plans keep
// their one immutable Run-owned slot. Only a dedicated original finalizer may
// install successful correspondence/evidence; generic writers preserve all of it.
type RunTerminalCustodyV1 struct {
	Version        uint32                    `json:"version"`
	Plan           TerminalCustodyPlanV1     `json:"plan"`
	Correspondence *TerminalCorrespondenceV1 `json:"correspondence,omitempty"`
	LocalEvidence  *LocalExecutionEvidenceV1 `json:"local_evidence,omitempty"`
}

type TerminalCustodyPlanV1 struct {
	Scope          AttemptOperationScopeV1 `json:"scope"`
	Capsule        BlobRef                 `json:"capsule"`
	FrontierDigest string                  `json:"frontier_digest"`
}

type TerminalCorrespondenceV1 struct {
	Version                     uint32         `json:"version"`
	CapsuleDigest               string         `json:"capsule_digest"`
	TerminalAssistantEventID    SessionEventID `json:"terminal_assistant_event_id"`
	CanonicalEventsDigest       string         `json:"canonical_events_digest"`
	DerivedContextDigest        string         `json:"derived_context_digest"`
	AccountingDispositionDigest string         `json:"accounting_disposition_digest"`
}

func terminalCustodyError() error {
	return ValidationError{Field: "run.terminal_custody", Reason: "requires closed bounded retained identities and exact original scope"}
}

func terminalHash(namespace string, body []byte) string {
	h := sha256.New()
	_, _ = h.Write([]byte(namespace))
	_, _ = h.Write(body)
	return hex.EncodeToString(h.Sum(nil))
}

// The versioned kind prefix separates objects; SHA256 is the exact raw capsule
// byte hash, as in the existing derived-context object convention.
func TerminalCustodyObjectKeyV1(scope AttemptOperationScopeV1, capsuleSHA256 string) (string, error) {
	if !derivedScopeValid(scope) || !derivedDigestValid(capsuleSHA256) {
		return "", terminalCustodyError()
	}
	body, err := json.Marshal(scope)
	if err != nil {
		return "", terminalCustodyError()
	}
	return SessionRunObjectPrefix(scope.TenantID, scope.SessionID, scope.RunID) + "terminal-custody-v1/" + terminalHash("sessionless.terminal-custody-scope.v1\x00", body) + "/sha256/" + capsuleSHA256 + ".json", nil
}

func (p TerminalCustodyPlanV1) Validate() error {
	if !derivedScopeValid(p.Scope) || !derivedDigestValid(p.FrontierDigest) || len(p.Capsule.Key) == 0 || len(p.Capsule.Key) > 1024 || p.Capsule.TenantID != p.Scope.TenantID || p.Capsule.Size <= 0 || p.Capsule.Size > MaxTerminalCapsuleBytesV1 || !derivedDigestValid(p.Capsule.SHA256) || p.Capsule.Validate() != nil {
		return terminalCustodyError()
	}
	key, err := TerminalCustodyObjectKeyV1(p.Scope, p.Capsule.SHA256)
	if err != nil || p.Capsule.Key != key {
		return terminalCustodyError()
	}
	return nil
}

// Nil is canonical JSON null, not an absent or caller-selected digest. This
// helper hashes the COMPLETE inventory/head, including failed/unpublished plans.
func TerminalDerivedContextDigestV1(metadata *RunDerivedContextV1) (string, error) {
	if metadata != nil && metadata.Validate() != nil {
		return "", terminalCustodyError()
	}
	body, err := json.Marshal(metadata)
	if err != nil || len(body) > MaxRunDerivedContextBytesV1 {
		return "", terminalCustodyError()
	}
	return terminalHash("sessionless.terminal-derived-context.v1\x00", body), nil
}

func (v RunTerminalCustodyV1) Validate() error {
	if v.Version != 1 || v.Plan.Validate() != nil {
		return terminalCustodyError()
	}
	if c := v.Correspondence; c != nil {
		if c.Version != 1 || len(c.TerminalAssistantEventID) == 0 || len(c.TerminalAssistantEventID) > 160 || c.TerminalAssistantEventID.Validate() != nil || c.CapsuleDigest != v.Plan.Capsule.SHA256 {
			return terminalCustodyError()
		}
		for _, d := range []string{c.CapsuleDigest, c.CanonicalEventsDigest, c.DerivedContextDigest, c.AccountingDispositionDigest} {
			if !derivedDigestValid(d) {
				return terminalCustodyError()
			}
		}
	}
	if e := v.LocalEvidence; e != nil {
		if v.Correspondence == nil || e.Validate() != nil || e.Scope != v.Plan.Scope || e.Capsule != v.Plan.Capsule || e.FrontierDigest != v.Plan.FrontierDigest || e.AccountingDispositionDigest != v.Correspondence.AccountingDispositionDigest {
			return terminalCustodyError()
		}
	}
	return nil
}

func (v RunTerminalCustodyV1) ValidateForRun(run Run) error {
	if v.Validate() != nil || v.Plan.Scope.TenantID != run.TenantID || v.Plan.Scope.SessionID != run.SessionID || v.Plan.Scope.RunID != run.ID {
		return terminalCustodyError()
	}
	if (v.Correspondence != nil || v.LocalEvidence != nil) && run.Status != RunSucceeded {
		return terminalCustodyError()
	}
	if e := v.LocalEvidence; e != nil {
		if e.StoppedAt.Before(run.CreatedAt) || run.FinishedAt == nil || e.ErasedAt.After(*run.FinishedAt) {
			return terminalCustodyError()
		}
	}
	if run.DerivedContext != nil {
		if run.DerivedContext.ValidateForRun(run) != nil {
			return terminalCustodyError()
		}
		for _, p := range run.DerivedContext.Inventory {
			if p.Scope.OwnerUserID != v.Plan.Scope.OwnerUserID {
				return terminalCustodyError()
			}
		}
	}
	if v.Correspondence != nil {
		d, err := TerminalDerivedContextDigestV1(run.DerivedContext)
		if err != nil || d != v.Correspondence.DerivedContextDigest {
			return terminalCustodyError()
		}
	}
	return nil
}

func (v RunTerminalCustodyV1) Clone() RunTerminalCustodyV1 {
	if v.Correspondence != nil {
		c := *v.Correspondence
		v.Correspondence = &c
	}
	if v.LocalEvidence != nil {
		e := v.LocalEvidence.Clone()
		v.LocalEvidence = &e
	}
	return v
}

func EqualRunTerminalCustodyV1(a, b *RunTerminalCustodyV1) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if a.Validate() != nil || b.Validate() != nil {
		return false
	}
	x, xe := json.Marshal(a)
	y, ye := json.Marshal(b)
	return xe == nil && ye == nil && len(x) <= MaxRunTerminalCustodyBytesV1 && len(y) <= MaxRunTerminalCustodyBytesV1 && bytes.Equal(x, y)
}

func (v *RunTerminalCustodyV1) UnmarshalJSON(body []byte) error {
	if v == nil || len(body) == 0 || len(body) > MaxRunTerminalCustodyBytesV1 || terminalJSONPreflight(body, "custody") != nil {
		return terminalCustodyError()
	}
	type plain RunTerminalCustodyV1
	var value plain
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if d.Decode(&value) != nil || RunTerminalCustodyV1(value).Validate() != nil {
		return terminalCustodyError()
	}
	*v = RunTerminalCustodyV1(value).Clone()
	return nil
}
