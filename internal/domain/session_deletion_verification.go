package domain

import (
	"bytes"
	"encoding/json"
	"math"
	"sort"
	"time"
)

const MaxRetainedDeletionVerificationBytesV1 = 2 << 10

// RetainedDeletionVerificationV1 is content-free historical DATA. Only the
// original Store's joined verifier may install it; decoding is not approval.
type RetainedDeletionVerificationV1 struct {
	Schema          string    `json:"schema"`
	TenantID        TenantID  `json:"tenant_id"`
	SessionID       SessionID `json:"session_id"`
	RequestDigest   string    `json:"request_digest"`
	MetadataDigest  string    `json:"metadata_digest"`
	InventoryDigest string    `json:"inventory_digest"`
	Rows            uint64    `json:"rows"`
	Objects         uint64    `json:"objects"`
	Bytes           uint64    `json:"bytes"`
	VerifiedRuns    uint64    `json:"verified_runs"`
	VerifiedAt      time.Time `json:"verified_at"`
}

func retainedDeletionError() error {
	return ValidationError{Field: "session_deletion.retained_verification", Reason: "requires the bounded original request and exact retained inventory commitment"}
}

func SessionDeletionRequestDigestV1(d SessionDeletion) (string, error) {
	if d.TenantID.Validate() != nil || d.SessionID.Validate() != nil || d.RequestedBy.Validate() != nil || d.RequestedAt.IsZero() || len(d.Reason) == 0 || len(d.Reason) > 1024 {
		return "", retainedDeletionError()
	}
	body, err := json.Marshal(struct {
		Tenant      TenantID
		Session     SessionID
		Owner       UserID
		Reason      string
		RequestedAt time.Time
	}{d.TenantID, d.SessionID, d.RequestedBy, d.Reason, d.RequestedAt})
	if err != nil {
		return "", retainedDeletionError()
	}
	return terminalHash("sessionless.retained-deletion-request.v1\x00", body), nil
}

func SessionDeletionInventoryRowsV1(i SessionDeletionInventory) (uint64, error) {
	var n uint64
	for _, v := range []uint64{i.EventRows, i.SnapshotRows, i.MaterialCurrentRows, i.MaterialBoundaryRows, i.MaterialSnapshotRows, i.RunRows, i.ManifestRows, i.DeliveryRows, i.CheckpointRows, i.ReceiptRows, i.ParticipantRows, i.OwnedAdmissionRows, i.BindingRows, i.ProjectionRows, i.RetainedRows} {
		if v > 10000-n {
			return 0, retainedDeletionError()
		}
		n += v
	}
	return n, nil
}

func SessionDeletionInventoryDigestV1(i SessionDeletionInventory) (string, error) {
	if len(i.Objects) > 10000 || len(i.RunIDs) > 10000 || i.RunRows != uint64(len(i.RunIDs)) {
		return "", retainedDeletionError()
	}
	if i.Validate(10000) != nil {
		return "", retainedDeletionError()
	}
	if _, err := SessionDeletionInventoryRowsV1(i); err != nil {
		return "", err
	}
	i.Objects = append([]BlobRef(nil), i.Objects...)
	i.RunIDs = append([]RunID(nil), i.RunIDs...)
	sort.Slice(i.Objects, func(a, b int) bool { return i.Objects[a].Key < i.Objects[b].Key })
	sort.Slice(i.RunIDs, func(a, b int) bool { return i.RunIDs[a] < i.RunIDs[b] })
	body, err := json.Marshal(i)
	if err != nil {
		return "", retainedDeletionError()
	}
	return terminalHash("sessionless.retained-deletion-inventory.v1\x00", body), nil
}

func (v RetainedDeletionVerificationV1) validate() error {
	if v.Schema != "sessionless.retained-deletion.v1" || v.TenantID.Validate() != nil || v.SessionID.Validate() != nil || !derivedDigestValid(v.RequestDigest) || !derivedDigestValid(v.MetadataDigest) || !derivedDigestValid(v.InventoryDigest) || v.Rows > 10000 || v.Objects > 10000 || v.VerifiedRuns == 0 || v.VerifiedRuns > v.Rows || v.VerifiedAt.IsZero() || v.VerifiedAt.Location() != time.UTC || v.VerifiedAt.Nanosecond()%1000 != 0 || v.VerifiedAt.Year() < 1 || v.VerifiedAt.Year() > 9999 || v.Bytes > math.MaxInt64 {
		return retainedDeletionError()
	}
	return nil
}

func (v RetainedDeletionVerificationV1) ValidateForDeletion(d SessionDeletion) error {
	r, err := SessionDeletionRequestDigestV1(d)
	if err != nil || v.validate() != nil || v.TenantID != d.TenantID || v.SessionID != d.SessionID || v.RequestDigest != r || d.State == SessionDeletionRequested || d.StartedAt == nil || v.VerifiedAt.Before(d.RequestedAt) || !v.VerifiedAt.Equal(*d.StartedAt) {
		return retainedDeletionError()
	}
	if d.State == SessionDeletionCompleted && (d.DeletedObjects != v.Objects || d.DeletedBytes != v.Bytes) {
		return retainedDeletionError()
	}
	return nil
}

func (v RetainedDeletionVerificationV1) ValidateInventory(d SessionDeletion, i SessionDeletionInventory) error {
	h, err := SessionDeletionInventoryDigestV1(i)
	n, e := SessionDeletionInventoryRowsV1(i)
	if v.ValidateForDeletion(d) != nil || err != nil || e != nil || v.InventoryDigest != h || v.Rows != n || v.Objects != uint64(len(i.Objects)) || v.Bytes != i.TotalBytes || i.TenantID != d.TenantID || i.SessionID != d.SessionID {
		return retainedDeletionError()
	}
	return nil
}

func (v *RetainedDeletionVerificationV1) UnmarshalJSON(body []byte) error {
	if v == nil || len(body) == 0 || len(body) > MaxRetainedDeletionVerificationBytesV1 || !terminalJSONUnicodeValid(body) {
		return retainedDeletionError()
	}
	type plain RetainedDeletionVerificationV1
	var p plain
	d := json.NewDecoder(bytes.NewReader(body))
	d.DisallowUnknownFields()
	if d.Decode(&p) != nil || RetainedDeletionVerificationV1(p).validate() != nil {
		return retainedDeletionError()
	}
	canonical, err := json.Marshal(p)
	// One canonical closed encoding rejects duplicates, aliases, missing fields
	// and trailing data before accepting any caller-owned result.
	if err != nil || !bytes.Equal(canonical, body) {
		return retainedDeletionError()
	}
	*v = RetainedDeletionVerificationV1(p)
	return nil
}
