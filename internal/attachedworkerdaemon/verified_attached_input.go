package attachedworkerdaemon

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
	"gitcode.com/urandon/sessionless/internal/sessioncontext"
)

const maxVerifiedAttachedInputBytes = 1 << 20

// VerifiedAttachedInputV1 preserves authenticated materialization data, not
// issuer authority. Only the authenticated sealed-input composition supplies
// its AcceptedAttempt. Executors must still use the accepted-attempt owner,
// credential owner and concrete driver's authority checks. It is never stdin
// or a wire DTO. All buffers are owned and borrowed only for RunPrepared.
type VerifiedAttachedInputV1 struct {
	AcceptedAttempt  domain.AttachedWorkerAttemptV1 `json:"-"`
	Job              domain.WorkerJob               `json:"-"`
	Manifest         domain.ArtifactManifest        `json:"-"`
	Context          []byte                         `json:"-"`
	Artifacts        []VerifiedAttachedArtifactV1   `json:"-"`
	CanonicalContext *sessioncontext.CanonicalProof `json:"-"`
}

type VerifiedAttachedArtifactV1 struct {
	Name string `json:"-"`
	Body []byte `json:"-"`
}

func (VerifiedAttachedInputV1) String() string         { return "VerifiedAttachedInputV1{input:[redacted]}" }
func (input VerifiedAttachedInputV1) GoString() string { return input.String() }
func (VerifiedAttachedArtifactV1) String() string {
	return "VerifiedAttachedArtifactV1{artifact:[redacted]}"
}
func (artifact VerifiedAttachedArtifactV1) GoString() string { return artifact.String() }

// This bounded internal copy shape is not exported or used as a wire input.
type verifiedAttachedCopy struct {
	AcceptedAttempt  domain.AttachedWorkerAttemptV1
	Job              domain.WorkerJob
	Manifest         domain.ArtifactManifest
	Context          []byte
	Artifacts        []verifiedArtifactCopy
	CanonicalContext *sessioncontext.CanonicalProof
}
type verifiedArtifactCopy struct {
	Name string
	Body []byte
}

func (input *VerifiedAttachedInputV1) copyShape() verifiedAttachedCopy {
	value := verifiedAttachedCopy{AcceptedAttempt: input.AcceptedAttempt, Job: input.Job,
		Manifest: input.Manifest, Context: input.Context, CanonicalContext: input.CanonicalContext}
	if len(input.Artifacts) != 0 {
		value.Artifacts = make([]verifiedArtifactCopy, len(input.Artifacts))
	}
	for i, artifact := range input.Artifacts {
		value.Artifacts[i] = verifiedArtifactCopy{Name: artifact.Name, Body: artifact.Body}
	}
	return value
}

// Clone bounds and validates before any deep copy. Go immutable metadata
// strings are relinquished by Clear; no claim of string-byte zeroization is
// made. JSON is used only to copy finite typed metadata and byte buffers.
func (input *VerifiedAttachedInputV1) Clone() (*VerifiedAttachedInputV1, error) {
	if input == nil || input.ValidateFor(InvocationIdentity{TenantID: input.AcceptedAttempt.TenantID,
		OwnerUserID: input.AcceptedAttempt.OwnerUserID, WorkerID: input.AcceptedAttempt.WorkerID,
		RunID: input.AcceptedAttempt.RunID, AttemptID: input.AcceptedAttempt.AttemptID,
		LeaseID: input.AcceptedAttempt.LeaseID, FenceToken: input.AcceptedAttempt.LeaseGeneration}) != nil {
		return nil, ErrInvocationInvalid
	}
	shape := input.copyShape()
	limit := maxVerifiedAttachedInputBytes
	if input.Job.Limits.MaxInputBytes < uint64(limit) {
		limit = int(input.Job.Limits.MaxInputBytes)
	}
	if !BoundedJSONAllocationV1(shape, limit) {
		return nil, ErrInvocationInvalid
	}
	encoded, err := json.Marshal(shape)
	defer clear(encoded)
	if err != nil || len(encoded) > maxVerifiedAttachedInputBytes || uint64(len(encoded)) > input.Job.Limits.MaxInputBytes {
		return nil, ErrInvocationInvalid
	}
	var value verifiedAttachedCopy
	if json.Unmarshal(encoded, &value) != nil {
		clearVerifiedCopy(&value)
		return nil, ErrInvocationInvalid
	}
	result := &VerifiedAttachedInputV1{AcceptedAttempt: value.AcceptedAttempt, Job: value.Job,
		Manifest: value.Manifest, Context: value.Context, CanonicalContext: value.CanonicalContext}
	if len(value.Artifacts) != 0 {
		result.Artifacts = make([]VerifiedAttachedArtifactV1, len(value.Artifacts))
	}
	for i, artifact := range value.Artifacts {
		result.Artifacts[i] = VerifiedAttachedArtifactV1{Name: artifact.Name, Body: artifact.Body}
	}
	return result, nil
}

// BoundedJSONAllocationV1 prospectively accounts for the closed canonical
// carrier's encoding/json shapes, including base64 and HTML/Unicode escaping.
// It is a size check, not source or execution authority. Unknown/custom shapes
// fail closed. Callers still perform the exact encoded-size check afterwards.
func BoundedJSONAllocationV1(value any, limit int) bool {
	if limit <= 0 || limit > maxVerifiedAttachedInputBytes {
		return false
	}
	remaining := limit
	consume := func(n int) bool {
		if n < 0 || n > remaining {
			return false
		}
		remaining -= n
		return true
	}
	quoted := func(value string) int {
		if remaining < 2 || len(value) > remaining-2 {
			return remaining + 1
		}
		n := 2
		for len(value) > 0 {
			r, size := utf8.DecodeRuneInString(value)
			value = value[size:]
			switch r {
			case '"', '\\', '\b', '\f', '\n', '\r', '\t':
				n += 2
			case '<', '>', '&', '\u2028', '\u2029':
				n += 6
			default:
				if r < 0x20 {
					n += 6
				} else if r == utf8.RuneError && size == 1 {
					n += 6
				} else {
					n += size
				}
			}
			if n > remaining {
				return remaining + 1
			}
		}
		return n
	}
	empty := func(v reflect.Value) bool {
		switch v.Kind() {
		case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
			return v.Len() == 0
		case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64, reflect.Interface, reflect.Pointer:
			return v.IsZero()
		}
		return false
	}
	var measure func(reflect.Value, int) bool
	measure = func(v reflect.Value, depth int) bool {
		if depth > 20 {
			return false
		}
		if !v.IsValid() {
			return consume(4)
		}
		if v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface {
			if v.IsNil() {
				return consume(4)
			}
			return measure(v.Elem(), depth+1)
		}
		if v.Type() == reflect.TypeOf(sessioncontext.AttachmentPayload{}) {
			attachment := v.Interface().(sessioncontext.AttachmentPayload)
			return measure(reflect.ValueOf(struct {
				Name      string         `json:"name"`
				MediaType string         `json:"media_type"`
				Blob      domain.BlobRef `json:"blob"`
				Body      []byte         `json:"body"`
			}{attachment.Name, attachment.MediaType, attachment.Blob, attachment.Body}), depth+1)
		}
		if v.Type() == reflect.TypeOf(time.Time{}) {
			encoded, err := v.Interface().(time.Time).MarshalJSON()
			return err == nil && consume(len(encoded))
		}
		if v.Type().Implements(reflect.TypeOf((*json.Marshaler)(nil)).Elem()) {
			return false
		}
		switch v.Kind() {
		case reflect.Interface, reflect.Pointer:
			if v.IsNil() {
				return consume(4)
			}
			return measure(v.Elem(), depth+1)
		case reflect.String:
			return consume(quoted(v.String()))
		case reflect.Bool:
			if v.Bool() {
				return consume(4)
			}
			return consume(5)
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return consume(len(strconv.FormatInt(v.Int(), 10)))
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return consume(len(strconv.FormatUint(v.Uint(), 10)))
		case reflect.Slice:
			if v.IsNil() {
				return consume(4)
			}
			if v.Type().Elem().Kind() == reflect.Uint8 {
				if v.Len() > limit {
					return false
				}
				return consume(2 + 4*((v.Len()+2)/3))
			}
			fallthrough
		case reflect.Array:
			if v.Len() > 4096 || !consume(2) {
				return false
			}
			for i := 0; i < v.Len(); i++ {
				if i > 0 && !consume(1) {
					return false
				}
				if !measure(v.Index(i), depth+1) {
					return false
				}
			}
			return true
		case reflect.Struct:
			if !consume(2) {
				return false
			}
			fields := 0
			for i := 0; i < v.NumField(); i++ {
				f := v.Type().Field(i)
				if f.PkgPath != "" {
					continue
				}
				tag := strings.Split(f.Tag.Get("json"), ",")
				if tag[0] == "-" {
					continue
				}
				omit := false
				for _, option := range tag[1:] {
					if option == "omitempty" && empty(v.Field(i)) {
						omit = true
					}
					if option == "string" || option == "omitzero" {
						return false
					}
				}
				if omit {
					continue
				}
				if f.Anonymous && tag[0] == "" {
					return false
				}
				name := tag[0]
				if name == "" {
					name = f.Name
				}
				if fields > 0 && !consume(1) {
					return false
				}
				fields++
				if !consume(quoted(name)+1) || !measure(v.Field(i), depth+1) {
					return false
				}
			}
			return true
		default:
			return false
		}
	}
	return measure(reflect.ValueOf(value), 0)
}

func clearVerifiedCopy(value *verifiedAttachedCopy) {
	clear(value.Context)
	for _, artifact := range value.Artifacts {
		clear(artifact.Body)
	}
	value.CanonicalContext.Clear()
	*value = verifiedAttachedCopy{}
}

func (input *VerifiedAttachedInputV1) Clear() {
	if input == nil {
		return
	}
	clear(input.Context)
	for _, artifact := range input.Artifacts {
		clear(artifact.Body)
	}
	input.CanonicalContext.Clear()
	*input = VerifiedAttachedInputV1{}
}

func (input *VerifiedAttachedInputV1) ValidateFor(identity InvocationIdentity) error {
	if input == nil || identity.Validate() != nil || !input.bounded() {
		return ErrInvocationInvalid
	}
	a, job := input.AcceptedAttempt, input.Job
	if a.Validate() != nil || a.State != domain.AttachedWorkerAttemptClaimed ||
		a.TenantID != identity.TenantID || a.OwnerUserID != identity.OwnerUserID || a.WorkerID != identity.WorkerID ||
		a.RunID != identity.RunID || a.AttemptID != identity.AttemptID || a.LeaseID != identity.LeaseID || a.LeaseGeneration != identity.FenceToken ||
		job.TenantID != a.TenantID || job.RunID != a.RunID || job.AttemptID != a.AttemptID || job.ReservationID != a.ReservationID ||
		job.CredentialOwnerUserID != a.OwnerUserID || job.ExecutionPlacementV2.OwnerUserID != a.OwnerUserID ||
		job.ExecutionPlacementV2.WorkerID != a.WorkerID || job.ExecutionPlacementV2.CapabilityDigest != a.CapabilityDigest ||
		job.ExecutionPlacementV2.PolicyDigest != a.PolicyDigest || job.HarnessBinding.ValidateForScope(job.TenantID,
		a.OwnerUserID, job.RunID, job.AttemptID, job.ExecutionPlacementV2) != nil {
		return ErrInvocationInvalid
	}
	digest, err := domain.AttachedWorkerJobContextDigestV1(job, input.Manifest)
	if err != nil || digest != a.ContextDigest {
		return ErrInvocationInvalid
	}
	if job.ContextWindow == nil {
		if input.CanonicalContext != nil || !verifiedBlobMatches(job.ContextSnapshot, input.Context) {
			return ErrInvocationInvalid
		}
	} else {
		history, refs, err := sessioncontext.ProjectCanonical(job, input.CanonicalContext, maxVerifiedAttachedInputBytes)
		defer clear(history)
		if err != nil || !bytes.Equal(history, input.Context) ||
			sessioncontext.VerifyCanonicalAttachments(history, refs, input.CanonicalContext, job.Limits.MaxContextBytes) != nil {
			return ErrInvocationInvalid
		}
	}
	if len(input.Artifacts) != len(input.Manifest.Artifacts) {
		return ErrInvocationInvalid
	}
	seen := make(map[string]bool, len(input.Artifacts))
	for _, artifact := range input.Artifacts {
		matched := false
		for _, ref := range input.Manifest.Artifacts {
			if artifact.Name == ref.Name && verifiedBlobMatches(ref.Blob, artifact.Body) {
				matched = true
				break
			}
		}
		if !matched || seen[artifact.Name] {
			return ErrInvocationInvalid
		}
		seen[artifact.Name] = true
	}
	return nil
}

func (input *VerifiedAttachedInputV1) validateCredential(identity InvocationIdentity, credential *CredentialInvocation) error {
	if input.ValidateFor(identity) != nil || credential == nil || credential.ValidateFor(identity) != nil {
		return ErrInvocationInvalid
	}
	job, a, request := input.Job, input.AcceptedAttempt, credential.IssueRequest
	if request.ProviderResource != job.HarnessBinding.Resource ||
		credential.ExpectedBindingGeneration != job.HarnessBinding.Resource.CredentialGeneration ||
		request.Run.SessionID != job.SessionID || request.Run.TriggerEventID != job.TriggerEventID ||
		!request.Lease.ExpiresAt.Equal(a.LeaseExpiresAt) {
		return ErrInvocationInvalid
	}
	return nil
}

// validateAPIRequest compares existing typed API DATA with the complete
// original sealed scope. It is not source/currentness/spending permission.
func (input *VerifiedAttachedInputV1) validateAPIRequest(identity InvocationIdentity, request ports.ProviderCredentialIssueRequestV1) error {
	if input.ValidateFor(identity) != nil || request.Validate() != nil ||
		request.HarnessBinding.Resource.Kind != domain.ProviderResourceAPIAccountV1 ||
		request.HarnessBinding.Backend.BackendKind != domain.HarnessBackendPiV1 ||
		request.HarnessBinding.Backend.CredentialDeliveryKind != domain.ProviderCredentialDeliveryFileV1 ||
		!reflect.DeepEqual(request.HarnessBinding, input.Job.HarnessBinding) ||
		request.WorkerID != string(identity.WorkerID) || request.LeaseID != identity.LeaseID || request.LeaseFence != identity.FenceToken ||
		request.ExpiresAt.IsZero() || request.ExpiresAt.After(input.AcceptedAttempt.LeaseExpiresAt) {
		return ErrInvocationInvalid
	}
	return nil
}

// Pointer ranges only detect borrowed buffer overlap; they are never converted
// back to pointers or treated as identity/authority/native-memory evidence.
func apiBytesOverlap(a, b []byte) bool {
	if len(a) == 0 || len(b) == 0 {
		return false
	}
	x, y := reflect.ValueOf(a).Pointer(), reflect.ValueOf(b).Pointer()
	return x <= y && y-x < uintptr(len(a)) || y <= x && x-y < uintptr(len(b))
}

func visitAPIInputBuffers(input *VerifiedAttachedInputV1, visit func([]byte) bool) bool {
	if !visit(input.Context) {
		return false
	}
	for _, a := range input.Artifacts {
		if !visit(a.Body) {
			return false
		}
	}
	if p := input.CanonicalContext; p != nil {
		if !visit(p.SnapshotBytes) {
			return false
		}
		for _, b := range p.EventBodies {
			if !visit(b) {
				return false
			}
		}
		for _, a := range p.Attachments {
			if !visit(a.Body) {
				return false
			}
		}
	}
	return true
}

func apiInputsIndependent(a, b *VerifiedAttachedInputV1) bool {
	if a == nil || b == nil || a == b || !a.bounded() || !b.bounded() ||
		a.CanonicalContext != nil && a.CanonicalContext == b.CanonicalContext {
		return false
	}
	return visitAPIInputBuffers(a, func(x []byte) bool {
		return visitAPIInputBuffers(b, func(y []byte) bool { return !apiBytesOverlap(x, y) })
	})
}

type apiMutableRegion struct {
	start, end uintptr
	partition  uint8
}

// apiMutableOwnership observes only the closed, typed DATA graph. It never
// dereferences an address converted from uintptr or invokes user methods.
// Strings and time.Location backing are immutable; pointer targets and the
// ENTIRE reachable slice capacity are mutable. The same parent ledger funds
// traversal and the exact temporary range table before its allocation. This
// bookkeeping is not a native-fit/currentness certificate or a new copy pool.
func apiMutableOwnership(data *PreparedAPIDataV1, invocation Invocation) (bool, bool) {
	if data == nil || data.ParentInput == nil || data.ChildInput == nil || invocation.VerifiedInput == nil ||
		nilPreparedAPIDependency(data.Ledger) || !data.ParentInput.bounded() || !data.ChildInput.bounded() || !invocation.VerifiedInput.bounded() {
		return false, false
	}
	ceiling := data.ParentRequest.Profile.Limits.ProcessMemoryBytes
	if ceiling == 0 {
		return false, false
	}
	// Fund shallow reflection root boxes/descriptor storage before constructing
	// them. Referenced bodies are not copied or debited again here.
	rootBytes := 3*uint64(reflect.TypeOf(AttemptSpec{}).Size()) +
		2*uint64(reflect.TypeOf(PreparedAPIRequestV1{}).Size()) +
		uint64(reflect.TypeOf(ports.ProviderCredentialIssueRequestV1{}).Size()) +
		uint64(reflect.TypeOf([3][4]any{}).Size())
	if rootBytes > ceiling || data.Ledger.Reserve(rootBytes, rootBytes, 0) != nil {
		return false, false
	}
	defer func() { _ = data.Ledger.EndLive(rootBytes) }()
	roots := [3][4]any{
		{invocation.VerifiedInput, invocation.Process},
		{data.ParentInput, data.ParentRequest, data.ParentProcess, data.Issue},
		{data.ChildInput, data.ChildRequest, data.ChildProcess},
	}
	var count, measured uint64
	var regions []apiMutableRegion
	position := 0
	var walk func(reflect.Value, uint8, bool) bool
	walk = func(v reflect.Value, partition uint8, fill bool) bool {
		if v.Type() == reflect.TypeOf(time.Time{}) {
			return true
		}
		add := func(start uintptr, size uint64) bool {
			if size == 0 {
				return true
			}
			if start == 0 || size > ceiling || size > uint64(^uintptr(0)-start) {
				return false
			}
			if !fill {
				if size > ceiling-measured {
					return false
				}
				measured += size
				count++
			} else {
				if position == len(regions) {
					return false
				}
				regions[position] = apiMutableRegion{start, start + uintptr(size), partition}
				position++
			}
			return true
		}
		switch v.Kind() {
		case reflect.Pointer:
			if v.IsNil() {
				return true
			}
			return add(v.Pointer(), uint64(v.Type().Elem().Size())) && walk(v.Elem(), partition, fill)
		case reflect.Slice:
			if v.Cap() == 0 {
				return true
			}
			size := uint64(v.Type().Elem().Size())
			if size == 0 || uint64(v.Cap()) > ceiling/size || !add(v.Pointer(), uint64(v.Cap())*size) {
				return false
			}
			// Primitive elements cannot hide mutable references. Their complete
			// backing region is enough; no byte/string contents are scanned.
			k := v.Type().Elem().Kind()
			if k != reflect.Struct && k != reflect.Pointer && k != reflect.Slice && k != reflect.Array && k != reflect.Interface && k != reflect.Map {
				return true
			}
			if !fill && data.Ledger.Reserve(0, 0, uint64(v.Cap())) != nil {
				return false
			}
			full := v.Slice(0, v.Cap())
			for i := 0; i < full.Len(); i++ {
				if !walk(full.Index(i), partition, fill) {
					return false
				}
			}
		case reflect.Struct:
			for i := 0; i < v.NumField(); i++ {
				if !walk(v.Field(i), partition, fill) {
					return false
				}
			}
		case reflect.Array:
			for i := 0; i < v.Len(); i++ {
				if !walk(v.Index(i), partition, fill) {
					return false
				}
			}
		case reflect.Interface, reflect.Map, reflect.Func, reflect.Chan, reflect.UnsafePointer:
			return false
		}
		return true
	}
	visit := func(fill bool) bool {
		for partition, group := range roots {
			for _, root := range group {
				if root != nil && !walk(reflect.ValueOf(root), uint8(partition), fill) {
					return false
				}
			}
		}
		return true
	}
	if !visit(false) {
		return false, false
	}
	size := uint64(reflect.TypeOf(apiMutableRegion{}).Size())
	if count > ceiling/size || count > uint64(int(^uint(0)>>1)) {
		return false, false
	}
	allocation := count * size
	if data.Ledger.Reserve(allocation, allocation, count) != nil {
		return false, false
	}
	defer func() { _ = data.Ledger.EndLive(allocation) }()
	regions = make([]apiMutableRegion, int(count))
	if !visit(true) || position != len(regions) {
		return false, false
	}
	slices.SortFunc(regions, func(a, b apiMutableRegion) int {
		if a.start < b.start {
			return -1
		}
		if a.start > b.start {
			return 1
		}
		return 0
	})
	callerIndependent, partitionsIndependent := true, true
	var ends [3]uintptr
	for _, region := range regions {
		for partition, end := range ends {
			if uint8(partition) != region.partition && end > region.start {
				partitionsIndependent = false
				if partition == 0 || region.partition == 0 {
					callerIndependent = false
				}
			}
		}
		if region.end > ends[region.partition] {
			ends[region.partition] = region.end
		}
	}
	return callerIndependent, partitionsIndependent
}

func verifiedBlobMatches(ref domain.BlobRef, body []byte) bool {
	digest := sha256.Sum256(body)
	return ref.Validate() == nil && ref.Size == int64(len(body)) && ref.SHA256 == hex.EncodeToString(digest[:])
}

func (input *VerifiedAttachedInputV1) bounded() bool {
	if !verifiedMetadataBound(reflect.ValueOf(input.AcceptedAttempt), 64) ||
		!verifiedMetadataBound(reflect.ValueOf(input.Job), 64) || !verifiedMetadataBound(reflect.ValueOf(input.Manifest), 64) ||
		len(input.Artifacts) > 64 || uint64(len(input.Artifacts)) > uint64(input.Job.Limits.MaxArtifacts) ||
		uint64(len(input.Context)) > input.Job.Limits.MaxContextBytes {
		return false
	}
	remaining := maxVerifiedAttachedInputBytes
	consume := func(body []byte) bool {
		if len(body) > remaining {
			return false
		}
		remaining -= len(body)
		return true
	}
	if !consume(input.Context) {
		return false
	}
	for _, artifact := range input.Artifacts {
		if len(artifact.Name) > 4096 || !consume(artifact.Body) {
			return false
		}
	}
	if proof := input.CanonicalContext; proof != nil {
		if !verifiedMetadataBound(reflect.ValueOf(proof.Input), 4096) || len(proof.EventBodies) > 4096 ||
			len(proof.Attachments) > 64 || uint64(len(proof.Attachments)+len(input.Artifacts)) > uint64(input.Job.Limits.MaxArtifacts) ||
			!consume(proof.SnapshotBytes) {
			return false
		}
		for _, body := range proof.EventBodies {
			if !consume(body) {
				return false
			}
		}
		for _, attachment := range proof.Attachments {
			if !verifiedMetadataBound(reflect.ValueOf(attachment.AttachmentRef), 64) || !consume(attachment.Body) {
				return false
			}
		}
	}
	return true
}

func verifiedMetadataBound(value reflect.Value, maxItems int) bool {
	remaining := maxVerifiedAttachedInputBytes
	var visit func(reflect.Value, int) bool
	visit = func(value reflect.Value, depth int) bool {
		if depth > 16 {
			return false
		}
		if value.Type() == reflect.TypeOf(time.Time{}) {
			return true
		}
		switch value.Kind() {
		case reflect.Interface, reflect.Pointer:
			return value.IsNil() || visit(value.Elem(), depth+1)
		case reflect.String:
			if value.Len() > 4096 || value.Len() > remaining {
				return false
			}
			remaining -= value.Len()
		case reflect.Slice, reflect.Array:
			if value.Len() > maxItems {
				return false
			}
			for i := 0; i < value.Len(); i++ {
				if !visit(value.Index(i), depth+1) {
					return false
				}
			}
		case reflect.Struct:
			for i := 0; i < value.NumField(); i++ {
				if !visit(value.Field(i), depth+1) {
					return false
				}
			}
		case reflect.Map:
			return false
		}
		return true
	}
	return visit(value, 0)
}
