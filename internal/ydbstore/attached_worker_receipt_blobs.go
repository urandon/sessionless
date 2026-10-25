package ydbstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"

	"gitcode.com/urandon/sessionless/internal/attachedworkeroutput"
	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

const maxPlannedReceiptBytes = 65 << 20

// plannedReceiptBlobs computes canonical references without writing objects.
// A pending receipt containing every planned ref is committed to YDB before
// Commit can put any bytes into Object Storage. Session deletion can therefore
// enumerate even a partially copied or abandoned attempt.
type plannedReceiptBlobs struct {
	upstream ports.BlobStore
	items    []plannedReceiptObject
	total    int64
	// Zero retains the V1 cohort. Inline V2 sets its smaller prospective cap
	// and supplies exact-sized private materializer readers, never a stream
	// whose size is discovered only after allocating it.
	maxBytes int64
}

type plannedReceiptObject struct {
	ref  domain.BlobRef
	body []byte
}

func (planned *plannedReceiptBlobs) Put(ctx context.Context, tenant domain.TenantID, key string, body io.Reader) (domain.BlobRef, error) {
	limit := planned.maxBytes
	if limit == 0 {
		limit = maxPlannedReceiptBytes
	}
	if body == nil || limit < 0 || limit > maxPlannedReceiptBytes || planned.total < 0 || planned.total > limit {
		return domain.BlobRef{}, attachedworkeroutput.ErrCandidateInvalid
	}
	var content []byte
	var err error
	if sized, ok := body.(interface{ Len() int }); ok {
		content, err = readExactReceiptBytes(ctx, body, int64(sized.Len()), limit-planned.total)
	} else if planned.maxBytes != 0 {
		// V2 is called only by the private, already verified materializer.
		// Reject an unknown-size reader BEFORE reading or allocating content.
		return domain.BlobRef{}, attachedworkeroutput.ErrCandidateInvalid
	} else {
		content, err = io.ReadAll(io.LimitReader(body, limit-planned.total+1))
	}
	keep := false
	defer func() {
		if !keep {
			clear(content)
		}
	}()
	if err != nil || len(content) == 0 || planned.total+int64(len(content)) > limit {
		return domain.BlobRef{}, attachedworkeroutput.ErrCandidateInvalid
	}
	digest := sha256.Sum256(content)
	ref := domain.BlobRef{TenantID: tenant, Key: key, Size: int64(len(content)), SHA256: hex.EncodeToString(digest[:])}
	if ref.Validate() != nil {
		return domain.BlobRef{}, attachedworkeroutput.ErrCandidateInvalid
	}
	for _, prior := range planned.items {
		if prior.ref.Key == key {
			if prior.ref != ref {
				return domain.BlobRef{}, attachedworkeroutput.ErrCandidateInvalid
			}
			return ref, nil
		}
	}
	planned.items = append(planned.items, plannedReceiptObject{ref: ref, body: content})
	planned.total += int64(len(content))
	keep = true
	return ref, nil
}

func (planned *plannedReceiptBlobs) Open(ctx context.Context, tenant domain.TenantID, ref domain.BlobRef) (io.ReadCloser, error) {
	for _, item := range planned.items {
		if item.ref.Key == ref.Key {
			if item.ref != ref || ref.TenantID != tenant {
				return nil, attachedworkeroutput.ErrCandidateInvalid
			}
			return io.NopCloser(bytes.NewReader(item.body)), nil
		}
	}
	return planned.upstream.Open(ctx, tenant, ref)
}

func (planned *plannedReceiptBlobs) Delete(context.Context, domain.TenantID, domain.BlobRef) error {
	return attachedworkeroutput.ErrCandidateInvalid
}

func (planned *plannedReceiptBlobs) Commit(ctx context.Context) error {
	for _, item := range planned.items {
		written, err := planned.upstream.Put(ctx, item.ref.TenantID, item.ref.Key, bytes.NewReader(item.body))
		if err != nil {
			return errors.Join(attachedworkeroutput.ErrCandidateInvalid, err)
		}
		if written != item.ref {
			return attachedworkeroutput.ErrCandidateInvalid
		}
		reader, err := planned.upstream.Open(ctx, item.ref.TenantID, item.ref)
		if err != nil {
			return err
		}
		content, readErr := readExactReceiptBytes(ctx, reader, item.ref.Size, item.ref.Size)
		closeErr := reader.Close()
		digest := sha256.Sum256(content)
		clear(content)
		content = nil
		if readErr != nil || closeErr != nil ||
			hex.EncodeToString(digest[:]) != item.ref.SHA256 {
			return errors.Join(attachedworkeroutput.ErrCandidateInvalid, readErr, closeErr)
		}
	}
	return nil
}

// Only the V2 caller releases these server copies after ALL physical writes
// and rereads succeeded, before its READY transition. Never call on ambiguous
// Commit: an upstream writer may still borrow its reader. Durable refs/custody
// and the original native producer's retained bytes are not retired here.
func (planned *plannedReceiptBlobs) clearBodies() {
	for i := range planned.items {
		clear(planned.items[i].body)
		planned.items[i].body = nil
	}
}

// Exact-size local copy with one trailing-byte EOF check. The limit is checked
// before allocation; capacity never grows beyond the admitted size. No retry,
// new deadline, goroutine or ownership of the borrowed reader is introduced.
// Errors clear only this temporary copy, not producer bytes or durable custody.
func readExactReceiptBytes(ctx context.Context, reader io.Reader, size, limit int64) ([]byte, error) {
	if ctx == nil || reader == nil || size < 0 || size > limit || limit < 0 || limit > maxPlannedReceiptBytes {
		return nil, attachedworkeroutput.ErrCandidateInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(attachedworkeroutput.ErrCandidateInvalid, err)
	}
	content := make([]byte, int(size))
	fail := func(err error) ([]byte, error) {
		clear(content)
		return nil, errors.Join(attachedworkeroutput.ErrCandidateInvalid, err)
	}
	// ReadFull deliberately drops an error accompanying the final required
	// bytes. Preserve that error: exact bytes plus a transport failure must
	// not turn an ambiguous/failed body into a verified canonical candidate.
	for offset, idle := 0, 0; offset < len(content); {
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		n, err := reader.Read(content[offset:])
		if n < 0 || n > len(content)-offset {
			return fail(nil)
		}
		offset += n
		if cause := ctx.Err(); cause != nil {
			return fail(cause)
		}
		if err != nil {
			if err == io.EOF && offset == len(content) {
				return content, nil
			}
			return fail(err)
		}
		if n == 0 {
			idle++
			if idle >= 100 {
				return fail(io.ErrNoProgress)
			}
		} else {
			idle = 0
		}
	}
	var extra [1]byte
	for idle := 0; idle < 100; idle++ {
		if err := ctx.Err(); err != nil {
			return fail(err)
		}
		n, err := reader.Read(extra[:])
		clear(extra[:])
		if cause := ctx.Err(); cause != nil {
			return fail(cause)
		}
		if n != 0 {
			return fail(err)
		}
		if err == io.EOF {
			return content, nil
		}
		if err != nil {
			return fail(err)
		}
	}
	return fail(io.ErrNoProgress)
}

var _ ports.BlobStore = (*plannedReceiptBlobs)(nil)
