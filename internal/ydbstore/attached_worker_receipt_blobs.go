package ydbstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
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
}

type plannedReceiptObject struct {
	ref  domain.BlobRef
	body []byte
}

func (planned *plannedReceiptBlobs) Put(_ context.Context, tenant domain.TenantID, key string, body io.Reader) (domain.BlobRef, error) {
	content, err := io.ReadAll(io.LimitReader(body, maxPlannedReceiptBytes-planned.total+1))
	if err != nil || planned.total+int64(len(content)) > maxPlannedReceiptBytes {
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
		if err != nil || written != item.ref {
			return attachedworkeroutput.ErrCandidateInvalid
		}
		reader, err := planned.upstream.Open(ctx, item.ref.TenantID, item.ref)
		if err != nil {
			return err
		}
		content, readErr := io.ReadAll(io.LimitReader(reader, item.ref.Size+1))
		closeErr := reader.Close()
		digest := sha256.Sum256(content)
		if readErr != nil || closeErr != nil || int64(len(content)) != item.ref.Size ||
			hex.EncodeToString(digest[:]) != item.ref.SHA256 {
			return attachedworkeroutput.ErrCandidateInvalid
		}
	}
	return nil
}

var _ ports.BlobStore = (*plannedReceiptBlobs)(nil)
