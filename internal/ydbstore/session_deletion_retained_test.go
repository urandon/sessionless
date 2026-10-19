package ydbstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
)

type deletionReadFixture struct {
	body       []byte
	openErr    error
	closeErr   error
	readErr    error
	opens      int
	closes     int
	cancelRead context.CancelFunc
}

func (f *deletionReadFixture) Open(context.Context, domain.TenantID, domain.BlobRef) (io.ReadCloser, error) {
	f.opens++
	return &deletionReadCloser{Reader: bytes.NewReader(f.body), owner: f}, f.openErr
}

type deletionReadCloser struct {
	*bytes.Reader
	owner *deletionReadFixture
}

func (r *deletionReadCloser) Read(p []byte) (int, error) {
	if r.owner.cancelRead != nil {
		r.owner.cancelRead()
	}
	if r.owner.readErr != nil {
		return 0, r.owner.readErr
	}
	return r.Reader.Read(p)
}
func (r *deletionReadCloser) Close() error { r.owner.closes++; return r.owner.closeErr }

func TestRetainedDeletionBlobExactBoundedRead(t *testing.T) {
	original := []byte("exact immutable original")
	sum := sha256.Sum256(original)
	ref := domain.BlobRef{TenantID: "tenant-delete-read", Key: "tenants/tenant-delete-read/test", Size: int64(len(original)), SHA256: hex.EncodeToString(sum[:])}
	for _, mode := range []string{"positive", "short", "long", "digest", "read_error", "close_error", "open_error_with_reader", "cancelled_during_read", "limit", "missing_budget", "active_sql", "exhausted_budget"} {
		t.Run(mode, func(t *testing.T) {
			ctx, finish := ownedAuthorityCallContext(t.Context())
			defer finish()
			b := ctx.Value(ownedAuthorityCallContextKey{}).(*ownedAuthorityBudget)
			f := &deletionReadFixture{body: bytes.Clone(original)}
			limit := uint64(len(original))
			switch mode {
			case "short":
				f.body = f.body[:len(f.body)-1]
			case "long":
				f.body = append(f.body, 'x')
			case "digest":
				f.body[0] ^= 1
			case "read_error":
				f.readErr = errors.New("injected read failure")
			case "close_error":
				f.closeErr = errors.New("injected Close failure")
			case "open_error_with_reader":
				f.openErr = errors.New("injected Open failure")
			case "cancelled_during_read":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				defer cancel()
				f.cancelRead = cancel
			case "limit":
				limit--
			case "missing_budget":
				ctx = t.Context()
			case "active_sql":
				callback, err := b.begin()
				if err != nil {
					t.Fatal(err)
				}
				defer b.finish(callback)
			case "exhausted_budget":
				b.bytes = ownedAuthorityMaxBytes
			}
			body, err := readRetainedDeletionBlob(ctx, f, ref, limit)
			defer clear(body)
			if mode == "positive" {
				if err != nil || !bytes.Equal(body, original) || f.opens != 1 || f.closes != 1 || b.bytes != uint64(len(original)+1) {
					t.Fatalf("exact read: err=%v opens=%d closes=%d charged=%d", err, f.opens, f.closes, b.bytes)
				}
			} else if err == nil || len(body) != 0 || f.opens != f.closes {
				t.Fatalf("failure mode=%s returned body=%d err=%v opens=%d closes=%d", mode, len(body), err, f.opens, f.closes)
			}
			if (mode == "limit" || mode == "missing_budget" || mode == "active_sql" || mode == "exhausted_budget") && f.opens != 0 {
				t.Fatal("denied before Open contract violated", mode, f.opens)
			}
		})
	}
}

func TestRetainedDeletionOwnershipStreamingBounds(t *testing.T) {
	run := currentWorkerAuthorityFixture(t).Run
	if err := run.Transition(domain.RunSucceeded, run.CreatedAt.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	records, _, _, _ := markerLegacyComposerData(t, run, "original-delete-inventory-manifest")
	for _, mode := range []string{"positive", "duplicate_event", "duplicate_run", "foreign_event", "foreign_run", "limit_event", "limit_run", "zero_index"} {
		t.Run(mode, func(t *testing.T) {
			v, err := newRetainedDeletionOwnershipData(run.TenantID, run.SessionID)
			if err != nil {
				t.Fatal(err)
			}
			if err := v.addEvent(records[0].Event); err != nil {
				t.Fatal("original canonical owner", err)
			}
			if err := v.addRun(run); err != nil {
				t.Fatal("original Run owner", err)
			}
			event := records[1].Event
			otherRun := run.Clone()
			otherRun.ID += "-next"
			switch mode {
			case "positive":
				err = v.addEvent(event)
				if err != nil || v.rows != 3 || len(v.events) != 2 || len(v.runs) != 1 {
					t.Fatal("streamed owning rows", err, v.rows)
				}
				return
			case "duplicate_event":
				err = v.addEvent(records[0].Event)
			case "duplicate_run":
				err = v.addRun(run)
			case "foreign_event":
				event.SessionID += "-foreign"
				err = v.addEvent(event)
			case "foreign_run":
				otherRun.SessionID += "-foreign"
				err = v.addRun(otherRun)
			case "limit_event":
				v.rows = maxSessionDeletionRows - 1
				if err := v.addEvent(event); err != nil || v.rows != maxSessionDeletionRows {
					t.Fatal("last row of aggregate budget", err, v.rows)
				}
				err = v.addEvent(records[2].Event)
			case "limit_run":
				v.rows = maxSessionDeletionRows
				err = v.addRun(otherRun)
			case "zero_index":
				v = retainedDeletionOwnershipData{}
				err = v.addRun(run)
			}
			if err == nil {
				t.Fatal("invalid owner or exhausted aggregate budget accepted", mode)
			}
		})
	}
}
