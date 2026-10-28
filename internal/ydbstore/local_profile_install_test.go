package ydbstore

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/localoperator"
)

// Only the DB clock advances here; profile/audit mutations, transactions and
// lost-ACK behavior remain the original controlled SQL owner. Publication and
// receipt acceptance use the actual private Source, not a publisher callback.
type firstLocalInstallSQL struct {
	*localInstallationSQLFixture
	expiredDBSample bool
}

func (f *firstLocalInstallSQL) Connect(context.Context) (driver.Conn, error) { return f, nil }
func (f *firstLocalInstallSQL) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if query == `SELECT CurrentUtcTimestamp()` {
		// Normal publication advances a small controlled amount inside the
		// synctest bubble, well within the original transaction bound.
		if !f.expiredDBSample {
			time.Sleep(time.Millisecond)
		}
		f.at = time.Now().UTC().Truncate(time.Microsecond)
		if f.expiredDBSample {
			// Explicit authoritative DB-clock skew boundary, not a timer or
			// elapsed-runtime test. Do not sleep through the SQL transaction
			// timeout: sql.awaitDone's mutex is not durably blocked in synctest.
			f.at = f.at.Add(11 * time.Second)
		}
	}
	return f.localInstallationSQLFixture.QueryContext(ctx, query, args)
}

func firstLocalInstallDB(t *testing.T) (*sql.DB, *firstLocalInstallSQL) {
	t.Helper()
	_, original, _, _ := newLocalInstallationFixture(t)
	f := &firstLocalInstallSQL{localInstallationSQLFixture: original}
	db := sql.OpenDB(f)
	db.SetMaxOpenConns(1)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return db, f
}

func TestLocalProfileInstallOriginalReceiptToExistingReader(t *testing.T) {
	for _, commit := range []string{"confirmed", "ambiguous"} {
		t.Run(commit, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				root, executable, _ := localOperatorFiles(t)
				path := filepath.Join(root, "receipt.json")
				db, f := firstLocalInstallDB(t)
				if commit == "ambiguous" {
					f.commitModes = []string{"ambiguous"}
				}
				before := time.Now()
				if err := InstallLocalProfile(t.Context(), db, root, executable, path); err != nil {
					t.Fatalf("actual first-install %s publication: %v", commit, err)
				}
				// Same bounded production reader intended for normal startup;
				// this is not command/Web/queued execution acceptance.
				receipt, err := localoperator.ReadProfileReceipt(t.Context(), path)
				if err != nil || !receipt.Profile.UpdatedAt.After(before) || !receipt.Profile.Window.PublishedAt.Equal(receipt.Profile.UpdatedAt) || receipt.Replayed != (commit == "ambiguous") {
					t.Fatalf("actual DB timestamp/replay receipt was not preserved: err=%v replayed=%t at=%s", err, receipt.Replayed, receipt.Profile.UpdatedAt)
				}
				if len(f.heads) != 1 || len(f.audits) != 1 || f.execCalls != 2 {
					t.Fatalf("one original install: heads=%d audits=%d writes=%d", len(f.heads), len(f.audits), f.execCalls)
				}
				row := f.heads[localInstallationHeadKey(receipt.Profile)]
				stored, err := domain.DecodeOwnedAgentProfileCurrentV1([]byte(row[0].(string)))
				if err != nil || !reflect.DeepEqual(stored, receipt.Profile) {
					t.Fatalf("receipt not exact committed profile: err=%v", err)
				}
				resumed, err := localoperator.OpenExisting(t.Context(), root, executable, receipt)
				if err != nil {
					t.Fatalf("actual original Source restart: %v", err)
				}
				t.Cleanup(func() { _ = resumed.Close() })
				if _, err := resumed.Publication(); !errors.Is(err, localoperator.ErrDenied) {
					t.Fatal("reopened receipt minted fresh publication authority")
				}
				writes, commits := f.execCalls, f.commits
				raw, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				if err := InstallLocalProfile(t.Context(), db, root, executable, path); !errors.Is(err, localoperator.ErrDenied) || f.execCalls != writes || f.commits != commits {
					t.Fatalf("existing receipt allowed another publication: err=%v writes=%d commits=%d", err, f.execCalls, f.commits)
				}
				after, err := os.ReadFile(path)
				if err != nil || string(after) != string(raw) {
					t.Fatalf("existing original receipt overwritten: %v", err)
				}
			})
		})
	}
}

func TestLocalProfileInstallIncompleteRetainsSlotAndNeverRestarts(t *testing.T) {
	for _, fault := range []string{"source", "audit", "unknown commit", "expired DB sample"} {
		t.Run(fault, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				root, executable, _ := localOperatorFiles(t)
				path := filepath.Join(root, "receipt.json")
				db, f := firstLocalInstallDB(t)
				switch fault {
				case "source":
					executable = filepath.Join(root, "missing-executable")
				case "audit":
					f.failAuditNumber = 1
				case "unknown commit":
					f.commitModes = []string{"unknown"}
				case "expired DB sample":
					f.expiredDBSample = true
				}
				if err := InstallLocalProfile(t.Context(), db, root, executable, path); !errors.Is(err, localoperator.ErrDenied) {
					t.Fatalf("%s reported installed: %v", fault, err)
				}
				info, err := os.Stat(path)
				if err != nil || info.Size() != 0 || info.Mode().Perm() != 0o600 {
					t.Fatalf("%s lost private incomplete slot: stat=%v err=%v", fault, info, err)
				}
				if _, err := localoperator.ReadProfileReceipt(t.Context(), path); !errors.Is(err, localoperator.ErrDenied) {
					t.Fatalf("%s incomplete slot became a receipt: %v", fault, err)
				}
				if fault == "unknown commit" {
					if len(f.heads) != 1 || len(f.audits) != 1 {
						t.Fatal("unknown commit scenario did not preserve actual committed rows")
					}
				} else if len(f.heads) != 0 || len(f.audits) != 0 {
					t.Fatalf("%s left committed publication despite denial", fault)
				}
				writes, commits, queries := f.execCalls, f.commits, len(f.statements)
				if err := InstallLocalProfile(t.Context(), db, root, executable, path); !errors.Is(err, localoperator.ErrDenied) || f.execCalls != writes || f.commits != commits || len(f.statements) != queries {
					t.Fatalf("%s reserved path admitted retry: err=%v writes=%d commits=%d queries=%d", fault, err, f.execCalls, f.commits, len(f.statements))
				}
			})
		})
	}
}
