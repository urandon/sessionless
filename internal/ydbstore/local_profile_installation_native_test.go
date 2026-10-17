package ydbstore

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"gitcode.com/urandon/sessionless/internal/domain"
	"gitcode.com/urandon/sessionless/internal/ports"
)

// Explicit disposable-loopback gate only. This uses the actual Store and YDB
// serializable CAS, but source authentication is a scripted preinstalled
// allowlist, not proof of a real operator source or model qualification.
func TestOwnedCurrentYDBLocalInstallationConcurrentSuccessors(t *testing.T) {
	f, ctx := nativeOwnedCurrentFixture(t)
	verifier := &fixtureProfileVerifier{key: []byte("native-local-source-script-only"), allowed: make(map[string]bool)}
	f.store.ownedProfilePublicationVerifier = verifier
	original := verifier.install(t, syntheticLocalInstallationGrant(t, f.profile))
	locators := []domain.OwnedAgentProfileLocatorV1{original.Profile.Locator}
	runCtx, cancelWorkers := context.WithCancel(ctx)
	var joined <-chan struct{}
	t.Cleanup(func() {
		cancelWorkers()
		cleanup, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if joined != nil {
			select {
			case <-joined:
			case <-cleanup.Done():
				t.Error("local replacement workers did not join before cleanup; refusing concurrent fixture deletion")
				return
			}
		}
		for _, l := range locators {
			digest, err := l.Digest()
			if err != nil {
				t.Error(err)
				continue
			}
			if _, err := f.store.db.ExecContext(cleanup, `DELETE FROM owned_profile_publication_audit_v1 WHERE tenant_id=$1 AND locator_digest=$2`, l.TenantID, digest); err != nil {
				t.Error(err)
			}
			if _, err := f.store.db.ExecContext(cleanup, `DELETE FROM owned_agent_profile_current_v1 WHERE tenant_id=$1 AND owner_user_id=$2 AND profile=$3 AND backend_profile_digest=$4 AND resource_kind=$5 AND resource_id=$6 AND model_vendor_id=$7 AND model_id=$8 AND input_data_class=$9`, l.TenantID, l.OwnerUserID, l.Profile, l.BackendProfileDigest, l.ResourceKind, l.ResourceID, l.ModelVendorID, l.ModelID, l.InputDataClass); err != nil {
				t.Error(err)
			}
		}
	})
	first, err := f.store.PublishOwnedProfile(ctx, original)
	if err != nil || first.Replayed {
		t.Fatalf("native local initial publication: err=%v replay=%t", err, first.Replayed)
	}
	one := replacementLocalGrant(t, verifier, original, first, "native-one")
	two := replacementLocalGrant(t, verifier, original, first, "native-two")
	// The common helper uses one successor URL. This second candidate must
	// have a genuinely different locator, not merely a different mutation ID.
	two.LocalInstallation.Profile.BaseURL = "http://127.0.0.1:11436/v1"
	two.Profile.Backend.BackendProfileDigest, err = two.LocalInstallation.Profile.Digest()
	if err != nil {
		t.Fatal(err)
	}
	two.Profile.Locator.BackendProfileDigest = two.Profile.Backend.BackendProfileDigest
	two = verifier.install(t, two)
	if one.Profile.Locator == two.Profile.Locator {
		t.Fatal("concurrent successors must target different exact locators")
	}
	locators = append(locators, one.Profile.Locator, two.Profile.Locator)
	// All allowlist writes precede worker launch; verification is read-only.
	grants := [2]ports.OwnedProfilePublicationGrantV1{one, two}
	type outcome struct {
		index   int
		receipt ports.OwnedProfilePublicationReceiptV1
		err     error
	}
	results := make(chan outcome, len(grants))
	start, done := make(chan struct{}), make(chan struct{})
	joined = done
	var workers sync.WaitGroup
	workers.Add(len(grants))
	for index, grant := range grants {
		go func(index int, grant ports.OwnedProfilePublicationGrantV1) {
			defer workers.Done()
			<-start
			receipt, err := f.store.PublishOwnedProfile(runCtx, grant)
			results <- outcome{index: index, receipt: receipt, err: err}
		}(index, grant)
	}
	go func() { workers.Wait(); close(done) }()
	close(start)
	select {
	case <-done:
	case <-ctx.Done():
		select {
		case <-done:
		default:
			t.Fatal("native local replacement workers exceeded enclosing context")
		}
	}
	winner, denied := -1, 0
	var winningReceipt ports.OwnedProfilePublicationReceiptV1
	for range grants {
		result := <-results
		if result.err == nil {
			if winner != -1 {
				t.Fatal("two native successor transactions committed from one predecessor")
			}
			winner, winningReceipt = result.index, result.receipt
		} else if errors.Is(result.err, domain.ErrMembershipDenied) {
			denied++
			if !reflect.DeepEqual(result.receipt, ports.OwnedProfilePublicationReceiptV1{}) {
				t.Error("losing CAS returned a usable partial receipt")
			}
		} else {
			t.Errorf("native replacement index=%d unexpected error=%v", result.index, result.err)
		}
	}
	if winner < 0 || denied != 1 || winningReceipt.Replayed {
		t.Fatalf("native replacement winner=%d denied=%d replay=%t", winner, denied, winningReceipt.Replayed)
	}
	loser := 1 - winner
	var before [3]domain.OwnedAgentProfileCurrentV1
	var found [3]bool
	err = f.store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		before, found = [3]domain.OwnedAgentProfileCurrentV1{}, [3]bool{}
		for index, l := range locators {
			var err error
			before[index], found[index], err = loadOwnedProfileProjection(ctx, q, l)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("read exact local replacement heads: %v", err)
	}
	if !found[0] || before[0].State != domain.OwnedAgentCurrentRevokedV1 || before[0].RevocationGeneration != first.Profile.RevocationGeneration+1 || before[0].MutationID != grants[winner].LocalInstallation.RetirementMutationID || !found[winner+1] || !reflect.DeepEqual(before[winner+1], winningReceipt.Profile) || found[loser+1] {
		t.Fatalf("two-head CAS invariant: old_state=%s old_generation=%d old_mutation=%s successor_presence=%v winner=%d", before[0].State, before[0].RevocationGeneration, before[0].MutationID, found, winner)
	}
	oldRef := domain.OwnedAgentProfileReferenceV1{Locator: first.Profile.Locator, RevocationGeneration: first.Profile.RevocationGeneration, Digest: first.Digest}
	err = f.store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		_, err := readOwnedProfileCurrent(ctx, q, oldRef)
		return err
	})
	if !errors.Is(err, domain.ErrMembershipDenied) {
		t.Fatalf("original local current reference survived replacement: %v", err)
	}
	countAudits := func() [3]uint64 {
		t.Helper()
		var counts [3]uint64
		for index, l := range locators {
			digest, err := l.Digest()
			if err != nil {
				t.Fatal(err)
			}
			if err := f.store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM owned_profile_publication_audit_v1 WHERE tenant_id=$1 AND locator_digest=$2`, l.TenantID, digest).Scan(&counts[index]); err != nil {
				t.Fatalf("count exact native local audits index=%d: %v", index, err)
			}
		}
		return counts
	}
	audits := countAudits()
	if audits[0] != 2 || audits[winner+1] != 1 || audits[loser+1] != 0 {
		t.Fatalf("atomic two-head audits got=%v winner=%d want old=2,winner=1,loser=0", audits, winner)
	}
	replayed, err := f.store.PublishOwnedProfile(ctx, grants[winner])
	if err != nil || !replayed.Replayed || replayed.Digest != winningReceipt.Digest || !reflect.DeepEqual(replayed.Profile, winningReceipt.Profile) {
		t.Fatalf("exact native local receipt replay: err=%v replay=%t digest_equal=%t", err, replayed.Replayed, replayed.Digest == winningReceipt.Digest)
	}
	if after := countAudits(); after != audits {
		t.Fatalf("historical replay wrote audits: before=%v after=%v", audits, after)
	}
	err = f.store.ownedAuthorityTx(ctx, func(ctx context.Context, q *ownedAuthorityQuery) error {
		for index, l := range locators {
			current, exists, err := loadOwnedProfileProjection(ctx, q, l)
			if err != nil || exists != found[index] || !reflect.DeepEqual(current, before[index]) {
				return domain.ErrMembershipDenied
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("historical replay changed exact native local heads: %v", err)
	}
}
